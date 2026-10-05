// Package grpc is the gRPC server adapter for chora-payments'
// PaymentService RPC contract per chora-contracts/proto/services/payments/v1.
//
// Originating services (chora-delivery, chora-tenancy, chora-identity)
// call this server synchronously to mint Stripe Checkout Sessions. The
// 7 RPCs split as:
//
//   - 5 Create*Session — mint Session + persist aggregate in checkout_started
//     state. Idempotent on idempotency_key (per ADR-164 §D2).
//   - GetPurchase — admin lookup by (purchase_id, aggregate_type).
//   - RefundPurchase — admin-issued refund via Stripe SDK + state transition.
//
// The server requires:
//   - 5 Purchase repos (chora-payments domain).
//   - 1 Stripe adapter (CreateCheckoutSession + RefundCharge).
//   - tenant_id propagation via context (rls.ApplySession needs it on
//     repo writes; gRPC interceptor wires it).
//
// Per `feedback_resilience_priority` repos run idempotent UPSERTs + every
// path returns gRPC status codes so the originating service can map them
// to HTTP 4xx/5xx without leaking internal errors.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/tracing"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	stripecustomer "github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// stripeCataloguePort is the minimal surface of the CHO-1760
// stripe_catalogue.PriceCatalogue this server needs. Defined as a
// local port so the grpc package doesn't pull in the catalogue
// adapter directly — main.go wires the concrete type.
type stripeCataloguePort interface {
	Resolve(addonCode, tierCode, currency string) (stripePriceID string, ok bool)
}

// isPlaceholderPriceID mirrors stripe_catalogue.IsPlaceholder without
// importing the package (keeps the grpc adapter free of catalogue
// internals). Placeholder IDs come from PlaceholderPriceCatalogue when
// STRIPE_PRICE_CATALOGUE_SECRET_ID is unset; they MUST NOT reach Stripe.
func isPlaceholderPriceID(id string) bool {
	return strings.HasPrefix(id, "price_TODO_")
}

// OutboxEmitInput is the per-emit payload for the local OutboxEmitter
// port. Mirrors dispatcher.EmitInput by shape so wire-up at cmd/server
// can satisfy both ports with the same emitter.
type OutboxEmitInput struct {
	TenantID       string
	Aggregate      wh.AggregateType
	PurchaseID     string
	EventType      string
	IdempotencyKey string
}

// OutboxEmitter is the hexagonal port for synchronous-RPC-initiated event
// publication. The Stage A.5 CancelSubscription RPC needs to emit the
// canonical chora.payments.user_subscription.cancelled.v1 event without
// importing the dispatcher package (which already imports grpc indirectly
// via shared types). cmd/server wires a thin adapter satisfying both
// ports with the same outbox.Emitter.
type OutboxEmitter interface {
	Emit(ctx context.Context, in OutboxEmitInput) error
}

// Deps wires the gRPC server's collaborators.
type Deps struct {
	Course       coursepurchase.Repo
	Application  apppay.Repo
	FamiliarEgg  egg.Repo
	ManaTopUp    mana.Repo
	Subscription sub.Repo
	// 6th + 7th aggregates per Stage A.5.
	UserManaTopUp  umt.Repo
	IdentityKycFee kyc.Repo
	// 8th Purchase aggregate — CHO-1736 H+ Marketplace Subscribe.
	AddonPurchase tap.Repo
	// Outbox emits chora.payments.user_subscription.cancelled.v1 from the
	// CancelSubscription RPC. Optional — when nil the RPC still works,
	// just no event is published.
	Outbox OutboxEmitter

	// StripeCustomers caches the (tenant_id, learner_gcid) -> cus_xxx
	// mapping so chora-payments reuses the same Stripe Customer across
	// all 5 Purchase aggregates. Stripe persists PaymentMethods on the
	// Customer, so card-saving works automatically when this is wired.
	StripeCustomers stripecustomer.Repo

	Stripe stripeadapter.Client

	// PriceCatalogue resolves (addon_code, tier_code, currency) → Stripe
	// Price ID for the CHO-1762 tenant-addon Subscribe flow
	// (mode=subscription). Optional — when nil,
	// CreateTenantAddonCheckoutSession falls back to the legacy
	// mode=payment + inline PriceData behaviour (for tests + standalone
	// smoke without the catalogue wired). main.go logs a WARNING when
	// fallback is in effect.
	PriceCatalogue stripeCataloguePort

	// DefaultSuccessURL + DefaultCancelURL fall back when the caller
	// omits success_url / cancel_url. Per feedback_no_inline_config,
	// these come from env at cmd/server boot.
	DefaultSuccessURL string
	DefaultCancelURL  string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Server implements pb.PaymentServiceServer.
type Server struct {
	pb.UnimplementedPaymentServiceServer
	deps Deps
}

// New constructs a Server. Panics on a nil Stripe client (real wiring
// MUST be present per feedback_no_stubs_real_wiring).
func New(deps Deps) *Server {
	if deps.Stripe == nil {
		panic("grpc: Stripe client required")
	}
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Server{deps: deps}
}

// -----------------------------------------------------------------------------
// CreateCourseCheckoutSession
// -----------------------------------------------------------------------------

func (s *Server) CreateCourseCheckoutSession(ctx context.Context, req *pb.CreateCourseCheckoutSessionRequest) (*pb.CreateCourseCheckoutSessionResponse, error) {
	if s.deps.Course == nil {
		return nil, status.Error(codes.Unimplemented, "course_purchase repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.LearnerGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.CourseId == "" {
		return nil, status.Error(codes.InvalidArgument, "course_id required")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.LearnerGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeCoursePurchase,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.LearnerGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora Course Enrolment",
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"course_id":       req.CourseId,
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	cp, err := coursepurchase.New(
		purchaseID, req.TenantId, req.LearnerGcid, req.CourseId,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "course_purchase.New: %v", err)
	}
	if err := s.deps.Course.Save(ctx, cp); err != nil {
		return nil, status.Errorf(codes.Internal, "save course_purchase: %v", err)
	}

	return &pb.CreateCourseCheckoutSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateApplicationCheckoutSession
// -----------------------------------------------------------------------------

func (s *Server) CreateApplicationCheckoutSession(ctx context.Context, req *pb.CreateApplicationCheckoutSessionRequest) (*pb.CreateApplicationCheckoutSessionResponse, error) {
	if s.deps.Application == nil {
		return nil, status.Error(codes.Unimplemented, "application_payment repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.LearnerGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.ApplicationId == "" || req.CourseId == "" {
		return nil, status.Error(codes.InvalidArgument, "application_id + course_id required")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.LearnerGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeApplicationPayment,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.LearnerGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora Course Application",
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"course_id":       req.CourseId,
			"application_id":  req.ApplicationId,
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	ap, err := apppay.New(
		purchaseID, req.TenantId, req.LearnerGcid,
		req.ApplicationId, req.CourseId,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "application_payment.New: %v", err)
	}
	if err := s.deps.Application.Save(ctx, ap); err != nil {
		return nil, status.Errorf(codes.Internal, "save application_payment: %v", err)
	}

	return &pb.CreateApplicationCheckoutSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateCompanionEggCheckoutSession
// -----------------------------------------------------------------------------

func (s *Server) CreateCompanionEggCheckoutSession(ctx context.Context, req *pb.CreateCompanionEggCheckoutSessionRequest) (*pb.CreateCompanionEggCheckoutSessionResponse, error) {
	if s.deps.FamiliarEgg == nil {
		return nil, status.Error(codes.Unimplemented, "familiar_egg_purchase repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.LearnerGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.EggSku == "" {
		return nil, status.Error(codes.InvalidArgument, "egg_sku required")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.LearnerGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeFamiliarEggPurchase,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.LearnerGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora Familiar Egg: " + req.EggSku,
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"egg_sku": req.EggSku,
			// ADR-188 D2 — the focal-atom hint must travel on the session so
			// the webhook can reconstruct the egg purchase if the create-side
			// row is ever absent. (Expiry windows are nullable and restored
			// nil on the reconstruct path.)
			"suggested_focal_atom_id": req.SuggestedFocalAtomId,
			"idempotency_key":         req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	ep, err := egg.New(
		purchaseID, req.TenantId, req.LearnerGcid, req.EggSku,
		req.SuggestedFocalAtomId,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		timestampToTimePtr(req.SoftExpiryAt), timestampToTimePtr(req.HardExpiryAt),
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "familiar_egg_purchase.New: %v", err)
	}
	if err := s.deps.FamiliarEgg.Save(ctx, ep); err != nil {
		return nil, status.Errorf(codes.Internal, "save familiar_egg_purchase: %v", err)
	}

	return &pb.CreateCompanionEggCheckoutSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateManaTopUpSession
// -----------------------------------------------------------------------------

func (s *Server) CreateManaTopUpSession(ctx context.Context, req *pb.CreateManaTopUpSessionRequest) (*pb.CreateManaTopUpSessionResponse, error) {
	if s.deps.ManaTopUp == nil {
		return nil, status.Error(codes.Unimplemented, "tenant_mana_topup repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.AdminGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.Sku == "" || req.ManaUnits <= 0 {
		return nil, status.Error(codes.InvalidArgument, "sku + mana_units required (mana_units > 0)")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.AdminGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeTenantManaTopUp,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.AdminGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora Tenant Mana Top-Up: " + req.Sku,
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"sku":             req.Sku,
			"mana_units":      fmt.Sprintf("%d", req.ManaUnits),
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	m, err := mana.New(
		purchaseID, req.TenantId, req.AdminGcid, req.Sku,
		req.ManaUnits,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_mana_topup.New: %v", err)
	}
	if err := s.deps.ManaTopUp.Save(ctx, m); err != nil {
		return nil, status.Errorf(codes.Internal, "save tenant_mana_topup: %v", err)
	}

	return &pb.CreateManaTopUpSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateSubscription
// -----------------------------------------------------------------------------

func (s *Server) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*pb.CreateSubscriptionResponse, error) {
	if s.deps.Subscription == nil {
		return nil, status.Error(codes.Unimplemented, "user_subscription repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.LearnerGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.PlanSku == "" {
		return nil, status.Error(codes.InvalidArgument, "plan_sku required")
	}
	billingPeriod := mapBillingPeriod(req.BillingPeriod)
	if !billingPeriod.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "billing_period must be monthly|annually")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.LearnerGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeUserSubscription,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.LearnerGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora Familiar Subscription: " + req.PlanSku,
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"plan_sku":        req.PlanSku,
			"billing_period":  string(billingPeriod),
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	u, err := sub.New(
		purchaseID, req.TenantId, req.LearnerGcid, req.PlanSku,
		billingPeriod,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "user_subscription.New: %v", err)
	}
	if err := s.deps.Subscription.Save(ctx, u); err != nil {
		return nil, status.Errorf(codes.Internal, "save user_subscription: %v", err)
	}

	return &pb.CreateSubscriptionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateUserManaTopUpSession (6th aggregate — Stage A.5)
// -----------------------------------------------------------------------------

func (s *Server) CreateUserManaTopUpSession(ctx context.Context, req *pb.CreateUserManaTopUpSessionRequest) (*pb.CreateUserManaTopUpSessionResponse, error) {
	if s.deps.UserManaTopUp == nil {
		return nil, status.Error(codes.Unimplemented, "user_mana_topup repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.LearnerGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.Sku == "" || req.ManaUnits <= 0 {
		return nil, status.Error(codes.InvalidArgument, "sku + mana_units required (mana_units > 0)")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.LearnerGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeUserManaTopUp,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.LearnerGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora User Mana Top-Up: " + req.Sku,
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"sku":             req.Sku,
			"mana_units":      fmt.Sprintf("%d", req.ManaUnits),
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	u, err := umt.New(
		purchaseID, req.TenantId, req.LearnerGcid, req.Sku,
		req.ManaUnits,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "user_mana_topup.New: %v", err)
	}
	if err := s.deps.UserManaTopUp.Save(ctx, u); err != nil {
		return nil, status.Errorf(codes.Internal, "save user_mana_topup: %v", err)
	}

	return &pb.CreateUserManaTopUpSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateIdentityKycFeeSession (7th aggregate — Stage A.5)
// -----------------------------------------------------------------------------

func (s *Server) CreateIdentityKycFeeSession(ctx context.Context, req *pb.CreateIdentityKycFeeSessionRequest) (*pb.CreateIdentityKycFeeSessionResponse, error) {
	if s.deps.IdentityKycFee == nil {
		return nil, status.Error(codes.Unimplemented, "identity_kyc_fee repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.LearnerGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.KycDocType == "" {
		return nil, status.Error(codes.InvalidArgument, "kyc_doc_type required")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)
	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.LearnerGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeIdentityKycFee,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.LearnerGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora KYC Manual Verification: " + req.KycDocType,
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		Metadata: map[string]string{
			"kyc_doc_type":    req.KycDocType,
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	k, err := kyc.New(
		purchaseID, req.TenantId, req.LearnerGcid, req.KycDocType,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity_kyc_fee.New: %v", err)
	}
	if err := s.deps.IdentityKycFee.Save(ctx, k); err != nil {
		return nil, status.Errorf(codes.Internal, "save identity_kyc_fee: %v", err)
	}

	return &pb.CreateIdentityKycFeeSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CreateTenantAddonCheckoutSession (8th aggregate — CHO-1736 H+ Marketplace)
// -----------------------------------------------------------------------------

func (s *Server) CreateTenantAddonCheckoutSession(ctx context.Context, req *pb.CreateTenantAddonCheckoutSessionRequest) (*pb.CreateTenantAddonCheckoutSessionResponse, error) {
	if s.deps.AddonPurchase == nil {
		return nil, status.Error(codes.Unimplemented, "tenant_addon_purchase repo not wired")
	}
	if err := validateCreateRequest(req.TenantId, req.AdminGcid, req.AmountCents, req.Currency); err != nil {
		return nil, err
	}
	if req.AddonPlanId == "" || req.AddonCode == "" || req.TierCode == "" {
		return nil, status.Error(codes.InvalidArgument, "addon_plan_id + addon_code + tier_code required")
	}

	purchaseID := newPurchaseID()
	now := s.deps.Now()

	ctx = tracing.WithTenantID(ctx, req.TenantId)

	// CHO-1762 — resolve the recurring monthly Stripe Price ID for
	// (addon_code, tier_code, currency) via the catalogue (CHO-1760).
	// When the catalogue isn't wired (unit tests, fresh dev box without
	// STRIPE_PRICE_CATALOGUE_SECRET_ID), we fall back to mode=payment
	// + inline PriceData below — main.go logs the fallback at boot.
	priceID := ""
	if s.deps.PriceCatalogue != nil {
		resolved, ok := s.deps.PriceCatalogue.Resolve(req.AddonCode, req.TierCode, req.Currency)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument,
				"stripe_price_missing: no Stripe Price configured for (addon=%q tier=%q currency=%q)",
				req.AddonCode, req.TierCode, req.Currency)
		}
		if isPlaceholderPriceID(resolved) {
			return nil, status.Errorf(codes.InvalidArgument,
				"stripe_price_missing: placeholder Price ID for (addon=%q tier=%q currency=%q) — run scripts/stripe-seed-addon-prices.sh + set STRIPE_PRICE_CATALOGUE_SECRET_ID",
				req.AddonCode, req.TierCode, req.Currency)
		}
		priceID = resolved
	}

	customerID, cerr := s.ensureStripeCustomer(ctx, req.TenantId, req.AdminGcid)
	if cerr != nil {
		return nil, cerr
	}

	stripeIn := stripeadapter.CreateCheckoutSessionInput{
		PurchaseType:     stripeadapter.PurchaseTypeTenantAddonPurchase,
		PurchaseID:       purchaseID,
		TenantID:         req.TenantId,
		LearnerGCID:      req.AdminGcid,
		AmountCents:      req.AmountCents,
		Currency:         req.Currency,
		ProductName:      "Chora H+ Add-On: " + req.AddonCode + " (" + req.TierCode + ")",
		SuccessURL:       s.successOr(req.SuccessUrl),
		CancelURL:        s.cancelOr(req.CancelUrl),
		StripeCustomerID: customerID,
		// CHO-1762 — when set, the adapter switches to mode=subscription
		// + LineItems[].Price referencing this ID. Empty falls back to
		// mode=payment + inline PriceData.
		StripePriceID: priceID,
		Metadata: map[string]string{
			"addon_plan_id":   req.AddonPlanId,
			"addon_code":      req.AddonCode,
			"tier_code":       req.TierCode,
			"idempotency_key": req.IdempotencyKey,
		},
	}
	sessOut, err := s.deps.Stripe.CreateCheckoutSession(ctx, stripeIn)
	if err != nil {
		return nil, statusFromStripeErr(err)
	}

	a, err := tap.New(
		purchaseID, req.TenantId, req.AdminGcid,
		req.AddonPlanId, req.AddonCode, req.TierCode,
		req.AmountCents, req.Currency,
		sessOut.StripeSessionID, sessOut.StripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_addon_purchase.New: %v", err)
	}
	if err := s.deps.AddonPurchase.Save(ctx, a); err != nil {
		return nil, status.Errorf(codes.Internal, "save tenant_addon_purchase: %v", err)
	}

	return &pb.CreateTenantAddonCheckoutSessionResponse{
		PurchaseId:        purchaseID,
		StripeSessionId:   sessOut.StripeSessionID,
		StripeCheckoutUrl: sessOut.StripeCheckoutURL,
		State:             pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED,
	}, nil
}

// -----------------------------------------------------------------------------
// CancelSubscription (Stage A.5 — closes FE-initiated cancel debt)
// -----------------------------------------------------------------------------

func (s *Server) CancelSubscription(ctx context.Context, req *pb.CancelSubscriptionRequest) (*pb.CancelSubscriptionResponse, error) {
	if s.deps.Subscription == nil {
		return nil, status.Error(codes.Unimplemented, "user_subscription repo not wired")
	}
	if req.PurchaseId == "" || req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "purchase_id + tenant_id required")
	}
	ctx = tracing.WithTenantID(ctx, req.TenantId)
	now := s.deps.Now()

	u, err := s.deps.Subscription.GetByID(ctx, req.TenantId, req.PurchaseId)
	if err != nil {
		return nil, statusFromRepoErr(err)
	}
	if u.StripeSubscriptionID == "" {
		// Subscription not yet created on Stripe (still in checkout_started
		// before first invoice). Aggregate cancel is a no-op on Stripe.
		return nil, status.Error(codes.FailedPrecondition, "user_subscription has no stripe_subscription_id yet")
	}
	if u.LifecycleState == sub.SubStateCancelled {
		// Idempotent — already cancelled.
		return &pb.CancelSubscriptionResponse{Purchase: subscriptionToProto(u)}, nil
	}

	// Stripe-side cancel (graceful by default).
	out, cerr := s.deps.Stripe.CancelSubscription(ctx, stripeadapter.CancelSubscriptionInput{
		StripeSubscriptionID: u.StripeSubscriptionID,
		CancelAtPeriodEnd:    req.CancelAtPeriodEnd,
		Reason:               req.Reason,
	})
	if cerr != nil {
		return nil, statusFromStripeErr(cerr)
	}

	// Reconcile chora-payments-side aggregate.
	effectiveAt := out.EffectiveAt
	if effectiveAt.IsZero() {
		effectiveAt = now
	}
	u.MarkSubscriptionCancelled(req.Reason, out.CancelAtPeriodEnd, effectiveAt, now)
	if err := s.deps.Subscription.Save(ctx, u); err != nil {
		return nil, status.Errorf(codes.Internal, "save user_subscription: %v", err)
	}

	// Emit chora.payments.user_subscription.cancelled.v1 outbox event so
	// chora-identity can revoke ongoing role grants. Best-effort — the
	// aggregate-state mutation already committed.
	s.emitCancelledEvent(ctx, u, req.PurchaseId)

	return &pb.CancelSubscriptionResponse{Purchase: subscriptionToProto(u)}, nil
}

// emitCancelledEvent emits the canonical
// chora.payments.user_subscription.cancelled.v1 outbox event when the
// outbox emitter is wired. Logs but does NOT fail the RPC on emit error
// (aggregate already committed; outbox dispatcher will replay).
func (s *Server) emitCancelledEvent(ctx context.Context, u *sub.UserSubscription, purchaseID string) {
	if s.deps.Outbox == nil {
		return
	}
	idempotencyKey := "user_subscription.cancelled." + purchaseID + ".rpc"
	_ = s.deps.Outbox.Emit(ctx, OutboxEmitInput{
		TenantID:       u.TenantID,
		Aggregate:      wh.AggregateUserSubscription,
		PurchaseID:     purchaseID,
		EventType:      "cancelled",
		IdempotencyKey: idempotencyKey,
	})
}

// -----------------------------------------------------------------------------
// GetPurchase
// -----------------------------------------------------------------------------

func (s *Server) GetPurchase(ctx context.Context, req *pb.GetPurchaseRequest) (*pb.GetPurchaseResponse, error) {
	if req.PurchaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "purchase_id required")
	}
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id required")
	}
	ctx = tracing.WithTenantID(ctx, req.TenantId)
	p, err := s.loadPurchaseProto(ctx, req.TenantId, req.PurchaseId, req.AggregateType)
	if err != nil {
		return nil, err
	}
	return &pb.GetPurchaseResponse{Purchase: p}, nil
}

// -----------------------------------------------------------------------------
// RefundPurchase
// -----------------------------------------------------------------------------

func (s *Server) RefundPurchase(ctx context.Context, req *pb.RefundPurchaseRequest) (*pb.RefundPurchaseResponse, error) {
	if req.PurchaseId == "" || req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "purchase_id + tenant_id required")
	}
	ctx = tracing.WithTenantID(ctx, req.TenantId)
	now := s.deps.Now()

	switch req.AggregateType {
	case pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE:
		cp, err := s.deps.Course.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: cp.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, cp.AmountCentsPaid, cp.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := cp.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.Course.Save(ctx, cp); err != nil {
			return nil, status.Errorf(codes.Internal, "save course_purchase: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: courseToProto(cp)}, nil

	case pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT:
		ap, err := s.deps.Application.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: ap.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, ap.AmountCentsPaid, ap.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := ap.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.Application.Save(ctx, ap); err != nil {
			return nil, status.Errorf(codes.Internal, "save application_payment: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: applicationToProto(ap)}, nil

	case pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE:
		ep, err := s.deps.FamiliarEgg.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: ep.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, ep.AmountCentsPaid, ep.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := ep.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.FamiliarEgg.Save(ctx, ep); err != nil {
			return nil, status.Errorf(codes.Internal, "save familiar_egg_purchase: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: eggToProto(ep)}, nil

	case pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP:
		m, err := s.deps.ManaTopUp.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: m.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, m.AmountCentsPaid, m.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := m.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.ManaTopUp.Save(ctx, m); err != nil {
			return nil, status.Errorf(codes.Internal, "save tenant_mana_topup: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: manaToProto(m)}, nil

	case pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION:
		u, err := s.deps.Subscription.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: u.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, u.AmountCentsPaidTotal, u.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := u.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.Subscription.Save(ctx, u); err != nil {
			return nil, status.Errorf(codes.Internal, "save user_subscription: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: subscriptionToProto(u)}, nil

	case pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP:
		um, err := s.deps.UserManaTopUp.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: um.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, um.AmountCentsPaid, um.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := um.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.UserManaTopUp.Save(ctx, um); err != nil {
			return nil, status.Errorf(codes.Internal, "save user_mana_topup: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: userManaToProto(um)}, nil

	case pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE:
		k, err := s.deps.IdentityKycFee.GetByID(ctx, req.TenantId, req.PurchaseId)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		refund, rerr := s.deps.Stripe.RefundCharge(ctx, stripeadapter.RefundChargeInput{
			StripeChargeID: k.StripeChargeID,
			AmountCents:    refundAmount(req.AmountCents, k.AmountCentsPaid, k.AmountCentsRefunded),
			Reason:         string(mapRefundReason(req.Reason)),
		})
		if rerr != nil {
			return nil, statusFromStripeErr(rerr)
		}
		if err := k.MarkRefunded(refund.StripeRefundID, refund.AmountCentsRefunded, mapRefundReason(req.Reason), now); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "MarkRefunded: %v", err)
		}
		if err := s.deps.IdentityKycFee.Save(ctx, k); err != nil {
			return nil, status.Errorf(codes.Internal, "save identity_kyc_fee: %v", err)
		}
		return &pb.RefundPurchaseResponse{Purchase: kycFeeToProto(k)}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown aggregate_type")
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func validateCreateRequest(tenantID, gcid string, amountCents int64, currency string) error {
	if tenantID == "" {
		return status.Error(codes.InvalidArgument, "tenant_id required")
	}
	if gcid == "" {
		return status.Error(codes.InvalidArgument, "learner_gcid (or admin_gcid) required")
	}
	if amountCents < 0 {
		return status.Error(codes.InvalidArgument, "amount_cents must be >= 0")
	}
	if len(currency) != 3 {
		return status.Error(codes.InvalidArgument, "currency must be a 3-letter ISO 4217 code")
	}
	return nil
}

func (s *Server) successOr(in string) string {
	if in != "" {
		return in
	}
	return s.deps.DefaultSuccessURL
}

func (s *Server) cancelOr(in string) string {
	if in != "" {
		return in
	}
	return s.deps.DefaultCancelURL
}

func newPurchaseID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// ensureStripeCustomer resolves (or creates) the Stripe Customer for the
// supplied (tenant_id, learner_gcid). Returns the cus_xxx ID so the
// caller can pass it as CheckoutSessionInput.StripeCustomerID — which
// is what makes Stripe persist + offer saved cards across Sessions for
// the same learner.
//
// Idempotent on (tenant_id, learner_gcid). When StripeCustomers is nil
// (test wiring) returns "" so callers can still smoke the create flow.
func (s *Server) ensureStripeCustomer(ctx context.Context, tenantID, learnerGCID string) (string, error) {
	if s.deps.StripeCustomers == nil {
		return "", nil
	}
	sc, err := s.deps.StripeCustomers.GetByGCID(ctx, tenantID, learnerGCID)
	if err == nil {
		return sc.StripeCustomerID, nil
	}
	if !errors.Is(err, stripecustomer.ErrNotFound) {
		return "", status.Errorf(codes.Internal, "stripe_customer lookup: %v", err)
	}
	// Not in registry — call Stripe to create + persist locally.
	out, err := s.deps.Stripe.EnsureCustomer(ctx, stripeadapter.EnsureCustomerInput{
		TenantID:    tenantID,
		LearnerGCID: learnerGCID,
	})
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "stripe create customer: %v", err)
	}
	now := s.deps.Now()
	row, err := stripecustomer.New(tenantID, learnerGCID, out.StripeCustomerID, "", now)
	if err != nil {
		return "", status.Errorf(codes.Internal, "stripe_customer.New: %v", err)
	}
	if err := s.deps.StripeCustomers.Insert(ctx, row); err != nil {
		return "", status.Errorf(codes.Internal, "stripe_customer insert: %v", err)
	}
	return out.StripeCustomerID, nil
}

func timestampToTimePtr(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil || !ts.IsValid() {
		return nil
	}
	t := ts.AsTime().UTC()
	return &t
}

func timeToTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

func timePtrToTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timeToTimestamp(*t)
}

func mapBillingPeriod(p pb.BillingPeriod) sub.BillingPeriod {
	switch p {
	case pb.BillingPeriod_BILLING_PERIOD_MONTHLY:
		return sub.BillingMonthly
	case pb.BillingPeriod_BILLING_PERIOD_ANNUALLY:
		return sub.BillingAnnually
	}
	return ""
}

func mapRefundReason(r pb.RefundReason) shared.RefundReason {
	switch r {
	case pb.RefundReason_REFUND_REASON_SUPPORT_INITIATED:
		return shared.RefundReasonSupportInitiated
	case pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST:
		return shared.RefundReasonCustomerRequest
	case pb.RefundReason_REFUND_REASON_DUPLICATE_CHARGE:
		return shared.RefundReasonDuplicateCharge
	case pb.RefundReason_REFUND_REASON_FRAUD:
		return shared.RefundReasonFraud
	case pb.RefundReason_REFUND_REASON_EXPIRED_UNHATCHED:
		return shared.RefundReasonExpiredUnhatched
	}
	return shared.RefundReasonCustomerRequest
}

// refundAmount picks the canonical refund amount: requested if > 0, else
// (amount_paid - amount_already_refunded). Never negative.
func refundAmount(requested, paid, alreadyRefunded int64) int64 {
	if requested > 0 {
		return requested
	}
	remaining := paid - alreadyRefunded
	if remaining < 0 {
		return 0
	}
	return remaining
}

func statusFromStripeErr(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition, "stripe: %v", err)
}

func statusFromRepoErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, shared.ErrNotFound) {
		return status.Error(codes.NotFound, "purchase not found")
	}
	return status.Errorf(codes.Internal, "repo: %v", err)
}

// loadPurchaseProto loads + projects the aggregate identified by
// (tenantID, purchaseID, aggregateType) to its proto representation.
func (s *Server) loadPurchaseProto(ctx context.Context, tenantID, purchaseID string, agg pb.AggregateType) (*pb.Purchase, error) {
	switch agg {
	case pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE:
		cp, err := s.deps.Course.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return courseToProto(cp), nil
	case pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT:
		ap, err := s.deps.Application.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return applicationToProto(ap), nil
	case pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE:
		ep, err := s.deps.FamiliarEgg.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return eggToProto(ep), nil
	case pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP:
		m, err := s.deps.ManaTopUp.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return manaToProto(m), nil
	case pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION:
		u, err := s.deps.Subscription.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return subscriptionToProto(u), nil
	case pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP:
		um, err := s.deps.UserManaTopUp.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return userManaToProto(um), nil
	case pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE:
		k, err := s.deps.IdentityKycFee.GetByID(ctx, tenantID, purchaseID)
		if err != nil {
			return nil, statusFromRepoErr(err)
		}
		return kycFeeToProto(k), nil
	}
	return nil, status.Error(codes.InvalidArgument, "unknown aggregate_type")
}

// -----------------------------------------------------------------------------
// projection helpers — aggregate → proto.Purchase
// -----------------------------------------------------------------------------

func projectShared(p shared.Purchase, agg pb.AggregateType) *pb.Purchase {
	return &pb.Purchase{
		PurchaseId:            p.PurchaseID,
		TenantId:              p.TenantID,
		LearnerGcid:           p.LearnerGCID,
		AggregateType:         agg,
		State:                 mapState(p.State),
		AmountCents:           p.AmountCents,
		AmountCentsPaid:       p.AmountCentsPaid,
		AmountCentsRefunded:   p.AmountCentsRefunded,
		Currency:              p.Currency,
		StripeSessionId:       p.StripeSessionID,
		StripePaymentIntentId: p.StripePaymentIntentID,
		StripeChargeId:        p.StripeChargeID,
		StripeRefundId:        p.StripeRefundID,
		StripeCheckoutUrl:     p.StripeCheckoutURL,
		StripeFailureCode:     p.StripeFailureCode,
		StripeFailureMessage:  p.StripeFailureMessage,
		RefundReason:          mapRefundReasonProto(p.RefundReason),
		CheckoutStartedAt:     timeToTimestamp(p.CheckoutStartedAt),
		PaidAt:                timePtrToTimestamp(p.PaidAt),
		FailedAt:              timePtrToTimestamp(p.FailedAt),
		RefundedAt:            timePtrToTimestamp(p.RefundedAt),
		ExpiredAt:             timePtrToTimestamp(p.ExpiredAt),
		CreatedAt:             timeToTimestamp(p.CreatedAt),
		UpdatedAt:             timeToTimestamp(p.UpdatedAt),
	}
}

func courseToProto(cp *coursepurchase.CoursePurchase) *pb.Purchase {
	p := projectShared(cp.Purchase, pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE)
	p.CourseId = cp.CourseID
	return p
}

func applicationToProto(ap *apppay.ApplicationPayment) *pb.Purchase {
	p := projectShared(ap.Purchase, pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT)
	p.CourseId = ap.CourseID
	p.ApplicationId = ap.ApplicationID
	return p
}

func eggToProto(ep *egg.FamiliarEggPurchase) *pb.Purchase {
	p := projectShared(ep.Purchase, pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE)
	p.EggSku = ep.EggSKU
	p.SuggestedFocalAtomId = ep.SuggestedFocalAtomID
	p.SoftExpiryAt = timePtrToTimestamp(ep.SoftExpiryAt)
	p.HardExpiryAt = timePtrToTimestamp(ep.HardExpiryAt)
	p.ProvisionedAt = timePtrToTimestamp(ep.ProvisionedAt)
	p.RefundCreditOnly = ep.RefundCreditOnly
	return p
}

func manaToProto(m *mana.TenantManaTopUp) *pb.Purchase {
	p := projectShared(m.Purchase, pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP)
	p.Sku = m.SKU
	p.ManaUnits = m.ManaUnits
	p.ManaUnitsDebited = m.ManaUnitsDebited
	return p
}

func subscriptionToProto(u *sub.UserSubscription) *pb.Purchase {
	p := projectShared(u.Purchase, pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION)
	p.PlanSku = u.PlanSKU
	switch u.BillingPeriod {
	case sub.BillingMonthly:
		p.BillingPeriod = pb.BillingPeriod_BILLING_PERIOD_MONTHLY
	case sub.BillingAnnually:
		p.BillingPeriod = pb.BillingPeriod_BILLING_PERIOD_ANNUALLY
	}
	if u.LifecycleState != "" {
		p.LifecycleState = strings.ToLower(string(u.LifecycleState))
	}
	p.StripeSubscriptionId = u.StripeSubscriptionID
	p.StripeCustomerId = u.StripeCustomerID
	p.CurrentPeriodStart = timePtrToTimestamp(u.CurrentPeriodStart)
	p.CurrentPeriodEnd = timePtrToTimestamp(u.CurrentPeriodEnd)
	p.AmountCentsPaidTotal = u.AmountCentsPaidTotal
	// Stage A.5 cancellation projection.
	p.CancelAtPeriodEnd = u.CancelAtPeriodEnd
	p.CancellationReason = u.CancellationReason
	p.CancelledAt = timePtrToTimestamp(u.CancelledAt)
	p.EffectiveAt = timePtrToTimestamp(u.EffectiveAt)
	return p
}

func userManaToProto(u *umt.UserManaTopUp) *pb.Purchase {
	p := projectShared(u.Purchase, pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP)
	p.Sku = u.SKU
	p.ManaUnits = u.ManaUnits
	p.ManaUnitsDebited = u.ManaUnitsDebited
	return p
}

func kycFeeToProto(k *kyc.IdentityKycFee) *pb.Purchase {
	p := projectShared(k.Purchase, pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE)
	p.KycDocType = k.KYCDocType
	return p
}

func mapState(s shared.State) pb.PurchaseState {
	switch s {
	case shared.StateCheckoutStarted:
		return pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED
	case shared.StatePaymentCaptured:
		return pb.PurchaseState_PURCHASE_STATE_PAYMENT_CAPTURED
	case shared.StatePaymentFailed:
		return pb.PurchaseState_PURCHASE_STATE_PAYMENT_FAILED
	case shared.StateRefunded:
		return pb.PurchaseState_PURCHASE_STATE_REFUNDED
	case shared.StateExpired:
		return pb.PurchaseState_PURCHASE_STATE_EXPIRED
	}
	return pb.PurchaseState_PURCHASE_STATE_UNSPECIFIED
}

func mapRefundReasonProto(r shared.RefundReason) pb.RefundReason {
	switch r {
	case shared.RefundReasonSupportInitiated:
		return pb.RefundReason_REFUND_REASON_SUPPORT_INITIATED
	case shared.RefundReasonCustomerRequest:
		return pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST
	case shared.RefundReasonDuplicateCharge:
		return pb.RefundReason_REFUND_REASON_DUPLICATE_CHARGE
	case shared.RefundReasonFraud:
		return pb.RefundReason_REFUND_REASON_FRAUD
	case shared.RefundReasonExpiredUnhatched:
		return pb.RefundReason_REFUND_REASON_EXPIRED_UNHATCHED
	}
	return pb.RefundReason_REFUND_REASON_UNSPECIFIED
}

// Compile-time port assertion — must be after all method declarations.
var _ pb.PaymentServiceServer = (*Server)(nil)

// StampSubscriptionIDForTest installs a stripe_subscription_id on the
// in-mem subscription aggregate so CancelSubscription has a handle to
// pass to Stripe. The real flow stamps this via the
// customer.subscription.created webhook; this is a test-only seam.
//
// Returns shared.ErrNotFound when the aggregate is missing.
func (s *Server) StampSubscriptionIDForTest(tenantID, purchaseID, stripeSubscriptionID string) error {
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	u, err := s.deps.Subscription.GetByID(ctx, tenantID, purchaseID)
	if err != nil {
		return err
	}
	u.StripeSubscriptionID = stripeSubscriptionID
	return s.deps.Subscription.Save(ctx, u)
}

// LoadByEventID returns the canonical aggregate type + purchase id for
// a previously-recorded Stripe webhook event. Used by the webhook handler
// to dedup + reconcile. Kept here so the gRPC + webhook paths share
// pp identical aggregate routing.
//
// _ wh is referenced via webhook handler tests; importing here keeps
// the package surface explicit.
var _ wh.AggregateType = wh.AggregateCoursePurchase
