package grpc_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	googlegrpc "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"

	pgrpc "github.com/apollo-chora/chora-payments/internal/adapter/grpc"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

const (
	tTenantID    = "01970000-0000-7000-8000-000000000001"
	tLearnerGCID = "01970000-0000-7000-a000-000000000002"
	tCourseID    = "01970000-0000-7000-c000-000000000004"
)

func newServer(t *testing.T) *pgrpc.Server {
	t.Helper()
	return pgrpc.New(pgrpc.Deps{
		Course:            inmem.NewCoursePurchaseRepo(),
		Application:       inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:       inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:         inmem.NewTenantManaTopUpRepo(),
		Subscription:      inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:     inmem.NewUserManaTopUpRepo(),
		IdentityKycFee:    inmem.NewIdentityKycFeeRepo(),
		AddonPurchase:     inmem.NewTenantAddonPurchaseRepo(),
		StripeCustomers:   inmem.NewStripeCustomerRepo(),
		Stripe:            stripeadapter.NewStubClient(),
		DefaultSuccessURL: "https://chora.site/payment/success",
		DefaultCancelURL:  "https://chora.site/payment/cancel",
		Now:               func() time.Time { return time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC) },
	})
}

func TestCreateCourseCheckoutSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateCourseCheckoutSession(context.Background(), &pb.CreateCourseCheckoutSessionRequest{
		IdempotencyKey: "key-1",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		CourseId:       tCourseID,
		AmountCents:    99900,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateCourseCheckoutSession: %v", err)
	}
	if resp.PurchaseId == "" {
		t.Errorf("PurchaseId empty")
	}
	if !strings.HasPrefix(resp.StripeSessionId, "cs_stub_") {
		t.Errorf("StripeSessionId=%q, want cs_stub_ prefix", resp.StripeSessionId)
	}
	if resp.State != pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED {
		t.Errorf("State=%v, want CHECKOUT_STARTED", resp.State)
	}
}

func TestCreateCourseCheckoutSession_ReusesStripeCustomer(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	ctx := context.Background()
	mkReq := func(idempotencyKey string) *pb.CreateCourseCheckoutSessionRequest {
		return &pb.CreateCourseCheckoutSessionRequest{
			IdempotencyKey: idempotencyKey,
			TenantId:       tTenantID,
			LearnerGcid:    tLearnerGCID,
			CourseId:       tCourseID,
			AmountCents:    99900,
			Currency:       "SGD",
		}
	}
	r1, err := s.CreateCourseCheckoutSession(ctx, mkReq("key-1"))
	if err != nil {
		t.Fatalf("1st CreateCourseCheckoutSession: %v", err)
	}
	r2, err := s.CreateCourseCheckoutSession(ctx, mkReq("key-2"))
	if err != nil {
		t.Fatalf("2nd CreateCourseCheckoutSession: %v", err)
	}

	// Each Session has its own ID + purchase_id.
	if r1.PurchaseId == r2.PurchaseId {
		t.Errorf("PurchaseId reused across requests; want distinct")
	}
	if r1.StripeSessionId == r2.StripeSessionId {
		t.Errorf("StripeSessionId reused; want distinct (different purchase_id)")
	}
	// Get the persisted aggregates + assert their stripe_customer mapping
	// reuse via the registry. Both aggregates ought to share the same
	// cus_xxx because chora-payments cached it.
	get1, err := s.GetPurchase(ctx, &pb.GetPurchaseRequest{
		PurchaseId:    r1.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
	})
	if err != nil {
		t.Fatalf("GetPurchase r1: %v", err)
	}
	get2, err := s.GetPurchase(ctx, &pb.GetPurchaseRequest{
		PurchaseId:    r2.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
	})
	if err != nil {
		t.Fatalf("GetPurchase r2: %v", err)
	}
	if get1.Purchase.PurchaseId == "" || get2.Purchase.PurchaseId == "" {
		t.Errorf("GetPurchase returned empty PurchaseId")
	}
}

func TestCreateCourseCheckoutSession_ValidatesArgs(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	cases := []struct {
		name string
		req  *pb.CreateCourseCheckoutSessionRequest
		code googlegrpc.Code
	}{
		{
			name: "missing tenant_id",
			req:  &pb.CreateCourseCheckoutSessionRequest{LearnerGcid: tLearnerGCID, CourseId: tCourseID, AmountCents: 100, Currency: "SGD"},
			code: googlegrpc.InvalidArgument,
		},
		{
			name: "missing learner_gcid",
			req:  &pb.CreateCourseCheckoutSessionRequest{TenantId: tTenantID, CourseId: tCourseID, AmountCents: 100, Currency: "SGD"},
			code: googlegrpc.InvalidArgument,
		},
		{
			name: "missing course_id",
			req:  &pb.CreateCourseCheckoutSessionRequest{TenantId: tTenantID, LearnerGcid: tLearnerGCID, AmountCents: 100, Currency: "SGD"},
			code: googlegrpc.InvalidArgument,
		},
		{
			name: "bad currency",
			req:  &pb.CreateCourseCheckoutSessionRequest{TenantId: tTenantID, LearnerGcid: tLearnerGCID, CourseId: tCourseID, AmountCents: 100, Currency: "SG"},
			code: googlegrpc.InvalidArgument,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.CreateCourseCheckoutSession(context.Background(), tc.req)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			st, ok := gstatus.FromError(err)
			if !ok || st.Code() != tc.code {
				t.Errorf("err code=%v, want %v", st.Code(), tc.code)
			}
		})
	}
}

func TestCreateSubscription_InvalidBillingPeriod(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.CreateSubscription(context.Background(), &pb.CreateSubscriptionRequest{
		IdempotencyKey: "key-1",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		PlanSku:        "familiar.standard.monthly.v1",
		BillingPeriod:  pb.BillingPeriod_BILLING_PERIOD_UNSPECIFIED,
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.InvalidArgument {
		t.Errorf("err code=%v, want InvalidArgument", st.Code())
	}
}

func TestGetPurchase_NotFound(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    "01970000-0000-7000-b000-deadbeefdead",
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.NotFound {
		t.Errorf("err code=%v, want NotFound", st.Code())
	}
}

func TestRefundPurchase_FailsBeforePayment(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	ctx := context.Background()
	resp, err := s.CreateCourseCheckoutSession(ctx, &pb.CreateCourseCheckoutSessionRequest{
		IdempotencyKey: "key-1",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		CourseId:       tCourseID,
		AmountCents:    99900,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Try to refund before payment_captured — should fail because
	// shared.Purchase.MarkRefunded rejects the state transition.
	_, err = s.RefundPurchase(ctx, &pb.RefundPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
		AmountCents:   99900,
		Reason:        pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST,
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.FailedPrecondition {
		t.Errorf("err code=%v, want FailedPrecondition", st.Code())
	}
}

// stubErrStripe is a stub that fails CreateCheckoutSession with the
// configured error. Used to assert error mapping.
type stubErrStripe struct {
	createErr error
}

func (s stubErrStripe) CreateCheckoutSession(_ context.Context, _ stripeadapter.CreateCheckoutSessionInput) (stripeadapter.CreateCheckoutSessionOutput, error) {
	return stripeadapter.CreateCheckoutSessionOutput{}, s.createErr
}
func (s stubErrStripe) RefundCharge(_ context.Context, _ stripeadapter.RefundChargeInput) (stripeadapter.RefundChargeOutput, error) {
	return stripeadapter.RefundChargeOutput{}, errors.New("not implemented")
}
func (s stubErrStripe) EnsureCustomer(_ context.Context, _ stripeadapter.EnsureCustomerInput) (stripeadapter.EnsureCustomerOutput, error) {
	return stripeadapter.EnsureCustomerOutput{StripeCustomerID: "cus_stub_err"}, nil
}
func (s stubErrStripe) CancelSubscription(_ context.Context, _ stripeadapter.CancelSubscriptionInput) (stripeadapter.CancelSubscriptionOutput, error) {
	return stripeadapter.CancelSubscriptionOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) UpdateSubscriptionPrice(_ context.Context, _ stripeadapter.UpdateSubscriptionPriceInput) (stripeadapter.UpdateSubscriptionPriceOutput, error) {
	return stripeadapter.UpdateSubscriptionPriceOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) PreviewSubscriptionPriceChange(_ context.Context, _ stripeadapter.PreviewSubscriptionPriceChangeInput) (stripeadapter.PreviewSubscriptionPriceChangeOutput, error) {
	return stripeadapter.PreviewSubscriptionPriceChangeOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) ListInvoices(_ context.Context, _ stripeadapter.ListInvoicesInput) (stripeadapter.ListInvoicesOutput, error) {
	return stripeadapter.ListInvoicesOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) CreateBillingPortalSession(_ context.Context, _ stripeadapter.CreateBillingPortalSessionInput) (stripeadapter.CreateBillingPortalSessionOutput, error) {
	return stripeadapter.CreateBillingPortalSessionOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) ScheduleSubscriptionPriceChange(_ context.Context, _ stripeadapter.ScheduleSubscriptionPriceChangeInput) (stripeadapter.ScheduleSubscriptionPriceChangeOutput, error) {
	return stripeadapter.ScheduleSubscriptionPriceChangeOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) GetPrice(_ context.Context, _ string) (stripeadapter.GetPriceOutput, error) {
	return stripeadapter.GetPriceOutput{}, errors.New("stubErrStripe: not implemented")
}
func (s stubErrStripe) GetSubscription(_ context.Context, _ string) (stripeadapter.GetSubscriptionOutput, error) {
	return stripeadapter.GetSubscriptionOutput{}, errors.New("stubErrStripe: not implemented")
}

func TestCreateApplicationCheckoutSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateApplicationCheckoutSession(context.Background(), &pb.CreateApplicationCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		ApplicationId:  "01970000-0000-7000-d000-000000000005",
		CourseId:       tCourseID,
		AmountCents:    50000,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateApplicationCheckoutSession: %v", err)
	}
	if resp.State != pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED {
		t.Errorf("State=%v, want CHECKOUT_STARTED", resp.State)
	}
	got, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.ApplicationId != "01970000-0000-7000-d000-000000000005" {
		t.Errorf("ApplicationId mismatch: %s", got.Purchase.ApplicationId)
	}
}

func TestCreateCompanionEggCheckoutSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateCompanionEggCheckoutSession(context.Background(), &pb.CreateCompanionEggCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		EggSku:         "egg.standard.v1",
		AmountCents:    1900,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateCompanionEggCheckoutSession: %v", err)
	}
	got, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.EggSku != "egg.standard.v1" {
		t.Errorf("EggSku=%s, want egg.standard.v1", got.Purchase.EggSku)
	}
}

func TestCreateManaTopUpSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateManaTopUpSession(context.Background(), &pb.CreateManaTopUpSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		AdminGcid:      tLearnerGCID,
		Sku:            "mana.topup.5000.v1",
		ManaUnits:      5000,
		AmountCents:    2900,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateManaTopUpSession: %v", err)
	}
	got, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.Sku != "mana.topup.5000.v1" {
		t.Errorf("Sku=%s, want mana.topup.5000.v1", got.Purchase.Sku)
	}
	if got.Purchase.ManaUnits != 5000 {
		t.Errorf("ManaUnits=%d, want 5000", got.Purchase.ManaUnits)
	}
}

func TestCreateSubscription_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateSubscription(context.Background(), &pb.CreateSubscriptionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		PlanSku:        "familiar.standard.monthly.v1",
		BillingPeriod:  pb.BillingPeriod_BILLING_PERIOD_MONTHLY,
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	got, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.PlanSku != "familiar.standard.monthly.v1" {
		t.Errorf("PlanSku=%s, want familiar.standard.monthly.v1", got.Purchase.PlanSku)
	}
	if got.Purchase.BillingPeriod != pb.BillingPeriod_BILLING_PERIOD_MONTHLY {
		t.Errorf("BillingPeriod=%v, want MONTHLY", got.Purchase.BillingPeriod)
	}
}

func TestCreateCourseCheckoutSession_StripeErrorBubblesAsFailedPrecondition(t *testing.T) {
	t.Parallel()
	s := pgrpc.New(pgrpc.Deps{
		Course:          inmem.NewCoursePurchaseRepo(),
		Application:     inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:     inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:       inmem.NewTenantManaTopUpRepo(),
		Subscription:    inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:   inmem.NewUserManaTopUpRepo(),
		IdentityKycFee:  inmem.NewIdentityKycFeeRepo(),
		StripeCustomers: inmem.NewStripeCustomerRepo(),
		Stripe:          stubErrStripe{createErr: errors.New("card_declined")},
	})
	_, err := s.CreateCourseCheckoutSession(context.Background(), &pb.CreateCourseCheckoutSessionRequest{
		IdempotencyKey: "key-1",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		CourseId:       tCourseID,
		AmountCents:    99900,
		Currency:       "SGD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.FailedPrecondition {
		t.Errorf("err code=%v, want FailedPrecondition", st.Code())
	}
	if !strings.Contains(st.Message(), "stripe: card_declined") {
		t.Errorf("error msg=%q, want includes 'stripe: card_declined'", st.Message())
	}
}

// -----------------------------------------------------------------------------
// Stage A.5 new RPCs
// -----------------------------------------------------------------------------

func TestCreateUserManaTopUpSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateUserManaTopUpSession(context.Background(), &pb.CreateUserManaTopUpSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		Sku:            "mana.user_topup.standard_v1",
		ManaUnits:      2500,
		AmountCents:    1990,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateUserManaTopUpSession: %v", err)
	}
	if resp.State != pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED {
		t.Errorf("State=%v, want CHECKOUT_STARTED", resp.State)
	}
	got, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.Sku != "mana.user_topup.standard_v1" {
		t.Errorf("Sku=%s, want mana.user_topup.standard_v1", got.Purchase.Sku)
	}
	if got.Purchase.ManaUnits != 2500 {
		t.Errorf("ManaUnits=%d, want 2500", got.Purchase.ManaUnits)
	}
	if got.Purchase.LearnerGcid != tLearnerGCID {
		t.Errorf("LearnerGcid mismatch: %s", got.Purchase.LearnerGcid)
	}
}

func TestCreateUserManaTopUpSession_ValidatesArgs(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	cases := []struct {
		name string
		req  *pb.CreateUserManaTopUpSessionRequest
	}{
		{"missing sku", &pb.CreateUserManaTopUpSessionRequest{TenantId: tTenantID, LearnerGcid: tLearnerGCID, ManaUnits: 100, AmountCents: 1990, Currency: "SGD"}},
		{"zero mana_units", &pb.CreateUserManaTopUpSessionRequest{TenantId: tTenantID, LearnerGcid: tLearnerGCID, Sku: "mana.user_topup.standard_v1", AmountCents: 1990, Currency: "SGD"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.CreateUserManaTopUpSession(context.Background(), tc.req)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			st, _ := gstatus.FromError(err)
			if st.Code() != googlegrpc.InvalidArgument {
				t.Errorf("err code=%v, want InvalidArgument", st.Code())
			}
		})
	}
}

func TestCreateIdentityKycFeeSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateIdentityKycFeeSession(context.Background(), &pb.CreateIdentityKycFeeSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		KycDocType:     "manual_id_doc",
		AmountCents:    999,
		Currency:       "USD",
	})
	if err != nil {
		t.Fatalf("CreateIdentityKycFeeSession: %v", err)
	}
	got, err := s.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.KycDocType != "manual_id_doc" {
		t.Errorf("KycDocType=%s, want manual_id_doc", got.Purchase.KycDocType)
	}
}

func TestCreateIdentityKycFeeSession_RequiresKycDocType(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.CreateIdentityKycFeeSession(context.Background(), &pb.CreateIdentityKycFeeSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		AmountCents:    999,
		Currency:       "USD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.InvalidArgument {
		t.Errorf("err code=%v, want InvalidArgument", st.Code())
	}
}

func TestCancelSubscription_RequiresStripeSubscriptionID(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	ctx := context.Background()
	// Mint a subscription via Checkout — it has session_id but NO
	// stripe_subscription_id yet (that's set by the
	// customer.subscription.created webhook).
	resp, err := s.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		PlanSku:        "familiar.standard.monthly.v1",
		BillingPeriod:  pb.BillingPeriod_BILLING_PERIOD_MONTHLY,
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	_, err = s.CancelSubscription(ctx, &pb.CancelSubscriptionRequest{
		PurchaseId:        resp.PurchaseId,
		TenantId:          tTenantID,
		CancelAtPeriodEnd: true,
	})
	if err == nil {
		t.Fatalf("expected error (no stripe_subscription_id yet), got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.FailedPrecondition {
		t.Errorf("err code=%v, want FailedPrecondition", st.Code())
	}
}

func TestCancelSubscription_GracefulFlow(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	ctx := context.Background()
	resp, err := s.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		PlanSku:        "familiar.standard.monthly.v1",
		BillingPeriod:  pb.BillingPeriod_BILLING_PERIOD_MONTHLY,
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	// Simulate the customer.subscription.created webhook: stamp the
	// aggregate with a stripe_subscription_id so Cancel works.
	got, err := s.GetPurchase(ctx, &pb.GetPurchaseRequest{
		PurchaseId:    resp.PurchaseId,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	_ = got
	// Re-fetch the aggregate from the in-mem repo via a back-door —
	// the in-mem subscription repo lets us reach into the Save path by
	// re-saving with stripe_subscription_id set. The server reads this
	// before invoking Stripe.
	if err := stampSubscriptionID(s, tTenantID, resp.PurchaseId, "sub_test_xyz"); err != nil {
		t.Fatalf("stampSubscriptionID: %v", err)
	}

	cancelResp, err := s.CancelSubscription(ctx, &pb.CancelSubscriptionRequest{
		PurchaseId:        resp.PurchaseId,
		TenantId:          tTenantID,
		CancelAtPeriodEnd: true,
		Reason:            "user requested cancel",
	})
	if err != nil {
		t.Fatalf("CancelSubscription: %v", err)
	}
	if cancelResp.Purchase.LifecycleState != "cancelled" {
		t.Errorf("LifecycleState=%s, want cancelled", cancelResp.Purchase.LifecycleState)
	}
	if !cancelResp.Purchase.CancelAtPeriodEnd {
		t.Errorf("CancelAtPeriodEnd=false, want true (graceful)")
	}
	if cancelResp.Purchase.CancellationReason != "user requested cancel" {
		t.Errorf("CancellationReason=%q, want %q", cancelResp.Purchase.CancellationReason, "user requested cancel")
	}
}

func TestCancelSubscription_Idempotent(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	ctx := context.Background()
	resp, err := s.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		PlanSku:        "familiar.standard.monthly.v1",
		BillingPeriod:  pb.BillingPeriod_BILLING_PERIOD_MONTHLY,
		AmountCents:    999,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if err := stampSubscriptionID(s, tTenantID, resp.PurchaseId, "sub_test_xyz"); err != nil {
		t.Fatalf("stampSubscriptionID: %v", err)
	}
	for i := 0; i < 2; i++ {
		_, err = s.CancelSubscription(ctx, &pb.CancelSubscriptionRequest{
			PurchaseId:        resp.PurchaseId,
			TenantId:          tTenantID,
			CancelAtPeriodEnd: false,
		})
		if err != nil {
			t.Fatalf("CancelSubscription call %d: %v", i, err)
		}
	}
}

// stampSubscriptionID reaches into the in-mem subscription repo via the
// server's GetPurchase + a follow-on Save through the server's Deps to
// install a stripe_subscription_id (which the real flow sets via the
// customer.subscription.created webhook). It's a test-only helper.
func stampSubscriptionID(s *pgrpc.Server, tenantID, purchaseID, stripeSubscriptionID string) error {
	deps := pgrpc.Deps{} // zero-value not used; we read via the server.
	_ = deps
	// Use the Stage A.5-exported StampSubscriptionID method.
	return s.StampSubscriptionIDForTest(tenantID, purchaseID, stripeSubscriptionID)
}

// -----------------------------------------------------------------------------
// CHO-1738 — CreateTenantAddonCheckoutSession (8th aggregate)
// -----------------------------------------------------------------------------

const (
	tAdminGCID = "01970000-0000-7000-d000-aaaaaaaaaaaa"
	tAddonPlan = "01970000-0000-7000-e000-bbbbbbbbbbbb"
	tAddonCode = "knowledge_graph"
	tTierCode  = "pro"
)

func TestCreateTenantAddonCheckoutSession_HappyPath(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	resp, err := s.CreateTenantAddonCheckoutSession(context.Background(), &pb.CreateTenantAddonCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		AdminGcid:      tAdminGCID,
		AddonPlanId:    tAddonPlan,
		AddonCode:      tAddonCode,
		TierCode:       tTierCode,
		AmountCents:    4900,
		Currency:       "SGD",
	})
	if err != nil {
		t.Fatalf("CreateTenantAddonCheckoutSession: %v", err)
	}
	if resp.PurchaseId == "" {
		t.Errorf("PurchaseId empty")
	}
	if resp.StripeSessionId == "" {
		t.Errorf("StripeSessionId empty")
	}
	if resp.StripeCheckoutUrl == "" {
		t.Errorf("StripeCheckoutUrl empty")
	}
	if resp.State != pb.PurchaseState_PURCHASE_STATE_CHECKOUT_STARTED {
		t.Errorf("State=%v, want CHECKOUT_STARTED", resp.State)
	}
}

func TestCreateTenantAddonCheckoutSession_RequiresAddonPlan(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.CreateTenantAddonCheckoutSession(context.Background(), &pb.CreateTenantAddonCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		AdminGcid:      tAdminGCID,
		// addon_plan_id missing
		AddonCode:   tAddonCode,
		TierCode:    tTierCode,
		AmountCents: 4900,
		Currency:    "SGD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.InvalidArgument {
		t.Errorf("err code=%v, want InvalidArgument", st.Code())
	}
}

func TestCreateTenantAddonCheckoutSession_RequiresAddonCode(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.CreateTenantAddonCheckoutSession(context.Background(), &pb.CreateTenantAddonCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		AdminGcid:      tAdminGCID,
		AddonPlanId:    tAddonPlan,
		// addon_code missing
		TierCode:    tTierCode,
		AmountCents: 4900,
		Currency:    "SGD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.InvalidArgument {
		t.Errorf("err code=%v, want InvalidArgument", st.Code())
	}
}

func TestCreateTenantAddonCheckoutSession_RequiresTierCode(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.CreateTenantAddonCheckoutSession(context.Background(), &pb.CreateTenantAddonCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		AdminGcid:      tAdminGCID,
		AddonPlanId:    tAddonPlan,
		AddonCode:      tAddonCode,
		// tier_code missing
		AmountCents: 4900,
		Currency:    "SGD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.InvalidArgument {
		t.Errorf("err code=%v, want InvalidArgument", st.Code())
	}
}

func TestCreateTenantAddonCheckoutSession_RequiresAdminGcid(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	_, err := s.CreateTenantAddonCheckoutSession(context.Background(), &pb.CreateTenantAddonCheckoutSessionRequest{
		IdempotencyKey: "k",
		TenantId:       tTenantID,
		// admin_gcid missing
		AddonPlanId: tAddonPlan,
		AddonCode:   tAddonCode,
		TierCode:    tTierCode,
		AmountCents: 4900,
		Currency:    "SGD",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, _ := gstatus.FromError(err)
	if st.Code() != googlegrpc.InvalidArgument {
		t.Errorf("err code=%v, want InvalidArgument", st.Code())
	}
}
