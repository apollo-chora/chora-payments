// tenant_addon_subscription_test.go — CHO-1762.
//
// Pins CreateTenantAddonCheckoutSession's switch from mode=payment to
// mode=subscription when a Stripe Price ID is resolvable via the
// PriceCatalogue (CHO-1760), and the fail-loud 422 returns when the
// catalogue misses or returns a placeholder.
package grpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgrpc "github.com/apollo-chora/chora-payments/internal/adapter/grpc"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// spyStripeClient captures the input passed to CreateCheckoutSession so
// the gRPC-layer test can assert StripePriceID propagation without a
// live Stripe backend.
type spyStripeClient struct {
	delegate  stripeadapter.Client
	lastInput stripeadapter.CreateCheckoutSessionInput
	called    int
}

func newSpyStripe() *spyStripeClient {
	return &spyStripeClient{delegate: stripeadapter.NewStubClient()}
}

func (s *spyStripeClient) CreateCheckoutSession(ctx context.Context, in stripeadapter.CreateCheckoutSessionInput) (stripeadapter.CreateCheckoutSessionOutput, error) {
	s.called++
	s.lastInput = in
	return s.delegate.CreateCheckoutSession(ctx, in)
}

func (s *spyStripeClient) EnsureCustomer(ctx context.Context, in stripeadapter.EnsureCustomerInput) (stripeadapter.EnsureCustomerOutput, error) {
	return s.delegate.EnsureCustomer(ctx, in)
}

func (s *spyStripeClient) RefundCharge(ctx context.Context, in stripeadapter.RefundChargeInput) (stripeadapter.RefundChargeOutput, error) {
	return s.delegate.RefundCharge(ctx, in)
}

func (s *spyStripeClient) CancelSubscription(ctx context.Context, in stripeadapter.CancelSubscriptionInput) (stripeadapter.CancelSubscriptionOutput, error) {
	return s.delegate.CancelSubscription(ctx, in)
}

func (s *spyStripeClient) UpdateSubscriptionPrice(ctx context.Context, in stripeadapter.UpdateSubscriptionPriceInput) (stripeadapter.UpdateSubscriptionPriceOutput, error) {
	return s.delegate.UpdateSubscriptionPrice(ctx, in)
}

func (s *spyStripeClient) PreviewSubscriptionPriceChange(ctx context.Context, in stripeadapter.PreviewSubscriptionPriceChangeInput) (stripeadapter.PreviewSubscriptionPriceChangeOutput, error) {
	return s.delegate.PreviewSubscriptionPriceChange(ctx, in)
}

func (s *spyStripeClient) ListInvoices(ctx context.Context, in stripeadapter.ListInvoicesInput) (stripeadapter.ListInvoicesOutput, error) {
	return s.delegate.ListInvoices(ctx, in)
}

func (s *spyStripeClient) CreateBillingPortalSession(ctx context.Context, in stripeadapter.CreateBillingPortalSessionInput) (stripeadapter.CreateBillingPortalSessionOutput, error) {
	return s.delegate.CreateBillingPortalSession(ctx, in)
}

func (s *spyStripeClient) ScheduleSubscriptionPriceChange(ctx context.Context, in stripeadapter.ScheduleSubscriptionPriceChangeInput) (stripeadapter.ScheduleSubscriptionPriceChangeOutput, error) {
	return s.delegate.ScheduleSubscriptionPriceChange(ctx, in)
}

func (s *spyStripeClient) GetPrice(ctx context.Context, priceID string) (stripeadapter.GetPriceOutput, error) {
	return s.delegate.GetPrice(ctx, priceID)
}

func (s *spyStripeClient) GetSubscription(ctx context.Context, subID string) (stripeadapter.GetSubscriptionOutput, error) {
	return s.delegate.GetSubscription(ctx, subID)
}

// fakeCatalogue satisfies the unexported stripeCataloguePort via the
// PriceCatalogue field on pgrpc.Deps.
type fakeCatalogue struct {
	priceIDs map[string]string // "addon:tier:currency" → price_id
}

func (f *fakeCatalogue) Resolve(addonCode, tierCode, currency string) (string, bool) {
	v, ok := f.priceIDs[strings.ToLower(addonCode+":"+tierCode+":"+currency)]
	return v, ok
}

func newTAPServerWithCatalogue(t *testing.T, cat *fakeCatalogue) (*pgrpc.Server, *spyStripeClient) {
	t.Helper()
	spy := newSpyStripe()
	deps := pgrpc.Deps{
		AddonPurchase:     inmem.NewTenantAddonPurchaseRepo(),
		StripeCustomers:   inmem.NewStripeCustomerRepo(),
		Stripe:            spy,
		DefaultSuccessURL: "https://chora.site/payment/success",
		DefaultCancelURL:  "https://chora.site/payment/cancel",
		Now:               func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) },
	}
	// Typed-nil avoidance: only assign the catalogue field when the
	// fake is non-nil. Passing a typed-nil *fakeCatalogue into the
	// interface slot would make the != nil guard see a non-nil
	// interface wrapping a nil pointer + panic on Resolve.
	if cat != nil {
		deps.PriceCatalogue = cat
	}
	return pgrpc.New(deps), spy
}

const (
	tapTestTenantID    = "11111111-1111-7111-8111-111111111111"
	tapTestAdminGCID   = "00000000-0000-7000-8000-000000001999"
	tapTestAddonPlanID = "0190bbbb-0000-7000-8000-000000000002"
)

func mkTAPReq() *pb.CreateTenantAddonCheckoutSessionRequest {
	return &pb.CreateTenantAddonCheckoutSessionRequest{
		IdempotencyKey: "key-1",
		TenantId:       tapTestTenantID,
		AdminGcid:      tapTestAdminGCID,
		AddonPlanId:    tapTestAddonPlanID,
		AddonCode:      "tms",
		TierCode:       "pro",
		AmountCents:    9900,
		Currency:       "USD",
		SuccessUrl:     "https://chora.site/h/marketplace/x?checkout=success",
		CancelUrl:      "https://chora.site/h/marketplace/x?checkout=cancel",
	}
}

// Resolved real Price ID → the Stripe call receives StripePriceID +
// switches to mode=subscription downstream (adapter test pins this).
func TestCreateTenantAddonCheckoutSession_PriceResolved_PassesStripePriceID(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalogue{priceIDs: map[string]string{
		"tms:pro:usd": "price_real_1Tabcde",
	}}
	srv, spy := newTAPServerWithCatalogue(t, cat)
	resp, err := srv.CreateTenantAddonCheckoutSession(context.Background(), mkTAPReq())
	if err != nil {
		t.Fatalf("expected success; got %v", err)
	}
	if resp.PurchaseId == "" {
		t.Fatalf("PurchaseId empty")
	}
	if spy.called != 1 {
		t.Fatalf("Stripe CreateCheckoutSession called %d times, want 1", spy.called)
	}
	if got := spy.lastInput.StripePriceID; got != "price_real_1Tabcde" {
		t.Errorf("Stripe input StripePriceID = %q, want price_real_1Tabcde", got)
	}
	if spy.lastInput.PurchaseType != stripeadapter.PurchaseTypeTenantAddonPurchase {
		t.Errorf("PurchaseType = %v, want TenantAddonPurchase", spy.lastInput.PurchaseType)
	}
}

// Catalogue misses → InvalidArgument with stripe_price_missing in the
// status message. The FE surfaces this as a 422 banner explaining the
// addon's Stripe Price hasn't been configured in this environment.
func TestCreateTenantAddonCheckoutSession_PriceMissing_ReturnsInvalidArgument(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalogue{priceIDs: map[string]string{
		"tms:starter:usd": "price_real_1Tstarter",
		// pro intentionally absent
	}}
	srv, spy := newTAPServerWithCatalogue(t, cat)
	_, err := srv.CreateTenantAddonCheckoutSession(context.Background(), mkTAPReq())
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("status code = %v, want InvalidArgument", st.Code())
	}
	if !strings.Contains(st.Message(), "stripe_price_missing") {
		t.Errorf("status message missing stripe_price_missing: %q", st.Message())
	}
	if spy.called != 0 {
		t.Errorf("expected 0 Stripe calls on missing price; got %d", spy.called)
	}
}

// Placeholder Price ID → same 422 path. The placeholder catalogue is
// the fallback when STRIPE_PRICE_CATALOGUE_SECRET_ID is unset; CHO-1762
// MUST reject it before hitting Stripe (Stripe would 400 on a fake ID).
func TestCreateTenantAddonCheckoutSession_PlaceholderPrice_ReturnsInvalidArgument(t *testing.T) {
	t.Parallel()
	cat := &fakeCatalogue{priceIDs: map[string]string{
		"tms:pro:usd": "price_TODO_tms_pro_usd",
	}}
	srv, spy := newTAPServerWithCatalogue(t, cat)
	_, err := srv.CreateTenantAddonCheckoutSession(context.Background(), mkTAPReq())
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("status code = %v, want InvalidArgument", st.Code())
	}
	if !strings.Contains(st.Message(), "stripe_price_missing") {
		t.Errorf("status message missing stripe_price_missing: %q", st.Message())
	}
	if spy.called != 0 {
		t.Errorf("expected 0 Stripe calls on placeholder; got %d", spy.called)
	}
}

// PriceCatalogue NOT wired (nil) → legacy mode=payment fallback. Keeps
// existing CHO-1738 tests + unit-test setups green; main.go logs a WARN
// at boot when the catalogue secret isn't set.
func TestCreateTenantAddonCheckoutSession_NilCatalogue_FallsBackToPaymentMode(t *testing.T) {
	t.Parallel()
	srv, spy := newTAPServerWithCatalogue(t, nil)
	resp, err := srv.CreateTenantAddonCheckoutSession(context.Background(), mkTAPReq())
	if err != nil {
		t.Fatalf("expected success; got %v", err)
	}
	if resp.PurchaseId == "" {
		t.Fatalf("PurchaseId empty")
	}
	if spy.lastInput.StripePriceID != "" {
		t.Errorf("StripePriceID should be empty in fallback path; got %q", spy.lastInput.StripePriceID)
	}
}

// PriceCatalogue is currency-case-tolerant (CHO-1760 normalises lower).
// gRPC layer passes whatever the FE sent; ensure mixed-case "USD"
// resolves the same as "usd".
func TestCreateTenantAddonCheckoutSession_MixedCaseCurrency_ResolvesPrice(t *testing.T) {
	t.Parallel()
	// CHO-1760 lowercases the currency portion of catalogue keys; the
	// gRPC layer forwards whatever the FE sent (here ISO-uppercase "USD"
	// — the chora-payments domain rejects anything else). The catalogue
	// normalisation closes the loop so this resolves.
	cat := &fakeCatalogue{priceIDs: map[string]string{
		"tms:pro:usd": "price_real_1Tabcde",
	}}
	srv, spy := newTAPServerWithCatalogue(t, cat)
	req := mkTAPReq() // already Currency=USD
	if _, err := srv.CreateTenantAddonCheckoutSession(context.Background(), req); err != nil {
		t.Fatalf("expected success; got %v", err)
	}
	if got := spy.lastInput.StripePriceID; got != "price_real_1Tabcde" {
		t.Errorf("expected price_real_1Tabcde with ISO-uppercase currency; got %q", got)
	}
}
