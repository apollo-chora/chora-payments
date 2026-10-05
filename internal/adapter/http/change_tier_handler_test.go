// change_tier_handler_test.go — CHO-1764. Pins the H+ Marketplace
// change-tier flow's BE wire shape.
package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-payments/internal/adapter/http"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	"github.com/apollo-chora/chora-payments/internal/adapter/stripe_catalogue"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

const (
	ctTestTenantID    = "11111111-1111-7111-8111-111111111111"
	ctTestAdminGCID   = "00000000-0000-7000-8000-000000001999"
	ctTestAddonPlanID = "0190bbbb-0000-7000-8000-000000000002"
	ctTestPurchaseID  = "0190cccc-0000-7000-8000-000000000003"
	ctTestSessionID   = "cs_test_change_tier"
)

// spyStripe records the input passed to UpdateSubscriptionPrice so
// tests can assert wire propagation.
type spyStripe struct {
	stripeadapter.Client
	upInput stripeadapter.UpdateSubscriptionPriceInput
	upCalls int
}

func newSpyStripe() *spyStripe {
	return &spyStripe{Client: stripeadapter.NewStubClient()}
}

func (s *spyStripe) UpdateSubscriptionPrice(ctx context.Context, in stripeadapter.UpdateSubscriptionPriceInput) (stripeadapter.UpdateSubscriptionPriceOutput, error) {
	s.upCalls++
	s.upInput = in
	return s.Client.UpdateSubscriptionPrice(ctx, in)
}

type fakeCatalogue struct {
	m map[string]string
}

func (f *fakeCatalogue) Resolve(addon, tier, currency string) (string, bool) {
	v, ok := f.m[strings.ToLower(addon+":"+tier+":"+currency)]
	return v, ok
}

// seedActiveTAPRow returns a repo + a TAP row in payment_captured state
// with a Stripe Subscription ID attached (the post-CHO-1763 webhook
// state). The change-tier handler operates on this shape.
func seedActiveTAPRow(t *testing.T, fromTier string) *inmem.TenantAddonPurchaseRepo {
	t.Helper()
	repo := inmem.NewTenantAddonPurchaseRepo()
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	a, err := tap.New(
		ctTestPurchaseID, ctTestTenantID, ctTestAdminGCID,
		ctTestAddonPlanID, "tms", fromTier,
		4900, "USD",
		ctTestSessionID, "https://checkout.stripe.com/c/pay/cs_test",
		now,
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	// Drive the state machine to payment_captured (mirrors what the
	// dispatcher does in handleCheckoutSessionCompleted).
	if err := a.MarkPaymentCaptured("pi_test", "ch_test", 4900, now); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	periodStart := now
	periodEnd := now.AddDate(0, 1, 0)
	if err := a.ApplyStripeSubscriptionState(
		"cus_test", "sub_test", tap.StatusActive,
		&periodStart, &periodEnd,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return repo
}

func newChangeTierServer(t *testing.T, repo *inmem.TenantAddonPurchaseRepo, cat stripe_catalogue.PriceCatalogue, stripe stripeadapter.Client) http.Handler {
	t.Helper()
	return httpadapter.NewChangeTierHandler(httpadapter.ChangeTierHandlerDeps{
		AddonPurchase:  repo,
		Stripe:         stripe,
		PriceCatalogue: cat,
		Now:            func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) },
	})
}

func ctBody(target string) *bytes.Buffer {
	b, _ := json.Marshal(httpadapter.ChangeTierRequest{
		TenantID:       ctTestTenantID,
		AdminGCID:      ctTestAdminGCID,
		AddonCode:      "tms",
		TargetTierCode: target,
		Currency:       "USD",
		ProrationMode:  "create_prorations",
	})
	return bytes.NewBuffer(b)
}

func ctDecode(t *testing.T, rec *httptest.ResponseRecorder) httpadapter.ChangeTierResponse {
	t.Helper()
	var resp httpadapter.ChangeTierResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

// Happy path: Pro → Enterprise tier change, Stripe called with the
// resolved Price ID, row persisted with new tier, response carries the
// proration delta from the stub.
func TestChangeTier_HappyPath_UpgradeFromProToEnterprise(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spy.upCalls != 1 {
		t.Fatalf("Stripe UpdateSubscriptionPrice called %d times, want 1", spy.upCalls)
	}
	if got := spy.upInput.NewPriceID; got != "price_real_enterprise" {
		t.Errorf("Stripe NewPriceID = %q, want price_real_enterprise", got)
	}
	if spy.upInput.StripeSubscriptionID != "sub_test" {
		t.Errorf("Stripe SubscriptionID = %q, want sub_test", spy.upInput.StripeSubscriptionID)
	}
	resp := ctDecode(t, rec)
	if resp.FromTier != "pro" || resp.ToTier != "enterprise" {
		t.Errorf("response tiers wrong: from=%q to=%q", resp.FromTier, resp.ToTier)
	}
	if resp.StripeSubscriptionID != "sub_test" {
		t.Errorf("StripeSubscriptionID dropped: %q", resp.StripeSubscriptionID)
	}
	if resp.BillingDeltaCents == 0 {
		t.Errorf("expected non-zero billing delta from stub on create_prorations; got 0")
	}

	stored, err := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if stored.TierCode != "enterprise" {
		t.Errorf("row TierCode not persisted; got %q", stored.TierCode)
	}
}

// Legacy row (no stripe_subscription_id) → 422 no_stripe_subscription;
// no Stripe call.
func TestChangeTier_NoStripeSubscription_Returns422(t *testing.T) {
	t.Parallel()
	repo := inmem.NewTenantAddonPurchaseRepo()
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	a, _ := tap.New(
		ctTestPurchaseID, ctTestTenantID, ctTestAdminGCID,
		ctTestAddonPlanID, "tms", "pro",
		4900, "USD",
		ctTestSessionID, "url",
		now,
	)
	_ = a.MarkPaymentCaptured("pi", "ch", 4900, now)
	// No ApplyStripeSubscriptionState — legacy pre-CHO-1762 row.
	_ = repo.Save(context.Background(), a)
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "no_stripe_subscription") {
		t.Errorf("response missing no_stripe_subscription: %s", rec.Body.String())
	}
	if spy.upCalls != 0 {
		t.Errorf("Stripe must not be called on legacy row; got %d", spy.upCalls)
	}
}

// Missing Price ID in catalogue → 422 stripe_price_missing; no Stripe call.
func TestChangeTier_PriceMissing_Returns422(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "stripe_price_missing") {
		t.Errorf("response missing stripe_price_missing: %s", rec.Body.String())
	}
	if spy.upCalls != 0 {
		t.Errorf("Stripe must not be called on missing Price; got %d", spy.upCalls)
	}
}

// Placeholder Price ID (CHO-1760 fallback) → 422 same path.
func TestChangeTier_PlaceholderPrice_Returns422(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_TODO_tms_enterprise_usd"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", rec.Code)
	}
	if spy.upCalls != 0 {
		t.Errorf("Stripe must not be called on placeholder; got %d", spy.upCalls)
	}
}

// Same-tier no-op → 409 tier_unchanged; no Stripe call.
func TestChangeTier_SameTier_Returns409(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("pro"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d, want 409", rec.Code)
	}
	if spy.upCalls != 0 {
		t.Errorf("Stripe must not be called on same-tier; got %d", spy.upCalls)
	}
}

// No row in repo for (tenant, addon) → 404 addon_not_found.
func TestChangeTier_AddonNotFound_Returns404(t *testing.T) {
	t.Parallel()
	repo := inmem.NewTenantAddonPurchaseRepo() // empty
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404; body=%s", rec.Code, rec.Body.String())
	}
	if spy.upCalls != 0 {
		t.Errorf("Stripe must not be called on missing row; got %d", spy.upCalls)
	}
}

// Method-not-allowed on GET / PUT / etc.
func TestChangeTier_RejectsNonPostMethod(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	srv := newChangeTierServer(t, repo, cat, newSpyStripe())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenant-addons:change-tier", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d, want 405", rec.Code)
	}
}

// Validation: empty fields → 400.
func TestChangeTier_ValidationErrors(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	srv := newChangeTierServer(t, repo, cat, newSpyStripe())

	cases := []struct {
		name string
		body httpadapter.ChangeTierRequest
	}{
		{"empty_tenant", httpadapter.ChangeTierRequest{AddonCode: "tms", TargetTierCode: "enterprise", Currency: "USD"}},
		{"empty_addon", httpadapter.ChangeTierRequest{TenantID: ctTestTenantID, TargetTierCode: "enterprise", Currency: "USD"}},
		{"empty_target_tier", httpadapter.ChangeTierRequest{TenantID: ctTestTenantID, AddonCode: "tms", Currency: "USD"}},
		{"empty_currency", httpadapter.ChangeTierRequest{TenantID: ctTestTenantID, AddonCode: "tms", TargetTierCode: "enterprise"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, _ := json.Marshal(c.body)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", bytes.NewBuffer(b))
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: status=%d, want 400", c.name, rec.Code)
			}
		})
	}
}

// Regression: the response body shape is stable (FE depends on these keys).
func TestChangeTier_ResponseShape_IsStable(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	srv := newChangeTierServer(t, repo, cat, newSpyStripe())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{
		"purchase_id", "stripe_subscription_id", "from_tier", "to_tier",
		"billing_delta_cents", "currency", "effective_at", "subscription_status",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("response missing key %q: %v", key, got)
		}
	}
}

// Sanity: the not-found repo error path doesn't leak as 500.
func TestChangeTier_NotFoundError_IsNotInternal(t *testing.T) {
	t.Parallel()
	repo := inmem.NewTenantAddonPurchaseRepo()
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	srv := newChangeTierServer(t, repo, cat, newSpyStripe())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", ctBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code >= 500 {
		t.Fatalf("status=%d, want < 500", rec.Code)
	}
	// Verify that the underlying error is in fact ErrNotFound (so
	// future repo refactors don't accidentally bury it).
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), ctTestTenantID, "tms"); err != shared.ErrNotFound {
		t.Errorf("expected repo to return ErrNotFound; got %v", err)
	}
}

// CHO-1786 — the "Immediately" path on /h/addons/<id>/change-tier should
// bill the prorated delta NOW (not defer to next invoice). Switching to
// `always_invoice` makes the FE result card's "Additional charge today"
// label accurate. Validation gate at line ~140 currently 400s anything
// outside {create_prorations, none} — widen to include always_invoice.
func TestChangeTier_AcceptsAlwaysInvoiceProrationMode(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)

	body := httpadapter.ChangeTierRequest{
		TenantID:       ctTestTenantID,
		AddonCode:      "tms",
		TargetTierCode: "pro",
		Currency:       "USD",
		ProrationMode:  "always_invoice",
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", bytes.NewBuffer(b))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 (always_invoice MUST be accepted)", rec.Code)
	}
	if spy.upCalls != 1 {
		t.Errorf("Stripe UpdateSubscriptionPrice calls = %d; want 1", spy.upCalls)
	}
	if spy.upInput.ProrationBehavior != "always_invoice" {
		t.Errorf("Stripe ProrationBehavior = %q; want always_invoice (must forward verbatim)",
			spy.upInput.ProrationBehavior)
	}
}

// Backwards-compat — both legacy values still accepted (each subtest uses
// a fresh repo because the seed row mutates on tier change).
func TestChangeTier_ProrationMode_LegacyCreateProrationsStillAccepted(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripe()
	srv := newChangeTierServer(t, repo, cat, spy)
	body := httpadapter.ChangeTierRequest{
		TenantID: ctTestTenantID, AddonCode: "tms",
		TargetTierCode: "pro", Currency: "USD",
		ProrationMode: "create_prorations",
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:change-tier", bytes.NewBuffer(b))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if spy.upInput.ProrationBehavior != "create_prorations" {
		t.Errorf("ProrationBehavior = %q; want create_prorations", spy.upInput.ProrationBehavior)
	}
}
