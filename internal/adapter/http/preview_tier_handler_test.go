// preview_tier_handler_test.go — CHO-1765.
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
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// spyPreviewStripe records the input passed to PreviewSubscriptionPriceChange.
type spyPreviewStripe struct {
	stripeadapter.Client
	in    stripeadapter.PreviewSubscriptionPriceChangeInput
	calls int
}

func newSpyPreviewStripe() *spyPreviewStripe {
	return &spyPreviewStripe{Client: stripeadapter.NewStubClient()}
}

func (s *spyPreviewStripe) PreviewSubscriptionPriceChange(ctx context.Context, in stripeadapter.PreviewSubscriptionPriceChangeInput) (stripeadapter.PreviewSubscriptionPriceChangeOutput, error) {
	s.calls++
	s.in = in
	return s.Client.PreviewSubscriptionPriceChange(ctx, in)
}

func newPreviewServer(t *testing.T, repo *inmem.TenantAddonPurchaseRepo, cat stripe_catalogue.PriceCatalogue, stripe stripeadapter.Client) http.Handler {
	t.Helper()
	return httpadapter.NewPreviewTierHandler(httpadapter.PreviewTierHandlerDeps{
		AddonPurchase:  repo,
		Stripe:         stripe,
		PriceCatalogue: cat,
		Now:            func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) },
	})
}

func ptBody(target string) *bytes.Buffer {
	b, _ := json.Marshal(httpadapter.PreviewTierRequest{
		TenantID:       ctTestTenantID,
		AddonCode:      "tms",
		TargetTierCode: target,
		Currency:       "USD",
		ProrationMode:  "create_prorations",
	})
	return bytes.NewBuffer(b)
}

// Happy path: Pro → Enterprise preview returns the synthesised delta
// from the stub + the new-tier line item; row is NOT mutated.
func TestPreviewTier_HappyPath_UpgradeReturnsDelta(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spy.calls != 1 {
		t.Fatalf("Stripe PreviewSubscriptionPriceChange called %d times, want 1", spy.calls)
	}
	if spy.in.NewPriceID != "price_real_enterprise" {
		t.Errorf("Stripe NewPriceID = %q, want price_real_enterprise", spy.in.NewPriceID)
	}

	var resp httpadapter.PreviewTierResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.FromTier != "pro" || resp.ToTier != "enterprise" {
		t.Errorf("tiers wrong: from=%q to=%q", resp.FromTier, resp.ToTier)
	}
	if resp.BillingDeltaCents <= 0 {
		t.Errorf("expected positive delta on create_prorations upgrade; got %d", resp.BillingDeltaCents)
	}
	if resp.NextInvoiceTotalCents <= 0 {
		t.Errorf("expected positive NextInvoiceTotalCents; got %d", resp.NextInvoiceTotalCents)
	}

	// Row must be unmutated — preview is read-only.
	stored, _ := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if stored.TierCode != "pro" {
		t.Errorf("row mutated by preview: TierCode = %q, want pro", stored.TierCode)
	}
}

// Same-tier no-op preview returns zero delta synchronously, NO Stripe call.
func TestPreviewTier_SameTier_ReturnsZeroDelta_NoStripeCall(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("pro"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if spy.calls != 0 {
		t.Errorf("Stripe must not be called on same-tier preview; got %d", spy.calls)
	}
	var resp httpadapter.PreviewTierResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.BillingDeltaCents != 0 || resp.NextInvoiceTotalCents != 0 {
		t.Errorf("expected zero delta on same-tier; got delta=%d next=%d", resp.BillingDeltaCents, resp.NextInvoiceTotalCents)
	}
}

// Legacy row (no stripe_subscription_id) → 422.
func TestPreviewTier_NoStripeSubscription_Returns422(t *testing.T) {
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
	_ = repo.Save(context.Background(), a)
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no_stripe_subscription") {
		t.Errorf("missing no_stripe_subscription: %s", rec.Body.String())
	}
	if spy.calls != 0 {
		t.Errorf("Stripe must not be called on legacy row; got %d", spy.calls)
	}
}

// Catalogue miss → 422.
func TestPreviewTier_PriceMissing_Returns422(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", rec.Code)
	}
	if spy.calls != 0 {
		t.Errorf("Stripe must not be called on missing Price; got %d", spy.calls)
	}
}

// Placeholder Price → 422.
func TestPreviewTier_PlaceholderPrice_Returns422(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_TODO_tms_enterprise_usd"}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", rec.Code)
	}
	if spy.calls != 0 {
		t.Errorf("Stripe must not be called on placeholder; got %d", spy.calls)
	}
}

// Empty repo → 404.
func TestPreviewTier_AddonNotFound_Returns404(t *testing.T) {
	t.Parallel()
	repo := inmem.NewTenantAddonPurchaseRepo()
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rec.Code)
	}
	if spy.calls != 0 {
		t.Errorf("Stripe must not be called; got %d", spy.calls)
	}
}

// Method-not-allowed on GET.
func TestPreviewTier_RejectsNonPostMethod(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	srv := newPreviewServer(t, repo, cat, newSpyPreviewStripe())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenant-addons:preview-tier-change", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d, want 405", rec.Code)
	}
}

// Response shape stable.
func TestPreviewTier_ResponseShape_IsStable(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "pro")
	cat := &fakeCatalogue{m: map[string]string{"tms:enterprise:usd": "price_real_enterprise"}}
	srv := newPreviewServer(t, repo, cat, newSpyPreviewStripe())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", ptBody("enterprise"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var got map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&got)
	for _, key := range []string{
		"purchase_id", "from_tier", "to_tier", "billing_delta_cents",
		"next_invoice_total_cents", "currency", "proration_mode",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("response missing key %q: %v", key, got)
		}
	}
}

// CHO-1786 — preview MUST accept always_invoice + forward it to Stripe so
// the upcoming-invoice forecast accounts for the immediate-bill flow
// (proration items move from the upcoming invoice to a separate one
// today). Without this gate widening, the FE 400s on preview before the
// user even gets to confirm.
func TestPreviewTier_AcceptsAlwaysInvoiceProrationMode(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyPreviewStripe()
	srv := newPreviewServer(t, repo, cat, spy)

	body := httpadapter.PreviewTierRequest{
		TenantID: ctTestTenantID, AddonCode: "tms",
		TargetTierCode: "pro", Currency: "USD",
		ProrationMode: "always_invoice",
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenant-addons:preview-tier-change", bytes.NewBuffer(b))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 (always_invoice MUST be accepted); body=%s",
			rec.Code, rec.Body.String())
	}
	if spy.in.ProrationBehavior != "always_invoice" {
		t.Errorf("Stripe ProrationBehavior = %q; want always_invoice", spy.in.ProrationBehavior)
	}
}
