// preview_tier_handler_next_invoice_test.go — CHO-1789 follow-up.
//
// Pins next_invoice_total_cents to the NEW tier's monthly price, NOT
// invoice.Upcoming's total. With proration_behavior=always_invoice
// Stripe's upcoming returns the proration today-invoice (the credit
// amount on downgrade), so threading it into "Next renewal total"
// renders nonsense like "Next renewal total: SGD 34.03" on a 49→14.90
// downgrade. Use a separate Stripe Price lookup for the next-cycle
// total instead.
package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpadapter "github.com/apollo-chora/chora-payments/internal/adapter/http"
)

// On the immediate path, NextInvoiceTotalCents MUST equal the new tier's
// monthly price (looked up via Stripe GetPrice) regardless of what
// invoice.Upcoming's total returns — Stripe's upcoming on always_invoice
// reports the today-invoice (proration), which is the wrong number for
// the FE "Next renewal total" label.
func TestPreviewTier_Immediate_NextInvoiceTotalEqualsNewTierMonthly(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyPreviewStripeWithSchedule()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:preview-tier-change",
		ptBodyWithEffectiveAt("pro", "immediate"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spy.calls != 1 {
		t.Errorf("immediate path should call PreviewSubscriptionPriceChange once; got %d", spy.calls)
	}
	if spy.priceCalls != 1 {
		t.Errorf("immediate path MUST call GetPrice once for the new tier monthly (CHO-1789); got %d", spy.priceCalls)
	}
	if spy.priceInput != "price_real_pro" {
		t.Errorf("GetPrice called with %q, want price_real_pro", spy.priceInput)
	}

	var resp httpadapter.PreviewTierResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.NextInvoiceTotalCents != 9900 {
		t.Errorf("NextInvoiceTotalCents=%d, want 9900 (new tier monthly from GetPrice stub) — surfacing invoice.Upcoming.total leaks the always_invoice proration into the next-renewal label",
			resp.NextInvoiceTotalCents)
	}
}
