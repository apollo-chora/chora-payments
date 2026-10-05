// preview_tier_handler_schedule_test.go — CHO-1772.
//
// Pins the end-of-cycle preview path: when `effective_at=end_of_cycle`,
// the handler MUST return billing_delta_cents=0 + next_invoice_total_cents
// pulled from Stripe.GetPrice on the target tier's Price ID, WITHOUT
// calling PreviewSubscriptionPriceChange (which simulates proration).
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpadapter "github.com/apollo-chora/chora-payments/internal/adapter/http"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
)

// spyPreviewStripeWithSchedule extends spyPreviewStripe to also record
// GetPrice calls (used by the end-of-cycle preview path).
type spyPreviewStripeWithSchedule struct {
	*spyPreviewStripe
	priceInput string
	priceCalls int
	priceOut   stripeadapter.GetPriceOutput
}

func newSpyPreviewStripeWithSchedule() *spyPreviewStripeWithSchedule {
	return &spyPreviewStripeWithSchedule{spyPreviewStripe: newSpyPreviewStripe()}
}

func (s *spyPreviewStripeWithSchedule) GetPrice(ctx context.Context, priceID string) (stripeadapter.GetPriceOutput, error) {
	s.priceCalls++
	s.priceInput = priceID
	// Use a fixed amount for assertable test values.
	s.priceOut = stripeadapter.GetPriceOutput{UnitAmountCents: 9900, Currency: "usd"}
	return s.priceOut, nil
}

func ptBodyWithEffectiveAt(target, effectiveAt string) *strings.Reader {
	body := `{"tenant_id":"` + ctTestTenantID +
		`","addon_code":"tms","target_tier_code":"` + target +
		`","currency":"USD","proration_mode":"create_prorations","effective_at":"` + effectiveAt + `"}`
	return strings.NewReader(body)
}

func TestPreviewTier_EndOfCycle_ReturnsZeroDeltaAndNewTierMonthly(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	row, _ := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if row.CurrentPeriodEnd == nil {
		t.Fatal("seed CurrentPeriodEnd nil")
	}
	wantEffective := *row.CurrentPeriodEnd

	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyPreviewStripeWithSchedule()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:preview-tier-change",
		ptBodyWithEffectiveAt("pro", "end_of_cycle"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	// PreviewSubscriptionPriceChange MUST NOT be called — that's the
	// proration-style simulation, irrelevant on the cycle-end path.
	if spy.calls != 0 {
		t.Errorf("end_of_cycle preview should NOT call PreviewSubscriptionPriceChange; got %d", spy.calls)
	}
	if spy.priceCalls != 1 {
		t.Errorf("end_of_cycle preview should call GetPrice once; got %d", spy.priceCalls)
	}
	if spy.priceInput != "price_real_pro" {
		t.Errorf("GetPrice called with %q, want price_real_pro", spy.priceInput)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"billing_delta_cents":0`) {
		t.Errorf("expected billing_delta_cents=0 on end_of_cycle, got: %s", body)
	}
	if !strings.Contains(body, `"next_invoice_total_cents":9900`) {
		t.Errorf("expected next_invoice_total_cents=9900 (from GetPrice stub), got: %s", body)
	}
	if !strings.Contains(body, `"deferred_to_cycle_end":true`) {
		t.Errorf("expected deferred_to_cycle_end=true, got: %s", body)
	}

	var resp httpadapter.PreviewTierResponse
	_ = json.NewDecoder(strings.NewReader(body)).Decode(&resp)
	if resp.FromTier != "starter" || resp.ToTier != "pro" {
		t.Errorf("tiers wrong: from=%q to=%q", resp.FromTier, resp.ToTier)
	}
	// EffectiveAt field is the cycle anchor.
	if resp.EffectiveAt == "" {
		t.Errorf("EffectiveAt empty; expected ISO timestamp from row.CurrentPeriodEnd")
	}
	if !strings.Contains(body, wantEffective.Format("2006-01-02")) {
		t.Errorf("EffectiveAt should include row CurrentPeriodEnd date (%s); body: %s",
			wantEffective.Format("2006-01-02"), body)
	}
}

func TestPreviewTier_EndOfCycle_RowMutation_NotMutated(t *testing.T) {
	// Preview is read-only — verify even the end_of_cycle path doesn't
	// stash a schedule on the row (only ChangeTier does that).
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyPreviewStripeWithSchedule()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:preview-tier-change",
		ptBodyWithEffectiveAt("pro", "end_of_cycle"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}

	stored, _ := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if stored.TierCode != "starter" {
		t.Errorf("preview mutated TierCode: %q", stored.TierCode)
	}
	if stored.StripeSubscriptionScheduleID != "" {
		t.Errorf("preview must not persist schedule_id; got %q", stored.StripeSubscriptionScheduleID)
	}
	if stored.ScheduledTierCode != "" {
		t.Errorf("preview must not persist scheduled_tier_code; got %q", stored.ScheduledTierCode)
	}
}

func TestPreviewTier_EffectiveAtImmediate_UsesPrevSubscriptionPriceChange(t *testing.T) {
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
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if spy.calls != 1 {
		t.Errorf("effective_at=immediate should call PreviewSubscriptionPriceChange once; got %d", spy.calls)
	}
	// CHO-1789 follow-up — the immediate path now ALSO calls GetPrice so
	// the "Next renewal total" label gets the new tier monthly instead
	// of Stripe's always_invoice quirk (today-invoice total).
	if spy.priceCalls != 1 {
		t.Errorf("effective_at=immediate should call GetPrice once for next-renewal total (CHO-1789); got %d", spy.priceCalls)
	}
}

func TestPreviewTier_InvalidEffectiveAt_Returns400(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyPreviewStripeWithSchedule()
	srv := newPreviewServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:preview-tier-change",
		ptBodyWithEffectiveAt("pro", "next_quarter"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}
