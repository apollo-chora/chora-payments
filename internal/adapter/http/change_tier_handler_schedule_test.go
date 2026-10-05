// change_tier_handler_schedule_test.go — CHO-1772.
//
// Pins the end-of-cycle dispatch path: when `effective_at=end_of_cycle`,
// the handler MUST call Stripe SubscriptionSchedule (NOT
// Subscription.update), persist the pending schedule onto the row
// without mutating the current TierCode, and return the schedule_id +
// scheduled_effective_at in the response.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
)

// spyStripeWithSchedule extends spyStripe to also record
// ScheduleSubscriptionPriceChange calls.
type spyStripeWithSchedule struct {
	*spyStripe
	schedInput stripeadapter.ScheduleSubscriptionPriceChangeInput
	schedCalls int
}

func newSpyStripeWithSchedule() *spyStripeWithSchedule {
	return &spyStripeWithSchedule{spyStripe: newSpyStripe()}
}

func (s *spyStripeWithSchedule) ScheduleSubscriptionPriceChange(ctx context.Context, in stripeadapter.ScheduleSubscriptionPriceChangeInput) (stripeadapter.ScheduleSubscriptionPriceChangeOutput, error) {
	s.schedCalls++
	s.schedInput = in
	return s.Client.ScheduleSubscriptionPriceChange(ctx, in)
}

func ctBodyWithEffectiveAt(target, effectiveAt string) *strings.Reader {
	body := `{"tenant_id":"` + ctTestTenantID +
		`","admin_gcid":"` + ctTestAdminGCID +
		`","addon_code":"tms","target_tier_code":"` + target +
		`","currency":"USD","proration_mode":"create_prorations","effective_at":"` + effectiveAt + `"}`
	return strings.NewReader(body)
}

// Happy path: effective_at=end_of_cycle → ScheduleSubscriptionPriceChange
// called with StartDate=row.CurrentPeriodEnd; UpdateSubscriptionPrice
// NOT called; row TierCode unchanged; schedule fields persisted.
func TestChangeTier_EndOfCycle_CallsScheduleAndDefersTierChange(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	// Fetch the row + grab its CurrentPeriodEnd so we can assert it
	// flows to Stripe as the schedule StartDate.
	row, err := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if err != nil {
		t.Fatalf("seed GetByID: %v", err)
	}
	if row.CurrentPeriodEnd == nil {
		t.Fatal("seed CurrentPeriodEnd nil; check seedActiveTAPRow")
	}
	wantStart := *row.CurrentPeriodEnd

	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripeWithSchedule()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:change-tier",
		ctBodyWithEffectiveAt("pro", "end_of_cycle"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spy.schedCalls != 1 {
		t.Fatalf("ScheduleSubscriptionPriceChange called %d times, want 1", spy.schedCalls)
	}
	if spy.upCalls != 0 {
		t.Errorf("end_of_cycle MUST NOT call UpdateSubscriptionPrice; got %d calls", spy.upCalls)
	}
	if spy.schedInput.StripeSubscriptionID != "sub_test" {
		t.Errorf("Stripe SubscriptionID = %q, want sub_test", spy.schedInput.StripeSubscriptionID)
	}
	if spy.schedInput.NewPriceID != "price_real_pro" {
		t.Errorf("Stripe NewPriceID = %q, want price_real_pro", spy.schedInput.NewPriceID)
	}
	if !spy.schedInput.StartDate.Equal(wantStart) {
		t.Errorf("Stripe StartDate = %v, want %v (row CurrentPeriodEnd)", spy.schedInput.StartDate, wantStart)
	}

	stored, err := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if err != nil {
		t.Fatalf("post-handler GetByID: %v", err)
	}
	if stored.TierCode != "starter" {
		t.Errorf("end_of_cycle MUST NOT mutate TierCode; got %q, want starter", stored.TierCode)
	}
	if stored.ScheduledTierCode != "pro" {
		t.Errorf("ScheduledTierCode = %q, want pro", stored.ScheduledTierCode)
	}
	if stored.StripeSubscriptionScheduleID == "" {
		t.Error("StripeSubscriptionScheduleID empty after end_of_cycle dispatch")
	}
	if stored.ScheduledEffectiveAt == nil || !stored.ScheduledEffectiveAt.Equal(wantStart) {
		t.Errorf("ScheduledEffectiveAt = %v, want %v", stored.ScheduledEffectiveAt, wantStart)
	}
}

// Response shape: schedule path returns schedule_id + scheduled_effective_at
// + billing_delta_cents=0 (no proration when the change waits for cycle end).
func TestChangeTier_EndOfCycle_ResponseCarriesScheduleFields(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripeWithSchedule()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:change-tier",
		ctBodyWithEffectiveAt("pro", "end_of_cycle"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"deferred_to_cycle_end":true`) {
		t.Errorf("response missing deferred_to_cycle_end=true: %s", body)
	}
	if !strings.Contains(body, `"schedule_id"`) {
		t.Errorf("response missing schedule_id field: %s", body)
	}
	if !strings.Contains(body, `"scheduled_tier_code":"pro"`) {
		t.Errorf("response missing scheduled_tier_code=pro: %s", body)
	}
	if !strings.Contains(body, `"billing_delta_cents":0`) {
		t.Errorf("end_of_cycle response should report billing_delta_cents=0; got: %s", body)
	}
	// from_tier reflects CURRENT paid-for tier (still starter, not yet promoted).
	if !strings.Contains(body, `"from_tier":"starter"`) {
		t.Errorf("response from_tier should be starter, got: %s", body)
	}
	if !strings.Contains(body, `"to_tier":"pro"`) {
		t.Errorf("response to_tier should be pro, got: %s", body)
	}
}

// effective_at=immediate explicitly takes the existing CHO-1764 path.
func TestChangeTier_EffectiveAtImmediate_BehavesLikeNoEffectiveAt(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripeWithSchedule()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:change-tier",
		ctBodyWithEffectiveAt("pro", "immediate"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spy.upCalls != 1 {
		t.Errorf("effective_at=immediate should call UpdateSubscriptionPrice once; got %d", spy.upCalls)
	}
	if spy.schedCalls != 0 {
		t.Errorf("effective_at=immediate must NOT call ScheduleSubscriptionPriceChange; got %d", spy.schedCalls)
	}
	stored, _ := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	if stored.TierCode != "pro" {
		t.Errorf("immediate path should promote TierCode to pro; got %q", stored.TierCode)
	}
}

// Invalid effective_at returns 400.
func TestChangeTier_InvalidEffectiveAt_Returns400(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripeWithSchedule()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:change-tier",
		ctBodyWithEffectiveAt("pro", "next_quarter"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400 for invalid effective_at; body=%s", rec.Code, rec.Body.String())
	}
	if spy.upCalls != 0 || spy.schedCalls != 0 {
		t.Errorf("no Stripe call should fire for invalid effective_at; up=%d sched=%d", spy.upCalls, spy.schedCalls)
	}
}

// end_of_cycle on a row missing CurrentPeriodEnd falls back to
// Stripe.GetSubscription so the change still succeeds (rows created
// before the CHO-1763 customer.subscription.created dispatcher landed
// have nil anchors; we shouldn't 422 the user). The stub returns
// now+30d so the schedule lands with a sane StartDate.
func TestChangeTier_EndOfCycle_NoCurrentPeriodEnd_FallsBackToStripeGetSubscription(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	row, _ := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	row.CurrentPeriodEnd = nil
	if err := repo.Save(context.Background(), row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripeWithSchedule()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:change-tier",
		ctBodyWithEffectiveAt("pro", "end_of_cycle"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spy.schedCalls != 1 {
		t.Errorf("ScheduleSubscriptionPriceChange called %d times, want 1", spy.schedCalls)
	}
	// StartDate must be in the future — the stub returns now+30d for
	// GetSubscription.CurrentPeriodEnd, which the handler uses as the
	// schedule anchor.
	if !spy.schedInput.StartDate.After(time.Now().Add(20 * 24 * time.Hour)) {
		t.Errorf("Stripe StartDate = %v, want > 20d ahead of now (from GetSubscription stub)",
			spy.schedInput.StartDate)
	}
}

// Belt + braces: even with a 30-day-stale "now", the schedule StartDate
// is read from row.CurrentPeriodEnd not h.deps.Now().
func TestChangeTier_EndOfCycle_StartDateFromRowNotNow(t *testing.T) {
	t.Parallel()
	repo := seedActiveTAPRow(t, "starter")
	row, _ := repo.GetByID(context.Background(), ctTestTenantID, ctTestPurchaseID)
	// Force a known period_end far from any test "now".
	stamped := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	row.CurrentPeriodEnd = &stamped
	_ = repo.Save(context.Background(), row)

	cat := &fakeCatalogue{m: map[string]string{"tms:pro:usd": "price_real_pro"}}
	spy := newSpyStripeWithSchedule()
	srv := newChangeTierServer(t, repo, cat, spy)

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/tenant-addons:change-tier",
		ctBodyWithEffectiveAt("pro", "end_of_cycle"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !spy.schedInput.StartDate.Equal(stamped) {
		t.Errorf("StartDate = %v, want %v (row.CurrentPeriodEnd)", spy.schedInput.StartDate, stamped)
	}
}
