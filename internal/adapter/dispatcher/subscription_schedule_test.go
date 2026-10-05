// subscription_schedule_test.go — CHO-1772.
//
// Pins the subscription_schedule.released webhook handler. When Stripe
// fires this event after the deferred-tier-change schedule's second
// phase begins, the dispatcher MUST:
//   - look up the TAP row by stripe_subscription_schedule_id
//   - promote ScheduledTierCode → TierCode via ReleaseSchedule
//   - clear the 3 schedule fields
//   - save
package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	"github.com/apollo-chora/chora-common/tracing"
	inmem "github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const tapTestScheduleID = "sub_sched_test_release"

// seedScheduledTAPRow returns a dispatcher + repo whose only TAP row has
// a pending end-of-cycle schedule attached (current tier = starter,
// scheduled tier = pro).
func seedScheduledTAPRow(t *testing.T) (*Dispatcher, *inmem.TenantAddonPurchaseRepo) {
	t.Helper()
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	tapRepo := inmem.NewTenantAddonPurchaseRepo()
	a, err := tap.New(
		testPurchaseID, testTenantID, testLearnerGCID,
		"0190dddd-0000-7000-8000-000000000002", "tms", "starter",
		4900, "USD",
		tapTestSessionID, "https://checkout.stripe.com/c/pay/cs_test_tap",
		now,
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	periodStart := now
	periodEnd := now.AddDate(0, 1, 0)
	if err := a.ApplyStripeSubscriptionState(tapTestCustomerID, tapTestSubscriptionID, tap.StatusActive, &periodStart, &periodEnd); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := a.ApplySchedule(tapTestScheduleID, "pro", periodEnd); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if err := tapRepo.Save(context.Background(), a); err != nil {
		t.Fatalf("seed tap: %v", err)
	}
	d := New(Deps{
		Course:         inmem.NewCoursePurchaseRepo(),
		Application:    inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:    inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:      inmem.NewTenantManaTopUpRepo(),
		Subscription:   inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:  inmem.NewUserManaTopUpRepo(),
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(),
		AddonPurchase:  tapRepo,
		Dispute:        inmem.NewDisputeRepo(),
		Outbox:         &recordingEmitter{},
		Now:            func() time.Time { return now },
	})
	return d, tapRepo
}

func scheduleReleasedEvent(t *testing.T, scheduleID string) *stripeGo.Event {
	t.Helper()
	body := map[string]any{
		"id":     scheduleID,
		"object": "subscription_schedule",
		"status": "released",
	}
	raw, _ := json.Marshal(body)
	return &stripeGo.Event{
		ID:   "evt_test_sched_released",
		Type: stripeGo.EventTypeSubscriptionScheduleReleased,
		Data: &stripeGo.EventData{Raw: raw},
	}
}

func TestSubscriptionScheduleReleased_PromotesTierAndClearsScheduleFields(t *testing.T) {
	d, tapRepo := seedScheduledTAPRow(t)
	event := scheduleReleasedEvent(t, tapTestScheduleID)

	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateTenantAddonPurchase {
		t.Errorf("agg = %v, want AggregateTenantAddonPurchase", agg)
	}
	if pid != testPurchaseID {
		t.Errorf("purchase_id = %q, want %q", pid, testPurchaseID)
	}

	stored, err := tapRepo.GetByID(tracing.WithTenantID(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if stored.TierCode != "pro" {
		t.Errorf("post-release TierCode = %q, want pro (promoted from ScheduledTierCode)", stored.TierCode)
	}
	if stored.StripeSubscriptionScheduleID != "" {
		t.Errorf("post-release StripeSubscriptionScheduleID = %q, want empty", stored.StripeSubscriptionScheduleID)
	}
	if stored.ScheduledTierCode != "" {
		t.Errorf("post-release ScheduledTierCode = %q, want empty", stored.ScheduledTierCode)
	}
	if stored.ScheduledEffectiveAt != nil {
		t.Errorf("post-release ScheduledEffectiveAt = %v, want nil", stored.ScheduledEffectiveAt)
	}
}

func TestSubscriptionScheduleReleased_UnknownScheduleID_AckOnlySkip(t *testing.T) {
	// Stripe may deliver schedule.released for schedules created via
	// the Dashboard or another tenant's account; we ack-only and don't
	// 500 so Stripe stops retrying. Same shape as
	// handleCustomerSubscriptionUpdated for unknown sub_ids.
	d, _ := seedScheduledTAPRow(t)
	event := scheduleReleasedEvent(t, "sub_sched_test_unknown")

	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("Dispatch unknown schedule should NOT error: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg = %v, want AggregateUnknown for unknown schedule", agg)
	}
	if pid != "" {
		t.Errorf("purchase_id = %q, want empty for unknown schedule", pid)
	}
}

func TestSubscriptionScheduleReleased_DuplicateDelivery_Idempotent(t *testing.T) {
	// Stripe retries the webhook if our response is 5xx. Even on
	// successful first delivery, network glitches can cause a duplicate
	// retry. Second delivery must be a no-op (ReleaseSchedule on a
	// row with no pending schedule returns nil per the domain test).
	d, tapRepo := seedScheduledTAPRow(t)
	event := scheduleReleasedEvent(t, tapTestScheduleID)

	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("first Dispatch: %v", err)
	}
	// Second delivery — row has no schedule_id any more, so the lookup
	// by schedule_id returns ErrNotFound, which the handler treats as
	// ack-only skip (same as unknown).
	agg, _, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Errorf("duplicate Dispatch should NOT error: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("duplicate Dispatch agg = %v, want AggregateUnknown", agg)
	}
	// Tier should still be pro (set by the first delivery).
	stored, _ := tapRepo.GetByID(context.Background(), testTenantID, testPurchaseID)
	if stored.TierCode != "pro" {
		t.Errorf("post-duplicate TierCode = %q, want pro", stored.TierCode)
	}
}

func TestSubscriptionScheduleReleased_EmitsOutboxEvent(t *testing.T) {
	// CHO-1779 — chora-tenancy needs the event to call
	// SubscriptionRegistry.PromoteScheduledTier on its in-memory registry.
	// The dispatcher MUST call d.emit(...) on the happy path so the outbox
	// dispatcher publishes
	// chora.payments.tenant_addon_purchase.subscription_schedule_released.v1.
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	tapRepo := inmem.NewTenantAddonPurchaseRepo()
	a, err := tap.New(
		testPurchaseID, testTenantID, testLearnerGCID,
		"0190dddd-0000-7000-8000-000000000002", "tms", "starter",
		4900, "USD",
		tapTestSessionID, "https://checkout.stripe.com/c/pay/cs_test_tap",
		now,
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	periodStart := now
	periodEnd := now.AddDate(0, 1, 0)
	if err := a.ApplyStripeSubscriptionState(tapTestCustomerID, tapTestSubscriptionID, tap.StatusActive, &periodStart, &periodEnd); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := a.ApplySchedule(tapTestScheduleID, "pro", periodEnd); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if err := tapRepo.Save(context.Background(), a); err != nil {
		t.Fatalf("seed tap: %v", err)
	}
	emitter := &recordingEmitter{}
	d := New(Deps{
		Course:         inmem.NewCoursePurchaseRepo(),
		Application:    inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:    inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:      inmem.NewTenantManaTopUpRepo(),
		Subscription:   inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:  inmem.NewUserManaTopUpRepo(),
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(),
		AddonPurchase:  tapRepo,
		Dispute:        inmem.NewDisputeRepo(),
		Outbox:         emitter,
		Now:            func() time.Time { return now },
	})
	event := scheduleReleasedEvent(t, tapTestScheduleID)

	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(emitter.emits) != 1 {
		t.Fatalf("emitter.emits = %d, want 1", len(emitter.emits))
	}
	em := emitter.emits[0]
	if em.EventType != EventSubscriptionScheduleReleased {
		t.Errorf("EventType = %q, want %q", em.EventType, EventSubscriptionScheduleReleased)
	}
	if em.Aggregate != wh.AggregateTenantAddonPurchase {
		t.Errorf("Aggregate = %v, want AggregateTenantAddonPurchase", em.Aggregate)
	}
	if em.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID = %q, want %q", em.PurchaseID, testPurchaseID)
	}
	if em.TenantID != testTenantID {
		t.Errorf("TenantID = %q, want %q", em.TenantID, testTenantID)
	}
	if em.IdempotencyKey == "" {
		t.Error("IdempotencyKey empty (Stripe replay dedup requires it)")
	}
}

func TestSubscriptionScheduleReleased_DuplicateDelivery_OnlyEmitsOnce(t *testing.T) {
	// Stripe webhook retry → second Dispatch sees no pending schedule
	// (cleared by first release) → no second emit. Outbox idempotency
	// keys would dedup anyway, but emitting twice wastes outbox rows +
	// confuses downstream observability ledgers.
	d, _ := seedScheduledTAPRow(t)
	event := scheduleReleasedEvent(t, tapTestScheduleID)
	// Need to swap the emitter for a recording one — seedScheduledTAPRow's
	// is a no-op recordingEmitter via Deps. Re-construct via the existing
	// repo so we can introspect emits.
	emitter := d.deps.Outbox.(*recordingEmitter)
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("first Dispatch: %v", err)
	}
	emitsAfterFirst := len(emitter.emits)
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Errorf("second Dispatch: %v", err)
	}
	if got := len(emitter.emits); got != emitsAfterFirst {
		t.Errorf("emits after duplicate = %d, want %d (no second emit)", got, emitsAfterFirst)
	}
}

// silence unused-import noise when tests compile without errors.
var _ = errors.New
var _ = shared.ErrNotFound
