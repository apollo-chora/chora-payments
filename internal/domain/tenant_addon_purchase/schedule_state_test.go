// schedule_state_test.go — CHO-1772.
//
// Pins the SubscriptionSchedule mutator on the TenantAddonPurchase
// aggregate. The schedule represents a pending end-of-cycle tier change:
// the row's current TierCode + Stripe Subscription stay put until the
// schedule releases. On release, ReleaseSchedule promotes the scheduled
// tier and clears the schedule fields.
package tenant_addon_purchase_test

import (
	"errors"
	"testing"
	"time"

	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

func mkScheduleFixture(t *testing.T) *tap.TenantAddonPurchase {
	t.Helper()
	a, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, testAddonCode, testTierCode,
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	return a
}

func TestApplySchedule_PersistsAllThreeFields(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	effectiveAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if err := a.ApplySchedule("sub_sched_test", "pro", effectiveAt); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if a.StripeSubscriptionScheduleID != "sub_sched_test" {
		t.Errorf("StripeSubscriptionScheduleID = %q, want sub_sched_test", a.StripeSubscriptionScheduleID)
	}
	if a.ScheduledTierCode != "pro" {
		t.Errorf("ScheduledTierCode = %q, want pro", a.ScheduledTierCode)
	}
	if a.ScheduledEffectiveAt == nil || !a.ScheduledEffectiveAt.Equal(effectiveAt) {
		t.Errorf("ScheduledEffectiveAt = %v, want %v", a.ScheduledEffectiveAt, effectiveAt)
	}
}

func TestApplySchedule_DoesNotMutateCurrentTier(t *testing.T) {
	// The whole point of end-of-cycle scheduling: TierCode stays at the
	// CURRENT paid-for tier until the schedule releases. The FE renders
	// "Pro starting 2026-07-15" using ScheduledTierCode separately.
	t.Parallel()
	a := mkScheduleFixture(t)
	before := a.TierCode
	effectiveAt := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	if err := a.ApplySchedule("sub_sched_test", "pro", effectiveAt); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if a.TierCode != before {
		t.Errorf("ApplySchedule mutated TierCode: before=%q after=%q", before, a.TierCode)
	}
}

func TestApplySchedule_NormalisesEffectiveAtToUTC(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	loc, err := time.LoadLocation("Asia/Singapore")
	if err != nil {
		t.Skipf("Asia/Singapore tz unavailable: %v", err)
	}
	sgt := time.Date(2026, 7, 15, 20, 0, 0, 0, loc)
	if err := a.ApplySchedule("sub_sched_test", "pro", sgt); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if a.ScheduledEffectiveAt == nil {
		t.Fatal("ScheduledEffectiveAt nil after ApplySchedule")
	}
	if a.ScheduledEffectiveAt.Location() != time.UTC {
		t.Errorf("ScheduledEffectiveAt location = %v, want UTC", a.ScheduledEffectiveAt.Location())
	}
	if !a.ScheduledEffectiveAt.Equal(sgt) {
		t.Errorf("ScheduledEffectiveAt = %v, expected equal to SGT %v", a.ScheduledEffectiveAt, sgt)
	}
}

func TestApplySchedule_FailLoudOnEmptyScheduleID(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	err := a.ApplySchedule("", "pro", time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, tap.ErrStripeScheduleRequired) {
		t.Errorf("ApplySchedule empty schedule_id err = %v, want ErrStripeScheduleRequired", err)
	}
}

func TestApplySchedule_FailLoudOnEmptyScheduledTier(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	err := a.ApplySchedule("sub_sched_test", "", time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, tap.ErrScheduledTierRequired) {
		t.Errorf("ApplySchedule empty tier err = %v, want ErrScheduledTierRequired", err)
	}
}

func TestApplySchedule_FailLoudOnZeroEffectiveAt(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	err := a.ApplySchedule("sub_sched_test", "pro", time.Time{})
	if !errors.Is(err, tap.ErrScheduledEffectiveAtRequired) {
		t.Errorf("ApplySchedule zero effective_at err = %v, want ErrScheduledEffectiveAtRequired", err)
	}
}

func TestReleaseSchedule_PromotesTierAndClearsScheduleFields(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	effectiveAt := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	if err := a.ApplySchedule("sub_sched_test", "pro", effectiveAt); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if err := a.ReleaseSchedule(); err != nil {
		t.Fatalf("ReleaseSchedule: %v", err)
	}
	if a.TierCode != "pro" {
		t.Errorf("post-release TierCode = %q, want pro (promoted from ScheduledTierCode)", a.TierCode)
	}
	if a.StripeSubscriptionScheduleID != "" {
		t.Errorf("post-release StripeSubscriptionScheduleID = %q, want empty", a.StripeSubscriptionScheduleID)
	}
	if a.ScheduledTierCode != "" {
		t.Errorf("post-release ScheduledTierCode = %q, want empty", a.ScheduledTierCode)
	}
	if a.ScheduledEffectiveAt != nil {
		t.Errorf("post-release ScheduledEffectiveAt = %v, want nil", a.ScheduledEffectiveAt)
	}
}

func TestReleaseSchedule_NoOpWhenNoScheduleAttached(t *testing.T) {
	// Idempotent under duplicate webhook delivery: if Stripe re-sends
	// subscription_schedule.released after we already processed it, the
	// row has no schedule fields left to clear. Don't error.
	t.Parallel()
	a := mkScheduleFixture(t)
	beforeTier := a.TierCode
	if err := a.ReleaseSchedule(); err != nil {
		t.Errorf("ReleaseSchedule on row without schedule should be no-op, got err: %v", err)
	}
	if a.TierCode != beforeTier {
		t.Errorf("ReleaseSchedule mutated TierCode on row without schedule: %q → %q", beforeTier, a.TierCode)
	}
}

func TestClearSchedule_NullsAllThreeFieldsWithoutPromoting(t *testing.T) {
	// ClearSchedule is the cancellation path: the user backed out before
	// the cycle rolled, so we tear down the schedule without promoting
	// ScheduledTierCode. The Stripe call to release the schedule on the
	// remote side is the caller's responsibility.
	t.Parallel()
	a := mkScheduleFixture(t)
	beforeTier := a.TierCode
	effectiveAt := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	if err := a.ApplySchedule("sub_sched_test", "pro", effectiveAt); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	a.ClearSchedule()
	if a.TierCode != beforeTier {
		t.Errorf("ClearSchedule mutated TierCode: before=%q after=%q (expected unchanged)", beforeTier, a.TierCode)
	}
	if a.StripeSubscriptionScheduleID != "" || a.ScheduledTierCode != "" || a.ScheduledEffectiveAt != nil {
		t.Errorf("ClearSchedule did not null all 3 fields: scheduleID=%q tier=%q effectiveAt=%v",
			a.StripeSubscriptionScheduleID, a.ScheduledTierCode, a.ScheduledEffectiveAt)
	}
}

func TestHasPendingSchedule_TrueOnlyWhenAllThreeFieldsSet(t *testing.T) {
	t.Parallel()
	a := mkScheduleFixture(t)
	if a.HasPendingSchedule() {
		t.Errorf("freshly-constructed aggregate HasPendingSchedule() = true, want false")
	}
	effectiveAt := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	if err := a.ApplySchedule("sub_sched_test", "pro", effectiveAt); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if !a.HasPendingSchedule() {
		t.Errorf("post-ApplySchedule HasPendingSchedule() = false, want true")
	}
	a.ClearSchedule()
	if a.HasPendingSchedule() {
		t.Errorf("post-ClearSchedule HasPendingSchedule() = true, want false")
	}
}
