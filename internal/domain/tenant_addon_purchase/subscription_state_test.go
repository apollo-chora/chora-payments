// subscription_state_test.go — CHO-1761.
//
// Pins the Subscription-state mutator + Status enum. These fields turn
// the one-time-payment tenant_addon_purchase aggregate into a recurring
// Stripe Subscription correlation (CHO-1762 switches Checkout to
// mode=subscription; CHO-1763 webhook fills these in).
package tenant_addon_purchase_test

import (
	"errors"
	"testing"
	"time"

	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

func mkSubscriptionStateFixture(t *testing.T) *tap.TenantAddonPurchase {
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

func TestApplyStripeSubscriptionState_TransitionsPendingToActive(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	if a.Status != "" {
		t.Fatalf("freshly-constructed aggregate should have empty Status, got %q", a.Status)
	}
	periodStart := now()
	periodEnd := now().AddDate(0, 1, 0)
	err := a.ApplyStripeSubscriptionState(
		"cus_abc123", "sub_xyz789", tap.StatusActive,
		&periodStart, &periodEnd,
	)
	if err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if a.StripeCustomerID != "cus_abc123" {
		t.Errorf("StripeCustomerID = %q, want cus_abc123", a.StripeCustomerID)
	}
	if a.StripeSubscriptionID != "sub_xyz789" {
		t.Errorf("StripeSubscriptionID = %q, want sub_xyz789", a.StripeSubscriptionID)
	}
	if a.Status != tap.StatusActive {
		t.Errorf("Status = %q, want active", a.Status)
	}
	if a.CurrentPeriodStart == nil || !a.CurrentPeriodStart.Equal(periodStart) {
		t.Errorf("CurrentPeriodStart = %v, want %v", a.CurrentPeriodStart, periodStart)
	}
	if a.CurrentPeriodEnd == nil || !a.CurrentPeriodEnd.Equal(periodEnd) {
		t.Errorf("CurrentPeriodEnd = %v, want %v", a.CurrentPeriodEnd, periodEnd)
	}
}

// Repeated webhook deliveries are normal; re-applying the same state must
// be idempotent (no error, fields unchanged).
func TestApplyStripeSubscriptionState_IsIdempotent(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	periodStart := now()
	periodEnd := now().AddDate(0, 1, 0)
	for i := 0; i < 3; i++ {
		if err := a.ApplyStripeSubscriptionState(
			"cus_abc", "sub_xyz", tap.StatusActive,
			&periodStart, &periodEnd,
		); err != nil {
			t.Fatalf("apply #%d: %v", i, err)
		}
	}
	if a.StripeSubscriptionID != "sub_xyz" {
		t.Errorf("StripeSubscriptionID drifted after re-apply: %q", a.StripeSubscriptionID)
	}
}

// Status transitions through the recognised lifecycle states: pending,
// active, past_due, cancelled. Anything else is a fail-loud bug — the
// webhook handler is the only caller and must pass a known status.
func TestApplyStripeSubscriptionState_RejectsUnknownStatus(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	err := a.ApplyStripeSubscriptionState(
		"cus_abc", "sub_xyz", tap.Status("nonsense"),
		nil, nil,
	)
	if err == nil {
		t.Fatalf("expected error on unknown status; got nil")
	}
	if !errors.Is(err, tap.ErrInvalidStatus) {
		t.Errorf("err = %v, want ErrInvalidStatus", err)
	}
}

func TestApplyStripeSubscriptionState_RejectsEmptyCustomerID(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	err := a.ApplyStripeSubscriptionState(
		"", "sub_xyz", tap.StatusActive, nil, nil,
	)
	if err == nil {
		t.Fatalf("expected error on empty customerID; got nil")
	}
	if !errors.Is(err, tap.ErrStripeCustomerRequired) {
		t.Errorf("err = %v, want ErrStripeCustomerRequired", err)
	}
}

func TestApplyStripeSubscriptionState_RejectsEmptySubscriptionID(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	err := a.ApplyStripeSubscriptionState(
		"cus_abc", "", tap.StatusActive, nil, nil,
	)
	if err == nil {
		t.Fatalf("expected error on empty subscriptionID; got nil")
	}
	if !errors.Is(err, tap.ErrStripeSubscriptionRequired) {
		t.Errorf("err = %v, want ErrStripeSubscriptionRequired", err)
	}
}

// Period boundaries are nullable in the wire shape (Stripe events emit
// `current_period_start`/`current_period_end` only on active
// subscriptions). Cancelled / past_due states may omit them.
func TestApplyStripeSubscriptionState_AllowsNilPeriods(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	if err := a.ApplyStripeSubscriptionState(
		"cus_abc", "sub_xyz", tap.StatusCancelled, nil, nil,
	); err != nil {
		t.Fatalf("nil periods should be allowed: %v", err)
	}
	if a.CurrentPeriodStart != nil {
		t.Errorf("CurrentPeriodStart = %v, want nil", a.CurrentPeriodStart)
	}
	if a.CurrentPeriodEnd != nil {
		t.Errorf("CurrentPeriodEnd = %v, want nil", a.CurrentPeriodEnd)
	}
}

// Period times round-trip in UTC. Stripe emits Unix seconds; we want
// strict UTC on the aggregate to avoid TZ drift in tests + audit logs.
func TestApplyStripeSubscriptionState_NormalisesPeriodsToUTC(t *testing.T) {
	t.Parallel()
	a := mkSubscriptionStateFixture(t)
	loc, err := time.LoadLocation("Asia/Singapore")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	sgStart := time.Date(2026, 6, 1, 10, 30, 0, 0, loc)
	sgEnd := sgStart.AddDate(0, 1, 0)
	if err := a.ApplyStripeSubscriptionState(
		"cus_abc", "sub_xyz", tap.StatusActive, &sgStart, &sgEnd,
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if a.CurrentPeriodStart.Location() != time.UTC {
		t.Errorf("CurrentPeriodStart not UTC: %v", a.CurrentPeriodStart.Location())
	}
	if a.CurrentPeriodEnd.Location() != time.UTC {
		t.Errorf("CurrentPeriodEnd not UTC: %v", a.CurrentPeriodEnd.Location())
	}
}

func TestIsValidStatus(t *testing.T) {
	t.Parallel()
	cases := map[tap.Status]bool{
		tap.StatusPending:    true,
		tap.StatusActive:     true,
		tap.StatusPastDue:    true,
		tap.StatusCancelled:  true,
		tap.Status(""):       false,
		tap.Status("foo"):    false,
		tap.Status("ACTIVE"): false, // lowercase only per Stripe wire shape
	}
	for in, want := range cases {
		if got := tap.IsValidStatus(in); got != want {
			t.Errorf("IsValidStatus(%q): want %v, got %v", in, want, got)
		}
	}
}
