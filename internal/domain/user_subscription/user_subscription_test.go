package user_subscription_test

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	us "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_sub"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_sub"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, err := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD",
		sessionID, checkoutURL,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if u.PlanSKU != "mana.subscription.standard_v1" {
		t.Errorf("PlanSKU=%s, want mana.subscription.standard_v1", u.PlanSKU)
	}
	if u.BillingPeriod != us.BillingMonthly {
		t.Errorf("BillingPeriod=%s, want monthly", u.BillingPeriod)
	}
	if u.LifecycleState != us.SubStateCreated {
		t.Errorf("LifecycleState=%s, want created", u.LifecycleState)
	}
	if u.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", u.State)
	}
}

func TestNew_ValidatesPlanSKU(t *testing.T) {
	t.Parallel()
	_, err := us.New(
		purchaseID, tenantID, learnerGCID, "",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, us.ErrPlanSKURequired) {
		t.Errorf("err=%v, want ErrPlanSKURequired", err)
	}
}

func TestNew_ValidatesBillingPeriod(t *testing.T) {
	t.Parallel()
	_, err := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		"weekly",
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, us.ErrBillingPeriodInvalid) {
		t.Errorf("err=%v, want ErrBillingPeriodInvalid", err)
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := us.New(
		purchaseID, "", learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err=%v, want shared.ErrTenantRequired", err)
	}
}

func TestBillingPeriodIsValid(t *testing.T) {
	t.Parallel()
	for _, b := range []us.BillingPeriod{us.BillingMonthly, us.BillingAnnually} {
		if !b.IsValid() {
			t.Errorf("BillingPeriod %s should be valid", b)
		}
	}
	if us.BillingPeriod("weekly").IsValid() {
		t.Errorf("BillingPeriod weekly should be invalid")
	}
}

func TestMarkSubscriptionCreated(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, _ := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, now,
	)
	start, end := now, now.Add(30*24*time.Hour)
	u.MarkSubscriptionCreated("sub_test", "cus_test", start, end, now.Add(time.Minute))
	if u.LifecycleState != us.SubStateCreated {
		t.Errorf("LifecycleState=%s, want created", u.LifecycleState)
	}
	if u.StripeSubscriptionID != "sub_test" {
		t.Errorf("StripeSubscriptionID=%s, want sub_test", u.StripeSubscriptionID)
	}
	if u.StripeCustomerID != "cus_test" {
		t.Errorf("StripeCustomerID=%s, want cus_test", u.StripeCustomerID)
	}
	if u.CurrentPeriodStart == nil || !u.CurrentPeriodStart.Equal(start) {
		t.Errorf("CurrentPeriodStart=%v, want %v", u.CurrentPeriodStart, start)
	}
	if u.CurrentPeriodEnd == nil || !u.CurrentPeriodEnd.Equal(end) {
		t.Errorf("CurrentPeriodEnd=%v, want %v", u.CurrentPeriodEnd, end)
	}
	if u.CreatedAtStripe == nil || !u.CreatedAtStripe.Equal(now.Add(time.Minute)) {
		t.Errorf("CreatedAtStripe=%v, want %v", u.CreatedAtStripe, now.Add(time.Minute))
	}
}

func TestMarkSubscriptionRenewed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, _ := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, now,
	)
	start, end := now.Add(30*24*time.Hour), now.Add(60*24*time.Hour)
	u.MarkSubscriptionRenewed(start, end, now.Add(30*24*time.Hour))
	if u.LifecycleState != us.SubStateActive {
		t.Errorf("LifecycleState=%s, want active", u.LifecycleState)
	}
	if u.CurrentPeriodStart == nil || !u.CurrentPeriodStart.Equal(start) {
		t.Errorf("CurrentPeriodStart=%v, want %v", u.CurrentPeriodStart, start)
	}
	if u.CurrentPeriodEnd == nil || !u.CurrentPeriodEnd.Equal(end) {
		t.Errorf("CurrentPeriodEnd=%v, want %v", u.CurrentPeriodEnd, end)
	}
	if u.RenewedAt == nil || !u.RenewedAt.Equal(now.Add(30*24*time.Hour)) {
		t.Errorf("RenewedAt=%v, want %v", u.RenewedAt, now.Add(30*24*time.Hour))
	}
}

func TestMarkSubscriptionPaused(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, _ := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, now,
	)
	resumesAt := now.Add(7 * 24 * time.Hour)
	u.MarkSubscriptionPaused("learner_request", &resumesAt, now.Add(time.Hour))
	if u.LifecycleState != us.SubStatePaused {
		t.Errorf("LifecycleState=%s, want paused", u.LifecycleState)
	}
	if u.PauseReason != "learner_request" {
		t.Errorf("PauseReason=%s, want learner_request", u.PauseReason)
	}
	if u.ResumesAt == nil || !u.ResumesAt.Equal(resumesAt) {
		t.Errorf("ResumesAt=%v, want %v", u.ResumesAt, resumesAt)
	}
	if u.PausedAt == nil || !u.PausedAt.Equal(now.Add(time.Hour)) {
		t.Errorf("PausedAt=%v, want %v", u.PausedAt, now.Add(time.Hour))
	}
}

func TestMarkSubscriptionCancelled(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, _ := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, now,
	)
	effectiveAt := now.Add(30 * 24 * time.Hour)
	u.MarkSubscriptionCancelled("customer_request", true, effectiveAt, now.Add(time.Hour))
	if u.LifecycleState != us.SubStateCancelled {
		t.Errorf("LifecycleState=%s, want cancelled", u.LifecycleState)
	}
	if u.CancellationReason != "customer_request" {
		t.Errorf("CancellationReason=%s, want customer_request", u.CancellationReason)
	}
	if !u.CancelAtPeriodEnd {
		t.Errorf("CancelAtPeriodEnd=false, want true")
	}
	if u.EffectiveAt == nil || !u.EffectiveAt.Equal(effectiveAt) {
		t.Errorf("EffectiveAt=%v, want %v", u.EffectiveAt, effectiveAt)
	}
	if u.CancelledAt == nil || !u.CancelledAt.Equal(now.Add(time.Hour)) {
		t.Errorf("CancelledAt=%v, want %v", u.CancelledAt, now.Add(time.Hour))
	}
}

func TestAccumulatePaid(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, _ := us.New(
		purchaseID, tenantID, learnerGCID, "mana.subscription.standard_v1",
		us.BillingMonthly,
		1990, "SGD", sessionID, checkoutURL, now,
	)
	u.AccumulatePaid(1990)
	u.AccumulatePaid(1990)
	if u.AmountCentsPaidTotal != 3980 {
		t.Errorf("AmountCentsPaidTotal=%d, want 3980", u.AmountCentsPaidTotal)
	}
}
