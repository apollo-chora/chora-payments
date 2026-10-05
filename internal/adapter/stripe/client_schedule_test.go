// client_schedule_test.go — CHO-1772.
//
// Pins the StubClient round-trip for the new
// ScheduleSubscriptionPriceChange port method. RealClient parity is
// out of scope here — the wire shape is exercised end-to-end in the
// Stripe sandbox smoke (CHO-1772 acceptance).
package stripe_test

import (
	"context"
	"strings"
	"testing"
	"time"

	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
)

func TestStub_ScheduleSubscriptionPriceChange_HappyPath(t *testing.T) {
	t.Parallel()
	c := stripeadapter.NewStubClient()
	effective := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	out, err := c.ScheduleSubscriptionPriceChange(context.Background(), stripeadapter.ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_test_abc",
		NewPriceID:           "price_pro_usd",
		StartDate:            effective,
	})
	if err != nil {
		t.Fatalf("ScheduleSubscriptionPriceChange: %v", err)
	}
	if !strings.HasPrefix(out.StripeSubscriptionScheduleID, "sub_sched_stub_") {
		t.Errorf("schedule_id = %q, want prefix sub_sched_stub_", out.StripeSubscriptionScheduleID)
	}
	if !out.EffectiveAt.Equal(effective) {
		t.Errorf("EffectiveAt = %v, want %v (StartDate echo)", out.EffectiveAt, effective)
	}
}

func TestStub_ScheduleSubscriptionPriceChange_DeterministicScheduleID(t *testing.T) {
	t.Parallel()
	c := stripeadapter.NewStubClient()
	in := stripeadapter.ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_test_xyz",
		NewPriceID:           "price_pro_usd",
		StartDate:            time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
	}
	a, _ := c.ScheduleSubscriptionPriceChange(context.Background(), in)
	b, _ := c.ScheduleSubscriptionPriceChange(context.Background(), in)
	if a.StripeSubscriptionScheduleID != b.StripeSubscriptionScheduleID {
		t.Errorf("schedule_id not deterministic: %q vs %q",
			a.StripeSubscriptionScheduleID, b.StripeSubscriptionScheduleID)
	}
}

func TestStub_ScheduleSubscriptionPriceChange_RequiresSubID(t *testing.T) {
	t.Parallel()
	c := stripeadapter.NewStubClient()
	_, err := c.ScheduleSubscriptionPriceChange(context.Background(), stripeadapter.ScheduleSubscriptionPriceChangeInput{
		NewPriceID: "price_pro_usd",
		StartDate:  time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Error("expected error for empty stripe_subscription_id, got nil")
	}
}

func TestStub_ScheduleSubscriptionPriceChange_RequiresPriceID(t *testing.T) {
	t.Parallel()
	c := stripeadapter.NewStubClient()
	_, err := c.ScheduleSubscriptionPriceChange(context.Background(), stripeadapter.ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_test_abc",
		StartDate:            time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Error("expected error for empty new_price_id, got nil")
	}
}

func TestStub_ScheduleSubscriptionPriceChange_RequiresStartDate(t *testing.T) {
	t.Parallel()
	c := stripeadapter.NewStubClient()
	_, err := c.ScheduleSubscriptionPriceChange(context.Background(), stripeadapter.ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_test_abc",
		NewPriceID:           "price_pro_usd",
	})
	if err == nil {
		t.Error("expected error for zero start_date, got nil")
	}
}
