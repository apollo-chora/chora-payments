package webhook_event_test

import (
	"errors"
	"testing"
	"time"

	we "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	w, err := we.New("evt_test_0001", "checkout.session.completed", now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if w.EventID != "evt_test_0001" {
		t.Errorf("EventID=%s, want evt_test_0001", w.EventID)
	}
	if w.EventType != "checkout.session.completed" {
		t.Errorf("EventType=%s, want checkout.session.completed", w.EventType)
	}
	if !w.ReceivedAt.Equal(now) {
		t.Errorf("ReceivedAt=%v, want %v", w.ReceivedAt, now)
	}
	if w.IsProcessed() {
		t.Errorf("IsProcessed()=true for fresh event")
	}
}

func TestNew_ValidatesEventID(t *testing.T) {
	t.Parallel()
	_, err := we.New("", "checkout.session.completed", time.Now().UTC())
	if !errors.Is(err, we.ErrEventIDRequired) {
		t.Errorf("err=%v, want ErrEventIDRequired", err)
	}
}

func TestNew_ValidatesEventType(t *testing.T) {
	t.Parallel()
	_, err := we.New("evt_test_0001", "", time.Now().UTC())
	if !errors.Is(err, we.ErrEventTypeRequired) {
		t.Errorf("err=%v, want ErrEventTypeRequired", err)
	}
}

func TestMarkProcessed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	w, _ := we.New("evt_test_0001", "checkout.session.completed", now)
	w.MarkProcessed(we.AggregateCoursePurchase, "purchase_0001", now.Add(time.Minute))
	if !w.IsProcessed() {
		t.Errorf("IsProcessed()=false after MarkProcessed")
	}
	if w.TargetAggregateType != we.AggregateCoursePurchase {
		t.Errorf("TargetAggregateType=%s, want course_purchase", w.TargetAggregateType)
	}
	if w.TargetPurchaseID != "purchase_0001" {
		t.Errorf("TargetPurchaseID=%s, want purchase_0001", w.TargetPurchaseID)
	}
	if w.ProcessedAt == nil || !w.ProcessedAt.Equal(now.Add(time.Minute)) {
		t.Errorf("ProcessedAt=%v, want %v", w.ProcessedAt, now.Add(time.Minute))
	}
}

func TestMarkFailed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	w, _ := we.New("evt_test_0001", "checkout.session.completed", now)
	w.MarkFailed("parse error: bad payload")
	if w.ProcessingError != "parse error: bad payload" {
		t.Errorf("ProcessingError=%s, want parse error: bad payload", w.ProcessingError)
	}
}

func TestAggregateTypeIsValid(t *testing.T) {
	t.Parallel()
	for _, a := range []we.AggregateType{
		we.AggregateCoursePurchase,
		we.AggregateApplicationPayment,
		we.AggregateFamiliarEggPurchase,
		we.AggregateTenantManaTopUp,
		we.AggregateUserSubscription,
		we.AggregateUserManaTopUp,
		we.AggregateIdentityKycFee,
		we.AggregateTenantAddonPurchase,
	} {
		if !a.IsValid() {
			t.Errorf("AggregateType %q should be valid", a)
		}
	}
	if we.AggregateUnknown.IsValid() {
		t.Errorf("AggregateUnknown should be invalid")
	}
	if we.AggregateType("mystery_aggregate").IsValid() {
		t.Errorf("unknown aggregate should be invalid")
	}
}
