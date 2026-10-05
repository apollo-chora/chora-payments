package dispute

import (
	"errors"
	"testing"
	"time"

	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func mustNew(t *testing.T) *Dispute {
	t.Helper()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	due := now.Add(14 * 24 * time.Hour)
	d, err := New(
		"01910000-0000-7000-8000-000000000001",
		"01910000-0000-7000-8000-000000000002",
		wh.AggregateCoursePurchase,
		"01910000-0000-7000-8000-000000000003",
		"dp_test123",
		"ch_test123",
		1999,
		"USD",
		ReasonFraudulent,
		due,
		now,
	)
	if err != nil {
		t.Fatalf("New: unexpected error %v", err)
	}
	return d
}

func TestNew_Defaults(t *testing.T) {
	d := mustNew(t)
	if d.State != StateRaised {
		t.Fatalf("expected initial State=raised, got %s", d.State)
	}
	if d.AggregateType != wh.AggregateCoursePurchase {
		t.Fatalf("aggregate_type not set")
	}
	if d.AmountCents != 1999 {
		t.Fatalf("amount_cents not propagated")
	}
	if d.Reason != ReasonFraudulent {
		t.Fatalf("reason not propagated")
	}
	if d.RaisedAt.IsZero() {
		t.Fatalf("raised_at must be set on construction")
	}
}

func TestNew_RequiresAggregateType(t *testing.T) {
	now := time.Now().UTC()
	_, err := New(
		"d1", "t1", wh.AggregateUnknown, "p1", "dp_x", "ch_x",
		100, "USD", ReasonGeneral, now, now,
	)
	if !errors.Is(err, ErrAggregateTypeRequired) {
		t.Fatalf("expected ErrAggregateTypeRequired, got %v", err)
	}
}

func TestNew_RequiresStripeDisputeID(t *testing.T) {
	now := time.Now().UTC()
	_, err := New(
		"d1", "t1", wh.AggregateCoursePurchase, "p1", "", "ch_x",
		100, "USD", ReasonGeneral, now, now,
	)
	if !errors.Is(err, ErrStripeDisputeRequired) {
		t.Fatalf("expected ErrStripeDisputeRequired, got %v", err)
	}
}

func TestNew_RequiresValidCurrency(t *testing.T) {
	now := time.Now().UTC()
	_, err := New(
		"d1", "t1", wh.AggregateCoursePurchase, "p1", "dp_x", "ch_x",
		100, "us", ReasonGeneral, now, now,
	)
	if err == nil {
		t.Fatalf("expected currency validation error")
	}
}

func TestNew_RejectsNegativeAmount(t *testing.T) {
	now := time.Now().UTC()
	_, err := New(
		"d1", "t1", wh.AggregateCoursePurchase, "p1", "dp_x", "ch_x",
		-1, "USD", ReasonGeneral, now, now,
	)
	if err == nil {
		t.Fatalf("expected amount validation error")
	}
}

func TestMarkClosed_Won(t *testing.T) {
	d := mustNew(t)
	closedAt := d.RaisedAt.Add(48 * time.Hour)
	if err := d.MarkClosed(OutcomeWon, closedAt); err != nil {
		t.Fatalf("MarkClosed won: %v", err)
	}
	if d.State != StateClosed {
		t.Fatalf("state not closed")
	}
	if d.Outcome != OutcomeWon {
		t.Fatalf("outcome not won")
	}
	if d.ClosedAt == nil || !d.ClosedAt.Equal(closedAt) {
		t.Fatalf("closed_at not set")
	}
}

func TestMarkClosed_Lost(t *testing.T) {
	d := mustNew(t)
	if err := d.MarkClosed(OutcomeLost, d.RaisedAt.Add(time.Hour)); err != nil {
		t.Fatalf("MarkClosed lost: %v", err)
	}
	if d.Outcome != OutcomeLost {
		t.Fatalf("outcome not lost")
	}
}

func TestMarkClosed_WarningClosed(t *testing.T) {
	d := mustNew(t)
	if err := d.MarkClosed(OutcomeWarningClosed, d.RaisedAt.Add(time.Hour)); err != nil {
		t.Fatalf("MarkClosed warning_closed: %v", err)
	}
	if d.Outcome != OutcomeWarningClosed {
		t.Fatalf("outcome not warning_closed")
	}
}

func TestMarkClosed_InvalidOutcome(t *testing.T) {
	d := mustNew(t)
	if err := d.MarkClosed("bogus", d.RaisedAt.Add(time.Hour)); err == nil {
		t.Fatalf("expected invalid outcome rejection")
	}
}

func TestMarkClosed_Idempotent(t *testing.T) {
	d := mustNew(t)
	closedAt := d.RaisedAt.Add(time.Hour)
	if err := d.MarkClosed(OutcomeWon, closedAt); err != nil {
		t.Fatalf("first MarkClosed: %v", err)
	}
	// Replaying the SAME outcome on a closed dispute is a no-op (idempotent).
	if err := d.MarkClosed(OutcomeWon, closedAt); err != nil {
		t.Fatalf("idempotent replay should not error: %v", err)
	}
}

func TestMarkClosed_RejectsConflictingOutcomeAfterClose(t *testing.T) {
	d := mustNew(t)
	if err := d.MarkClosed(OutcomeWon, d.RaisedAt.Add(time.Hour)); err != nil {
		t.Fatalf("MarkClosed: %v", err)
	}
	// Stripe shouldn't fire conflicting close events; if it does, refuse.
	err := d.MarkClosed(OutcomeLost, d.RaisedAt.Add(2*time.Hour))
	if err == nil {
		t.Fatalf("expected conflicting-outcome rejection")
	}
}

func TestRecordFundsWithdrawn(t *testing.T) {
	d := mustNew(t)
	withdrawnAt := d.RaisedAt.Add(time.Hour)
	if err := d.RecordFundsWithdrawn(withdrawnAt); err != nil {
		t.Fatalf("RecordFundsWithdrawn: %v", err)
	}
	if !d.FundsWithdrawn {
		t.Fatalf("expected FundsWithdrawn=true")
	}
	if d.FundsWithdrawnAt == nil {
		t.Fatalf("expected FundsWithdrawnAt set")
	}
}

func TestRecordFundsWithdrawn_Idempotent(t *testing.T) {
	d := mustNew(t)
	withdrawnAt := d.RaisedAt.Add(time.Hour)
	if err := d.RecordFundsWithdrawn(withdrawnAt); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := d.RecordFundsWithdrawn(withdrawnAt); err != nil {
		t.Fatalf("idempotent replay should not error: %v", err)
	}
}

func TestRecordFundsReinstated(t *testing.T) {
	d := mustNew(t)
	withdrawnAt := d.RaisedAt.Add(time.Hour)
	if err := d.RecordFundsWithdrawn(withdrawnAt); err != nil {
		t.Fatalf("setup: %v", err)
	}
	reinstatedAt := withdrawnAt.Add(48 * time.Hour)
	if err := d.RecordFundsReinstated(reinstatedAt); err != nil {
		t.Fatalf("RecordFundsReinstated: %v", err)
	}
	if !d.FundsReinstated {
		t.Fatalf("expected FundsReinstated=true")
	}
}

func TestRecordFundsReinstated_WithoutPriorWithdrawn(t *testing.T) {
	// Stripe sometimes fires funds_reinstated without a prior funds_withdrawn
	// (warning-closed disputes). Accept gracefully.
	d := mustNew(t)
	reinstatedAt := d.RaisedAt.Add(time.Hour)
	if err := d.RecordFundsReinstated(reinstatedAt); err != nil {
		t.Fatalf("RecordFundsReinstated without prior withdraw: %v", err)
	}
	if !d.FundsReinstated {
		t.Fatalf("expected FundsReinstated=true")
	}
}

func TestOutcome_IsValid(t *testing.T) {
	cases := map[Outcome]bool{
		OutcomeWon:           true,
		OutcomeLost:          true,
		OutcomeWarningClosed: true,
		"":                   false,
		"random":             false,
	}
	for o, want := range cases {
		if got := o.IsValid(); got != want {
			t.Errorf("Outcome(%q).IsValid()=%v want %v", o, got, want)
		}
	}
}

func TestState_IsValid(t *testing.T) {
	if !StateRaised.IsValid() {
		t.Fatalf("StateRaised should be valid")
	}
	if !StateClosed.IsValid() {
		t.Fatalf("StateClosed should be valid")
	}
	if State("bogus").IsValid() {
		t.Fatalf("bogus state should not be valid")
	}
}
