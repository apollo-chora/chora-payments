package inmem

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func newDispute(t *testing.T, disputeID, stripeDisputeID string, agg wh.AggregateType, purchaseID string) *dispute.Dispute {
	t.Helper()
	now := time.Now().UTC()
	d, err := dispute.New(
		disputeID,
		"01910000-0000-7000-8000-000000000002",
		agg,
		purchaseID,
		stripeDisputeID,
		"ch_test"+disputeID,
		1999,
		"USD",
		dispute.ReasonFraudulent,
		now.Add(14*24*time.Hour),
		now,
	)
	if err != nil {
		t.Fatalf("dispute.New: %v", err)
	}
	return d
}

func TestDisputeRepo_InsertGetByStripeID(t *testing.T) {
	r := NewDisputeRepo()
	d := newDispute(t, "d1", "dp_test_1", wh.AggregateCoursePurchase, "p1")

	ctx := context.Background()
	if err := r.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := r.GetByStripeDisputeID(ctx, "dp_test_1")
	if err != nil {
		t.Fatalf("GetByStripeDisputeID: %v", err)
	}
	if got.DisputeID != "d1" {
		t.Fatalf("expected d1, got %s", got.DisputeID)
	}
}

func TestDisputeRepo_InsertDuplicateStripeID(t *testing.T) {
	r := NewDisputeRepo()
	d1 := newDispute(t, "d1", "dp_dup", wh.AggregateCoursePurchase, "p1")
	d2 := newDispute(t, "d2", "dp_dup", wh.AggregateCoursePurchase, "p2")

	ctx := context.Background()
	if err := r.Insert(ctx, d1); err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	err := r.Insert(ctx, d2)
	if !errors.Is(err, dispute.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestDisputeRepo_GetByStripeID_NotFound(t *testing.T) {
	r := NewDisputeRepo()
	_, err := r.GetByStripeDisputeID(context.Background(), "dp_missing")
	if !errors.Is(err, dispute.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDisputeRepo_GetByID_TenantScoped(t *testing.T) {
	r := NewDisputeRepo()
	d := newDispute(t, "d1", "dp_x", wh.AggregateCoursePurchase, "p1")
	if err := r.Insert(context.Background(), d); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Wrong tenant returns not-found (RLS-equivalent at adapter layer).
	_, err := r.GetByID(context.Background(), "other-tenant", "d1")
	if !errors.Is(err, dispute.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for wrong tenant, got %v", err)
	}

	// Right tenant returns the dispute.
	got, err := r.GetByID(context.Background(), d.TenantID, "d1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.DisputeID != "d1" {
		t.Fatalf("expected d1, got %s", got.DisputeID)
	}
}

func TestDisputeRepo_ListByPurchase(t *testing.T) {
	r := NewDisputeRepo()
	d1 := newDispute(t, "d1", "dp_1", wh.AggregateCoursePurchase, "p_shared")
	d2 := newDispute(t, "d2", "dp_2", wh.AggregateCoursePurchase, "p_shared")
	d3 := newDispute(t, "d3", "dp_3", wh.AggregateCoursePurchase, "p_other")

	ctx := context.Background()
	for _, d := range []*dispute.Dispute{d1, d2, d3} {
		if err := r.Insert(ctx, d); err != nil {
			t.Fatalf("Insert %s: %v", d.DisputeID, err)
		}
	}

	got, err := r.ListByPurchase(ctx, d1.TenantID, wh.AggregateCoursePurchase, "p_shared")
	if err != nil {
		t.Fatalf("ListByPurchase: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 disputes for p_shared, got %d", len(got))
	}
}

func TestDisputeRepo_Save_UpsertSemantics(t *testing.T) {
	r := NewDisputeRepo()
	d := newDispute(t, "d1", "dp_save", wh.AggregateCoursePurchase, "p1")
	ctx := context.Background()
	if err := r.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Mutate + Save.
	closedAt := d.RaisedAt.Add(time.Hour)
	if err := d.MarkClosed(dispute.OutcomeWon, closedAt); err != nil {
		t.Fatalf("MarkClosed: %v", err)
	}
	if err := r.Save(ctx, d); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := r.GetByStripeDisputeID(ctx, "dp_save")
	if err != nil {
		t.Fatalf("GetByStripeDisputeID after Save: %v", err)
	}
	if got.State != dispute.StateClosed {
		t.Fatalf("expected closed, got %s", got.State)
	}
	if got.Outcome != dispute.OutcomeWon {
		t.Fatalf("expected won, got %s", got.Outcome)
	}
}
