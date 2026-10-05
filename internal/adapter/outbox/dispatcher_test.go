package outbox

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// TestDispatcher_BackoffMath exercises the backoff helper.
func TestDispatcher_BackoffMath(t *testing.T) {
	t.Parallel()
	base := 5 * time.Second
	cap := 5 * time.Minute
	cases := []struct {
		attempts int
		min, max time.Duration
	}{
		{0, base, base},
		{1, base, base},         // base * 2^0 = base
		{2, 2 * base, 2 * base}, // base * 2^1
		{3, 4 * base, 4 * base}, // base * 2^2
		{10, cap, cap},          // exceeds cap
	}
	for _, tc := range cases {
		got := backoff(tc.attempts, base, cap)
		if got < tc.min || got > tc.max {
			t.Errorf("backoff(%d) = %v; want in [%v, %v]", tc.attempts, got, tc.min, tc.max)
		}
	}
}

// TestEnvelopeFromRow_PopulatesCanonicalFields verifies the canonical
// envelope fields the dispatcher reconstructs from the outbox row.
func TestEnvelopeFromRow_PopulatesCanonicalFields(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 6, 15, 23, 0, 45, 348_000_000, time.UTC)
	row := &pg.OutboxRow{
		EventID:        "01970000-0000-7000-e000-000000000001",
		IdempotencyKey: "key-1",
		TenantID:       "01970000-0000-7000-8000-000000000001",
		AggregateType:  "course_purchase",
		AggregateID:    "01970000-0000-7000-b000-000000000003",
		Topic:          "chora.payments.course_purchase.payment_captured.v1",
		Traceparent:    "00-trace-span-01",
		SchemaVersion:  1,
		CreatedAt:      createdAt,
	}
	env := envelopeFromRow(row, createdAt)
	if env.EventID != row.EventID {
		t.Errorf("EventID = %q; want %q", env.EventID, row.EventID)
	}
	if env.IdempotencyKey != row.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q; want %q", env.IdempotencyKey, row.IdempotencyKey)
	}
	if env.TenantID != row.TenantID {
		t.Errorf("TenantID = %q; want %q", env.TenantID, row.TenantID)
	}
	if env.Traceparent != row.Traceparent {
		t.Errorf("Traceparent = %q; want %q", env.Traceparent, row.Traceparent)
	}
	if env.SourceProject != sourceProject {
		t.Errorf("SourceProject = %q; want %q", env.SourceProject, sourceProject)
	}
	if env.SourceService != "chora-payments" {
		t.Errorf("SourceService = %q; want chora-payments", env.SourceService)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d; want 1", env.SchemaVersion)
	}
	if !env.OccurredAt.Equal(createdAt) {
		t.Errorf("OccurredAt = %v; want %v", env.OccurredAt, createdAt)
	}
	if env.PublishedAt.IsZero() {
		t.Errorf("PublishedAt must be stamped at dispatch time")
	}
}

// TestEnvelopeFromRow_Timestamps locks down the round-trip the eventbus
// relies on. OccurredAt MUST equal the row's CreatedAt and PublishedAt
// MUST be the dispatch time — the bus projects both onto NATS headers
// and subscribers reject a zero timestamp.
func TestEnvelopeFromRow_Timestamps(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 6, 15, 23, 0, 45, 348_000_000, time.UTC)
	row := &pg.OutboxRow{
		EventID:        "evt-1",
		IdempotencyKey: "key-1",
		TenantID:       "01970000-0000-7000-8000-000000000001",
		AggregateType:  "tenant_addon_purchase",
		AggregateID:    "01970000-0000-7000-b000-000000000004",
		Topic:          "chora.payments.tenant_addon_purchase.payment_captured.v1",
		CreatedAt:      createdAt,
	}
	dispatchTime := time.Date(2026, 6, 15, 23, 0, 50, 0, time.UTC)
	env := envelopeFromRow(row, dispatchTime)
	if !env.OccurredAt.Equal(createdAt) {
		t.Errorf("OccurredAt = %v; want %v (row CreatedAt)", env.OccurredAt, createdAt)
	}
	if !env.PublishedAt.Equal(dispatchTime) {
		t.Errorf("PublishedAt = %v; want %v (dispatch time)", env.PublishedAt, dispatchTime)
	}
}

// TestEnvelopeFromRow_FallsBackToNowOnZeroCreatedAt — defensive: if
// the outbox row hasn't been stamped with CreatedAt (in-mem dev path),
// the dispatcher must NOT emit a zero OccurredAt — the eventbus
// dead-letters an envelope with a zero occurred_at.
func TestEnvelopeFromRow_FallsBackToNowOnZeroCreatedAt(t *testing.T) {
	t.Parallel()
	row := &pg.OutboxRow{
		EventID:        "evt-2",
		IdempotencyKey: "key-2",
		TenantID:       "01970000-0000-7000-8000-000000000001",
		AggregateType:  "tenant_addon_purchase",
		AggregateID:    "01970000-0000-7000-b000-000000000005",
		Topic:          "chora.payments.tenant_addon_purchase.payment_captured.v1",
		// CreatedAt deliberately zero.
	}
	dispatchTime := time.Date(2026, 6, 15, 23, 0, 50, 0, time.UTC)
	env := envelopeFromRow(row, dispatchTime)
	if env.OccurredAt.IsZero() {
		t.Errorf("OccurredAt must fall back to dispatch time on zero CreatedAt")
	}
	if !env.OccurredAt.Equal(dispatchTime) {
		t.Errorf("OccurredAt = %v; want %v (dispatch time fallback)", env.OccurredAt, dispatchTime)
	}
}

// TestTopicForEvent verifies the canonical subject name shape.
func TestTopicForEvent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		agg  wh.AggregateType
		evt  dispatcher.EventType
		want string
	}{
		{wh.AggregateCoursePurchase, dispatcher.EventPaymentCaptured, "chora.payments.course_purchase.payment_captured.v1"},
		{wh.AggregateFamiliarEggPurchase, dispatcher.EventExpired, "chora.payments.familiar_egg_purchase.expired.v1"},
		{wh.AggregateUserSubscription, dispatcher.EventRefunded, "chora.payments.user_subscription.refunded.v1"},
	}
	for _, tc := range cases {
		got := topicForEvent(tc.agg, tc.evt)
		if got != tc.want {
			t.Errorf("topicForEvent(%s, %s) = %q; want %q", tc.agg, tc.evt, got, tc.want)
		}
	}
}
