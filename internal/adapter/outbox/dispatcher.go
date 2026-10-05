// dispatcher.go — Outbox-to-eventbus Dispatcher worker.
//
// Drains chora_payments.outbox_events rows where dispatched_at IS NULL,
// publishes to the NATS JetStream event bus, marks dispatched_at on
// success / bumps dispatch_attempts + next_retry_at on transient
// failure. Mirrors the canonical chora-delivery outbox dispatcher
// (D6.2 Pillar 2).
package outbox

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
)

// Publisher is the eventbus publisher contract. Structurally identical to
// eventbus.Publisher, so the bus is passed directly with no adapter.
type Publisher interface {
	Publish(ctx context.Context, subject string, env envelope.Envelope, payload []byte) error
}

// DispatcherConfig tunes the Dispatcher.
type DispatcherConfig struct {
	Outbox    *pg.OutboxRepo
	Publisher Publisher

	// BatchSize caps the rows claimed per drain cycle. Defaults to 50.
	BatchSize int

	// PollInterval is the sleep between empty drain cycles. Defaults to
	// 250ms.
	PollInterval time.Duration

	// MaxAttempts caps the per-row retry count before the row is left in
	// place (dispatched_at IS NULL, dispatch_attempts == MaxAttempts) for
	// operator inspection. The eventbus consumer's DLQ is a separate
	// failure path; this counter is the producer-side ceiling. Defaults
	// to 5.
	MaxAttempts int

	// BackoffBase + BackoffCap shape the exponential backoff used to
	// stamp next_retry_at. Defaults: 5s base, 5min cap.
	BackoffBase time.Duration
	BackoffCap  time.Duration

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Dispatcher runs the drain loop.
type Dispatcher struct {
	cfg DispatcherConfig
}

// NewDispatcher constructs a Dispatcher. Panics on missing Outbox or
// Publisher (per feedback_no_stubs_real_wiring).
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.Outbox == nil {
		panic("outbox.NewDispatcher: Outbox repo required")
	}
	if cfg.Publisher == nil {
		panic("outbox.NewDispatcher: Publisher required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 5 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Dispatcher{cfg: cfg}
}

// DrainOnce claims + publishes a single batch. Returns the count of
// successfully-dispatched rows.
func (d *Dispatcher) DrainOnce(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := d.cfg.Now()
	rows, err := d.cfg.Outbox.Claim(ctx, d.cfg.BatchSize, now)
	if err != nil {
		return 0, err
	}
	dispatched := 0
	for _, row := range rows {
		select {
		case <-ctx.Done():
			return dispatched, ctx.Err()
		default:
		}
		if err := d.publishOne(ctx, &row); err == nil {
			dispatched++
		}
	}
	return dispatched, nil
}

// publishOne attempts a single publish + records the outcome via the
// Outbox repo (MarkDispatched on success; MarkRetry on transient).
func (d *Dispatcher) publishOne(ctx context.Context, row *pg.OutboxRow) error {
	env := envelopeFromRow(row, d.cfg.Now())
	if err := d.cfg.Publisher.Publish(ctx, row.Topic, env, row.Payload); err != nil {
		attempts := row.DispatchAttempts + 1
		nextRetry := d.cfg.Now().Add(backoff(attempts, d.cfg.BackoffBase, d.cfg.BackoffCap))
		if mErr := d.cfg.Outbox.MarkRetry(ctx, row.EventID, pg.SafeStripError(err), nextRetry); mErr != nil {
			log.Printf("payments/outbox-dispatcher: MarkRetry failed event_id=%s err=%v", row.EventID, mErr)
		}
		if attempts >= d.cfg.MaxAttempts {
			log.Printf("payments/outbox-dispatcher: row event_id=%s topic=%s exhausted attempts=%d — left for operator inspection",
				row.EventID, row.Topic, attempts)
		}
		return err
	}
	if err := d.cfg.Outbox.MarkDispatched(ctx, row.EventID, d.cfg.Now()); err != nil {
		log.Printf("payments/outbox-dispatcher: MarkDispatched failed event_id=%s err=%v", row.EventID, err)
		return err
	}
	log.Printf("payments/outbox-dispatcher: published event_id=%s topic=%s tenant=%s",
		row.EventID, row.Topic, row.TenantID)
	return nil
}

// Run drives DrainOnce in a loop until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := d.DrainOnce(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			log.Printf("payments/outbox-dispatcher: drain error err=%v", err)
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.cfg.PollInterval):
			}
		}
	}
}

// envelopeFromRow reconstructs the transport envelope from the outbox row.
// The row carries every envelope field the emitter stamped (event_id,
// idempotency_key, tenant_id, traceparent, schema_version, created_at);
// source_project/source_service are the publisher's own identity, and
// published_at is the dispatch time. The payload proto embeds the same
// envelope, so the reconstruction is byte-identical to what subscribers
// decode from the payload.
func envelopeFromRow(row *pg.OutboxRow, now time.Time) envelope.Envelope {
	occurredAt := row.CreatedAt
	if occurredAt.IsZero() {
		occurredAt = now
	}
	return envelope.Envelope{
		EventID:        row.EventID,
		IdempotencyKey: row.IdempotencyKey,
		TenantID:       row.TenantID,
		OccurredAt:     occurredAt,
		PublishedAt:    now,
		Traceparent:    row.Traceparent,
		Tracestate:     row.Tracestate,
		SourceProject:  sourceProject,
		SourceService:  "chora-payments",
		SchemaVersion:  row.SchemaVersion,
	}
}

// backoff computes an exponential backoff capped at backoffCap.
//
// attempts is 1-indexed (1 = first retry after the original publish failed).
func backoff(attempts int, base, cap time.Duration) time.Duration {
	if attempts <= 0 {
		return base
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(attempts-1)))
	if d > cap {
		return cap
	}
	if d < base {
		return base
	}
	return d
}
