// outbox.go — Postgres adapter for the chora_payments.outbox_events table.
//
// SCHEMA: migrations/0002_outbox.up.sql.
//
// Unlike the per-aggregate Purchase repos, this adapter does NOT call
// rls.ApplySession — the outbox is an operational table (no RLS policy;
// `tenant_id` is on the row for audit but the dispatcher must drain
// rows regardless of caller's tenant context).
//
// Mirrors the canonical chora-delivery outbox pattern but adapted to the
// chora_payments schema:
//   - PRIMARY KEY is event_id (UUIDv7), not id.
//   - No outbox_dead_letters table; DLQ tracked by `dispatched_at IS NULL`
//   - `dispatch_attempts >= MaxAttempts` (operational dashboards alert).
//   - `next_retry_at` cursor for exponential backoff.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// OutboxRow mirrors one row of chora_payments.outbox_events.
type OutboxRow struct {
	EventID          string // UUIDv7 (PK)
	AggregateType    string // e.g. "course_purchase"
	AggregateID      string // purchase_id
	TenantID         string
	Topic            string // e.g. "chora.payments.course_purchase.payment_captured.v1"
	Payload          []byte // proto3 binary
	Traceparent      string
	Tracestate       string
	IdempotencyKey   string // UNIQUE
	SchemaVersion    int32
	Envelope         map[string]string // shaped on the way out for the dispatcher
	CreatedAt        time.Time
	DispatchedAt     *time.Time
	DispatchAttempts int
	LastError        string
	NextRetryAt      *time.Time
}

// ErrDuplicateIdempotencyKey is returned by OutboxRepo.Insert when the
// supplied row collides on idempotency_key.
var ErrDuplicateIdempotencyKey = errors.New("pg/outbox: duplicate idempotency_key")

const SQLInsertOutboxEvent = `
INSERT INTO outbox_events (
    event_id, aggregate_type, aggregate_id, tenant_id, topic,
    payload, traceparent, tracestate, idempotency_key, schema_version,
    created_at
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11
)
`

// defaultClaimReservationSeconds is the window during which a claimed-but-not-
// yet-dispatched row is hidden from other dispatchers (next_retry_at is bumped
// this far into the future at claim time). It must exceed the worst-case time
// to publish one DrainOnce batch; it also bounds how long a row reserved by a
// crashed dispatcher waits before re-claim (the built-in reaper).
const defaultClaimReservationSeconds = 60

// SQLClaimUndispatchedOutboxEvents atomically CLAIMS up to $1 undispatched rows
// whose next_retry_at is past or NULL, RETURNING them.
//
// CHO-1615: this is a claiming UPDATE, not a bare SELECT. The pre-fix query was
// `SELECT ... FOR UPDATE SKIP LOCKED` run inside r.tx.RunInTx, which begins AND
// commits its own transaction — so the row locks released the instant Claim
// returned and the rows stayed `dispatched_at IS NULL` through the
// publish+MarkDispatched gap. A second dispatcher (a prod + a payments-dev
// co-deployment, or an HPA replica) re-grabbed the same rows and double-published.
//
// The claim now durably RESERVES each row by bumping next_retry_at = $2 + window
// inside the UPDATE; the committed bump + the `next_retry_at <= $2` predicate
// exclude just-claimed rows from a concurrent claim. A row reserved by a crashed
// dispatcher becomes re-claimable once the window elapses. No migration —
// next_retry_at + dispatched_at already exist.
const SQLClaimUndispatchedOutboxEvents = `
UPDATE outbox_events
   SET next_retry_at = $2 + make_interval(secs => $3)
 WHERE event_id IN (
       SELECT event_id FROM outbox_events
        WHERE dispatched_at IS NULL
          AND (next_retry_at IS NULL OR next_retry_at <= $2)
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED)
RETURNING event_id, aggregate_type, aggregate_id, tenant_id, topic,
       payload, traceparent, tracestate, idempotency_key, schema_version,
       created_at, dispatch_attempts, last_error
`

const SQLMarkOutboxEventDispatched = `
UPDATE outbox_events
   SET dispatched_at = $2
 WHERE event_id = $1
`

const SQLMarkOutboxEventRetry = `
UPDATE outbox_events
   SET dispatch_attempts = dispatch_attempts + 1,
       last_error = $2,
       next_retry_at = $3
 WHERE event_id = $1
`

// OutboxRepo is the Postgres-backed outbox adapter.
type OutboxRepo struct {
	tx TxRunner
}

func NewOutboxRepo(tx TxRunner) *OutboxRepo {
	return &OutboxRepo{tx: tx}
}

// ErrOutboxTableMissing is returned by Preflight when the underlying
// chora_payments.outbox_events table does not exist. Surfaces
// SQLSTATE 42P01 (undefined_table) — typically because migration 0002
// has not been applied to the target database.
var ErrOutboxTableMissing = errors.New("pg/outbox: outbox_events table missing")

// Preflight runs a zero-row probe against outbox_events. Intended to be
// called at server boot — a missing table at startup is a fail-loud
// condition (the outbox dispatcher would otherwise spam-error every
// PollInterval without surfacing the gap to liveness probes).
//
// Returns ErrOutboxTableMissing if the table is missing; any other
// pg error is returned wrapped so callers can decide whether to fail
// boot or retry.
func (r *OutboxRepo) Preflight(ctx context.Context) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		// WHERE 1=0 forces zero rows; the query still validates table
		// existence + grant correctness without scanning.
		_, err := q.Exec(ctx, "SELECT 1 FROM outbox_events WHERE 1=0")
		if err == nil {
			return nil
		}
		if isUndefinedTable(err) {
			return fmt.Errorf("%w: apply migration 0002_outbox.up.sql to chora_payments", ErrOutboxTableMissing)
		}
		return fmt.Errorf("pg/outbox: preflight: %w", err)
	})
}

// isUndefinedTable detects Postgres SQLSTATE 42P01 (undefined_table) by
// inspecting the error message — avoids importing pgx into this adapter
// per pg.go's "no pgx in import graph" rule.
func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE 42P01") ||
		strings.Contains(msg, "does not exist")
}

// Insert writes a new pending outbox row inside the supplied TX-bound
// Querier. Use when the caller already has an in-flight transaction
// they want to chain the outbox write onto (atomic aggregate-state +
// outbox emit). For free-standing inserts use InsertStandalone.
func (r *OutboxRepo) Insert(ctx context.Context, q Querier, row OutboxRow) error {
	if row.EventID == "" {
		return errors.New("pg/outbox: event_id required")
	}
	if row.IdempotencyKey == "" {
		return errors.New("pg/outbox: idempotency_key required")
	}
	schemaVersion := row.SchemaVersion
	if schemaVersion <= 0 {
		schemaVersion = 1
	}
	_, err := q.Exec(ctx, SQLInsertOutboxEvent,
		row.EventID, row.AggregateType, row.AggregateID, row.TenantID, row.Topic,
		row.Payload, nullStr(row.Traceparent), nullStr(row.Tracestate),
		row.IdempotencyKey, schemaVersion,
		row.CreatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: %s", ErrDuplicateIdempotencyKey, row.IdempotencyKey)
		}
		return fmt.Errorf("pg/outbox: Insert: %w", err)
	}
	return nil
}

// InsertStandalone opens a fresh tx + writes the outbox row. Used by
// the dispatcher subscriber-side teardown (e.g. forwarded events).
func (r *OutboxRepo) InsertStandalone(ctx context.Context, row OutboxRow) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		return r.Insert(ctx, q, row)
	})
}

// Claim atomically reserves + returns up to `limit` undispatched rows whose
// next_retry_at has passed (or is NULL). The claim bumps next_retry_at a
// reservation window into the future (defaultClaimReservationSeconds) so a
// concurrent dispatcher draining the same table never re-claims an in-flight
// row — see SQLClaimUndispatchedOutboxEvents (CHO-1615). The window doubles as
// the crashed-dispatcher reaper; callers no longer need to hold the tx.
func (r *OutboxRepo) Claim(ctx context.Context, limit int, now time.Time) ([]OutboxRow, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if limit <= 0 {
		limit = 50
	}
	var out []OutboxRow
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, SQLClaimUndispatchedOutboxEvents, limit, now, defaultClaimReservationSeconds)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			or, err := scanOutboxRow(rows.Scan)
			if err != nil {
				return err
			}
			out = append(out, or)
		}
		return rows.Err()
	})
	return out, err
}

// MarkDispatched flips dispatched_at = now() for the row.
func (r *OutboxRepo) MarkDispatched(ctx context.Context, eventID string, dispatchedAt time.Time) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		_, err := q.Exec(ctx, SQLMarkOutboxEventDispatched, eventID, dispatchedAt)
		if err != nil {
			return fmt.Errorf("pg/outbox: MarkDispatched: %w", err)
		}
		return nil
	})
}

// MarkRetry bumps dispatch_attempts + last_error + next_retry_at (the
// next-attempt cursor). Caller computes the backoff delay.
func (r *OutboxRepo) MarkRetry(ctx context.Context, eventID, lastErr string, nextRetryAt time.Time) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		_, err := q.Exec(ctx, SQLMarkOutboxEventRetry, eventID, truncate(lastErr, 1000), nextRetryAt)
		if err != nil {
			return fmt.Errorf("pg/outbox: MarkRetry: %w", err)
		}
		return nil
	})
}

func scanOutboxRow(scan func(...any) error) (OutboxRow, error) {
	var (
		row         OutboxRow
		traceparent *string
		tracestate  *string
		lastErr     *string
	)
	if err := scan(
		&row.EventID, &row.AggregateType, &row.AggregateID, &row.TenantID, &row.Topic,
		&row.Payload, &traceparent, &tracestate, &row.IdempotencyKey, &row.SchemaVersion,
		&row.CreatedAt, &row.DispatchAttempts, &lastErr,
	); err != nil {
		return OutboxRow{}, err
	}
	if traceparent != nil {
		row.Traceparent = *traceparent
	}
	if tracestate != nil {
		row.Tracestate = *tracestate
	}
	if lastErr != nil {
		row.LastError = *lastErr
	}
	return row, nil
}

// truncate caps s at n bytes (defence against unbounded error messages).
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// SafeStripError trims a common substring from err.Error() that often
// leaks DSN fragments under PgBouncer / cloudsql-proxy port-mismatch.
// Used by MarkRetry callers to avoid persisting connection strings.
// Strips at the EARLIEST DSN-fragment match in the message.
func SafeStripError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	earliest := -1
	for _, frag := range []string{"sslmode=", "host=", "password=", "user="} {
		if i := strings.Index(msg, frag); i >= 0 {
			if earliest < 0 || i < earliest {
				earliest = i
			}
		}
	}
	if earliest >= 0 {
		return msg[:earliest] + "<redacted>"
	}
	return msg
}
