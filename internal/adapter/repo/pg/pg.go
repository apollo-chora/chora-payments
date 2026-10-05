// Package pg is the Postgres adapter for chora-payments' 5 Purchase
// aggregates + the global Stripe webhook dedup table.
//
// Mirrors services/chora-delivery/internal/adapter/repo/pg/ — minimal
// Querier + Row + Rows + TxRunner shape so pgx is wired only at
// cmd/server bootstrap and never leaks into the adapter's import graph.
//
// All reads/writes wrap in rls.ApplySession before queries so the
// tenant_isolation policy on the 5 Purchase tables filters by
// chora.tenant_id.
//
// HARD RULE per .claude/rules/ddd-enforcement.md: chora-payments reads
// ONLY chora_payments — no cross-DB queries.
//
// SCHEMA: see migrations/0001_initial.up.sql + migrations/0002_outbox.up.sql.
package pg

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-common/rls"
)

// Querier is the minimal contract from a pgx-shaped driver.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Row is a single-row result.
type Row interface {
	Scan(dest ...any) error
}

// Rows is a multi-row result.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// TxRunner abstracts pool.BeginTx so chora-payments doesn't import pgx.
type TxRunner interface {
	RunInTx(ctx context.Context, fn func(ctx context.Context, q Querier) error) error
}

// ErrNotImplemented — returned when a repo has no underlying TxRunner wired.
var ErrNotImplemented = errors.New("pg: pgx adapter not wired")

// ErrInvalidAggregate — returned when a nil aggregate is passed to a
// Save / mutating call.
var ErrInvalidAggregate = errors.New("pg: aggregate is nil")

// qExecer adapts Querier to rls.Execer.
type qExecer struct{ q Querier }

func (q qExecer) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
	return q.q.Exec(ctx, sql, args...)
}

func qToExecer(q Querier) rls.Execer { return qExecer{q: q} }

// nullTime returns nil for zero times (so Postgres NULLs match).
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// nullStr returns nil for empty strings.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// deref returns the time pointed to by t, or zero time when t is nil.
func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// timeUTC returns t.UTC() when t is non-nil, nil otherwise.
func timeUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
