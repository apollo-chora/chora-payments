package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestOutboxRepo_Preflight_HappyPath(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewOutboxRepo(newStubTxRunner(q))

	if err := repo.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if len(q.execs) != 1 {
		t.Fatalf("expected one Exec (the probe), got %d", len(q.execs))
	}
	if got := q.execs[0].sql; got != "SELECT 1 FROM outbox_events WHERE 1=0" {
		t.Fatalf("expected probe SQL, got %q", got)
	}
}

func TestOutboxRepo_Preflight_TableMissing(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	// Simulate Postgres SQLSTATE 42P01 (undefined_table).
	q.execErr = fmt.Errorf(`ERROR: relation "outbox_events" does not exist (SQLSTATE 42P01)`)
	q.execErrOnIdx = 0
	repo := NewOutboxRepo(newStubTxRunner(q))

	err := repo.Preflight(context.Background())
	if !errors.Is(err, ErrOutboxTableMissing) {
		t.Fatalf("expected ErrOutboxTableMissing, got %v", err)
	}
	// Hint to operator should be in the error message.
	if got := err.Error(); !strings.Contains(got, "0002_outbox.up.sql") {
		t.Fatalf("expected migration hint in error message, got %q", got)
	}
}

func TestOutboxRepo_Preflight_TableMissing_AlternateMessage(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	// pgx sometimes wraps without the SQLSTATE prefix; the "does not exist"
	// substring is the cross-driver-stable signal.
	q.execErr = errors.New(`pq: relation "outbox_events" does not exist`)
	q.execErrOnIdx = 0
	repo := NewOutboxRepo(newStubTxRunner(q))

	if err := repo.Preflight(context.Background()); !errors.Is(err, ErrOutboxTableMissing) {
		t.Fatalf("expected ErrOutboxTableMissing on 'does not exist' substring, got %v", err)
	}
}

func TestOutboxRepo_Preflight_GenericPgError_WrappedNotMissing(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	// A transient connection failure — NOT the missing-table sentinel.
	q.execErr = errors.New("connection refused")
	q.execErrOnIdx = 0
	repo := NewOutboxRepo(newStubTxRunner(q))

	err := repo.Preflight(context.Background())
	if err == nil {
		t.Fatalf("expected error for connection-refused")
	}
	if errors.Is(err, ErrOutboxTableMissing) {
		t.Fatalf("connection error must NOT collapse to ErrOutboxTableMissing")
	}
}

func TestOutboxRepo_Preflight_NilRepo(t *testing.T) {
	t.Parallel()
	var repo *OutboxRepo // nil tx
	if err := repo.Preflight(context.Background()); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented on nil tx, got %v", err)
	}
}
