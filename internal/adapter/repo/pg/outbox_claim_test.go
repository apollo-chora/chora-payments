package pg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestOutboxRepo_Claim_AtomicallyReservesRows is the RED-first guard for the
// outbox double-dispatch fix (CHO-1615), chora-payments variant.
//
// Claim previously ran SQLClaimUndispatchedOutboxEvents — a bare
// `SELECT ... FOR UPDATE SKIP LOCKED` — inside r.tx.RunInTx, which begins AND
// commits its own transaction. The row locks therefore released the instant
// Claim returned, even though the method's doc comment said "Caller MUST commit
// the same TX after MarkDispatched". The dispatcher then published +
// MarkDispatched in separate calls, so a co-dispatching payments-dev pod (or an
// HPA replica) re-claimed the still-`dispatched_at IS NULL` rows and
// double-published every event.
//
// The claim must now durably RESERVE the rows it returns by bumping
// next_retry_at into the future inside a claiming UPDATE ... RETURNING, gated by
// the existing `next_retry_at <= now` predicate so a concurrent claim excludes
// just-reserved rows. The reservation window doubles as the crashed-worker
// reaper. No migration — next_retry_at + dispatched_at already exist.
func TestOutboxRepo_Claim_AtomicallyReservesRows(t *testing.T) {
	q := newStubQuerier() // nextN defaults to 0 → serves no rows, no scan needed
	repo := NewOutboxRepo(newStubTxRunner(q))

	rows, err := repo.Claim(context.Background(), 10, fixedTime())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %d; want 0 (stub serves none)", len(rows))
	}
	if len(q.queries) != 1 {
		t.Fatalf("recorded queries = %d; want 1", len(q.queries))
	}
	sql := q.queries[0].sql
	for _, want := range []string{
		"UPDATE outbox_events", // claim, not a bare SELECT
		"SET next_retry_at",    // reserve the row
		"FOR UPDATE SKIP LOCKED",
		"RETURNING",
		"dispatched_at IS NULL",
		"next_retry_at <=", // reservation-window predicate (also the reaper)
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("Claim SQL missing %q; got:\n%s", want, sql)
		}
	}
	args := q.queries[0].args
	if len(args) != 3 {
		t.Fatalf("Claim args = %v; want 3 (limit, now, reservationSeconds)", args)
	}
	if args[0] != 10 {
		t.Errorf("args[0] = %v; want limit 10", args[0])
	}
	if resv, ok := args[2].(int); !ok || resv <= 0 {
		t.Errorf("args[2] = %v; want positive reservation seconds", args[2])
	}
}

// TestOutboxRepo_Claim_ServesFullRows exercises scanOutboxRow's nullable
// pointer mapping (traceparent / tracestate / last_error) plus the
// limit-default branch.
func TestOutboxRepo_Claim_ServesFullRows(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.nextN = 2
	q.rowsScan = func(idx int, dest ...any) error {
		now := fixedTime()
		*(dest[0].(*string)) = "evt-full"
		*(dest[1].(*string)) = "course_purchase"
		*(dest[2].(*string)) = "p-1"
		*(dest[3].(*string)) = testTenantID
		*(dest[4].(*string)) = "chora.payments.course_purchase.payment_captured.v1"
		*(dest[5].(*[]byte)) = []byte(`{"x":1}`)
		if idx == 0 {
			tp := "00-trace-span-01"
			*(dest[6].(**string)) = &tp
			te := "congo=t61rcWkgMzE"
			*(dest[7].(**string)) = &te
			le := "boom"
			*(dest[12].(**string)) = &le
		}
		*(dest[8].(*string)) = "idem-1"
		*(dest[9].(*int32)) = 1
		*(dest[10].(*time.Time)) = now
		*(dest[11].(*int)) = 2
		return nil
	}
	repo := NewOutboxRepo(newStubTxRunner(q))

	// limit <= 0 → default 50.
	rows, err := repo.Claim(context.Background(), 0, fixedTime())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d, want 2", len(rows))
	}
	full := rows[0]
	if full.Traceparent != "00-trace-span-01" || full.Tracestate != "congo=t61rcWkgMzE" {
		t.Errorf("trace fields: %+v", full)
	}
	if full.LastError != "boom" {
		t.Errorf("LastError=%q", full.LastError)
	}
	if full.SchemaVersion != 1 || full.DispatchAttempts != 2 || full.TenantID != testTenantID {
		t.Errorf("full=%+v", full)
	}
	row1 := rows[1]
	if row1.Traceparent != "" || row1.Tracestate != "" || row1.LastError != "" {
		t.Errorf("row1 pointer fields not nil: %+v", row1)
	}
}

// TestOutboxRepo_Claim_ScanErrorPropagates covers the mid-iteration scan
// failure path.
func TestOutboxRepo_Claim_ScanErrorPropagates(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.nextN = 1
	q.rowsScan = func(idx int, dest ...any) error {
		return errors.New("bad column")
	}
	repo := NewOutboxRepo(newStubTxRunner(q))
	if _, err := repo.Claim(context.Background(), 5, fixedTime()); err == nil {
		t.Error("scan error should propagate")
	}
}
