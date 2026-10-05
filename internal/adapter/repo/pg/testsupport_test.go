// testsupport_test.go — hermetic stubs for Querier + TxRunner used by
// the per-repo RED→GREEN tests in this package.
//
// The stubs assert the SQL shape (which template was emitted) + the arg
// count + the RLS-apply ordering (the first Exec MUST be the SET LOCAL
// chora.tenant_id) before allowing the user query through.
//
// Tests run hermetic — no live Postgres required.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
)

// -----------------------------------------------------------------------------
// stubQuerier
// -----------------------------------------------------------------------------

// stubQuerier records every Exec / QueryRow / Query call so tests can
// assert on the order + SQL shape + args.
//
// QueryRow returns a stubRow seeded from rowScan; Query returns a
// stubRows seeded from rowsScan. Tests can override per-call.
type stubQuerier struct {
	execs   []recordedCall
	queries []recordedCall

	// rowScan is invoked when QueryRow().Scan(...) is called.
	rowScan func(dest ...any) error

	// rowsScan is the per-row scan invoked for Query iteration. nextN is
	// the row count the stub serves.
	rowsScan func(idx int, dest ...any) error
	nextN    int

	// execErr triggers on the Nth Exec call (0-indexed). Use to simulate
	// SQL failures mid-transaction.
	execErr      error
	execErrOnIdx int
}

type recordedCall struct {
	sql  string
	args []any
}

func newStubQuerier() *stubQuerier {
	return &stubQuerier{execErrOnIdx: -1}
}

func (s *stubQuerier) Exec(_ context.Context, sql string, args ...any) (rls.CommandTag, error) {
	s.execs = append(s.execs, recordedCall{sql: sql, args: args})
	if s.execErrOnIdx >= 0 && len(s.execs)-1 == s.execErrOnIdx {
		return rls.CommandTag{}, s.execErr
	}
	return rls.CommandTag{RowsAffected: 1}, nil
}

func (s *stubQuerier) QueryRow(_ context.Context, sql string, args ...any) Row {
	s.queries = append(s.queries, recordedCall{sql: sql, args: args})
	return &stubRow{scan: s.rowScan}
}

func (s *stubQuerier) Query(_ context.Context, sql string, args ...any) (Rows, error) {
	s.queries = append(s.queries, recordedCall{sql: sql, args: args})
	if s.rowsScan == nil {
		return &stubRows{}, nil
	}
	return &stubRows{
		nextN: s.nextN,
		scan:  s.rowsScan,
	}, nil
}

type stubRow struct {
	scan func(dest ...any) error
}

func (r *stubRow) Scan(dest ...any) error {
	if r.scan == nil {
		return errors.New("stubRow: no scan func wired")
	}
	return r.scan(dest...)
}

type stubRows struct {
	nextN int
	idx   int
	scan  func(idx int, dest ...any) error
}

func (r *stubRows) Next() bool {
	if r.idx >= r.nextN {
		return false
	}
	return true
}

func (r *stubRows) Scan(dest ...any) error {
	if r.scan == nil {
		return errors.New("stubRows: no scan func wired")
	}
	err := r.scan(r.idx, dest...)
	r.idx++
	return err
}

func (r *stubRows) Close() error { return nil }
func (r *stubRows) Err() error   { return nil }

// -----------------------------------------------------------------------------
// stubTxRunner
// -----------------------------------------------------------------------------

// stubTxRunner runs fn against an injected stubQuerier and records it so
// tests can inspect the recorded calls.
type stubTxRunner struct {
	q   *stubQuerier
	err error
}

func newStubTxRunner(q *stubQuerier) *stubTxRunner {
	return &stubTxRunner{q: q}
}

func (t *stubTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q Querier) error) error {
	if t.err != nil {
		return t.err
	}
	return fn(ctx, t.q)
}

// -----------------------------------------------------------------------------
// helpers — RLS assertions used by every pg repo test in this package.
// -----------------------------------------------------------------------------

// assertRLSApplied verifies that the first recorded Exec on q is the
// SET LOCAL chora.tenant_id emitted by rls.ApplySession. This is the
// canonical pre-flight check for every pg repo method.
func assertRLSApplied(t testingT, q *stubQuerier, wantTenant string) {
	t.Helper()
	if len(q.execs) == 0 {
		t.Fatalf("no Exec calls recorded — rls.ApplySession must run first")
	}
	first := q.execs[0]
	if !strings.HasPrefix(first.sql, "SET LOCAL chora.tenant_id") {
		t.Fatalf("first Exec sql = %q; want SET LOCAL chora.tenant_id ...", first.sql)
	}
	if !strings.Contains(first.sql, wantTenant) {
		t.Fatalf("first Exec sql %q does not include tenant_id %q", first.sql, wantTenant)
	}
}

// withTenant returns a ctx with the supplied tenant_id installed (via
// the chora-go-common tracing helper).
func withTenant(ctx context.Context, tenantID string) context.Context {
	return tracing.WithTenantID(ctx, tenantID)
}

// testingT is the minimal subset of *testing.T we use in assertion
// helpers. Mirrors stdlib testing.TB without pulling stretchr testify in.
type testingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// fmtError is a tiny helper for hand-assembling test failure messages.
func fmtError(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

// fixedTime returns a deterministic timestamp for use in test fixtures.
func fixedTime() time.Time {
	return time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
}
