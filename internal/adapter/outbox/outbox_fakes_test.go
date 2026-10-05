// outbox_fakes_test.go — hermetic stubs for pg.TxRunner / pg.Querier used
// by outbox_coverage_test.go. No live Postgres required.
package outbox

import (
	"context"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
)

// claimedRowSpec describes one row served by the fake Claim query.
type claimedRowSpec struct {
	eventID, aggType, aggID, tenantID, topic string
	createdAt                                time.Time
	attempts                                 int
	lastErr                                  string
}

// claimedRowScanner returns a Scan func matching scanOutboxRow's field order.
func claimedRowScanner(spec claimedRowSpec) func(dest ...any) error {
	var lastErr *string
	if spec.lastErr != "" {
		lastErr = &spec.lastErr
	}
	row := struct {
		eventID, aggType, aggID, tenantID, topic string
		payload                                  []byte
		traceparent, tracestate                  *string
		idempotencyKey                           string
		schemaVersion                            int32
		createdAt                                time.Time
		attempts                                 int
		lastErr                                  *string
	}{
		eventID:        spec.eventID,
		aggType:        spec.aggType,
		aggID:          spec.aggID,
		tenantID:       spec.tenantID,
		topic:          spec.topic,
		payload:        []byte(`{"test":true}`),
		idempotencyKey: "idem-" + spec.eventID,
		schemaVersion:  1,
		createdAt:      spec.createdAt,
		attempts:       spec.attempts,
		lastErr:        lastErr,
	}
	return func(dest ...any) error {
		vals := []any{
			row.eventID, row.aggType, row.aggID, row.tenantID, row.topic,
			row.payload, row.traceparent, row.tracestate, row.idempotencyKey,
			row.schemaVersion, row.createdAt, row.attempts, row.lastErr,
		}
		for i, d := range dest {
			switch v := vals[i].(type) {
			case string:
				*(d.(*string)) = v
			case []byte:
				*(d.(*[]byte)) = v
			case *string:
				// nil → leaves the destination zero-valued
			case int32:
				*(d.(*int32)) = v
			case int:
				*(d.(*int)) = v
			case time.Time:
				*(d.(*time.Time)) = v
			}
		}
		return nil
	}
}

// stubRows implements pg.Rows over a fixed set of scan funcs.
type stubRows struct {
	idx   int
	scans []func(dest ...any) error
}

func (r *stubRows) Next() bool {
	r.idx++
	return r.idx <= len(r.scans)
}

func (r *stubRows) Scan(dest ...any) error { return r.scans[r.idx-1](dest...) }
func (r *stubRows) Close() error           { return nil }
func (r *stubRows) Err() error             { return nil }

// stubRow implements pg.Row (QueryRow fallback that never returns data).
type stubRow struct{}

func (stubRow) Scan(dest ...any) error { return pg.ErrNotImplemented }

// stubQuerier implements pg.Querier.
type stubQuerier struct {
	queryErr error
	claimR   *stubRows

	execs            []string
	inserts          []pg.OutboxRow
	markedDispatched []string
	markedRetry      []string
	execErr          error
}

func (s *stubQuerier) Exec(_ context.Context, sql string, args ...any) (rls.CommandTag, error) {
	s.execs = append(s.execs, sql)
	switch {
	case sql == pg.SQLInsertOutboxEvent:
		s.inserts = append(s.inserts, pg.OutboxRow{EventID: args[0].(string)})
	case sql == pg.SQLMarkOutboxEventDispatched:
		s.markedDispatched = append(s.markedDispatched, args[0].(string))
	case sql == pg.SQLMarkOutboxEventRetry:
		s.markedRetry = append(s.markedRetry, args[0].(string))
	}
	return rls.CommandTag{}, s.execErr
}

func (s *stubQuerier) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return s.claimR, nil
}

func (s *stubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	return stubRow{}
}

// stubTxRunner implements pg.TxRunner, routing the fake OutboxRepo calls.
type stubTxRunner struct {
	q         *stubQuerier
	queryErr  error
	claimRows []func(dest ...any) error
}

func (t *stubTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q pg.Querier) error) error {
	if t.queryErr != nil {
		return t.queryErr
	}
	t.q.queryErr = t.queryErr
	t.q.claimR = &stubRows{scans: t.claimRows}
	return fn(ctx, t.q)
}

// fakeOutboxRepo is a *pg.OutboxRepo wired to hermetic stubs, exposing the
// stubQuerier for inline inspection.
type fakeOutboxRepo struct {
	*pg.OutboxRepo
	tx *stubTxRunner
}

func newFakeOutboxRepo() *fakeOutboxRepo {
	q := &stubQuerier{}
	tx := &stubTxRunner{q: q}
	return &fakeOutboxRepo{OutboxRepo: pg.NewOutboxRepo(tx), tx: tx}
}

// scanOutboxRowValues builds a Claim row scanner from positional values.
func scanOutboxRowValues(eventID, aggType, aggID, tenantID, topic string, createdAt time.Time) func(dest ...any) error {
	return claimedRowScanner(claimedRowSpec{
		eventID: eventID, aggType: aggType, aggID: aggID,
		tenantID: tenantID, topic: topic, createdAt: createdAt,
	})
}

// scanOutboxRowValuesFinal is scanOutboxRowValues with a specific
// dispatch_attempts count (for the exhausted-attempts path).
func scanOutboxRowValuesFinal(attempts int) func(dest ...any) error {
	return claimedRowScanner(claimedRowSpec{
		eventID: "evt-1", aggType: "course_purchase", aggID: "purchase-1",
		tenantID: "t-1", topic: "chora.payments.course_purchase.payment_captured.v1",
		attempts: attempts,
	})
}
