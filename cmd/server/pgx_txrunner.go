// pgx_txrunner.go — production pgxpool-backed adapter for pg.TxRunner.
//
// Mirrors services/chora-delivery/cmd/server/pgx_txrunner.go — the pg
// adapter does NOT import pgx; this binding lives at cmd/server where
// pgxpool is already imported by bootstrap.go.
package main

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-common/rls"
	paymentspg "github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
)

// pgxTxRunner adapts *pgxpool.Pool to paymentspg.TxRunner.
type pgxTxRunner struct {
	pool *pgxpool.Pool
}

func newPgxTxRunner(pool *pgxpool.Pool) *pgxTxRunner {
	if pool == nil {
		return nil
	}
	return &pgxTxRunner{pool: pool}
}

func (t *pgxTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q paymentspg.Querier) error) (err error) {
	if t == nil || t.pool == nil {
		return paymentspg.ErrNotImplemented
	}
	tx, err := t.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
			return
		}
		err = tx.Commit(ctx)
	}()
	return fn(ctx, &pgxQuerier{tx: tx})
}

type pgxQuerier struct {
	tx pgx.Tx
}

func (q *pgxQuerier) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
	tag, err := q.tx.Exec(ctx, sql, args...)
	if err != nil {
		return rls.CommandTag{}, err
	}
	return rls.CommandTag{RowsAffected: tag.RowsAffected()}, nil
}

func (q *pgxQuerier) QueryRow(ctx context.Context, sql string, args ...any) paymentspg.Row {
	return &pgxRow{r: q.tx.QueryRow(ctx, sql, args...)}
}

func (q *pgxQuerier) Query(ctx context.Context, sql string, args ...any) (paymentspg.Rows, error) {
	rs, err := q.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{r: rs}, nil
}

type pgxRow struct{ r pgx.Row }

func (r *pgxRow) Scan(dest ...any) error { return r.r.Scan(dest...) }

type pgxRows struct{ r pgx.Rows }

func (r *pgxRows) Next() bool             { return r.r.Next() }
func (r *pgxRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *pgxRows) Close() error           { r.r.Close(); return nil }
func (r *pgxRows) Err() error             { return r.r.Err() }

var _ paymentspg.TxRunner = (*pgxTxRunner)(nil)
