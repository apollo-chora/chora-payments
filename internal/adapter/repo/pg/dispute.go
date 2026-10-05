// dispute.go — Postgres adapter for Dispute (cross-aggregate chargeback
// record, ADR-164 §229.5).
//
// SCHEMA: migrations/0007_disputes.up.sql.
//
// Unlike the Purchase aggregates this adapter does NOT use ON CONFLICT
// for Insert — Insert is the create-once path and a UNIQUE violation on
// stripe_dispute_id is meaningful (Stripe redelivery). Save is a separate
// UPSERT keyed on dispute_id for state + funds-movement updates.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const disputeCols = `
    dispute_id, tenant_id, aggregate_type, purchase_id,
    stripe_dispute_id, stripe_charge_id,
    state, outcome,
    amount_cents, currency,
    reason,
    evidence_due_by,
    funds_withdrawn, funds_withdrawn_at,
    funds_reinstated, funds_reinstated_at,
    raised_at, closed_at,
    created_at, updated_at
`

const SQLInsertDispute = `
INSERT INTO disputes (` + disputeCols + `) VALUES (
    $1, $2, $3, $4,
    $5, $6,
    $7, $8,
    $9, $10,
    $11,
    $12,
    $13, $14,
    $15, $16,
    $17, $18,
    $19, $20
)
`

const SQLUpsertDispute = `
INSERT INTO disputes (` + disputeCols + `) VALUES (
    $1, $2, $3, $4,
    $5, $6,
    $7, $8,
    $9, $10,
    $11,
    $12,
    $13, $14,
    $15, $16,
    $17, $18,
    $19, $20
)
ON CONFLICT (dispute_id) DO UPDATE SET
    state                = EXCLUDED.state,
    outcome              = EXCLUDED.outcome,
    funds_withdrawn      = EXCLUDED.funds_withdrawn,
    funds_withdrawn_at   = EXCLUDED.funds_withdrawn_at,
    funds_reinstated     = EXCLUDED.funds_reinstated,
    funds_reinstated_at  = EXCLUDED.funds_reinstated_at,
    closed_at            = EXCLUDED.closed_at,
    updated_at           = EXCLUDED.updated_at
`

const SQLSelectDisputeByStripeID = `
SELECT ` + disputeCols + `
FROM disputes
WHERE stripe_dispute_id = $1
`

const SQLSelectDisputeByID = `
SELECT ` + disputeCols + `
FROM disputes
WHERE dispute_id = $1
  AND tenant_id = $2
`

const SQLListDisputesByPurchase = `
SELECT ` + disputeCols + `
FROM disputes
WHERE tenant_id = $1
  AND aggregate_type = $2
  AND purchase_id = $3
ORDER BY raised_at DESC
`

type DisputeRepo struct {
	tx TxRunner
}

func NewDisputeRepo(tx TxRunner) *DisputeRepo {
	return &DisputeRepo{tx: tx}
}

func (r *DisputeRepo) Insert(ctx context.Context, d *dispute.Dispute) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if d == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLInsertDispute,
			d.DisputeID, d.TenantID, string(d.AggregateType), d.PurchaseID,
			d.StripeDisputeID, d.StripeChargeID,
			string(d.State), nullStr(string(d.Outcome)),
			d.AmountCents, d.Currency,
			string(d.Reason),
			d.EvidenceDueBy,
			d.FundsWithdrawn, nullTime(deref(d.FundsWithdrawnAt)),
			d.FundsReinstated, nullTime(deref(d.FundsReinstatedAt)),
			d.RaisedAt, nullTime(deref(d.ClosedAt)),
			d.CreatedAt, d.UpdatedAt,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return dispute.ErrAlreadyExists
			}
			return fmt.Errorf("pg: insert dispute: %w", err)
		}
		return nil
	})
}

func (r *DisputeRepo) Save(ctx context.Context, d *dispute.Dispute) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if d == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertDispute,
			d.DisputeID, d.TenantID, string(d.AggregateType), d.PurchaseID,
			d.StripeDisputeID, d.StripeChargeID,
			string(d.State), nullStr(string(d.Outcome)),
			d.AmountCents, d.Currency,
			string(d.Reason),
			d.EvidenceDueBy,
			d.FundsWithdrawn, nullTime(deref(d.FundsWithdrawnAt)),
			d.FundsReinstated, nullTime(deref(d.FundsReinstatedAt)),
			d.RaisedAt, nullTime(deref(d.ClosedAt)),
			d.CreatedAt, d.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert dispute: %w", err)
		}
		return nil
	})
}

func (r *DisputeRepo) GetByStripeDisputeID(ctx context.Context, stripeDisputeID string) (*dispute.Dispute, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if stripeDisputeID == "" {
		return nil, dispute.ErrStripeDisputeRequired
	}
	var found *dispute.Dispute
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		row := q.QueryRow(ctx, SQLSelectDisputeByStripeID, stripeDisputeID)
		d, scanErr := scanDispute(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return dispute.ErrNotFound
			}
			return scanErr
		}
		found = d
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *DisputeRepo) GetByID(ctx context.Context, tenantID, disputeID string) (*dispute.Dispute, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if tenantID == "" {
		return nil, shared.ErrTenantRequired
	}
	var found *dispute.Dispute
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectDisputeByID, disputeID, tenantID)
		d, scanErr := scanDispute(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return dispute.ErrNotFound
			}
			return scanErr
		}
		found = d
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *DisputeRepo) ListByPurchase(ctx context.Context, tenantID string, aggregateType wh.AggregateType, purchaseID string) ([]*dispute.Dispute, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if tenantID == "" {
		return nil, shared.ErrTenantRequired
	}
	if !aggregateType.IsValid() {
		return nil, dispute.ErrAggregateTypeRequired
	}
	var out []*dispute.Dispute
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, SQLListDisputesByPurchase, tenantID, string(aggregateType), purchaseID)
		if err != nil {
			return fmt.Errorf("pg: list disputes by purchase: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			d, scanErr := scanDispute(rows.Scan)
			if scanErr != nil {
				return scanErr
			}
			out = append(out, d)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _ dispute.Repo = (*DisputeRepo)(nil)

func scanDispute(scan func(...any) error) (*dispute.Dispute, error) {
	var (
		disputeID, tenantID, aggregateType, purchaseID string
		stripeDisputeID, stripeChargeID                string
		state                                          string
		outcome                                        *string
		amountCents                                    int64
		currency                                       string
		reason                                         string
		evidenceDueBy                                  time.Time
		fundsWithdrawn                                 bool
		fundsWithdrawnAt                               *time.Time
		fundsReinstated                                bool
		fundsReinstatedAt                              *time.Time
		raisedAt                                       time.Time
		closedAt                                       *time.Time
		createdAt, updatedAt                           time.Time
	)
	if err := scan(
		&disputeID, &tenantID, &aggregateType, &purchaseID,
		&stripeDisputeID, &stripeChargeID,
		&state, &outcome,
		&amountCents, &currency,
		&reason,
		&evidenceDueBy,
		&fundsWithdrawn, &fundsWithdrawnAt,
		&fundsReinstated, &fundsReinstatedAt,
		&raisedAt, &closedAt,
		&createdAt, &updatedAt,
	); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	d := &dispute.Dispute{
		DisputeID:         disputeID,
		TenantID:          tenantID,
		AggregateType:     wh.AggregateType(aggregateType),
		PurchaseID:        purchaseID,
		StripeDisputeID:   stripeDisputeID,
		StripeChargeID:    stripeChargeID,
		State:             dispute.State(state),
		AmountCents:       amountCents,
		Currency:          currency,
		Reason:            dispute.Reason(reason),
		EvidenceDueBy:     evidenceDueBy.UTC(),
		FundsWithdrawn:    fundsWithdrawn,
		FundsWithdrawnAt:  timeUTC(fundsWithdrawnAt),
		FundsReinstated:   fundsReinstated,
		FundsReinstatedAt: timeUTC(fundsReinstatedAt),
		RaisedAt:          raisedAt.UTC(),
		ClosedAt:          timeUTC(closedAt),
		CreatedAt:         createdAt.UTC(),
		UpdatedAt:         updatedAt.UTC(),
	}
	if outcome != nil {
		d.Outcome = dispute.Outcome(*outcome)
	}
	return d, nil
}

// isUniqueViolation is defined in webhook_event.go and shared across
// repos in this package.
