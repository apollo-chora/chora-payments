// tenant_mana_topup.go — Postgres adapter for TenantManaTopUp.
//
// SCHEMA: migrations/0001_initial.up.sql (`tenant_mana_topups`).
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
)

const tenantManaTopUpCols = `
    purchase_id, tenant_id, admin_gcid, sku,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    mana_units, mana_units_debited,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at, updated_at
`

const SQLUpsertTenantManaTopUp = `
INSERT INTO tenant_mana_topups (` + tenantManaTopUpCols + `) VALUES (
    $1, $2, $3, $4,
    $5,
    $6, $7, $8, $9,
    $10, $11,
    $12, $13, $14,
    $15, $16, $17, $18,
    $19,
    $20, $21, $22, $23, $24,
    $25, $26
)
ON CONFLICT (purchase_id) DO UPDATE SET
    state                    = EXCLUDED.state,
    amount_cents_paid        = EXCLUDED.amount_cents_paid,
    amount_cents_refunded    = EXCLUDED.amount_cents_refunded,
    mana_units_debited       = EXCLUDED.mana_units_debited,
    stripe_payment_intent_id = EXCLUDED.stripe_payment_intent_id,
    stripe_charge_id         = EXCLUDED.stripe_charge_id,
    stripe_refund_id         = EXCLUDED.stripe_refund_id,
    stripe_failure_code      = EXCLUDED.stripe_failure_code,
    stripe_failure_message   = EXCLUDED.stripe_failure_message,
    refund_reason            = EXCLUDED.refund_reason,
    paid_at                  = EXCLUDED.paid_at,
    failed_at                = EXCLUDED.failed_at,
    refunded_at              = EXCLUDED.refunded_at,
    expired_at               = EXCLUDED.expired_at,
    updated_at               = EXCLUDED.updated_at
`

const SQLSelectTenantManaTopUpByID = `
SELECT ` + tenantManaTopUpCols + `
FROM tenant_mana_topups
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectTenantManaTopUpBySession = `
SELECT ` + tenantManaTopUpCols + `
FROM tenant_mana_topups
WHERE stripe_session_id = $1
`

type TenantManaTopUpRepo struct {
	tx TxRunner
}

func NewTenantManaTopUpRepo(tx TxRunner) *TenantManaTopUpRepo {
	return &TenantManaTopUpRepo{tx: tx}
}

func (r *TenantManaTopUpRepo) Save(ctx context.Context, m *mana.TenantManaTopUp) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if m == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertTenantManaTopUp,
			m.PurchaseID, m.TenantID, m.LearnerGCID, m.SKU,
			string(m.State),
			m.AmountCents, m.AmountCentsPaid, m.AmountCentsRefunded, m.Currency,
			m.ManaUnits, m.ManaUnitsDebited,
			m.StripeSessionID, nullStr(m.StripePaymentIntentID), nullStr(m.StripeChargeID),
			nullStr(m.StripeRefundID), nullStr(m.StripeCheckoutURL),
			nullStr(m.StripeFailureCode), nullStr(m.StripeFailureMessage),
			nullStr(string(m.RefundReason)),
			m.CheckoutStartedAt, nullTime(deref(m.PaidAt)),
			nullTime(deref(m.FailedAt)), nullTime(deref(m.RefundedAt)),
			nullTime(deref(m.ExpiredAt)),
			m.CreatedAt, m.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert tenant_mana_topup: %w", err)
		}
		return nil
	})
}

func (r *TenantManaTopUpRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*mana.TenantManaTopUp, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *mana.TenantManaTopUp
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantManaTopUpByID, purchaseID, tenantID)
		m, scanErr := scanTenantManaTopUp(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *TenantManaTopUpRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*mana.TenantManaTopUp, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *mana.TenantManaTopUp
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantManaTopUpBySession, sessID)
		m, scanErr := scanTenantManaTopUp(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

var _ mana.Repo = (*TenantManaTopUpRepo)(nil)

func scanTenantManaTopUp(scan func(...any) error) (*mana.TenantManaTopUp, error) {
	var (
		purchaseID, tenantID, adminGCID, sku              string
		state                                             string
		amountCents, amountCentsPaid, amountCentsRefunded int64
		currency                                          string
		manaUnits, manaUnitsDebited                       int64
		sessionID                                         string
		paymentIntent, chargeID, refundID, checkoutURL    *string
		failureCode, failureMessage                       *string
		refundReason                                      *string
		checkoutStartedAt                                 time.Time
		paidAt, failedAt, refundedAt, expiredAt           *time.Time
		createdAt, updatedAt                              time.Time
	)
	if err := scan(
		&purchaseID, &tenantID, &adminGCID, &sku,
		&state,
		&amountCents, &amountCentsPaid, &amountCentsRefunded, &currency,
		&manaUnits, &manaUnitsDebited,
		&sessionID, &paymentIntent, &chargeID,
		&refundID, &checkoutURL, &failureCode, &failureMessage,
		&refundReason,
		&checkoutStartedAt, &paidAt, &failedAt, &refundedAt, &expiredAt,
		&createdAt, &updatedAt,
	); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	m := &mana.TenantManaTopUp{
		Purchase: shared.Purchase{
			PurchaseID:          purchaseID,
			TenantID:            tenantID,
			LearnerGCID:         adminGCID,
			State:               shared.State(state),
			AmountCents:         amountCents,
			AmountCentsPaid:     amountCentsPaid,
			AmountCentsRefunded: amountCentsRefunded,
			Currency:            currency,
			StripeSessionID:     sessionID,
			CheckoutStartedAt:   checkoutStartedAt.UTC(),
			CreatedAt:           createdAt.UTC(),
			UpdatedAt:           updatedAt.UTC(),
			PaidAt:              timeUTC(paidAt),
			FailedAt:            timeUTC(failedAt),
			RefundedAt:          timeUTC(refundedAt),
			ExpiredAt:           timeUTC(expiredAt),
		},
		SKU:              sku,
		ManaUnits:        manaUnits,
		ManaUnitsDebited: manaUnitsDebited,
	}
	if paymentIntent != nil {
		m.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		m.StripeChargeID = *chargeID
	}
	if refundID != nil {
		m.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		m.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		m.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		m.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		m.RefundReason = shared.RefundReason(*refundReason)
	}
	return m, nil
}
