// user_mana_topup.go — Postgres adapter for UserManaTopUp (6th aggregate).
//
// SCHEMA: migrations/0005_user_mana_topups.up.sql.
// Mirrors tenant_mana_topup.go (canonical pattern) with admin_gcid →
// learner_gcid swap.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
)

const userManaTopUpCols = `
    purchase_id, tenant_id, learner_gcid, sku,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    mana_units, mana_units_debited,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at, updated_at
`

const SQLUpsertUserManaTopUp = `
INSERT INTO user_mana_topups (` + userManaTopUpCols + `) VALUES (
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

const SQLSelectUserManaTopUpByID = `
SELECT ` + userManaTopUpCols + `
FROM user_mana_topups
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectUserManaTopUpBySession = `
SELECT ` + userManaTopUpCols + `
FROM user_mana_topups
WHERE stripe_session_id = $1
`

type UserManaTopUpRepo struct {
	tx TxRunner
}

func NewUserManaTopUpRepo(tx TxRunner) *UserManaTopUpRepo {
	return &UserManaTopUpRepo{tx: tx}
}

func (r *UserManaTopUpRepo) Save(ctx context.Context, u *umt.UserManaTopUp) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if u == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertUserManaTopUp,
			u.PurchaseID, u.TenantID, u.LearnerGCID, u.SKU,
			string(u.State),
			u.AmountCents, u.AmountCentsPaid, u.AmountCentsRefunded, u.Currency,
			u.ManaUnits, u.ManaUnitsDebited,
			u.StripeSessionID, nullStr(u.StripePaymentIntentID), nullStr(u.StripeChargeID),
			nullStr(u.StripeRefundID), nullStr(u.StripeCheckoutURL),
			nullStr(u.StripeFailureCode), nullStr(u.StripeFailureMessage),
			nullStr(string(u.RefundReason)),
			u.CheckoutStartedAt, nullTime(deref(u.PaidAt)),
			nullTime(deref(u.FailedAt)), nullTime(deref(u.RefundedAt)),
			nullTime(deref(u.ExpiredAt)),
			u.CreatedAt, u.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert user_mana_topup: %w", err)
		}
		return nil
	})
}

func (r *UserManaTopUpRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*umt.UserManaTopUp, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *umt.UserManaTopUp
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectUserManaTopUpByID, purchaseID, tenantID)
		u, scanErr := scanUserManaTopUp(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = u
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *UserManaTopUpRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*umt.UserManaTopUp, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *umt.UserManaTopUp
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectUserManaTopUpBySession, sessID)
		u, scanErr := scanUserManaTopUp(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = u
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

var _ umt.Repo = (*UserManaTopUpRepo)(nil)

func scanUserManaTopUp(scan func(...any) error) (*umt.UserManaTopUp, error) {
	var (
		purchaseID, tenantID, learnerGCID, sku            string
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
		&purchaseID, &tenantID, &learnerGCID, &sku,
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
	u := &umt.UserManaTopUp{
		Purchase: shared.Purchase{
			PurchaseID:          purchaseID,
			TenantID:            tenantID,
			LearnerGCID:         learnerGCID,
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
		u.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		u.StripeChargeID = *chargeID
	}
	if refundID != nil {
		u.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		u.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		u.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		u.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		u.RefundReason = shared.RefundReason(*refundReason)
	}
	return u, nil
}
