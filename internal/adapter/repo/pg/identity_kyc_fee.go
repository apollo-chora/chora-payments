// identity_kyc_fee.go — Postgres adapter for IdentityKycFee (7th aggregate).
//
// SCHEMA: migrations/0006_identity_kyc_fees.up.sql.
// Mirrors application_payment.go (canonical pattern) — simple one-time
// charge with the canonical shared 5-state FSM.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const identityKycFeeCols = `
    purchase_id, tenant_id, learner_gcid, kyc_doc_type,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at, updated_at
`

const SQLUpsertIdentityKycFee = `
INSERT INTO identity_kyc_fees (` + identityKycFeeCols + `) VALUES (
    $1, $2, $3, $4,
    $5,
    $6, $7, $8, $9,
    $10, $11, $12,
    $13, $14, $15, $16,
    $17,
    $18, $19, $20, $21, $22,
    $23, $24
)
ON CONFLICT (purchase_id) DO UPDATE SET
    state                    = EXCLUDED.state,
    amount_cents_paid        = EXCLUDED.amount_cents_paid,
    amount_cents_refunded    = EXCLUDED.amount_cents_refunded,
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

const SQLSelectIdentityKycFeeByID = `
SELECT ` + identityKycFeeCols + `
FROM identity_kyc_fees
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectIdentityKycFeeBySession = `
SELECT ` + identityKycFeeCols + `
FROM identity_kyc_fees
WHERE stripe_session_id = $1
`

type IdentityKycFeeRepo struct {
	tx TxRunner
}

func NewIdentityKycFeeRepo(tx TxRunner) *IdentityKycFeeRepo {
	return &IdentityKycFeeRepo{tx: tx}
}

func (r *IdentityKycFeeRepo) Save(ctx context.Context, k *kyc.IdentityKycFee) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if k == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertIdentityKycFee,
			k.PurchaseID, k.TenantID, k.LearnerGCID, k.KYCDocType,
			string(k.State),
			k.AmountCents, k.AmountCentsPaid, k.AmountCentsRefunded, k.Currency,
			k.StripeSessionID, nullStr(k.StripePaymentIntentID), nullStr(k.StripeChargeID),
			nullStr(k.StripeRefundID), nullStr(k.StripeCheckoutURL),
			nullStr(k.StripeFailureCode), nullStr(k.StripeFailureMessage),
			nullStr(string(k.RefundReason)),
			k.CheckoutStartedAt, nullTime(deref(k.PaidAt)),
			nullTime(deref(k.FailedAt)), nullTime(deref(k.RefundedAt)),
			nullTime(deref(k.ExpiredAt)),
			k.CreatedAt, k.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert identity_kyc_fee: %w", err)
		}
		return nil
	})
}

func (r *IdentityKycFeeRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*kyc.IdentityKycFee, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *kyc.IdentityKycFee
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectIdentityKycFeeByID, purchaseID, tenantID)
		k, scanErr := scanIdentityKycFee(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = k
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *IdentityKycFeeRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*kyc.IdentityKycFee, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *kyc.IdentityKycFee
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectIdentityKycFeeBySession, sessID)
		k, scanErr := scanIdentityKycFee(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = k
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

var _ kyc.Repo = (*IdentityKycFeeRepo)(nil)

func scanIdentityKycFee(scan func(...any) error) (*kyc.IdentityKycFee, error) {
	var (
		purchaseID, tenantID, learnerGCID, kycDocType     string
		state                                             string
		amountCents, amountCentsPaid, amountCentsRefunded int64
		currency                                          string
		sessionID                                         string
		paymentIntent, chargeID, refundID, checkoutURL    *string
		failureCode, failureMessage                       *string
		refundReason                                      *string
		checkoutStartedAt                                 time.Time
		paidAt, failedAt, refundedAt, expiredAt           *time.Time
		createdAt, updatedAt                              time.Time
	)
	if err := scan(
		&purchaseID, &tenantID, &learnerGCID, &kycDocType,
		&state,
		&amountCents, &amountCentsPaid, &amountCentsRefunded, &currency,
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
	k := &kyc.IdentityKycFee{
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
		KYCDocType: kycDocType,
	}
	if paymentIntent != nil {
		k.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		k.StripeChargeID = *chargeID
	}
	if refundID != nil {
		k.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		k.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		k.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		k.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		k.RefundReason = shared.RefundReason(*refundReason)
	}
	return k, nil
}
