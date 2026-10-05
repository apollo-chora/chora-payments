// application_payment.go — Postgres adapter for ApplicationPayment.
//
// SCHEMA: see migrations/0001_initial.up.sql (`application_payments` table).
//
// Cross-DB queries FORBIDDEN — only reads chora_payments.application_payments.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const applicationPaymentCols = `
    purchase_id, tenant_id, learner_gcid, application_id, course_id,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at, updated_at
`

const SQLUpsertApplicationPayment = `
INSERT INTO application_payments (` + applicationPaymentCols + `) VALUES (
    $1, $2, $3, $4, $5,
    $6,
    $7, $8, $9, $10,
    $11, $12, $13,
    $14, $15, $16, $17,
    $18,
    $19, $20, $21, $22, $23,
    $24, $25
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

const SQLSelectApplicationPaymentByID = `
SELECT ` + applicationPaymentCols + `
FROM application_payments
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectApplicationPaymentBySession = `
SELECT ` + applicationPaymentCols + `
FROM application_payments
WHERE stripe_session_id = $1
`

type ApplicationPaymentRepo struct {
	tx TxRunner
}

func NewApplicationPaymentRepo(tx TxRunner) *ApplicationPaymentRepo {
	return &ApplicationPaymentRepo{tx: tx}
}

func (r *ApplicationPaymentRepo) Save(ctx context.Context, ap *apppay.ApplicationPayment) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if ap == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertApplicationPayment,
			ap.PurchaseID, ap.TenantID, ap.LearnerGCID, ap.ApplicationID, ap.CourseID,
			string(ap.State),
			ap.AmountCents, ap.AmountCentsPaid, ap.AmountCentsRefunded, ap.Currency,
			ap.StripeSessionID, nullStr(ap.StripePaymentIntentID), nullStr(ap.StripeChargeID),
			nullStr(ap.StripeRefundID), nullStr(ap.StripeCheckoutURL),
			nullStr(ap.StripeFailureCode), nullStr(ap.StripeFailureMessage),
			nullStr(string(ap.RefundReason)),
			ap.CheckoutStartedAt, nullTime(deref(ap.PaidAt)),
			nullTime(deref(ap.FailedAt)), nullTime(deref(ap.RefundedAt)),
			nullTime(deref(ap.ExpiredAt)),
			ap.CreatedAt, ap.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert application_payment: %w", err)
		}
		return nil
	})
}

func (r *ApplicationPaymentRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*apppay.ApplicationPayment, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *apppay.ApplicationPayment
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectApplicationPaymentByID, purchaseID, tenantID)
		ap, scanErr := scanApplicationPayment(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = ap
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *ApplicationPaymentRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*apppay.ApplicationPayment, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *apppay.ApplicationPayment
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectApplicationPaymentBySession, sessID)
		ap, scanErr := scanApplicationPayment(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = ap
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

var _ apppay.Repo = (*ApplicationPaymentRepo)(nil)

func scanApplicationPayment(scan func(...any) error) (*apppay.ApplicationPayment, error) {
	var (
		purchaseID, tenantID, learnerGCID, applicationID, courseID string
		state                                                      string
		amountCents, amountCentsPaid, amountCentsRefunded          int64
		currency                                                   string
		sessionID                                                  string
		paymentIntent, chargeID, refundID, checkoutURL             *string
		failureCode, failureMessage                                *string
		refundReason                                               *string
		checkoutStartedAt                                          time.Time
		paidAt, failedAt, refundedAt, expiredAt                    *time.Time
		createdAt, updatedAt                                       time.Time
	)
	if err := scan(
		&purchaseID, &tenantID, &learnerGCID, &applicationID, &courseID,
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
	ap := &apppay.ApplicationPayment{
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
		ApplicationID: applicationID,
		CourseID:      courseID,
	}
	if paymentIntent != nil {
		ap.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		ap.StripeChargeID = *chargeID
	}
	if refundID != nil {
		ap.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		ap.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		ap.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		ap.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		ap.RefundReason = shared.RefundReason(*refundReason)
	}
	return ap, nil
}
