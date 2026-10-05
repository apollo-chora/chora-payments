// familiar_egg_purchase.go — Postgres adapter for FamiliarEggPurchase.
//
// SCHEMA: migrations/0001_initial.up.sql (`familiar_egg_purchases`).
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const familiarEggCols = `
    purchase_id, tenant_id, learner_gcid, egg_sku, suggested_focal_atom_id,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason, refund_credit_only,
    soft_expiry_at, hard_expiry_at,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at, provisioned_at,
    created_at, updated_at
`

const SQLUpsertFamiliarEggPurchase = `
INSERT INTO familiar_egg_purchases (` + familiarEggCols + `) VALUES (
    $1, $2, $3, $4, $5,
    $6,
    $7, $8, $9, $10,
    $11, $12, $13,
    $14, $15, $16, $17,
    $18, $19,
    $20, $21,
    $22, $23, $24, $25, $26, $27,
    $28, $29
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
    refund_credit_only       = EXCLUDED.refund_credit_only,
    paid_at                  = EXCLUDED.paid_at,
    failed_at                = EXCLUDED.failed_at,
    refunded_at              = EXCLUDED.refunded_at,
    expired_at               = EXCLUDED.expired_at,
    provisioned_at           = EXCLUDED.provisioned_at,
    updated_at               = EXCLUDED.updated_at
`

const SQLSelectFamiliarEggPurchaseByID = `
SELECT ` + familiarEggCols + `
FROM familiar_egg_purchases
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectFamiliarEggPurchaseBySession = `
SELECT ` + familiarEggCols + `
FROM familiar_egg_purchases
WHERE stripe_session_id = $1
`

type FamiliarEggPurchaseRepo struct {
	tx TxRunner
}

func NewFamiliarEggPurchaseRepo(tx TxRunner) *FamiliarEggPurchaseRepo {
	return &FamiliarEggPurchaseRepo{tx: tx}
}

func (r *FamiliarEggPurchaseRepo) Save(ctx context.Context, ep *egg.FamiliarEggPurchase) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if ep == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertFamiliarEggPurchase,
			ep.PurchaseID, ep.TenantID, ep.LearnerGCID, ep.EggSKU, nullStr(ep.SuggestedFocalAtomID),
			string(ep.State),
			ep.AmountCents, ep.AmountCentsPaid, ep.AmountCentsRefunded, ep.Currency,
			ep.StripeSessionID, nullStr(ep.StripePaymentIntentID), nullStr(ep.StripeChargeID),
			nullStr(ep.StripeRefundID), nullStr(ep.StripeCheckoutURL),
			nullStr(ep.StripeFailureCode), nullStr(ep.StripeFailureMessage),
			nullStr(string(ep.RefundReason)), ep.RefundCreditOnly,
			nullTime(deref(ep.SoftExpiryAt)), nullTime(deref(ep.HardExpiryAt)),
			ep.CheckoutStartedAt, nullTime(deref(ep.PaidAt)),
			nullTime(deref(ep.FailedAt)), nullTime(deref(ep.RefundedAt)),
			nullTime(deref(ep.ExpiredAt)), nullTime(deref(ep.ProvisionedAt)),
			ep.CreatedAt, ep.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert familiar_egg_purchase: %w", err)
		}
		return nil
	})
}

func (r *FamiliarEggPurchaseRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*egg.FamiliarEggPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *egg.FamiliarEggPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectFamiliarEggPurchaseByID, purchaseID, tenantID)
		ep, scanErr := scanFamiliarEggPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = ep
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *FamiliarEggPurchaseRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*egg.FamiliarEggPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *egg.FamiliarEggPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectFamiliarEggPurchaseBySession, sessID)
		ep, scanErr := scanFamiliarEggPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = ep
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

var _ egg.Repo = (*FamiliarEggPurchaseRepo)(nil)

func scanFamiliarEggPurchase(scan func(...any) error) (*egg.FamiliarEggPurchase, error) {
	var (
		purchaseID, tenantID, learnerGCID, eggSKU         string
		suggestedFocalAtomID                              *string
		state                                             string
		amountCents, amountCentsPaid, amountCentsRefunded int64
		currency                                          string
		sessionID                                         string
		paymentIntent, chargeID, refundID, checkoutURL    *string
		failureCode, failureMessage                       *string
		refundReason                                      *string
		refundCreditOnly                                  bool
		softExpiryAt, hardExpiryAt                        *time.Time
		checkoutStartedAt                                 time.Time
		paidAt, failedAt, refundedAt, expiredAt           *time.Time
		provisionedAt                                     *time.Time
		createdAt, updatedAt                              time.Time
	)
	if err := scan(
		&purchaseID, &tenantID, &learnerGCID, &eggSKU, &suggestedFocalAtomID,
		&state,
		&amountCents, &amountCentsPaid, &amountCentsRefunded, &currency,
		&sessionID, &paymentIntent, &chargeID,
		&refundID, &checkoutURL, &failureCode, &failureMessage,
		&refundReason, &refundCreditOnly,
		&softExpiryAt, &hardExpiryAt,
		&checkoutStartedAt, &paidAt, &failedAt, &refundedAt, &expiredAt, &provisionedAt,
		&createdAt, &updatedAt,
	); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	ep := &egg.FamiliarEggPurchase{
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
		EggSKU:           eggSKU,
		RefundCreditOnly: refundCreditOnly,
		SoftExpiryAt:     timeUTC(softExpiryAt),
		HardExpiryAt:     timeUTC(hardExpiryAt),
		ProvisionedAt:    timeUTC(provisionedAt),
	}
	if suggestedFocalAtomID != nil {
		ep.SuggestedFocalAtomID = *suggestedFocalAtomID
	}
	if paymentIntent != nil {
		ep.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		ep.StripeChargeID = *chargeID
	}
	if refundID != nil {
		ep.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		ep.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		ep.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		ep.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		ep.RefundReason = shared.RefundReason(*refundReason)
	}
	return ep, nil
}
