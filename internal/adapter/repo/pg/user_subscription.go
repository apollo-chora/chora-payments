// user_subscription.go — Postgres adapter for UserSubscription.
//
// SCHEMA: migrations/0001_initial.up.sql (`user_subscriptions`).
//
// UserSubscription has a richer lifecycle than the other 4 aggregates:
// shared.State drives the per-invoice payment FSM; LifecycleState drives
// the subscription-level FSM (created → active → grace → paused → cancelled).
// The `state` column stores the more-specific state (LifecycleState wins
// when set; shared.State as the fallback for the per-invoice phase).
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
)

const userSubscriptionCols = `
    purchase_id, tenant_id, learner_gcid, plan_sku, billing_period,
    state,
    amount_cents, amount_cents_paid_total, amount_cents_refunded, currency,
    stripe_session_id, stripe_subscription_id, stripe_customer_id,
    stripe_payment_intent_id, stripe_charge_id, stripe_invoice_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    current_period_start, current_period_end,
    refund_reason, pause_reason, cancellation_reason,
    cancel_at_period_end, effective_at,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at_stripe, renewed_at, paused_at, resumes_at, cancelled_at,
    created_at, updated_at
`

const SQLUpsertUserSubscription = `
INSERT INTO user_subscriptions (` + userSubscriptionCols + `) VALUES (
    $1, $2, $3, $4, $5,
    $6,
    $7, $8, $9, $10,
    $11, $12, $13,
    $14, $15, $16,
    $17, $18, $19, $20,
    $21, $22,
    $23, $24, $25,
    $26, $27,
    $28, $29, $30, $31, $32,
    $33, $34, $35, $36, $37,
    $38, $39
)
ON CONFLICT (purchase_id) DO UPDATE SET
    state                    = EXCLUDED.state,
    amount_cents_paid_total  = EXCLUDED.amount_cents_paid_total,
    amount_cents_refunded    = EXCLUDED.amount_cents_refunded,
    stripe_subscription_id   = EXCLUDED.stripe_subscription_id,
    stripe_customer_id       = EXCLUDED.stripe_customer_id,
    stripe_payment_intent_id = EXCLUDED.stripe_payment_intent_id,
    stripe_charge_id         = EXCLUDED.stripe_charge_id,
    stripe_invoice_id        = EXCLUDED.stripe_invoice_id,
    stripe_refund_id         = EXCLUDED.stripe_refund_id,
    stripe_failure_code      = EXCLUDED.stripe_failure_code,
    stripe_failure_message   = EXCLUDED.stripe_failure_message,
    current_period_start     = EXCLUDED.current_period_start,
    current_period_end       = EXCLUDED.current_period_end,
    refund_reason            = EXCLUDED.refund_reason,
    pause_reason             = EXCLUDED.pause_reason,
    cancellation_reason      = EXCLUDED.cancellation_reason,
    cancel_at_period_end     = EXCLUDED.cancel_at_period_end,
    effective_at             = EXCLUDED.effective_at,
    paid_at                  = EXCLUDED.paid_at,
    failed_at                = EXCLUDED.failed_at,
    refunded_at              = EXCLUDED.refunded_at,
    expired_at               = EXCLUDED.expired_at,
    created_at_stripe        = EXCLUDED.created_at_stripe,
    renewed_at               = EXCLUDED.renewed_at,
    paused_at                = EXCLUDED.paused_at,
    resumes_at               = EXCLUDED.resumes_at,
    cancelled_at             = EXCLUDED.cancelled_at,
    updated_at               = EXCLUDED.updated_at
`

const SQLSelectUserSubscriptionByID = `
SELECT ` + userSubscriptionCols + `
FROM user_subscriptions
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectUserSubscriptionBySession = `
SELECT ` + userSubscriptionCols + `
FROM user_subscriptions
WHERE stripe_session_id = $1
`

const SQLSelectUserSubscriptionBySubID = `
SELECT ` + userSubscriptionCols + `
FROM user_subscriptions
WHERE stripe_subscription_id = $1
`

type UserSubscriptionRepo struct {
	tx TxRunner
}

func NewUserSubscriptionRepo(tx TxRunner) *UserSubscriptionRepo {
	return &UserSubscriptionRepo{tx: tx}
}

// effectiveState picks the column state to persist:
//   - LifecycleState when set (created/active/grace/paused/cancelled).
//   - Else shared.State (per-invoice payment phase).
func effectiveStateFor(u *sub.UserSubscription) string {
	if u.LifecycleState != "" {
		return string(u.LifecycleState)
	}
	return string(u.State)
}

func (r *UserSubscriptionRepo) Save(ctx context.Context, u *sub.UserSubscription) error {
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
		_, err := q.Exec(ctx, SQLUpsertUserSubscription,
			u.PurchaseID, u.TenantID, u.LearnerGCID, u.PlanSKU, string(u.BillingPeriod),
			effectiveStateFor(u),
			u.AmountCents, u.AmountCentsPaidTotal, u.AmountCentsRefunded, u.Currency,
			nullStr(u.StripeSessionID), nullStr(u.StripeSubscriptionID), nullStr(u.StripeCustomerID),
			nullStr(u.StripePaymentIntentID), nullStr(u.StripeChargeID), nullStr(u.StripeInvoiceID),
			nullStr(u.StripeRefundID), nullStr(u.StripeCheckoutURL),
			nullStr(u.StripeFailureCode), nullStr(u.StripeFailureMessage),
			nullTime(deref(u.CurrentPeriodStart)), nullTime(deref(u.CurrentPeriodEnd)),
			nullStr(string(u.RefundReason)), nullStr(u.PauseReason), nullStr(u.CancellationReason),
			u.CancelAtPeriodEnd, nullTime(deref(u.EffectiveAt)),
			u.CheckoutStartedAt, nullTime(deref(u.PaidAt)),
			nullTime(deref(u.FailedAt)), nullTime(deref(u.RefundedAt)),
			nullTime(deref(u.ExpiredAt)),
			nullTime(deref(u.CreatedAtStripe)), nullTime(deref(u.RenewedAt)),
			nullTime(deref(u.PausedAt)), nullTime(deref(u.ResumesAt)),
			nullTime(deref(u.CancelledAt)),
			u.CreatedAt, u.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert user_subscription: %w", err)
		}
		return nil
	})
}

func (r *UserSubscriptionRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*sub.UserSubscription, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *sub.UserSubscription
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectUserSubscriptionByID, purchaseID, tenantID)
		u, scanErr := scanUserSubscription(row.Scan)
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

func (r *UserSubscriptionRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*sub.UserSubscription, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *sub.UserSubscription
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectUserSubscriptionBySession, sessID)
		u, scanErr := scanUserSubscription(row.Scan)
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

func (r *UserSubscriptionRepo) GetByStripeSubscriptionID(ctx context.Context, subID string) (*sub.UserSubscription, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if subID == "" {
		return nil, shared.ErrNotFound
	}
	var found *sub.UserSubscription
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		row := q.QueryRow(ctx, SQLSelectUserSubscriptionBySubID, subID)
		u, scanErr := scanUserSubscription(row.Scan)
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

var _ sub.Repo = (*UserSubscriptionRepo)(nil)

func scanUserSubscription(scan func(...any) error) (*sub.UserSubscription, error) {
	var (
		purchaseID, tenantID, learnerGCID, planSKU, billingPeriod    string
		state                                                        string
		amountCents, amountCentsPaidTotal, amountCentsRefunded       int64
		currency                                                     string
		sessionID, subscriptionID, customerID                        *string
		paymentIntent, chargeID, invoiceID                           *string
		refundID, checkoutURL, failureCode, failureMessage           *string
		currentPeriodStart, currentPeriodEnd                         *time.Time
		refundReason, pauseReason, cancellationReason                *string
		cancelAtPeriodEnd                                            bool
		effectiveAt                                                  *time.Time
		checkoutStartedAt                                            time.Time
		paidAt, failedAt, refundedAt, expiredAt                      *time.Time
		createdAtStripe, renewedAt, pausedAt, resumesAt, cancelledAt *time.Time
		createdAt, updatedAt                                         time.Time
	)
	if err := scan(
		&purchaseID, &tenantID, &learnerGCID, &planSKU, &billingPeriod,
		&state,
		&amountCents, &amountCentsPaidTotal, &amountCentsRefunded, &currency,
		&sessionID, &subscriptionID, &customerID,
		&paymentIntent, &chargeID, &invoiceID,
		&refundID, &checkoutURL, &failureCode, &failureMessage,
		&currentPeriodStart, &currentPeriodEnd,
		&refundReason, &pauseReason, &cancellationReason,
		&cancelAtPeriodEnd, &effectiveAt,
		&checkoutStartedAt, &paidAt, &failedAt, &refundedAt, &expiredAt,
		&createdAtStripe, &renewedAt, &pausedAt, &resumesAt, &cancelledAt,
		&createdAt, &updatedAt,
	); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	u := &sub.UserSubscription{
		Purchase: shared.Purchase{
			PurchaseID:          purchaseID,
			TenantID:            tenantID,
			LearnerGCID:         learnerGCID,
			AmountCents:         amountCents,
			AmountCentsRefunded: amountCentsRefunded,
			Currency:            currency,
			CheckoutStartedAt:   checkoutStartedAt.UTC(),
			CreatedAt:           createdAt.UTC(),
			UpdatedAt:           updatedAt.UTC(),
			PaidAt:              timeUTC(paidAt),
			FailedAt:            timeUTC(failedAt),
			RefundedAt:          timeUTC(refundedAt),
			ExpiredAt:           timeUTC(expiredAt),
		},
		PlanSKU:              planSKU,
		BillingPeriod:        sub.BillingPeriod(billingPeriod),
		AmountCentsPaidTotal: amountCentsPaidTotal,
		CurrentPeriodStart:   timeUTC(currentPeriodStart),
		CurrentPeriodEnd:     timeUTC(currentPeriodEnd),
		CancelAtPeriodEnd:    cancelAtPeriodEnd,
		EffectiveAt:          timeUTC(effectiveAt),
		CreatedAtStripe:      timeUTC(createdAtStripe),
		RenewedAt:            timeUTC(renewedAt),
		PausedAt:             timeUTC(pausedAt),
		ResumesAt:            timeUTC(resumesAt),
		CancelledAt:          timeUTC(cancelledAt),
	}
	// Split state across shared.State (payment phase) + LifecycleState
	// (subscription FSM). Lifecycle states are a superset of payment states.
	if isLifecycleState(state) {
		u.LifecycleState = sub.SubscriptionLifecycleState(state)
	} else {
		u.State = shared.State(state)
	}
	if sessionID != nil {
		u.StripeSessionID = *sessionID
	}
	if subscriptionID != nil {
		u.StripeSubscriptionID = *subscriptionID
	}
	if customerID != nil {
		u.StripeCustomerID = *customerID
	}
	if paymentIntent != nil {
		u.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		u.StripeChargeID = *chargeID
	}
	if invoiceID != nil {
		u.StripeInvoiceID = *invoiceID
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
	if pauseReason != nil {
		u.PauseReason = *pauseReason
	}
	if cancellationReason != nil {
		u.CancellationReason = *cancellationReason
	}
	return u, nil
}

// isLifecycleState reports whether s is one of the subscription-only
// lifecycle states (created/active/grace/paused/cancelled). The 5
// payment-phase states (checkout_started/payment_captured/...) come
// from shared.State and are NOT lifecycle states.
func isLifecycleState(s string) bool {
	switch s {
	case "created", "active", "grace", "paused", "cancelled":
		return true
	}
	return false
}
