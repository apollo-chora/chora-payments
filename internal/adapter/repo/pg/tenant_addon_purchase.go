// tenant_addon_purchase.go — Postgres adapter for TenantAddonPurchase.
//
// SCHEMA: migrations/0008_tenant_addon_purchases.up.sql.
//
// 8th Purchase aggregate (CHO-1738). Mirrors the tenant-scoped
// tenant_mana_topup adapter: admin_gcid in the canonical learner_gcid
// shared-Purchase slot + 3 aggregate-specific columns
// (addon_plan_id, addon_code, tier_code) instead of (sku, mana_units,
// mana_units_debited).
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

const tenantAddonPurchaseCols = `
    purchase_id, tenant_id, admin_gcid,
    addon_plan_id, addon_code, tier_code,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at, updated_at,
    stripe_customer_id, stripe_subscription_id, sub_status,
    current_period_start, current_period_end,
    stripe_subscription_schedule_id, scheduled_tier_code, scheduled_effective_at
`

const SQLUpsertTenantAddonPurchase = `
INSERT INTO tenant_addon_purchases (` + tenantAddonPurchaseCols + `) VALUES (
    $1, $2, $3,
    $4, $5, $6,
    $7,
    $8, $9, $10, $11,
    $12, $13, $14,
    $15, $16, $17, $18,
    $19,
    $20, $21, $22, $23, $24,
    $25, $26,
    $27, $28, $29,
    $30, $31,
    $32, $33, $34
)
ON CONFLICT (purchase_id) DO UPDATE SET
    state                           = EXCLUDED.state,
    tier_code                       = EXCLUDED.tier_code,
    amount_cents_paid               = EXCLUDED.amount_cents_paid,
    amount_cents_refunded           = EXCLUDED.amount_cents_refunded,
    stripe_payment_intent_id        = EXCLUDED.stripe_payment_intent_id,
    stripe_charge_id                = EXCLUDED.stripe_charge_id,
    stripe_refund_id                = EXCLUDED.stripe_refund_id,
    stripe_failure_code             = EXCLUDED.stripe_failure_code,
    stripe_failure_message          = EXCLUDED.stripe_failure_message,
    refund_reason                   = EXCLUDED.refund_reason,
    paid_at                         = EXCLUDED.paid_at,
    failed_at                       = EXCLUDED.failed_at,
    refunded_at                     = EXCLUDED.refunded_at,
    expired_at                      = EXCLUDED.expired_at,
    updated_at                      = EXCLUDED.updated_at,
    stripe_customer_id              = EXCLUDED.stripe_customer_id,
    stripe_subscription_id          = EXCLUDED.stripe_subscription_id,
    sub_status                      = EXCLUDED.sub_status,
    current_period_start            = EXCLUDED.current_period_start,
    current_period_end              = EXCLUDED.current_period_end,
    stripe_subscription_schedule_id = EXCLUDED.stripe_subscription_schedule_id,
    scheduled_tier_code             = EXCLUDED.scheduled_tier_code,
    scheduled_effective_at          = EXCLUDED.scheduled_effective_at
`

const SQLSelectTenantAddonPurchaseByID = `
SELECT ` + tenantAddonPurchaseCols + `
FROM tenant_addon_purchases
WHERE purchase_id = $1
  AND tenant_id = $2
`

const SQLSelectTenantAddonPurchaseBySession = `
SELECT ` + tenantAddonPurchaseCols + `
FROM tenant_addon_purchases
WHERE stripe_session_id = $1
`

const SQLSelectTenantAddonPurchaseBySubscription = `
SELECT ` + tenantAddonPurchaseCols + `
FROM tenant_addon_purchases
WHERE stripe_subscription_id = $1
`

// CHO-1772 — schedule_id is globally unique + Stripe-issued so RLS bypass
// is safe (mirrors GetByStripeSubscriptionID).
const SQLSelectTenantAddonPurchaseByScheduleID = `
SELECT ` + tenantAddonPurchaseCols + `
FROM tenant_addon_purchases
WHERE stripe_subscription_schedule_id = $1
`

const SQLSelectTenantAddonPurchaseActiveByCode = `
SELECT ` + tenantAddonPurchaseCols + `
FROM tenant_addon_purchases
WHERE tenant_id  = $1
  AND addon_code = $2
  AND state      = 'payment_captured'
ORDER BY updated_at DESC
LIMIT 1
`

// CHO-1759-followup billing — resolve the tenant's Stripe Customer ID
// without requiring the caller to pass a purchase_id. One Customer per
// tenant by CHO-1762 contract, so any row with a non-empty customer_id
// is the right answer. Skips empty customer_id rows (half-bootstrapped
// pre-webhook). Returns no rows when the tenant has no purchases yet —
// the caller treats that as an empty invoice list.
const SQLSelectStripeCustomerIDByTenant = `
SELECT stripe_customer_id
FROM tenant_addon_purchases
WHERE tenant_id = $1
  AND stripe_customer_id IS NOT NULL
  AND stripe_customer_id <> ''
ORDER BY updated_at DESC
LIMIT 1
`

type TenantAddonPurchaseRepo struct {
	tx TxRunner
}

func NewTenantAddonPurchaseRepo(tx TxRunner) *TenantAddonPurchaseRepo {
	return &TenantAddonPurchaseRepo{tx: tx}
}

func (r *TenantAddonPurchaseRepo) Save(ctx context.Context, a *tap.TenantAddonPurchase) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if a == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertTenantAddonPurchase,
			a.PurchaseID, a.TenantID, a.LearnerGCID,
			a.AddonPlanID, a.AddonCode, a.TierCode,
			string(a.State),
			a.AmountCents, a.AmountCentsPaid, a.AmountCentsRefunded, a.Currency,
			a.StripeSessionID, nullStr(a.StripePaymentIntentID), nullStr(a.StripeChargeID),
			nullStr(a.StripeRefundID), nullStr(a.StripeCheckoutURL),
			nullStr(a.StripeFailureCode), nullStr(a.StripeFailureMessage),
			nullStr(string(a.RefundReason)),
			a.CheckoutStartedAt, nullTime(deref(a.PaidAt)),
			nullTime(deref(a.FailedAt)), nullTime(deref(a.RefundedAt)),
			nullTime(deref(a.ExpiredAt)),
			a.CreatedAt, a.UpdatedAt,
			// CHO-1761 Stripe Subscription correlation.
			nullStr(a.StripeCustomerID), nullStr(a.StripeSubscriptionID), nullStr(string(a.Status)),
			nullTime(deref(a.CurrentPeriodStart)), nullTime(deref(a.CurrentPeriodEnd)),
			// CHO-1772 Stripe SubscriptionSchedule correlation.
			nullStr(a.StripeSubscriptionScheduleID), nullStr(a.ScheduledTierCode),
			nullTime(deref(a.ScheduledEffectiveAt)),
		)
		if err != nil {
			return fmt.Errorf("pg: upsert tenant_addon_purchase: %w", err)
		}
		return nil
	})
}

// GetByStripeSubscriptionID — CHO-1761 webhook dispatch lookup. The
// stripe_subscription_id is UNIQUE + Stripe-issued (no cross-tenant
// collision risk), so this skips the RLS scope filter the same way
// GetByStripeSessionID does.
func (r *TenantAddonPurchaseRepo) GetByStripeSubscriptionID(ctx context.Context, subID string) (*tap.TenantAddonPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if subID == "" {
		return nil, tap.ErrStripeSubscriptionRequired
	}
	var found *tap.TenantAddonPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		// The tenant_isolation policy on tenant_addon_purchases evaluates
		// `current_setting('chora.tenant_id', true)::uuid` for every row
		// touched, even when the WHERE clause filters by a globally
		// unique Stripe ID. Without ApplySession the cast hits "" and
		// SQLSTATE 22P02 fails the whole webhook. Mirrors GetByID + the
		// sibling repos (familiar_egg_purchase, stripe_customer).
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantAddonPurchaseBySubscription, subID)
		a, scanErr := scanTenantAddonPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = a
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// GetActiveByTenantAndAddonCode — CHO-1764 change-tier lookup. RLS
// scoped via the caller's ctx tenant (the SELECT also filters on
// tenant_id explicitly so the predicate is correct even if RLS isn't
// active for some path).
func (r *TenantAddonPurchaseRepo) GetActiveByTenantAndAddonCode(ctx context.Context, tenantID, addonCode string) (*tap.TenantAddonPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if tenantID == "" {
		return nil, tap.ErrTenantRequired
	}
	if addonCode == "" {
		return nil, tap.ErrAddonCodeRequired
	}
	var found *tap.TenantAddonPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantAddonPurchaseActiveByCode, tenantID, addonCode)
		a, scanErr := scanTenantAddonPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = a
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *TenantAddonPurchaseRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*tap.TenantAddonPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *tap.TenantAddonPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantAddonPurchaseByID, purchaseID, tenantID)
		a, scanErr := scanTenantAddonPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = a
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// GetByStripeScheduleID — CHO-1772 webhook dispatch lookup for the
// subscription_schedule.released event. RLS bypass safe because the
// schedule ID is globally unique + Stripe-issued.
func (r *TenantAddonPurchaseRepo) GetByStripeScheduleID(ctx context.Context, scheduleID string) (*tap.TenantAddonPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if scheduleID == "" {
		return nil, tap.ErrStripeScheduleRequired
	}
	var found *tap.TenantAddonPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantAddonPurchaseByScheduleID, scheduleID)
		a, scanErr := scanTenantAddonPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = a
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *TenantAddonPurchaseRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*tap.TenantAddonPurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *tap.TenantAddonPurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectTenantAddonPurchaseBySession, sessID)
		a, scanErr := scanTenantAddonPurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = a
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// GetStripeCustomerIDByTenant — H+ Billing. RLS-scoped via ctx; the
// query also filters on tenant_id explicitly so the predicate is
// correct even on RLS-bypass paths. Empty tenantID fail-loud per
// tap.ErrTenantRequired. No matching row → empty string + nil error so
// the billing handler renders empty invoice list (not 404).
func (r *TenantAddonPurchaseRepo) GetStripeCustomerIDByTenant(ctx context.Context, tenantID string) (string, error) {
	if r == nil || r.tx == nil {
		return "", ErrNotImplemented
	}
	if tenantID == "" {
		return "", tap.ErrTenantRequired
	}
	var found string
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectStripeCustomerIDByTenant, tenantID)
		if scanErr := row.Scan(&found); scanErr != nil {
			if isNoRows(scanErr) {
				return nil // empty string + nil error
			}
			return scanErr
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return found, nil
}

var _ tap.Repo = (*TenantAddonPurchaseRepo)(nil)

func scanTenantAddonPurchase(scan func(...any) error) (*tap.TenantAddonPurchase, error) {
	var (
		purchaseID, tenantID, adminGCID                   string
		addonPlanID, addonCode, tierCode                  string
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
		// CHO-1761 Subscription correlation (nullable).
		stripeCustomerID, stripeSubscriptionID, subStatus *string
		currentPeriodStart, currentPeriodEnd              *time.Time
		// CHO-1772 SubscriptionSchedule correlation (nullable).
		stripeSubscriptionScheduleID, scheduledTierCode *string
		scheduledEffectiveAt                            *time.Time
	)
	if err := scan(
		&purchaseID, &tenantID, &adminGCID,
		&addonPlanID, &addonCode, &tierCode,
		&state,
		&amountCents, &amountCentsPaid, &amountCentsRefunded, &currency,
		&sessionID, &paymentIntent, &chargeID,
		&refundID, &checkoutURL, &failureCode, &failureMessage,
		&refundReason,
		&checkoutStartedAt, &paidAt, &failedAt, &refundedAt, &expiredAt,
		&createdAt, &updatedAt,
		&stripeCustomerID, &stripeSubscriptionID, &subStatus,
		&currentPeriodStart, &currentPeriodEnd,
		&stripeSubscriptionScheduleID, &scheduledTierCode, &scheduledEffectiveAt,
	); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	a := &tap.TenantAddonPurchase{
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
		AddonPlanID: addonPlanID,
		AddonCode:   addonCode,
		TierCode:    tierCode,
	}
	if paymentIntent != nil {
		a.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		a.StripeChargeID = *chargeID
	}
	if refundID != nil {
		a.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		a.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		a.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		a.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		a.RefundReason = shared.RefundReason(*refundReason)
	}
	// CHO-1761 hydrate Subscription correlation (legacy rows have all NULL).
	if stripeCustomerID != nil {
		a.StripeCustomerID = *stripeCustomerID
	}
	if stripeSubscriptionID != nil {
		a.StripeSubscriptionID = *stripeSubscriptionID
	}
	if subStatus != nil {
		a.Status = tap.Status(*subStatus)
	}
	if currentPeriodStart != nil {
		utc := currentPeriodStart.UTC()
		a.CurrentPeriodStart = &utc
	}
	if currentPeriodEnd != nil {
		utc := currentPeriodEnd.UTC()
		a.CurrentPeriodEnd = &utc
	}
	// CHO-1772 hydrate Schedule correlation (rows without a pending
	// schedule have all 3 NULL).
	if stripeSubscriptionScheduleID != nil {
		a.StripeSubscriptionScheduleID = *stripeSubscriptionScheduleID
	}
	if scheduledTierCode != nil {
		a.ScheduledTierCode = *scheduledTierCode
	}
	if scheduledEffectiveAt != nil {
		utc := scheduledEffectiveAt.UTC()
		a.ScheduledEffectiveAt = &utc
	}
	return a, nil
}
