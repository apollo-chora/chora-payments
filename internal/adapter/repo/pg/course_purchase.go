// course_purchase.go — Postgres adapter for the CoursePurchase aggregate.
//
// SCHEMA: see migrations/0001_initial.up.sql (`course_purchases` table).
//
// Wraps Save / GetByID / GetByStripeSessionID. RLS-aware (every method
// applies rls.ApplySession before the query) per the tenant_isolation
// policy on `course_purchases`.
//
// Cross-DB queries FORBIDDEN — only reads chora_payments.course_purchases.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// -----------------------------------------------------------------------------
// SQL templates
// -----------------------------------------------------------------------------

const coursePurchaseCols = `
    purchase_id, tenant_id, learner_gcid, course_id,
    state,
    amount_cents, amount_cents_paid, amount_cents_refunded, currency,
    stripe_session_id, stripe_payment_intent_id, stripe_charge_id,
    stripe_refund_id, stripe_checkout_url, stripe_failure_code, stripe_failure_message,
    refund_reason,
    checkout_started_at, paid_at, failed_at, refunded_at, expired_at,
    created_at, updated_at
`

// SQLUpsertCoursePurchase — INSERT … ON CONFLICT(purchase_id) DO UPDATE.
//
// Idempotent on purchase_id. Mirrors chora-delivery's course UPSERT.
const SQLUpsertCoursePurchase = `
INSERT INTO course_purchases (` + coursePurchaseCols + `) VALUES (
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

// SQLSelectCoursePurchaseByID — by purchase_id within RLS tenant scope.
const SQLSelectCoursePurchaseByID = `
SELECT ` + coursePurchaseCols + `
FROM course_purchases
WHERE purchase_id = $1
  AND tenant_id = $2
`

// SQLSelectCoursePurchaseBySession — by stripe_session_id (UNIQUE
// globally, RLS-bypass safe — the row's tenant_id is returned for
// downstream gate-check).
const SQLSelectCoursePurchaseBySession = `
SELECT ` + coursePurchaseCols + `
FROM course_purchases
WHERE stripe_session_id = $1
`

// -----------------------------------------------------------------------------
// CoursePurchaseRepo
// -----------------------------------------------------------------------------

// CoursePurchaseRepo is the Postgres-backed CoursePurchase repo.
type CoursePurchaseRepo struct {
	tx TxRunner
}

// NewCoursePurchaseRepo constructs a CoursePurchaseRepo around a TxRunner.
func NewCoursePurchaseRepo(tx TxRunner) *CoursePurchaseRepo {
	return &CoursePurchaseRepo{tx: tx}
}

// Save persists a CoursePurchase (UPSERT on purchase_id). Idempotent.
//
// The caller MUST have set tenant_id on ctx via tracing.WithTenantID;
// rls.ApplySession returns ErrNoTenantContext otherwise.
func (r *CoursePurchaseRepo) Save(ctx context.Context, cp *coursepurchase.CoursePurchase) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if cp == nil {
		return ErrInvalidAggregate
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLUpsertCoursePurchase,
			cp.PurchaseID, cp.TenantID, cp.LearnerGCID, cp.CourseID,
			string(cp.State),
			cp.AmountCents, cp.AmountCentsPaid, cp.AmountCentsRefunded, cp.Currency,
			cp.StripeSessionID, nullStr(cp.StripePaymentIntentID), nullStr(cp.StripeChargeID),
			nullStr(cp.StripeRefundID), nullStr(cp.StripeCheckoutURL),
			nullStr(cp.StripeFailureCode), nullStr(cp.StripeFailureMessage),
			nullStr(string(cp.RefundReason)),
			cp.CheckoutStartedAt, nullTime(deref(cp.PaidAt)),
			nullTime(deref(cp.FailedAt)), nullTime(deref(cp.RefundedAt)),
			nullTime(deref(cp.ExpiredAt)),
			cp.CreatedAt, cp.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert course_purchase: %w", err)
		}
		return nil
	})
}

// GetByID returns a CoursePurchase by (tenantID, purchaseID).
// Returns shared.ErrNotFound when no row matches.
func (r *CoursePurchaseRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*coursepurchase.CoursePurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var found *coursepurchase.CoursePurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectCoursePurchaseByID, purchaseID, tenantID)
		cp, scanErr := scanCoursePurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// GetByStripeSessionID returns a CoursePurchase by Stripe Session ID.
// The caller MUST have set tenant_id on ctx via tracing.WithTenantID;
// the tenant_isolation RLS policy on course_purchases is rewritten into
// every SELECT against the table (Postgres applies it before the
// statement WHERE), so even a session_id lookup needs the RLS session
// var set or the policy `tenant_id = (current_setting('chora.tenant_id',
// true))::uuid` evaluates against NULL and filters every row.
//
// The dispatcher extracts tenant_id from the Stripe session metadata
// and propagates via tracing.WithTenantID before calling this method;
// see services/chora-payments/internal/adapter/dispatcher/dispatcher.go.
func (r *CoursePurchaseRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*coursepurchase.CoursePurchase, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if sessID == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	var found *coursepurchase.CoursePurchase
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectCoursePurchaseBySession, sessID)
		cp, scanErr := scanCoursePurchase(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// Compile-time conformance.
var _ coursepurchase.Repo = (*CoursePurchaseRepo)(nil)

// -----------------------------------------------------------------------------
// Scanner
// -----------------------------------------------------------------------------

// errScanNotFound is the sentinel scanners return when the underlying
// row Scan reports "no rows" (driver-specific). Mapped to shared.ErrNotFound
// at the public surface.
var errScanNotFound = errors.New("pg: scan: no rows")

func scanCoursePurchase(scan func(...any) error) (*coursepurchase.CoursePurchase, error) {
	var (
		purchaseID, tenantID, learnerGCID, courseID       string
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
		&purchaseID, &tenantID, &learnerGCID, &courseID,
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
	cp := &coursepurchase.CoursePurchase{
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
		CourseID: courseID,
	}
	if paymentIntent != nil {
		cp.StripePaymentIntentID = *paymentIntent
	}
	if chargeID != nil {
		cp.StripeChargeID = *chargeID
	}
	if refundID != nil {
		cp.StripeRefundID = *refundID
	}
	if checkoutURL != nil {
		cp.StripeCheckoutURL = *checkoutURL
	}
	if failureCode != nil {
		cp.StripeFailureCode = *failureCode
	}
	if failureMessage != nil {
		cp.StripeFailureMessage = *failureMessage
	}
	if refundReason != nil {
		cp.RefundReason = shared.RefundReason(*refundReason)
	}
	return cp, nil
}

// isNoRows reports whether err is the driver's "no rows in result set"
// sentinel. The driver-specific check is loose (string match) so this
// package binds neither pq nor pgx at compile time.
func isNoRows(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "no rows in result set") || contains(msg, "sql: no rows")
}

func contains(s, sub string) bool {
	if sub == "" {
		return true
	}
	if len(s) < len(sub) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
