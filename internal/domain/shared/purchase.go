package shared

import (
	"time"
)

// Purchase is the canonical base struct for all 5 chora-payments aggregates.
// Each aggregate (CoursePurchase, ApplicationPayment, FamiliarEggPurchase,
// TenantManaTopUp, UserSubscription) embeds this struct + adds its
// aggregate-specific fields (course_id / application_id / egg_sku /
// sku+mana_units / plan_sku+billing_period).
//
// Field layout matches the 5 Purchase tables in migrations/0001_initial.up.sql.
// All field-level invariants are enforced by the FSM methods below
// (MarkPaymentCaptured, MarkPaymentFailed, MarkRefunded, MarkExpired) +
// supplemented by per-aggregate validation in each domain package.
//
// Concurrency: this struct is NOT goroutine-safe. Callers must serialise
// access (typical pattern: pg adapter `SELECT … FOR UPDATE` + in-TX
// transition + UPSERT).
type Purchase struct {
	PurchaseID  string // UUIDv7
	TenantID    string
	LearnerGCID string

	State State

	AmountCents         int64
	AmountCentsPaid     int64
	AmountCentsRefunded int64
	Currency            string // ISO 4217 3-letter code

	StripeSessionID       string
	StripePaymentIntentID string
	StripeChargeID        string
	StripeRefundID        string
	StripeCheckoutURL     string
	StripeFailureCode     string
	StripeFailureMessage  string

	RefundReason RefundReason

	CheckoutStartedAt time.Time
	PaidAt            *time.Time
	FailedAt          *time.Time
	RefundedAt        *time.Time
	ExpiredAt         *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewPurchase initialises a Purchase in `checkout_started` state. The
// caller is expected to populate the aggregate-specific fields (course_id
// etc.) on the wrapping struct.
func NewPurchase(
	purchaseID, tenantID, learnerGCID string,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (Purchase, error) {
	if tenantID == "" {
		return Purchase{}, ErrTenantRequired
	}
	if learnerGCID == "" {
		return Purchase{}, ErrLearnerRequired
	}
	if err := ValidateAmount(amountCents); err != nil {
		return Purchase{}, err
	}
	if err := ValidateCurrency(currency); err != nil {
		return Purchase{}, err
	}
	if stripeSessionID == "" {
		return Purchase{}, ErrStripeSessionRequired
	}
	return Purchase{
		PurchaseID:        purchaseID,
		TenantID:          tenantID,
		LearnerGCID:       learnerGCID,
		State:             StateCheckoutStarted,
		AmountCents:       amountCents,
		Currency:          currency,
		StripeSessionID:   stripeSessionID,
		StripeCheckoutURL: stripeCheckoutURL,
		CheckoutStartedAt: now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

// MarkPaymentCaptured transitions checkout_started → payment_captured.
// Idempotent on re-application (already payment_captured → no-op).
//
// Returns ErrInvalidTransition if state is failed / refunded / expired.
func (p *Purchase) MarkPaymentCaptured(
	stripePaymentIntentID, stripeChargeID string,
	amountCentsPaid int64, now time.Time,
) error {
	if p.State == StatePaymentCaptured {
		return nil // idempotent
	}
	if p.State != StateCheckoutStarted {
		return TransitionError{From: p.State, To: StatePaymentCaptured}
	}
	if err := ValidateAmount(amountCentsPaid); err != nil {
		return err
	}
	p.State = StatePaymentCaptured
	p.StripePaymentIntentID = stripePaymentIntentID
	p.StripeChargeID = stripeChargeID
	p.AmountCentsPaid = amountCentsPaid
	p.PaidAt = &now
	p.UpdatedAt = now
	return nil
}

// MarkPaymentFailed transitions checkout_started → payment_failed.
// Idempotent on re-application.
func (p *Purchase) MarkPaymentFailed(
	stripeFailureCode, stripeFailureMessage string,
	now time.Time,
) error {
	if p.State == StatePaymentFailed {
		return nil // idempotent
	}
	if p.State != StateCheckoutStarted {
		return TransitionError{From: p.State, To: StatePaymentFailed}
	}
	p.State = StatePaymentFailed
	p.StripeFailureCode = stripeFailureCode
	p.StripeFailureMessage = stripeFailureMessage
	p.FailedAt = &now
	p.UpdatedAt = now
	return nil
}

// MarkRefunded transitions payment_captured → refunded (full or partial).
// Idempotent on re-application.
//
// Note: partial refunds (amount_cents_refunded < amount_cents_paid) keep
// the aggregate in `refunded` state with a non-zero balance — the SQL
// shape allows multiple refund events to accumulate amount_cents_refunded.
func (p *Purchase) MarkRefunded(
	stripeRefundID string,
	amountCentsRefunded int64,
	reason RefundReason,
	now time.Time,
) error {
	if p.State == StateRefunded {
		// Allow accumulating partial refunds in the same terminal state.
		// Skip strict idempotency for refunds — Stripe may fire multiple
		// charge.refunded events for partial refunds; each one bumps
		// amount_cents_refunded.
	} else if p.State != StatePaymentCaptured {
		return TransitionError{From: p.State, To: StateRefunded}
	}
	if !reason.IsValid() {
		return ErrRefundReasonInvalid
	}
	if err := ValidateAmount(amountCentsRefunded); err != nil {
		return err
	}
	p.State = StateRefunded
	p.StripeRefundID = stripeRefundID
	p.AmountCentsRefunded += amountCentsRefunded
	p.RefundReason = reason
	p.RefundedAt = &now
	p.UpdatedAt = now
	return nil
}

// MarkExpired transitions checkout_started → expired (Stripe Checkout
// Session expired or chora-payments sweeper marked it stale).
// Idempotent on re-application.
func (p *Purchase) MarkExpired(now time.Time) error {
	if p.State == StateExpired {
		return nil // idempotent
	}
	if p.State != StateCheckoutStarted {
		return TransitionError{From: p.State, To: StateExpired}
	}
	p.State = StateExpired
	p.ExpiredAt = &now
	p.UpdatedAt = now
	return nil
}
