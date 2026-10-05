// Package shared carries the cross-aggregate domain primitives — State enum,
// RefundReason enum, Currency helpers, error sentinels — shared across the
// 5 Purchase aggregates (course_purchase, application_payment,
// familiar_egg_purchase, tenant_mana_topup, user_subscription).
//
// Per ADR-164 the 5 aggregates share a canonical 5-state payment FSM:
//
//	checkout_started → payment_captured  → refunded
//	                 → payment_failed
//	                 → expired
//
// (`user_subscription` adds 4 extra lifecycle states — `created` / `active`
// / `grace` / `paused` / `cancelled` — handled separately in that package.)
package shared

import (
	"errors"
	"fmt"
)

// State is the canonical payment-lifecycle state shared across all
// 5 Purchase aggregates.
type State string

const (
	StateCheckoutStarted State = "checkout_started"
	StatePaymentCaptured State = "payment_captured"
	StatePaymentFailed   State = "payment_failed"
	StateRefunded        State = "refunded"
	StateExpired         State = "expired"
)

// IsValid reports whether s is one of the 5 canonical lifecycle states.
func (s State) IsValid() bool {
	switch s {
	case StateCheckoutStarted, StatePaymentCaptured,
		StatePaymentFailed, StateRefunded, StateExpired:
		return true
	}
	return false
}

// IsTerminal reports whether s is a terminal state (no further transitions
// allowed).
func (s State) IsTerminal() bool {
	switch s {
	case StateRefunded, StateExpired:
		return true
	}
	return false
}

// RefundReason mirrors the SQL CHECK constraint on the `refund_reason`
// column.
type RefundReason string

const (
	RefundReasonSupportInitiated RefundReason = "support_initiated"
	RefundReasonCustomerRequest  RefundReason = "customer_request"
	RefundReasonDuplicateCharge  RefundReason = "duplicate_charge"
	RefundReasonFraud            RefundReason = "fraud"

	// ExpiredUnhatched is FamiliarEgg-specific (ADR-149 60d hard-expiry
	// auto-refund-credit) but the enum value is included here for
	// cross-aggregate consistency.
	RefundReasonExpiredUnhatched RefundReason = "expired_unhatched"
)

// IsValid reports whether r is a recognised RefundReason.
func (r RefundReason) IsValid() bool {
	switch r {
	case RefundReasonSupportInitiated, RefundReasonCustomerRequest,
		RefundReasonDuplicateCharge, RefundReasonFraud,
		RefundReasonExpiredUnhatched:
		return true
	}
	return false
}

// -----------------------------------------------------------------------------
// Errors (cross-aggregate sentinels).
// -----------------------------------------------------------------------------

var (
	// ErrTenantRequired — no tenant_id provided on a Purchase write/read.
	ErrTenantRequired = errors.New("payments: tenant_id required")

	// ErrLearnerRequired — no learner_gcid provided.
	ErrLearnerRequired = errors.New("payments: learner_gcid required")

	// ErrAmountNegative — amount_cents < 0.
	ErrAmountNegative = errors.New("payments: amount_cents must be >= 0")

	// ErrCurrencyInvalid — currency is not a 3-letter ISO 4217 code.
	ErrCurrencyInvalid = errors.New("payments: currency must be a 3-letter ISO 4217 code")

	// ErrStripeSessionRequired — Stripe session_id missing where it must
	// be present (post-Session creation).
	ErrStripeSessionRequired = errors.New("payments: stripe_session_id required")

	// ErrInvalidTransition — caller attempted a state transition the FSM
	// does not allow from the current state.
	ErrInvalidTransition = errors.New("payments: invalid state transition")

	// ErrRefundReasonInvalid — refund_reason not in the allowed enum.
	ErrRefundReasonInvalid = errors.New("payments: refund_reason invalid")

	// ErrNotFound — repo lookup returned no row.
	ErrNotFound = errors.New("payments: purchase not found")
)

// TransitionError wraps ErrInvalidTransition with the source + target
// states so error messages are actionable.
type TransitionError struct {
	From State
	To   State
}

func (e TransitionError) Error() string {
	return fmt.Sprintf("payments: invalid state transition: %s → %s", e.From, e.To)
}

func (e TransitionError) Unwrap() error {
	return ErrInvalidTransition
}

// ValidateAmount enforces the non-negative-cents invariant.
func ValidateAmount(amountCents int64) error {
	if amountCents < 0 {
		return ErrAmountNegative
	}
	return nil
}

// ValidateCurrency enforces the 3-letter ISO 4217 invariant.
func ValidateCurrency(c string) error {
	if len(c) != 3 {
		return ErrCurrencyInvalid
	}
	for _, r := range c {
		if r < 'A' || r > 'Z' {
			return ErrCurrencyInvalid
		}
	}
	return nil
}
