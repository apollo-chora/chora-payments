// Package user_subscription is the per-learner Familiar mana subscription
// aggregate (replaces chora.identity.user_subscription.* at Wave 1 Stage E
// cutover).
//
// Topic family: chora.payments.user_subscription.*.v1
// Originating service: chora-identity (drives the UserSubscription FSM).
//
// Unlike the other 4 aggregates, subscription has a richer lifecycle:
// payment-lifecycle 5 events + subscription-lifecycle 4 events
// (created, renewed, paused, cancelled).
package user_subscription

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// SubscriptionLifecycleState extends shared.State with subscription-
// specific lifecycle states beyond the 5 payment-lifecycle states.
type SubscriptionLifecycleState string

const (
	SubStateCreated   SubscriptionLifecycleState = "created"
	SubStateActive    SubscriptionLifecycleState = "active"
	SubStateGrace     SubscriptionLifecycleState = "grace"
	SubStatePaused    SubscriptionLifecycleState = "paused"
	SubStateCancelled SubscriptionLifecycleState = "cancelled"
)

// BillingPeriod is the Stripe Price recurrence interval.
type BillingPeriod string

const (
	BillingMonthly  BillingPeriod = "monthly"
	BillingAnnually BillingPeriod = "annually"
)

func (b BillingPeriod) IsValid() bool {
	return b == BillingMonthly || b == BillingAnnually
}

// UserSubscription is the aggregate root for a recurring Familiar mana
// subscription.
type UserSubscription struct {
	shared.Purchase

	PlanSKU       string
	BillingPeriod BillingPeriod

	// Lifecycle state — distinct from shared.State (payment-lifecycle).
	// Subscription invariant: shared.State drives the per-invoice
	// payment FSM; LifecycleState drives the subscription-level FSM.
	LifecycleState SubscriptionLifecycleState

	// Stripe Subscription-level handles.
	StripeSubscriptionID string
	StripeCustomerID     string
	StripeInvoiceID      string

	CurrentPeriodStart *time.Time
	CurrentPeriodEnd   *time.Time

	PauseReason        string
	CancellationReason string
	CancelAtPeriodEnd  bool
	EffectiveAt        *time.Time

	CreatedAtStripe *time.Time
	RenewedAt       *time.Time
	PausedAt        *time.Time
	ResumesAt       *time.Time
	CancelledAt     *time.Time

	// AmountCentsPaidTotal accumulates per-invoice paid amounts (a single
	// Subscription fires N invoice.paid events over its lifetime).
	AmountCentsPaidTotal int64
}

var (
	ErrPlanSKURequired      = errors.New("payments/user_subscription: plan_sku required")
	ErrBillingPeriodInvalid = errors.New("payments/user_subscription: billing_period must be monthly|annually")
)

// New constructs a UserSubscription in checkout_started + LifecycleState=created.
func New(
	purchaseID, tenantID, learnerGCID, planSKU string,
	billingPeriod BillingPeriod,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*UserSubscription, error) {
	if planSKU == "" {
		return nil, ErrPlanSKURequired
	}
	if !billingPeriod.IsValid() {
		return nil, ErrBillingPeriodInvalid
	}
	base, err := shared.NewPurchase(
		purchaseID, tenantID, learnerGCID,
		amountCents, currency,
		stripeSessionID, stripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, err
	}
	return &UserSubscription{
		Purchase:       base,
		PlanSKU:        planSKU,
		BillingPeriod:  billingPeriod,
		LifecycleState: SubStateCreated,
	}, nil
}

// MarkSubscriptionCreated captures the Stripe customer.subscription.created
// event (subscription object created, BEFORE first invoice is paid).
func (u *UserSubscription) MarkSubscriptionCreated(
	stripeSubscriptionID, stripeCustomerID string,
	currentPeriodStart, currentPeriodEnd time.Time,
	now time.Time,
) {
	u.LifecycleState = SubStateCreated
	u.StripeSubscriptionID = stripeSubscriptionID
	u.StripeCustomerID = stripeCustomerID
	u.CurrentPeriodStart = &currentPeriodStart
	u.CurrentPeriodEnd = &currentPeriodEnd
	u.CreatedAtStripe = &now
	u.UpdatedAt = now
}

// MarkSubscriptionRenewed captures customer.subscription.updated with
// period rollover. Advances CurrentPeriodStart + End.
func (u *UserSubscription) MarkSubscriptionRenewed(
	currentPeriodStart, currentPeriodEnd time.Time,
	now time.Time,
) {
	u.LifecycleState = SubStateActive
	u.CurrentPeriodStart = &currentPeriodStart
	u.CurrentPeriodEnd = &currentPeriodEnd
	u.RenewedAt = &now
	u.UpdatedAt = now
}

// MarkSubscriptionPaused captures customer.subscription.paused.
func (u *UserSubscription) MarkSubscriptionPaused(reason string, resumesAt *time.Time, now time.Time) {
	u.LifecycleState = SubStatePaused
	u.PauseReason = reason
	u.ResumesAt = resumesAt
	u.PausedAt = &now
	u.UpdatedAt = now
}

// MarkSubscriptionCancelled captures customer.subscription.deleted.
func (u *UserSubscription) MarkSubscriptionCancelled(reason string, atPeriodEnd bool, effectiveAt time.Time, now time.Time) {
	u.LifecycleState = SubStateCancelled
	u.CancellationReason = reason
	u.CancelAtPeriodEnd = atPeriodEnd
	u.EffectiveAt = &effectiveAt
	u.CancelledAt = &now
	u.UpdatedAt = now
}

// AccumulatePaid increments AmountCentsPaidTotal on a per-invoice
// payment_captured event (use AFTER the shared.Purchase MarkPaymentCaptured
// transition; that resets per-invoice AmountCentsPaid, but for recurring
// subscriptions the lifetime total lives in AmountCentsPaidTotal).
func (u *UserSubscription) AccumulatePaid(invoiceAmountCents int64) {
	u.AmountCentsPaidTotal += invoiceAmountCents
}

type Repo interface {
	Save(ctx context.Context, u *UserSubscription) error
	GetByID(ctx context.Context, tenantID, purchaseID string) (*UserSubscription, error)
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*UserSubscription, error)
	GetByStripeSubscriptionID(ctx context.Context, stripeSubscriptionID string) (*UserSubscription, error)
}
