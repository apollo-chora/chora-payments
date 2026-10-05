// Package webhook_event is the inbound Stripe webhook receipt aggregate.
// Backs the chora_payments.stripe_webhook_events table (RLS-disabled global
// dedup) per ADR-164.
//
// Lifecycle: received → processed | failed
//
// The dedup gate is the UNIQUE constraint on event_id (`evt_xxx`) — Stripe
// retries cannot create a second row. The Process / Fail transitions are
// purely bookkeeping; the actual aggregate transition happens in the
// corresponding Purchase aggregate's repo via a separate atomic update.
package webhook_event

import (
	"context"
	"errors"
	"time"
)

// State is the inbound webhook FSM state.
type State string

const (
	StateReceived  State = "received"
	StateProcessed State = "processed"
	StateFailed    State = "failed"
)

// AggregateType is the target Purchase aggregate this webhook event drives.
type AggregateType string

const (
	AggregateCoursePurchase      AggregateType = "course_purchase"
	AggregateApplicationPayment  AggregateType = "application_payment"
	AggregateFamiliarEggPurchase AggregateType = "familiar_egg_purchase"
	AggregateTenantManaTopUp     AggregateType = "tenant_mana_topup"
	AggregateUserSubscription    AggregateType = "user_subscription"
	// AggregateUserManaTopUp is the 6th aggregate (Stage A.5).
	AggregateUserManaTopUp AggregateType = "user_mana_topup"
	// AggregateIdentityKycFee is the 7th aggregate (Stage A.5).
	AggregateIdentityKycFee AggregateType = "identity_kyc_fee"
	// AggregateTenantAddonPurchase is the 8th aggregate (CHO-1736 H+
	// Marketplace Subscribe).
	AggregateTenantAddonPurchase AggregateType = "tenant_addon_purchase"
	AggregateUnknown             AggregateType = ""
)

// IsValid reports whether a is a recognised AggregateType.
func (a AggregateType) IsValid() bool {
	switch a {
	case AggregateCoursePurchase, AggregateApplicationPayment,
		AggregateFamiliarEggPurchase, AggregateTenantManaTopUp,
		AggregateUserSubscription,
		AggregateUserManaTopUp, AggregateIdentityKycFee,
		AggregateTenantAddonPurchase:
		return true
	}
	return false
}

// WebhookEvent is the aggregate root for a single Stripe webhook receipt.
type WebhookEvent struct {
	EventID             string // Stripe evt_xxx (PRIMARY KEY)
	EventType           string // e.g. "checkout.session.completed"
	ReceivedAt          time.Time
	ProcessedAt         *time.Time
	ProcessingError     string
	TargetAggregateType AggregateType
	TargetPurchaseID    string
}

var (
	ErrEventIDRequired   = errors.New("payments/webhook_event: event_id required")
	ErrEventTypeRequired = errors.New("payments/webhook_event: event_type required")
	ErrAlreadyProcessed  = errors.New("payments/webhook_event: already processed (idempotent skip)")
)

// New constructs a WebhookEvent in received state.
func New(eventID, eventType string, now time.Time) (*WebhookEvent, error) {
	if eventID == "" {
		return nil, ErrEventIDRequired
	}
	if eventType == "" {
		return nil, ErrEventTypeRequired
	}
	return &WebhookEvent{
		EventID:    eventID,
		EventType:  eventType,
		ReceivedAt: now,
	}, nil
}

// MarkProcessed transitions received → processed.
func (w *WebhookEvent) MarkProcessed(targetAggregate AggregateType, targetPurchaseID string, now time.Time) {
	w.TargetAggregateType = targetAggregate
	w.TargetPurchaseID = targetPurchaseID
	w.ProcessedAt = &now
}

// MarkFailed records a processing error (received → failed).
func (w *WebhookEvent) MarkFailed(err string) {
	w.ProcessingError = err
}

// IsProcessed reports whether the webhook has already been processed.
func (w *WebhookEvent) IsProcessed() bool {
	return w.ProcessedAt != nil
}

// Repo is the hexagonal port for the global Stripe webhook dedup table.
type Repo interface {
	// Insert attempts to INSERT a new webhook event. If the event_id
	// already exists (UNIQUE violation), returns ErrAlreadyProcessed
	// (idempotent gate: Stripe redelivery → 200 OK + skip).
	Insert(ctx context.Context, w *WebhookEvent) error

	// MarkProcessed updates the row's processed_at + target_aggregate_type
	// + target_purchase_id.
	MarkProcessed(ctx context.Context, eventID string, targetAggregate AggregateType, targetPurchaseID string, processedAt time.Time) error

	// MarkFailed records the processing error.
	MarkFailed(ctx context.Context, eventID, processingError string) error

	// GetByEventID — lookup for replay debugging.
	GetByEventID(ctx context.Context, eventID string) (*WebhookEvent, error)
}
