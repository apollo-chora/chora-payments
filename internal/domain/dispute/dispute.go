// Package dispute is the chargeback-lifecycle aggregate for chora-payments
// (ADR-164 §229.5).
//
// Dispute is a SEPARATE aggregate root (chosen 2026-05-24 over Purchase-
// extending columns) to keep dispute lifecycle cleanly distinct from the
// 5-state Purchase payment FSM. Joins to its parent Purchase by
// (aggregate_type, purchase_id) — no FK constraint per ddd-enforcement
// cross-aggregate invariant; integrity is enforced at write time by the
// dispatcher resolving the Purchase aggregate before persisting the
// dispute row.
//
// Lifecycle FSM:
//
//	raised → closed (with outcome ∈ {won, lost, warning_closed})
//
// Funds-movement tracking is orthogonal to lifecycle — Stripe may fire
// funds_withdrawn during the raised window and funds_reinstated either
// before close (warning_closed) or after close (won). The aggregate
// carries both as discrete booleans + timestamps.
//
// Cross-aggregate topic family: chora.payments.dispute.{raised,closed,
// funds_withdrawn,funds_reinstated}.v1 (see proto/events/payments/
// dispute.proto for the rationale).
package dispute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// State is the dispute lifecycle state.
type State string

const (
	StateRaised State = "raised"
	StateClosed State = "closed"
)

// IsValid reports whether s is a recognised State.
func (s State) IsValid() bool {
	switch s {
	case StateRaised, StateClosed:
		return true
	}
	return false
}

// Outcome is the resolved verdict of a dispute (set on close).
type Outcome string

const (
	OutcomeWon           Outcome = "won"
	OutcomeLost          Outcome = "lost"
	OutcomeWarningClosed Outcome = "warning_closed"
)

// IsValid reports whether o is a recognised Outcome.
func (o Outcome) IsValid() bool {
	switch o {
	case OutcomeWon, OutcomeLost, OutcomeWarningClosed:
		return true
	}
	return false
}

// Reason mirrors the Stripe Dispute.Reason enum (string-typed so unknown
// values from new Stripe API releases don't break ingestion).
//
// Canonical values per Stripe API:
//   - fraudulent
//   - duplicate
//   - subscription_canceled
//   - product_not_received
//   - product_unacceptable
//   - credit_not_processed
//   - general
//   - unrecognized
//   - bank_cannot_process
//   - debit_not_authorized
//   - check_returned
//   - incorrect_account_details
//   - insufficient_funds
//   - customer_initiated
type Reason string

const (
	ReasonFraudulent   Reason = "fraudulent"
	ReasonDuplicate    Reason = "duplicate"
	ReasonGeneral      Reason = "general"
	ReasonUnrecognized Reason = "unrecognized"
	// Other Stripe-defined reasons accepted as opaque string values.
)

// Dispute is the aggregate root for a single chargeback record.
type Dispute struct {
	DisputeID string // UUIDv7 (chora-payments-issued)
	TenantID  string // tenant scope inherited from parent Purchase

	AggregateType wh.AggregateType
	PurchaseID    string

	StripeDisputeID string // dp_xxx (UNIQUE)
	StripeChargeID  string // ch_xxx

	State   State
	Outcome Outcome

	AmountCents int64
	Currency    string // ISO 4217 3-letter

	Reason Reason

	EvidenceDueBy time.Time

	// Funds-movement tracking (independent of lifecycle).
	FundsWithdrawn    bool
	FundsWithdrawnAt  *time.Time
	FundsReinstated   bool
	FundsReinstatedAt *time.Time

	RaisedAt time.Time
	ClosedAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

var (
	ErrAggregateTypeRequired = errors.New("payments/dispute: aggregate_type required")
	ErrPurchaseIDRequired    = errors.New("payments/dispute: purchase_id required")
	ErrStripeDisputeRequired = errors.New("payments/dispute: stripe_dispute_id required")
	ErrStripeChargeRequired  = errors.New("payments/dispute: stripe_charge_id required")
	ErrDisputeIDRequired     = errors.New("payments/dispute: dispute_id required")
	ErrOutcomeInvalid        = errors.New("payments/dispute: outcome must be won|lost|warning_closed")
	ErrOutcomeConflict       = errors.New("payments/dispute: conflicting close outcome on already-closed dispute")
	ErrNotFound              = errors.New("payments/dispute: not found")
)

// New constructs a Dispute in `raised` state from a charge.dispute.created
// webhook payload.
//
// Validation mirrors shared.Purchase invariants: non-empty tenant + aggregate
// + stripe handles, valid currency + non-negative amount.
func New(
	disputeID, tenantID string,
	aggregateType wh.AggregateType,
	purchaseID string,
	stripeDisputeID, stripeChargeID string,
	amountCents int64, currency string,
	reason Reason,
	evidenceDueBy time.Time,
	now time.Time,
) (*Dispute, error) {
	if disputeID == "" {
		return nil, ErrDisputeIDRequired
	}
	if tenantID == "" {
		return nil, shared.ErrTenantRequired
	}
	if !aggregateType.IsValid() {
		return nil, ErrAggregateTypeRequired
	}
	if purchaseID == "" {
		return nil, ErrPurchaseIDRequired
	}
	if stripeDisputeID == "" {
		return nil, ErrStripeDisputeRequired
	}
	if stripeChargeID == "" {
		return nil, ErrStripeChargeRequired
	}
	if err := shared.ValidateAmount(amountCents); err != nil {
		return nil, err
	}
	if err := shared.ValidateCurrency(currency); err != nil {
		return nil, err
	}
	return &Dispute{
		DisputeID:       disputeID,
		TenantID:        tenantID,
		AggregateType:   aggregateType,
		PurchaseID:      purchaseID,
		StripeDisputeID: stripeDisputeID,
		StripeChargeID:  stripeChargeID,
		State:           StateRaised,
		AmountCents:     amountCents,
		Currency:        currency,
		Reason:          reason,
		EvidenceDueBy:   evidenceDueBy,
		RaisedAt:        now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

// MarkClosed transitions raised → closed with an outcome.
//
// Idempotent on repeat of the SAME outcome (Stripe webhook redelivery).
// Refuses conflicting outcomes on an already-closed dispute — Stripe
// shouldn't fire conflicting close events; an attempt is treated as a
// real integrity violation.
func (d *Dispute) MarkClosed(outcome Outcome, closedAt time.Time) error {
	if !outcome.IsValid() {
		return ErrOutcomeInvalid
	}
	if d.State == StateClosed {
		if d.Outcome == outcome {
			return nil // idempotent replay
		}
		return fmt.Errorf("%w: existing=%s incoming=%s", ErrOutcomeConflict, d.Outcome, outcome)
	}
	d.State = StateClosed
	d.Outcome = outcome
	d.ClosedAt = &closedAt
	d.UpdatedAt = closedAt
	return nil
}

// RecordFundsWithdrawn marks that Stripe debited our balance for this
// dispute. Idempotent.
func (d *Dispute) RecordFundsWithdrawn(at time.Time) error {
	if d.FundsWithdrawn {
		return nil // idempotent
	}
	d.FundsWithdrawn = true
	d.FundsWithdrawnAt = &at
	d.UpdatedAt = at
	return nil
}

// RecordFundsReinstated marks that Stripe returned funds for this dispute.
// Accepts arrival without a prior funds_withdrawn (warning-closed disputes
// often skip the withdraw step). Idempotent.
func (d *Dispute) RecordFundsReinstated(at time.Time) error {
	if d.FundsReinstated {
		return nil // idempotent
	}
	d.FundsReinstated = true
	d.FundsReinstatedAt = &at
	d.UpdatedAt = at
	return nil
}

// Repo is the hexagonal port for the disputes table.
type Repo interface {
	// Insert attempts to INSERT a new dispute. Honors UNIQUE constraint on
	// stripe_dispute_id — if Stripe replays the create event, the second
	// insert MUST return ErrAlreadyExists or be silently absorbed by the
	// dispatcher's stripe_webhook_events dedup. Adapter implementations
	// should report uniqueness violations distinctly so the dispatcher can
	// fall through to a no-op replay path.
	Insert(ctx context.Context, d *Dispute) error

	// Save UPSERTs an existing dispute (used after lifecycle transitions
	// and funds-movement updates).
	Save(ctx context.Context, d *Dispute) error

	// GetByStripeDisputeID is the primary read path — incoming Stripe
	// webhook events (closed / funds_withdrawn / funds_reinstated) carry
	// the dp_xxx handle but not the chora-payments dispute_id.
	GetByStripeDisputeID(ctx context.Context, stripeDisputeID string) (*Dispute, error)

	// GetByID — administrative lookup by chora-payments dispute_id.
	GetByID(ctx context.Context, tenantID, disputeID string) (*Dispute, error)

	// ListByPurchase — administrative lookup of all disputes attached to
	// a single Purchase aggregate (rare, but supports replay debugging).
	ListByPurchase(ctx context.Context, tenantID string, aggregateType wh.AggregateType, purchaseID string) ([]*Dispute, error)
}

// ErrAlreadyExists — adapter-level sentinel for UNIQUE-violation on insert
// (stripe_dispute_id conflict). Distinct from ErrNotFound + dispatcher
// uses errors.Is to decide whether a duplicate insert is a replay (silent
// skip) or a true conflict.
var ErrAlreadyExists = errors.New("payments/dispute: stripe_dispute_id already exists")
