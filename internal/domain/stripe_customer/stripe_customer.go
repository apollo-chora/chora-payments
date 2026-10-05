// Package stripe_customer is the lightweight aggregate that maps
// (tenant_id, learner_gcid) → stripe_customer_id so chora-payments can
// reuse the same Stripe Customer object across all 5 Purchase aggregates.
//
// Stripe persists PaymentMethods (saved cards) against the Customer
// object; passing the same `customer: cus_xxx` to every CheckoutSession
// for a learner is what makes "remember card for future checkouts" work.
//
// This is NOT a Purchase aggregate — it's an idempotent registry. No
// FSM, no state transitions, no Pub/Sub events. Just a stable cross-
// session mapping.
package stripe_customer

import (
	"context"
	"errors"
	"time"
)

// StripeCustomer is the registry row.
type StripeCustomer struct {
	TenantID         string
	LearnerGCID      string
	StripeCustomerID string // cus_xxx
	Email            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

var (
	ErrTenantRequired      = errors.New("payments/stripe_customer: tenant_id required")
	ErrLearnerRequired     = errors.New("payments/stripe_customer: learner_gcid required")
	ErrStripeCustomerEmpty = errors.New("payments/stripe_customer: stripe_customer_id required")
	ErrNotFound            = errors.New("payments/stripe_customer: not found")
)

// New constructs a StripeCustomer with validation.
func New(tenantID, learnerGCID, stripeCustomerID, email string, now time.Time) (*StripeCustomer, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	if learnerGCID == "" {
		return nil, ErrLearnerRequired
	}
	if stripeCustomerID == "" {
		return nil, ErrStripeCustomerEmpty
	}
	return &StripeCustomer{
		TenantID:         tenantID,
		LearnerGCID:      learnerGCID,
		StripeCustomerID: stripeCustomerID,
		Email:            email,
		CreatedAt:        now,
		UpdatedAt:        now,
	}, nil
}

// Repo is the hexagonal port.
type Repo interface {
	// GetByGCID returns the StripeCustomer for (tenantID, learnerGCID),
	// or ErrNotFound when no row matches.
	GetByGCID(ctx context.Context, tenantID, learnerGCID string) (*StripeCustomer, error)

	// GetByGCIDs returns map[gcid]→StripeCustomer for the supplied GCID
	// set. Missing GCIDs (no row) are omitted from the map. Used by the
	// H+ Transaction History UNION ALL projection to attach learner_email
	// in batch (avoids N+1 GetByGCID per row).
	GetByGCIDs(ctx context.Context, tenantID string, gcids []string) (map[string]*StripeCustomer, error)

	// Insert persists a new row. Idempotent on (tenant_id, learner_gcid):
	// duplicate inserts return the existing row unchanged.
	Insert(ctx context.Context, sc *StripeCustomer) error
}
