// Package user_mana_topup is the per-learner FE-initiated mana top-up
// Stripe Checkout aggregate. Mirrors tenant_mana_topup but for learners
// (admin_gcid → learner_gcid; no tenant subsidy field).
//
// Topic family: chora.payments.user_mana_topup.*.v1
// Originating service: chora-identity (credits per-user mana balance on
//
//	payment_captured).
//
// Closes Stage E debt: `/api/v1/me/mana/topup` (per-user FE-initiated
// mana purchase) was retired from chora-identity but did not previously
// map onto ADR-164's 5 aggregates (tenant_mana_topup is ADMIN-initiated).
// Stage A.5 introduces this 6th aggregate as a per-user mirror.
package user_mana_topup

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// UserManaTopUp is the aggregate root for a learner-initiated mana
// top-up.
type UserManaTopUp struct {
	shared.Purchase

	// SKU from the platform user-mana-topup catalog.
	SKU string

	// ManaUnits to credit on payment_captured (snapshot from SKU at
	// session creation).
	ManaUnits int64

	// ManaUnitsDebited tracks proportional debits on refund.
	ManaUnitsDebited int64
}

var (
	ErrSKURequired       = errors.New("payments/user_mana_topup: sku required")
	ErrManaUnitsRequired = errors.New("payments/user_mana_topup: mana_units must be > 0")
)

// New constructs a UserManaTopUp in checkout_started state.
func New(
	purchaseID, tenantID, learnerGCID, sku string,
	manaUnits int64,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*UserManaTopUp, error) {
	if sku == "" {
		return nil, ErrSKURequired
	}
	if manaUnits <= 0 {
		return nil, ErrManaUnitsRequired
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
	return &UserManaTopUp{
		Purchase:  base,
		SKU:       sku,
		ManaUnits: manaUnits,
	}, nil
}

type Repo interface {
	Save(ctx context.Context, t *UserManaTopUp) error
	GetByID(ctx context.Context, tenantID, purchaseID string) (*UserManaTopUp, error)
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*UserManaTopUp, error)
}
