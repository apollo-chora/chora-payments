// Package familiar_egg_purchase is the ADR-149 Familiar-egg Stripe Checkout
// aggregate (replaces chora.tenancy.familiar_egg.* at Wave 1 Stage D cutover).
//
// Topic family: chora.payments.familiar_egg_purchase.*.v1
// Originating service: chora-tenancy (provisions familiar instance);
// downstream: chora-consumption (Stage-0 familiar_instances row).
package familiar_egg_purchase

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// FamiliarEggPurchase is the aggregate root for an ADR-149 Familiar-egg buy.
type FamiliarEggPurchase struct {
	shared.Purchase

	// EggSKU from the tenant's catalog (e.g., "egg.standard.v1").
	EggSKU string

	// SuggestedFocalAtomID is the pre-bound focal-atom hint from the SKU
	// (may be empty for open-ended SKUs where the learner picks at hatching).
	SuggestedFocalAtomID string

	// SoftExpiryAt + HardExpiryAt — egg expiry deadlines snapshotted at
	// checkout time. ADR-149 §"Unhatched egg expiry": HardExpiryAt triggers
	// the auto-refund-credit path (state → refunded, reason=
	// expired_unhatched, credit_only=true).
	SoftExpiryAt *time.Time
	HardExpiryAt *time.Time

	// RefundCreditOnly is true if a refund yields mana_credit instead of
	// real-money refund. Set by the ADR-149 hard-expiry sweeper or by
	// support-initiated credit refunds.
	RefundCreditOnly bool

	// ProvisionedAt is set by chora-tenancy on its subscriber when the
	// Stage-0 familiar_instances row is provisioned. Optional bookkeeping
	// only — does not affect chora-payments state.
	ProvisionedAt *time.Time
}

var ErrEggSKURequired = errors.New("payments/familiar_egg_purchase: egg_sku required")

// New constructs a FamiliarEggPurchase in checkout_started state.
func New(
	purchaseID, tenantID, learnerGCID, eggSKU string,
	suggestedFocalAtomID string,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	softExpiryAt, hardExpiryAt *time.Time,
	now time.Time,
) (*FamiliarEggPurchase, error) {
	if eggSKU == "" {
		return nil, ErrEggSKURequired
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
	return &FamiliarEggPurchase{
		Purchase:             base,
		EggSKU:               eggSKU,
		SuggestedFocalAtomID: suggestedFocalAtomID,
		SoftExpiryAt:         softExpiryAt,
		HardExpiryAt:         hardExpiryAt,
	}, nil
}

type Repo interface {
	Save(ctx context.Context, ep *FamiliarEggPurchase) error
	GetByID(ctx context.Context, tenantID, purchaseID string) (*FamiliarEggPurchase, error)
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*FamiliarEggPurchase, error)
}
