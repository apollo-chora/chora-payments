// Package tenant_mana_topup is the tenant mana pool top-up Stripe Checkout
// aggregate (replaces chora.tenancy.tenant_mana_pool.topped_up.v1 at
// Wave 1 Stage D cutover).
//
// Topic family: chora.payments.tenant_mana_topup.*.v1
// Originating service: chora-tenancy (credits tenant_mana_pool.balance_units).
package tenant_mana_topup

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// TenantManaTopUp is the aggregate root for a tenant-admin-initiated mana
// pool top-up.
type TenantManaTopUp struct {
	shared.Purchase

	// SKU from the platform mana-topup catalog.
	SKU string

	// ManaUnits to credit on payment_captured (snapshot from SKU at
	// session creation).
	ManaUnits int64

	// ManaUnitsDebited tracks proportional debits on refund.
	ManaUnitsDebited int64

	// AdminGCID is an alias for LearnerGCID — the tenant_admin who
	// initiated the top-up. We use LearnerGCID under the hood to keep
	// the shared.Purchase shape uniform; expose AdminGCID() for API
	// clarity.
}

// AdminGCID returns LearnerGCID (the canonical caller of the top-up).
func (t *TenantManaTopUp) AdminGCID() string {
	return t.LearnerGCID
}

var (
	ErrSKURequired       = errors.New("payments/tenant_mana_topup: sku required")
	ErrManaUnitsRequired = errors.New("payments/tenant_mana_topup: mana_units must be > 0")
)

// New constructs a TenantManaTopUp in checkout_started state.
//
// adminGCID is the tenant_admin GCID; persisted into shared.Purchase.LearnerGCID
// for uniform field layout.
func New(
	purchaseID, tenantID, adminGCID, sku string,
	manaUnits int64,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*TenantManaTopUp, error) {
	if sku == "" {
		return nil, ErrSKURequired
	}
	if manaUnits <= 0 {
		return nil, ErrManaUnitsRequired
	}
	base, err := shared.NewPurchase(
		purchaseID, tenantID, adminGCID,
		amountCents, currency,
		stripeSessionID, stripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, err
	}
	return &TenantManaTopUp{
		Purchase:  base,
		SKU:       sku,
		ManaUnits: manaUnits,
	}, nil
}

type Repo interface {
	Save(ctx context.Context, t *TenantManaTopUp) error
	GetByID(ctx context.Context, tenantID, purchaseID string) (*TenantManaTopUp, error)
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*TenantManaTopUp, error)
}
