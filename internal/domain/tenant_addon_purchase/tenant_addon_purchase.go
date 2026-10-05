// Package tenant_addon_purchase is the H+ Marketplace tenant-add-on
// subscription Stripe Checkout aggregate (CHO-1736 epic; reads from the
// catalog shipped in CHO-1735).
//
// Topic family: chora.payments.tenant_addon_purchase.*.v1
// Originating service: chora-tenancy (activates the AddonSubscription
//
//	via the Sub 4 event bus subscriber).
//
// Aggregate-specific shape vs. the sibling tenant_mana_topup
// (tenant-scoped):
//   - AdminGCID() aliases shared.LearnerGCID (uniform field layout —
//     same trick as TenantManaTopUp.AdminGCID()).
//   - AddonPlanID is the marketplace catalog primary key (UUID).
//   - AddonCode is denormalised alongside AddonPlanID so downstream
//     consumers can index without a tenancy lookup (cross-DB queries
//     forbidden).
//   - TierCode is the selected pricing tier (e.g., "starter" / "pro").
package tenant_addon_purchase

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// TenantAddonPurchase is the aggregate root for a single tenant-admin-
// initiated H+ Marketplace add-on subscription Checkout flow.
//
// CHO-1761 extension: the post-CHO-1762 Subscribe flow creates a real
// Stripe Subscription (mode=subscription instead of mode=payment), so
// the aggregate carries the Stripe Subscription correlation fields
// alongside the original one-time-payment fields. Legacy rows (created
// pre-CHO-1762) have empty StripeSubscriptionID / Status — that's the
// signal CHO-1764 uses to refuse change-tier with `no_stripe_subscription`.
type TenantAddonPurchase struct {
	shared.Purchase

	// AddonPlanID is the marketplace catalog primary key — the AddOnPlan
	// being subscribed to. Soft FK to chora_tenancy.addon_plans;
	// chora-payments does NOT validate plan existence (cross-DB queries
	// forbidden).
	AddonPlanID string

	// AddonCode is the human-readable code (e.g., "knowledge_graph",
	// "familiar") — denormalised at session-creation time so downstream
	// consumers can index without a tenancy lookup.
	AddonCode string

	// TierCode is the selected pricing tier (e.g., "starter", "pro").
	TierCode string

	// --- CHO-1761 Stripe Subscription correlation (post-CHO-1762) ----

	// StripeCustomerID is the Stripe Customer holding the recurring
	// subscription's saved payment method. Empty for legacy rows.
	StripeCustomerID string

	// StripeSubscriptionID is the Stripe Subscription's `sub_...` ID.
	// Empty for legacy (mode=payment) rows; populated by CHO-1763
	// webhook handler when `customer.subscription.created` fires.
	StripeSubscriptionID string

	// Status mirrors Stripe's subscription status (lowercase wire shape).
	// Empty until the webhook hits.
	Status Status

	// CurrentPeriodStart / End mirror Stripe's billing cycle anchors.
	// Nullable on active subscriptions Stripe emits both; on cancelled
	// or past_due they may be absent. Stored UTC.
	CurrentPeriodStart *time.Time
	CurrentPeriodEnd   *time.Time

	// --- CHO-1772 Stripe SubscriptionSchedule correlation -----------

	// StripeSubscriptionScheduleID is the Stripe SubscriptionSchedule
	// `sub_sched_...` ID populated when the admin opts to defer a tier
	// change to the next billing anchor (effective_at=end_of_cycle).
	// Empty when no schedule is pending. While non-empty:
	//   - TierCode reflects the CURRENT paid-for tier (NOT the scheduled tier)
	//   - ScheduledTierCode is the future tier
	//   - ScheduledEffectiveAt is when Stripe will activate the new phase
	// On the `subscription_schedule.released` webhook the dispatcher
	// calls ReleaseSchedule, which promotes ScheduledTierCode → TierCode
	// and nulls all three schedule fields.
	StripeSubscriptionScheduleID string

	// ScheduledTierCode is the deferred-target tier_code. Drives the
	// "Pro starting 2026-07-15" badge on /h/addons when set.
	ScheduledTierCode string

	// ScheduledEffectiveAt is when the schedule's second phase begins
	// (typically the Stripe `current_period_end` at the time the
	// schedule was created). Stored UTC.
	ScheduledEffectiveAt *time.Time
}

// Status is the wire-shape Stripe Subscription lifecycle status,
// constrained to the 4 cases chora-payments handles in CHO-1763.
type Status string

const (
	StatusPending   Status = "pending"
	StatusActive    Status = "active"
	StatusPastDue   Status = "past_due"
	StatusCancelled Status = "cancelled"
)

// IsValidStatus reports whether the given Status is one of the 4
// recognised values. Empty + uppercase + arbitrary strings all return
// false — the wire shape is strict lowercase per Stripe's payload.
func IsValidStatus(s Status) bool {
	switch s {
	case StatusPending, StatusActive, StatusPastDue, StatusCancelled:
		return true
	default:
		return false
	}
}

// ApplyStripeSubscriptionState upserts the Stripe Subscription
// correlation onto the aggregate. The CHO-1763 webhook handler is the
// sole expected caller; idempotent under repeated delivery.
//
// Fail-loud on invalid inputs so silent typos in the webhook decode
// don't leave aggregates with garbage state. Nil period boundaries are
// accepted — Stripe emits them only on active subscriptions.
func (t *TenantAddonPurchase) ApplyStripeSubscriptionState(
	stripeCustomerID, stripeSubscriptionID string,
	status Status,
	currentPeriodStart, currentPeriodEnd *time.Time,
) error {
	if stripeCustomerID == "" {
		return ErrStripeCustomerRequired
	}
	if stripeSubscriptionID == "" {
		return ErrStripeSubscriptionRequired
	}
	if !IsValidStatus(status) {
		return ErrInvalidStatus
	}
	t.StripeCustomerID = stripeCustomerID
	t.StripeSubscriptionID = stripeSubscriptionID
	t.Status = status
	if currentPeriodStart != nil {
		utc := currentPeriodStart.UTC()
		t.CurrentPeriodStart = &utc
	} else {
		t.CurrentPeriodStart = nil
	}
	if currentPeriodEnd != nil {
		utc := currentPeriodEnd.UTC()
		t.CurrentPeriodEnd = &utc
	} else {
		t.CurrentPeriodEnd = nil
	}
	return nil
}

// AdminGCID returns LearnerGCID (the canonical caller of the
// subscription — the tenant_admin). Mirrors TenantManaTopUp.AdminGCID().
func (t *TenantAddonPurchase) AdminGCID() string {
	return t.LearnerGCID
}

var (
	ErrAddonPlanRequired = errors.New("payments/tenant_addon_purchase: addon_plan_id required")
	ErrAddonCodeRequired = errors.New("payments/tenant_addon_purchase: addon_code required")
	ErrTierCodeRequired  = errors.New("payments/tenant_addon_purchase: tier_code required")
	// CHO-1761 — Stripe Subscription correlation guards.
	ErrStripeCustomerRequired     = errors.New("payments/tenant_addon_purchase: stripe_customer_id required")
	ErrStripeSubscriptionRequired = errors.New("payments/tenant_addon_purchase: stripe_subscription_id required")
	ErrInvalidStatus              = errors.New("payments/tenant_addon_purchase: invalid subscription status")
	// CHO-1772 — SubscriptionSchedule correlation guards.
	ErrStripeScheduleRequired       = errors.New("payments/tenant_addon_purchase: stripe_subscription_schedule_id required")
	ErrScheduledTierRequired        = errors.New("payments/tenant_addon_purchase: scheduled_tier_code required")
	ErrScheduledEffectiveAtRequired = errors.New("payments/tenant_addon_purchase: scheduled_effective_at required")
)

// ApplySchedule attaches a Stripe SubscriptionSchedule (end-of-cycle
// deferred tier change) to the aggregate. TierCode is NOT mutated — it
// stays at the current paid-for tier until the schedule releases. On
// subscription_schedule.released the dispatcher calls ReleaseSchedule
// to promote ScheduledTierCode → TierCode.
func (t *TenantAddonPurchase) ApplySchedule(scheduleID, scheduledTier string, effectiveAt time.Time) error {
	if scheduleID == "" {
		return ErrStripeScheduleRequired
	}
	if scheduledTier == "" {
		return ErrScheduledTierRequired
	}
	if effectiveAt.IsZero() {
		return ErrScheduledEffectiveAtRequired
	}
	utc := effectiveAt.UTC()
	t.StripeSubscriptionScheduleID = scheduleID
	t.ScheduledTierCode = scheduledTier
	t.ScheduledEffectiveAt = &utc
	return nil
}

// ReleaseSchedule promotes ScheduledTierCode → TierCode and clears the
// three schedule fields. Idempotent: calling on a row without a pending
// schedule is a no-op (returns nil) so duplicate Stripe webhook
// deliveries don't error.
func (t *TenantAddonPurchase) ReleaseSchedule() error {
	if !t.HasPendingSchedule() {
		return nil
	}
	t.TierCode = t.ScheduledTierCode
	t.ClearSchedule()
	return nil
}

// ClearSchedule nulls the three schedule fields WITHOUT promoting
// ScheduledTierCode to TierCode. The cancellation path: the admin
// backed out before the cycle rolled, so we tear down the local
// schedule state. Cancelling the remote Stripe schedule is the
// caller's responsibility.
func (t *TenantAddonPurchase) ClearSchedule() {
	t.StripeSubscriptionScheduleID = ""
	t.ScheduledTierCode = ""
	t.ScheduledEffectiveAt = nil
}

// HasPendingSchedule reports whether all three schedule fields are
// populated. Used by the FE to render "Pro starting 2026-07-15" copy +
// by ReleaseSchedule for idempotency.
func (t *TenantAddonPurchase) HasPendingSchedule() bool {
	return t.StripeSubscriptionScheduleID != "" &&
		t.ScheduledTierCode != "" &&
		t.ScheduledEffectiveAt != nil
}

// New constructs a TenantAddonPurchase in checkout_started state.
//
// adminGCID is the tenant_admin GCID; persisted into
// shared.Purchase.LearnerGCID for uniform field layout (same as
// TenantManaTopUp).
func New(
	purchaseID, tenantID, adminGCID string,
	addonPlanID, addonCode, tierCode string,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*TenantAddonPurchase, error) {
	if addonPlanID == "" {
		return nil, ErrAddonPlanRequired
	}
	if addonCode == "" {
		return nil, ErrAddonCodeRequired
	}
	if tierCode == "" {
		return nil, ErrTierCodeRequired
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
	return &TenantAddonPurchase{
		Purchase:    base,
		AddonPlanID: addonPlanID,
		AddonCode:   addonCode,
		TierCode:    tierCode,
	}, nil
}

// -----------------------------------------------------------------------------
// Repo port
// -----------------------------------------------------------------------------

// Repo is the hexagonal port for persisting + retrieving TenantAddonPurchase
// aggregates. Production wires the pg adapter; unit tests wire the
// inmem adapter.
type Repo interface {
	// Save UPSERTs the aggregate by PurchaseID.
	Save(ctx context.Context, t *TenantAddonPurchase) error
	// GetByID looks up a TenantAddonPurchase by PurchaseID inside the RLS
	// scope of `tenantID`. Returns shared.ErrNotFound when no row matches.
	GetByID(ctx context.Context, tenantID, purchaseID string) (*TenantAddonPurchase, error)
	// GetByStripeSessionID — webhook dispatch lookup. RLS-bypass safe
	// because session_id is UNIQUE + Stripe-issued (no cross-tenant
	// collision risk).
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*TenantAddonPurchase, error)
	// GetByStripeSubscriptionID — CHO-1761 webhook dispatch lookup for
	// the customer.subscription.{created,updated,deleted} + invoice.* events
	// that don't carry session_id. Returns shared.ErrNotFound on miss.
	// Empty subscriptionID returns ErrStripeSubscriptionRequired (no
	// catch-all "first row with empty sub_id" foot-gun).
	GetByStripeSubscriptionID(ctx context.Context, stripeSubscriptionID string) (*TenantAddonPurchase, error)
	// GetActiveByTenantAndAddonCode — CHO-1764 change-tier lookup.
	// Returns the most-recent payment_captured row for the (tenant,
	// addon_code) pair. The change-tier handler doesn't know the
	// purchase_id (chora-tenancy's view is keyed by addon_code only),
	// so payments resolves the row internally. Empty inputs return
	// ErrTenantRequired / ErrAddonCodeRequired; no match returns
	// shared.ErrNotFound.
	GetActiveByTenantAndAddonCode(ctx context.Context, tenantID, addonCode string) (*TenantAddonPurchase, error)
	// GetStripeCustomerIDByTenant — H+ Billing lookup. Returns the
	// tenant's Stripe Customer ID (one Customer per tenant per the
	// CHO-1762 contract). Empty tenantID returns ErrTenantRequired;
	// a tenant with NO active TAP rows returns empty string + nil
	// error so the billing handler can render an empty invoice list
	// without a 404. Rows that haven't reached payment_captured (no
	// Customer wired yet) are skipped — only non-empty customer IDs
	// satisfy the lookup.
	GetStripeCustomerIDByTenant(ctx context.Context, tenantID string) (string, error)
	// GetByStripeScheduleID — CHO-1772 webhook dispatch lookup for the
	// `subscription_schedule.released` event. The schedule ID is UNIQUE
	// + Stripe-issued (no cross-tenant collision), so this skips the
	// RLS scope filter the same way GetByStripeSubscriptionID does.
	// Empty scheduleID returns ErrStripeScheduleRequired; no match
	// returns shared.ErrNotFound.
	GetByStripeScheduleID(ctx context.Context, stripeScheduleID string) (*TenantAddonPurchase, error)
}

// ErrTenantRequired is returned by GetActiveByTenantAndAddonCode on
// empty tenantID; shared with other Repo callers via the package.
var ErrTenantRequired = errors.New("payments/tenant_addon_purchase: tenant_id required")
