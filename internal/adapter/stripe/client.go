// Package stripe is the canonical Stripe SDK adapter for chora-payments.
// Per ADR-164, this is the ONLY place in the monorepo that imports
// `github.com/stripe/stripe-go/v78` (chora-delivery + chora-identity +
// chora-tenancy lose their copies in Wave 1 Stage C/D/E rewires).
//
// Multi-aggregate routing: CreateCheckoutSession takes a PurchaseType
// discriminator + metadata; the same Stripe Checkout Sessions API
// underpins all 5 aggregates (course_purchase, application_payment,
// familiar_egg_purchase, tenant_mana_topup, user_subscription).
//
// Stub vs Real:
//   - STRIPE_API_KEY unset → StubClient (deterministic URLs for local dev
//   - standalone smoke gate).
//   - STRIPE_API_KEY set   → RealClient (Stripe REST API via stripe-go).
//
// The stub is dev-only per `feedback_no_stubs_real_wiring`; the boot log
// makes the gating visible (WARNING line in cmd/server/main.go).
package stripe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PurchaseType is the aggregate-type discriminator for multi-aggregate
// Session creation.
type PurchaseType string

const (
	PurchaseTypeCoursePurchase      PurchaseType = "course_purchase"
	PurchaseTypeApplicationPayment  PurchaseType = "application_payment"
	PurchaseTypeFamiliarEggPurchase PurchaseType = "familiar_egg_purchase"
	PurchaseTypeTenantManaTopUp     PurchaseType = "tenant_mana_topup"
	PurchaseTypeUserSubscription    PurchaseType = "user_subscription"
	// 6th + 7th aggregates per Stage A.5.
	PurchaseTypeUserManaTopUp  PurchaseType = "user_mana_topup"
	PurchaseTypeIdentityKycFee PurchaseType = "identity_kyc_fee"
	// 8th aggregate — CHO-1736 H+ Marketplace Subscribe.
	PurchaseTypeTenantAddonPurchase PurchaseType = "tenant_addon_purchase"
)

// CreateCheckoutSessionInput carries the parameters for a Stripe Checkout
// Session creation. Metadata is propagated verbatim to Stripe + back via
// the webhook so chora-payments can resolve the target aggregate +
// purchase_id post-payment.
type CreateCheckoutSessionInput struct {
	PurchaseType PurchaseType
	PurchaseID   string // UUIDv7 of the chora_payments row
	TenantID     string
	LearnerGCID  string
	AmountCents  int64
	Currency     string // ISO 4217
	ProductName  string // Stripe Price product name (e.g., "CSPO Course — Phyllis CJ#2")
	SuccessURL   string
	CancelURL    string
	// StripeCustomerID is the Stripe Customer (cus_xxx) to attach the
	// Session to. When non-empty, Stripe persists the PaymentMethod on
	// the Customer on first checkout and shows the saved card on
	// subsequent Sessions for the same Customer. Per ADR-164 §D2 the
	// caller (chora-payments gRPC server) must EnsureCustomer first.
	StripeCustomerID string
	// Metadata is merged into Stripe Session.metadata. The standard keys
	// (purchase_id, tenant_id, learner_gcid, purchase_type) are always
	// added by CreateCheckoutSession; callers can add aggregate-specific
	// keys (course_id, application_id, egg_sku, sku, plan_sku, etc.).
	Metadata map[string]string

	// StripePriceID — CHO-1762. When non-empty AND PurchaseType is
	// PurchaseTypeTenantAddonPurchase, the Session is built with
	// mode=subscription + LineItems[].Price referencing this Stripe
	// Price ID (recurring monthly). Resolved at the gRPC layer via the
	// CHO-1760 Price catalogue. The gRPC server is responsible for
	// rejecting empty / placeholder values before they reach the
	// adapter; if empty here the adapter falls back to the legacy
	// mode=payment + inline PriceData path for backwards compatibility
	// with the user_subscription + mana_topup flows.
	StripePriceID string
}

// EnsureCustomerInput carries the parameters for an idempotent Stripe
// Customer create.
type EnsureCustomerInput struct {
	TenantID    string
	LearnerGCID string
	Email       string // optional — Stripe Customer.email
	// Metadata is attached to the Stripe Customer. The standard keys
	// (tenant_id, learner_gcid) are always added.
	Metadata map[string]string
}

// EnsureCustomerOutput is the result of EnsureCustomer.
type EnsureCustomerOutput struct {
	StripeCustomerID string
}

// CreateCheckoutSessionOutput is the result of Session creation.
type CreateCheckoutSessionOutput struct {
	StripeSessionID   string
	StripeCheckoutURL string
}

// RefundChargeInput carries the parameters for a Stripe refund.
type RefundChargeInput struct {
	StripeChargeID string
	AmountCents    int64 // 0 = full refund
	Reason         string
}

// RefundChargeOutput is the result of a refund.
type RefundChargeOutput struct {
	StripeRefundID      string
	AmountCentsRefunded int64
}

// CancelSubscriptionInput carries the parameters for a Stripe Subscription
// cancellation. Per ADR-164 Stage A.5: graceful cancel_at_period_end=true
// is the default (Stripe lets the subscription run through the current
// period); =false cancels immediately. Prorated refunds are NOT issued
// automatically — admins use RefundCharge for that.
type CancelSubscriptionInput struct {
	StripeSubscriptionID string
	CancelAtPeriodEnd    bool
	// Optional cancellation reason recorded on the Stripe Subscription
	// `cancellation_details.feedback` field (max 1000 chars). Stripe
	// recognises only a fixed enum; arbitrary strings are stored as the
	// `comment` instead.
	Reason string
}

// CancelSubscriptionOutput captures the post-cancel Stripe Subscription
// state so the caller can reconcile the aggregate.
type CancelSubscriptionOutput struct {
	StripeSubscriptionID string
	// CancelAtPeriodEnd echoes the Stripe post-update value (true for
	// graceful, false for immediate).
	CancelAtPeriodEnd bool
	// EffectiveAt is when the cancellation takes effect:
	//   graceful → Stripe.current_period_end
	//   immediate → now (Stripe returns CanceledAt)
	EffectiveAt time.Time
	// CancelledAt captures Stripe's CanceledAt (zero for graceful cancels
	// until the period rolls over).
	CancelledAt time.Time
	// Status mirrors Stripe Subscription.Status ("active",
	// "canceled", etc.).
	Status string
}

// UpdateSubscriptionPriceInput carries the parameters for a Stripe
// Subscription tier change (CHO-1764). The caller looks up the existing
// Subscription's first SubscriptionItem ID via Subscription.retrieve;
// Stripe rejects the update if the item ID doesn't match the live
// subscription.
type UpdateSubscriptionPriceInput struct {
	StripeSubscriptionID string
	// NewPriceID is the recurring monthly Stripe Price ID for the target
	// tier (resolved from the CHO-1760 catalogue).
	NewPriceID string
	// ProrationBehavior — one of "create_prorations" (default), "none",
	// or "always_invoice". `create_prorations` is the upgrade-immediately
	// + bill-prorated-delta path Stripe documents as the standard tier
	// change.
	ProrationBehavior string
}

// UpdateSubscriptionPriceOutput captures the Stripe-side post-update
// snapshot so the caller can reconcile + emit telemetry. BillingDeltaCents
// is the prorated delta Stripe will bill on the next invoice (positive =
// charge, negative = credit).
type UpdateSubscriptionPriceOutput struct {
	StripeSubscriptionID string
	NewPriceID           string
	// Status reflects Stripe.Subscription.Status post-update.
	Status string
	// CurrentPeriodStart / End mirror Stripe's billing-cycle anchors.
	CurrentPeriodStart time.Time
	CurrentPeriodEnd   time.Time
	// BillingDeltaCents — sum of proration line items on the upcoming
	// invoice (Stripe.Invoice.upcoming after the update). 0 for
	// proration_behavior=none.
	BillingDeltaCents int64
	Currency          string
}

// Client is the hexagonal port for the Stripe SDK adapter.
type Client interface {
	CreateCheckoutSession(ctx context.Context, in CreateCheckoutSessionInput) (CreateCheckoutSessionOutput, error)
	RefundCharge(ctx context.Context, in RefundChargeInput) (RefundChargeOutput, error)
	// EnsureCustomer creates a Stripe Customer object for the supplied
	// learner. The caller is responsible for caching the returned
	// stripe_customer_id locally so subsequent calls re-use the same
	// Customer (and therefore the saved PaymentMethods).
	EnsureCustomer(ctx context.Context, in EnsureCustomerInput) (EnsureCustomerOutput, error)
	// CancelSubscription cancels (or schedules cancellation of) a Stripe
	// Subscription. Per Stage A.5 closes the FE-initiated cancel gap:
	// chora-identity previously only flipped its local mana-entitlement
	// FSM; Stripe-side cancel is now executed here.
	CancelSubscription(ctx context.Context, in CancelSubscriptionInput) (CancelSubscriptionOutput, error)
	// UpdateSubscriptionPrice (CHO-1764) switches the Stripe Subscription's
	// first SubscriptionItem to a different recurring Price + bills the
	// prorated delta. Used by the H+ Marketplace change-tier flow.
	UpdateSubscriptionPrice(ctx context.Context, in UpdateSubscriptionPriceInput) (UpdateSubscriptionPriceOutput, error)
	// PreviewSubscriptionPriceChange (CHO-1765) returns the proration
	// delta Stripe WOULD bill if the Subscription's first SubscriptionItem
	// were updated to the supplied Price. Read-only — does NOT mutate
	// the live Subscription. Backs the H+ Marketplace preview-tier-change
	// flow.
	PreviewSubscriptionPriceChange(ctx context.Context, in PreviewSubscriptionPriceChangeInput) (PreviewSubscriptionPriceChangeOutput, error)
	// ScheduleSubscriptionPriceChange (CHO-1772) creates a Stripe
	// SubscriptionSchedule that holds the current tier until StartDate,
	// then activates the new Price. Backs the H+ change-tier flow when
	// the admin picks `effective_at=end_of_cycle`. The schedule's
	// EndBehavior is `release` — once the second phase consumes itself,
	// Stripe drops the schedule and keeps the bare Subscription running
	// at the new tier. Stripe fires `subscription_schedule.released` at
	// that point; the dispatcher promotes ScheduledTierCode → TierCode
	// on the TenantAddonPurchase row.
	ScheduleSubscriptionPriceChange(ctx context.Context, in ScheduleSubscriptionPriceChangeInput) (ScheduleSubscriptionPriceChangeOutput, error)
	// GetPrice (CHO-1772) returns the recurring monthly UnitAmount + currency
	// for a Stripe Price ID. Used by the preview-tier-change handler when
	// effective_at=end_of_cycle so the FE can render "next invoice = USD
	// X.XX" without a proration computation.
	GetPrice(ctx context.Context, priceID string) (GetPriceOutput, error)
	// GetSubscription (CHO-1772 follow-up) returns the live cycle anchor
	// for a Stripe Subscription. Used by the end_of_cycle path as a
	// fallback when the TAP row's current_period_end is nil (the
	// customer.subscription.created webhook may not have populated it on
	// rows seeded before that handler landed).
	GetSubscription(ctx context.Context, subscriptionID string) (GetSubscriptionOutput, error)
	// ListInvoices (H+ Billing) returns paginated Stripe Invoices for a
	// Customer. The cursor (NextCursor) is the last invoice ID — pass it
	// as StartingAfter on the next call to advance.
	ListInvoices(ctx context.Context, in ListInvoicesInput) (ListInvoicesOutput, error)
	// CreateBillingPortalSession (H+ Billing) mints a one-shot Stripe
	// Customer Portal session URL. The FE redirects to it; the user
	// manages payment methods + downloads invoices on Stripe's hosted
	// page and returns to ReturnURL on completion.
	CreateBillingPortalSession(ctx context.Context, in CreateBillingPortalSessionInput) (CreateBillingPortalSessionOutput, error)
}

// -----------------------------------------------------------------------------
// H+ Billing — ListInvoices + CreateBillingPortalSession (CHO-1759 followup)
// -----------------------------------------------------------------------------

// ListInvoicesInput parameterises the Stripe Invoice list call.
type ListInvoicesInput struct {
	// StripeCustomerID is the customer whose invoices to list. Required.
	StripeCustomerID string
	// Limit caps the page size. 0 → default 20. Stripe max is 100.
	Limit int64
	// StartingAfter is the cursor — the last invoice ID from the previous
	// page. Empty on the first call.
	StartingAfter string
}

// InvoiceRow is one row in the Billing & Invoices table.
type InvoiceRow struct {
	StripeInvoiceID  string
	Number           string
	PeriodStart      time.Time
	PeriodEnd        time.Time
	Status           string // 'paid' / 'open' / 'void' / 'uncollectible' / 'draft'
	TotalCents       int64
	Currency         string
	HostedInvoiceURL string
	InvoicePDFURL    string
	// SubscriptionID lets the caller join back to TenantAddonPurchase
	// to build the display description (addon_code:tier_code).
	StripeSubscriptionID string
}

// ListInvoicesOutput holds one page of invoices + a forward cursor.
type ListInvoicesOutput struct {
	Items []InvoiceRow
	// NextCursor is the last item's StripeInvoiceID when more pages exist;
	// empty when the result set is exhausted.
	NextCursor string
}

// CreateBillingPortalSessionInput parameterises the Stripe Customer Portal
// session-mint call.
type CreateBillingPortalSessionInput struct {
	StripeCustomerID string
	ReturnURL        string
}

// CreateBillingPortalSessionOutput carries the one-shot Portal URL the
// FE redirects to.
type CreateBillingPortalSessionOutput struct {
	URL string
}

// PreviewSubscriptionPriceChangeInput carries the parameters for a
// non-mutating Stripe Invoice.upcoming simulation (CHO-1765). Mirrors
// the UpdateSubscriptionPrice input shape; Stripe simulates the invoice
// AS IF the subscription were updated to the new Price without touching
// the live record.
type PreviewSubscriptionPriceChangeInput struct {
	StripeSubscriptionID string
	// NewPriceID — the recurring monthly Stripe Price ID for the target
	// tier (resolved from the CHO-1760 catalogue).
	NewPriceID string
	// ProrationBehavior — "create_prorations" (default) or "none".
	// `none` is rare for previews (you'd only preview a non-prorated
	// switch at cycle anchor), but supported for completeness.
	ProrationBehavior string
}

// PreviewSubscriptionPriceChangeOutput captures the simulated upcoming
// invoice. BillingDeltaCents is the sum of proration line items + the
// new-tier line item, mirroring Stripe Dashboard's "Upcoming invoice"
// preview the admin would see post-update.
type PreviewSubscriptionPriceChangeOutput struct {
	BillingDeltaCents     int64
	NextInvoiceTotalCents int64
	Currency              string
}

// ScheduleSubscriptionPriceChangeInput (CHO-1772) carries the parameters
// for a deferred end-of-cycle tier change via Stripe SubscriptionSchedule.
type ScheduleSubscriptionPriceChangeInput struct {
	// StripeSubscriptionID — the live Subscription to schedule a change
	// against. Stripe seeds Phase 0 of the schedule from this Subscription's
	// current SubscriptionItem; Phase 1 carries the new Price.
	StripeSubscriptionID string
	// NewPriceID — the recurring monthly Stripe Price ID for the target
	// tier (resolved from the CHO-1760 catalogue).
	NewPriceID string
	// StartDate — when Phase 1 begins. Typically equals the live
	// Subscription's current_period_end so the customer finishes the
	// month they've already paid for at the current tier. Stored UTC.
	StartDate time.Time
}

// ScheduleSubscriptionPriceChangeOutput captures the post-create schedule
// snapshot the caller persists on the TenantAddonPurchase row.
type ScheduleSubscriptionPriceChangeOutput struct {
	// StripeSubscriptionScheduleID — the `sub_sched_...` ID Stripe issues.
	StripeSubscriptionScheduleID string
	// EffectiveAt echoes StartDate (UTC). The caller writes this onto
	// scheduled_effective_at so the FE can render "Pro starting
	// YYYY-MM-DD" copy without re-reading from Stripe.
	EffectiveAt time.Time
}

// GetPriceOutput is the result of GetPrice — the recurring monthly
// UnitAmount + ISO 4217 currency Stripe has on file for the Price.
type GetPriceOutput struct {
	UnitAmountCents int64
	Currency        string
}

// GetSubscriptionOutput captures the cycle-anchor fields the
// end_of_cycle handlers need from Stripe when the local row's anchors
// haven't been populated by the customer.subscription.created webhook.
type GetSubscriptionOutput struct {
	CurrentPeriodStart time.Time
	CurrentPeriodEnd   time.Time
	Status             string
}

// -----------------------------------------------------------------------------
// StubClient — deterministic stub for local dev + standalone smoke gate.
// -----------------------------------------------------------------------------

// StubClient implements Client without touching Stripe's API. URLs are
// deterministic based on a SHA256 of the input payload so tests can
// assert exact strings.
type StubClient struct{}

// NewStubClient returns a StubClient.
func NewStubClient() *StubClient { return &StubClient{} }

func (s *StubClient) CreateCheckoutSession(_ context.Context, in CreateCheckoutSessionInput) (CreateCheckoutSessionOutput, error) {
	if err := validateInput(in); err != nil {
		return CreateCheckoutSessionOutput{}, err
	}
	// Deterministic session id from PurchaseID + amount.
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s", in.PurchaseType, in.PurchaseID, in.AmountCents, in.Currency)))
	sid := "cs_stub_" + hex.EncodeToString(hash[:16])
	return CreateCheckoutSessionOutput{
		StripeSessionID:   sid,
		StripeCheckoutURL: "https://checkout.stripe.invalid/stub/" + sid,
	}, nil
}

// EnsureCustomer (stub) — deterministic cus_stub_xxx ID derived from
// (tenant_id, learner_gcid).
func (s *StubClient) EnsureCustomer(_ context.Context, in EnsureCustomerInput) (EnsureCustomerOutput, error) {
	if in.TenantID == "" {
		return EnsureCustomerOutput{}, errors.New("stripe/stub: tenant_id required")
	}
	if in.LearnerGCID == "" {
		return EnsureCustomerOutput{}, errors.New("stripe/stub: learner_gcid required")
	}
	hash := sha256.Sum256([]byte("customer|" + in.TenantID + "|" + in.LearnerGCID))
	return EnsureCustomerOutput{
		StripeCustomerID: "cus_stub_" + hex.EncodeToString(hash[:14]),
	}, nil
}

func (s *StubClient) RefundCharge(_ context.Context, in RefundChargeInput) (RefundChargeOutput, error) {
	if strings.TrimSpace(in.StripeChargeID) == "" {
		return RefundChargeOutput{}, errors.New("stripe/stub: stripe_charge_id required")
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("refund|%s|%d|%s|%d",
		in.StripeChargeID, in.AmountCents, in.Reason, time.Now().UnixNano())))
	return RefundChargeOutput{
		StripeRefundID:      "re_stub_" + hex.EncodeToString(hash[:16]),
		AmountCentsRefunded: in.AmountCents,
	}, nil
}

// CancelSubscription (stub) — deterministic effective_at based on
// cancel_at_period_end (graceful → 30d ahead; immediate → now). Status
// reflects the Stripe-side terminal: immediate → "canceled"; graceful →
// "active" (cancellation is scheduled, not yet effective).
func (s *StubClient) CancelSubscription(_ context.Context, in CancelSubscriptionInput) (CancelSubscriptionOutput, error) {
	if strings.TrimSpace(in.StripeSubscriptionID) == "" {
		return CancelSubscriptionOutput{}, errors.New("stripe/stub: stripe_subscription_id required")
	}
	now := time.Now().UTC()
	out := CancelSubscriptionOutput{
		StripeSubscriptionID: in.StripeSubscriptionID,
		CancelAtPeriodEnd:    in.CancelAtPeriodEnd,
	}
	if in.CancelAtPeriodEnd {
		// Graceful — effective in 30d (canonical billing period); not yet
		// canceled on Stripe's side.
		out.EffectiveAt = now.AddDate(0, 0, 30)
		out.Status = "active"
	} else {
		// Immediate cancel.
		out.EffectiveAt = now
		out.CancelledAt = now
		out.Status = "canceled"
	}
	return out, nil
}

// UpdateSubscriptionPrice (stub) — deterministic round-trip. The "delta"
// is synthesised from a SHA256 of (sub_id, new_price_id) so tests get
// stable values, with a fixed prorated amount > 0 for upgrades. Tests
// that need specific delta values wire a recording fake.
func (s *StubClient) UpdateSubscriptionPrice(_ context.Context, in UpdateSubscriptionPriceInput) (UpdateSubscriptionPriceOutput, error) {
	if strings.TrimSpace(in.StripeSubscriptionID) == "" {
		return UpdateSubscriptionPriceOutput{}, errors.New("stripe/stub: stripe_subscription_id required")
	}
	if strings.TrimSpace(in.NewPriceID) == "" {
		return UpdateSubscriptionPriceOutput{}, errors.New("stripe/stub: new_price_id required")
	}
	now := time.Now().UTC()
	periodStart := now
	periodEnd := now.AddDate(0, 1, 0)
	// proration_behavior=none → 0 delta; create_prorations → synthetic.
	delta := int64(0)
	if in.ProrationBehavior == "" || in.ProrationBehavior == "create_prorations" {
		hash := sha256.Sum256([]byte("delta|" + in.StripeSubscriptionID + "|" + in.NewPriceID))
		delta = int64(hash[0])*100 + int64(hash[1]) // 0..25599 cents
	}
	return UpdateSubscriptionPriceOutput{
		StripeSubscriptionID: in.StripeSubscriptionID,
		NewPriceID:           in.NewPriceID,
		Status:               "active",
		CurrentPeriodStart:   periodStart,
		CurrentPeriodEnd:     periodEnd,
		BillingDeltaCents:    delta,
		Currency:             "usd",
	}, nil
}

// PreviewSubscriptionPriceChange (stub) — deterministic delta from
// SHA256(sub_id, new_price_id, "preview"). Tests requiring specific
// values wire a recording fake.
func (s *StubClient) PreviewSubscriptionPriceChange(_ context.Context, in PreviewSubscriptionPriceChangeInput) (PreviewSubscriptionPriceChangeOutput, error) {
	if strings.TrimSpace(in.StripeSubscriptionID) == "" {
		return PreviewSubscriptionPriceChangeOutput{}, errors.New("stripe/stub: stripe_subscription_id required")
	}
	if strings.TrimSpace(in.NewPriceID) == "" {
		return PreviewSubscriptionPriceChangeOutput{}, errors.New("stripe/stub: new_price_id required")
	}
	out := PreviewSubscriptionPriceChangeOutput{Currency: "usd"}
	if in.ProrationBehavior != "none" {
		hash := sha256.Sum256([]byte("preview|" + in.StripeSubscriptionID + "|" + in.NewPriceID))
		out.BillingDeltaCents = int64(hash[0])*100 + int64(hash[1])
		out.NextInvoiceTotalCents = out.BillingDeltaCents + 4900 // synthetic
	}
	return out, nil
}

// GetSubscription (stub) — deterministic cycle anchor (now → now+30d)
// + fixed "active" status. Tests requiring specific values wire a
// recording fake.
func (s *StubClient) GetSubscription(_ context.Context, subID string) (GetSubscriptionOutput, error) {
	if strings.TrimSpace(subID) == "" {
		return GetSubscriptionOutput{}, errors.New("stripe/stub: subscription_id required")
	}
	now := time.Now().UTC()
	return GetSubscriptionOutput{
		CurrentPeriodStart: now,
		CurrentPeriodEnd:   now.AddDate(0, 1, 0),
		Status:             "active",
	}, nil
}

// GetPrice (stub) — deterministic UnitAmount derived from SHA256(price_id).
// Tests requiring specific tier prices wire a recording fake.
func (s *StubClient) GetPrice(_ context.Context, priceID string) (GetPriceOutput, error) {
	if strings.TrimSpace(priceID) == "" {
		return GetPriceOutput{}, errors.New("stripe/stub: price_id required")
	}
	hash := sha256.Sum256([]byte("price|" + priceID))
	// 0..25599 cents — enough variation to make tier mismatches obvious.
	return GetPriceOutput{
		UnitAmountCents: int64(hash[0])*100 + int64(hash[1]),
		Currency:        "usd",
	}, nil
}

// ScheduleSubscriptionPriceChange (stub) — deterministic schedule_id
// derived from (sub_id, new_price_id, "schedule"). Echoes StartDate
// as EffectiveAt. Tests requiring specific values wire a recording fake.
func (s *StubClient) ScheduleSubscriptionPriceChange(_ context.Context, in ScheduleSubscriptionPriceChangeInput) (ScheduleSubscriptionPriceChangeOutput, error) {
	if strings.TrimSpace(in.StripeSubscriptionID) == "" {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/stub: stripe_subscription_id required")
	}
	if strings.TrimSpace(in.NewPriceID) == "" {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/stub: new_price_id required")
	}
	if in.StartDate.IsZero() {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/stub: start_date required")
	}
	hash := sha256.Sum256([]byte("schedule|" + in.StripeSubscriptionID + "|" + in.NewPriceID))
	return ScheduleSubscriptionPriceChangeOutput{
		StripeSubscriptionScheduleID: "sub_sched_stub_" + hex.EncodeToString(hash[:14]),
		EffectiveAt:                  in.StartDate.UTC(),
	}, nil
}

// ListInvoices (stub) — deterministic. Returns 2 paid invoices anchored
// to "now" with synthetic IDs derived from SHA256(customer_id). Cursor
// support is non-functional (stub always returns 2 + empty cursor) —
// real Stripe pagination is exercised in the integration smoke.
func (s *StubClient) ListInvoices(_ context.Context, in ListInvoicesInput) (ListInvoicesOutput, error) {
	if strings.TrimSpace(in.StripeCustomerID) == "" {
		return ListInvoicesOutput{}, errors.New("stripe/stub: stripe_customer_id required")
	}
	now := time.Now().UTC()
	hash := sha256.Sum256([]byte("invoices|" + in.StripeCustomerID))
	mkID := func(i int) string {
		return fmt.Sprintf("in_test_%x_%d", hash[i], i)
	}
	rows := []InvoiceRow{
		{
			StripeInvoiceID:      mkID(0),
			Number:               fmt.Sprintf("INV-%02d-001", now.Month()),
			PeriodStart:          now.AddDate(0, -1, 0),
			PeriodEnd:            now,
			Status:               "paid",
			TotalCents:           4900,
			Currency:             "usd",
			HostedInvoiceURL:     "https://invoice.stripe.com/test/" + mkID(0),
			InvoicePDFURL:        "https://invoice.stripe.com/test/" + mkID(0) + "/pdf",
			StripeSubscriptionID: "sub_stub_" + in.StripeCustomerID,
		},
		{
			StripeInvoiceID:      mkID(1),
			Number:               fmt.Sprintf("INV-%02d-002", now.Month()),
			PeriodStart:          now.AddDate(0, -2, 0),
			PeriodEnd:            now.AddDate(0, -1, 0),
			Status:               "paid",
			TotalCents:           4900,
			Currency:             "usd",
			HostedInvoiceURL:     "https://invoice.stripe.com/test/" + mkID(1),
			InvoicePDFURL:        "https://invoice.stripe.com/test/" + mkID(1) + "/pdf",
			StripeSubscriptionID: "sub_stub_" + in.StripeCustomerID,
		},
	}
	return ListInvoicesOutput{Items: rows, NextCursor: ""}, nil
}

// CreateBillingPortalSession (stub) — deterministic synthesised URL.
func (s *StubClient) CreateBillingPortalSession(_ context.Context, in CreateBillingPortalSessionInput) (CreateBillingPortalSessionOutput, error) {
	if strings.TrimSpace(in.StripeCustomerID) == "" {
		return CreateBillingPortalSessionOutput{}, errors.New("stripe/stub: stripe_customer_id required")
	}
	hash := sha256.Sum256([]byte("portal|" + in.StripeCustomerID + "|" + in.ReturnURL))
	return CreateBillingPortalSessionOutput{
		URL: fmt.Sprintf("https://billing.stripe.com/p/session/test_%x", hash[:8]),
	}, nil
}

var _ Client = (*StubClient)(nil)

// validateInput enforces the cross-adapter input invariants.
func validateInput(in CreateCheckoutSessionInput) error {
	if in.PurchaseID == "" {
		return errors.New("stripe: purchase_id required")
	}
	if in.TenantID == "" {
		return errors.New("stripe: tenant_id required")
	}
	if in.LearnerGCID == "" {
		return errors.New("stripe: learner_gcid required")
	}
	if in.AmountCents < 0 {
		return errors.New("stripe: amount_cents must be >= 0")
	}
	if len(in.Currency) != 3 {
		return errors.New("stripe: currency must be a 3-letter ISO 4217 code")
	}
	if in.SuccessURL == "" || in.CancelURL == "" {
		return errors.New("stripe: success_url + cancel_url required")
	}
	return nil
}

// metadataWithDefaults merges the caller-supplied metadata with the
// standard chora-payments tags.
func metadataWithDefaults(in CreateCheckoutSessionInput) map[string]string {
	m := make(map[string]string, len(in.Metadata)+4)
	for k, v := range in.Metadata {
		m[k] = v
	}
	m["purchase_id"] = in.PurchaseID
	m["tenant_id"] = in.TenantID
	m["learner_gcid"] = in.LearnerGCID
	m["purchase_type"] = string(in.PurchaseType)
	return m
}

// customerMetadataWithDefaults merges caller metadata with the standard
// chora-payments Customer tags.
func customerMetadataWithDefaults(in EnsureCustomerInput) map[string]string {
	m := make(map[string]string, len(in.Metadata)+2)
	for k, v := range in.Metadata {
		m[k] = v
	}
	m["tenant_id"] = in.TenantID
	m["learner_gcid"] = in.LearnerGCID
	return m
}
