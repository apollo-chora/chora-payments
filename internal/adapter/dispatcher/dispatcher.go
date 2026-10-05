// Package dispatcher routes verified Stripe webhook events to the matching
// chora-payments aggregate handler.
//
// Per ADR-164 §D3 receive-path: the webhook handler verifies HMAC + dedupes
// via stripe_webhook_events, then calls Dispatch(event). Dispatch:
//  1. Reads event.Data.Object (Session / PaymentIntent / Charge) into a
//     typed view.
//  2. Extracts purchase_type + purchase_id from Session.metadata (set at
//     Session creation time via the stripe adapter's
//     metadataWithDefaults helper).
//  3. Routes to the matching Purchase aggregate repo + applies the
//     matching FSM transition.
//  4. Returns (aggregate, purchase_id) for the stripe_webhook_events
//     audit row.
//
// Outbox publish (chora.payments.{aggregate}.{event_type}.v1) is deferred
// to Wave 1 Stage A.4 — the dispatcher signals via the OutboxEmitter port
// today; cmd/server wires a no-op emitter at Stage A.3, the real outbox
// dispatcher in A.4.
package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/tracing"

	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// EventType is the per-event-type tag in the canonical outbox topic name
// chora.payments.{aggregate}.{event_type}.v1.
type EventType string

const (
	EventCheckoutStarted EventType = "checkout_started"
	EventPaymentCaptured EventType = "payment_captured"
	EventPaymentFailed   EventType = "payment_failed"
	EventRefunded        EventType = "refunded"
	EventExpired         EventType = "expired"
	// EventSubscriptionCancelled is the lifecycle event emitted by the
	// CancelSubscription RPC + the customer.subscription.deleted webhook
	// handler. Topic: chora.payments.user_subscription.cancelled.v1
	// (also tenant_addon_purchase.cancelled.v1 from CHO-1771).
	EventSubscriptionCancelled EventType = "cancelled"
	// EventPaymentRecovered is the dunning-recovery event emitted from
	// invoice.payment_succeeded when the prior sub_status was past_due
	// (CHO-1775). Topic: chora.payments.tenant_addon_purchase.payment_recovered.v1.
	EventPaymentRecovered EventType = "payment_recovered"
	// EventSubscriptionPaymentFailed is the dunning-entry event emitted
	// from invoice.payment_failed on a recurring subscription invoice
	// (CHO-1775). Distinct from EventPaymentFailed (one-shot mode=payment
	// failures during Checkout) — the subscription path is keyed by
	// stripe_subscription_id and triggers chora-notifications dunning.
	// Topic: chora.payments.tenant_addon_purchase.subscription_payment_failed.v1.
	EventSubscriptionPaymentFailed EventType = "subscription_payment_failed"
	// EventSubscriptionScheduleReleased is emitted from
	// subscription_schedule.released when Stripe activates the second
	// phase of a deferred end-of-cycle tier change (CHO-1779). Carries
	// from_tier + to_tier so chora-tenancy can call
	// SubscriptionRegistry.PromoteScheduledTier on its in-memory
	// state. Topic: chora.payments.tenant_addon_purchase.
	// subscription_schedule_released.v1.
	EventSubscriptionScheduleReleased EventType = "subscription_schedule_released"

	// ADR-164 §229.5 — dispute lifecycle events (cross-aggregate family).
	// Topics: chora.payments.dispute.{raised,closed,funds_withdrawn,funds_reinstated}.v1.
	EventDisputeRaised          EventType = "raised"
	EventDisputeClosed          EventType = "closed"
	EventDisputeFundsWithdrawn  EventType = "funds_withdrawn"
	EventDisputeFundsReinstated EventType = "funds_reinstated"
)

// EmitInput is the unified payload passed to OutboxEmitter.Emit.
//
// The emitter loads the matching Purchase aggregate from the repo
// (RLS-scoped via TenantID on ctx), marshals the canonical proto event,
// and writes it into chora_payments.outbox_events. The dispatcher
// drains the outbox to Cloud Pub/Sub asynchronously.
type EmitInput struct {
	TenantID   string
	Aggregate  wh.AggregateType
	PurchaseID string
	EventType  EventType
	// IdempotencyKey is the per-event dedup key. Recommended:
	// "{aggregate}.{event_type}.{purchase_id}.{stripe_event_id}" so
	// replays of the same Stripe webhook event do not double-publish.
	IdempotencyKey string
}

// EmitDisputeInput is the payload for the cross-aggregate dispute event
// family (ADR-164 §229.5). The emitter loads the matching Dispute
// aggregate (NOT a Purchase) and marshals one of the 4 dispute event
// messages (DisputeRaised / DisputeClosed / DisputeFundsWithdrawn /
// DisputeFundsReinstated). Topic family is
// chora.payments.dispute.{event_type}.v1.
type EmitDisputeInput struct {
	TenantID       string
	DisputeID      string
	EventType      EventType // one of EventDispute*
	IdempotencyKey string
}

// OutboxEmitter is the hexagonal port for outbound chora.payments.*.v1
// event publication. Concrete impls:
//   - dispatcher.NoOpEmitter (logs only; dev fallback)
//   - outbox.Emitter (production — writes to chora_payments.outbox_events;
//     the outbox.Dispatcher worker drains the table to Pub/Sub)
type OutboxEmitter interface {
	Emit(ctx context.Context, in EmitInput) error
	// EmitDispute publishes one of the 4 cross-aggregate dispute events.
	EmitDispute(ctx context.Context, in EmitDisputeInput) error
}

// NoOpEmitter logs but does not publish. Used at standalone smoke when
// the event bus is unset; the boot WARN line makes the gap
// visible per feedback_no_stubs_real_wiring.
type NoOpEmitter struct{}

func (NoOpEmitter) Emit(_ context.Context, in EmitInput) error {
	log.Printf("payments/dispatcher: NoOpEmitter — would publish chora.payments.%s.%s.v1 purchase=%s tenant=%s",
		in.Aggregate, in.EventType, in.PurchaseID, in.TenantID)
	return nil
}

func (NoOpEmitter) EmitDispute(_ context.Context, in EmitDisputeInput) error {
	log.Printf("payments/dispatcher: NoOpEmitter — would publish chora.payments.dispute.%s.v1 dispute=%s tenant=%s",
		in.EventType, in.DisputeID, in.TenantID)
	return nil
}

// Deps wires the dispatcher's repos + outbox emitter.
type Deps struct {
	Course       coursepurchase.Repo
	Application  apppay.Repo
	FamiliarEgg  egg.Repo
	ManaTopUp    mana.Repo
	Subscription sub.Repo
	// 6th + 7th aggregates per Stage A.5.
	UserManaTopUp  umt.Repo
	IdentityKycFee kyc.Repo
	// 8th Purchase aggregate — CHO-1736 H+ Marketplace Subscribe.
	AddonPurchase tap.Repo
	// Dispute is the cross-aggregate chargeback record (separate FSM
	// from Purchase aggregates per ADR-164 §229.5).
	Dispute dispute.Repo
	Outbox  OutboxEmitter
	Now     func() time.Time
}

// Dispatcher implements http.WebhookDispatcher.
type Dispatcher struct {
	deps Deps
}

// New constructs a Dispatcher.
func New(deps Deps) *Dispatcher {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	if deps.Outbox == nil {
		deps.Outbox = NoOpEmitter{}
	}
	return &Dispatcher{deps: deps}
}

// Dispatch routes a verified Stripe event to the matching aggregate.
//
// Signature matches httpadapter.WebhookDispatcher (ctx is interface{}
// because importing this package from http would create a cycle).
func (d *Dispatcher) Dispatch(ctxAny interface{}, event *stripeGo.Event) (wh.AggregateType, string, error) {
	ctx, ok := ctxAny.(context.Context)
	if !ok {
		ctx = context.Background()
	}

	switch event.Type {
	case stripeGo.EventTypeCheckoutSessionCompleted:
		return d.handleCheckoutSessionCompleted(ctx, event)
	case stripeGo.EventTypeCheckoutSessionExpired:
		return d.handleCheckoutSessionExpired(ctx, event)
	case stripeGo.EventTypePaymentIntentPaymentFailed:
		return d.handlePaymentIntentFailed(ctx, event)
	case stripeGo.EventTypeChargeRefunded:
		return d.handleChargeRefunded(ctx, event)

	// ADR-164 §229.5 — async-payment events (ACH / SEPA / wire / bank-debit
	// flows that clear or fail asynchronously after the Checkout Session
	// completes). Same downstream domain transitions as the sync paths.
	case stripeGo.EventTypeCheckoutSessionAsyncPaymentSucceeded:
		return d.handleCheckoutSessionCompleted(ctx, event)
	case stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed:
		return d.handleCheckoutSessionAsyncPaymentFailed(ctx, event)

	// ADR-164 §229.5 — dispute lifecycle (chargeback raised / closed) +
	// funds movement (Stripe debits/credits our balance pending or
	// post-resolution).
	case stripeGo.EventTypeChargeDisputeCreated:
		return d.handleChargeDisputeCreated(ctx, event)
	case stripeGo.EventTypeChargeDisputeClosed:
		return d.handleChargeDisputeClosed(ctx, event)
	case stripeGo.EventTypeChargeDisputeFundsWithdrawn:
		return d.handleChargeDisputeFundsWithdrawn(ctx, event)
	case stripeGo.EventTypeChargeDisputeFundsReinstated:
		return d.handleChargeDisputeFundsReinstated(ctx, event)

	// CHO-1763 — Stripe Subscription lifecycle. mode=subscription
	// (CHO-1762) creates a Subscription; customer.subscription.created
	// is fired to deliver the resolved stripe_subscription_id +
	// stripe_customer_id + cycle anchor. We persist these onto the
	// tenant_addon_purchase row so CHO-1764 can call
	// Subscription.update for tier changes. No outbox emit —
	// checkout.session.completed already publishes
	// chora.payments.tenant_addon_purchase.payment_captured.v1 which
	// drives the tenancy AddOnActivator (CHO-1740 path).
	//
	// Out of scope this PR (deferred to follow-up Jiras):
	//   - customer.subscription.updated   (tier_changed via webhook)
	//   - customer.subscription.deleted   (cancellation propagation)
	//   - invoice.payment_failed          (past_due transition)
	//   - invoice.payment_succeeded       (past_due → active recovery)
	case stripeGo.EventTypeCustomerSubscriptionCreated:
		return d.handleCustomerSubscriptionCreated(ctx, event)
	// CHO-1771 — 4 remaining Stripe Subscription lifecycle webhooks.
	// customer.subscription.updated catches Dashboard-side mutations +
	// auto-renewal cycle-anchor bumps. customer.subscription.deleted
	// emits cancelled for the chora-tenancy Deactivate sync.
	// invoice.payment_{failed,succeeded} drive past_due / recovered
	// transitions for the dunning UI.
	case stripeGo.EventTypeCustomerSubscriptionUpdated:
		return d.handleCustomerSubscriptionUpdated(ctx, event)
	case stripeGo.EventTypeCustomerSubscriptionDeleted:
		return d.handleCustomerSubscriptionDeleted(ctx, event)
	case stripeGo.EventTypeInvoicePaymentFailed:
		return d.handleInvoicePaymentFailed(ctx, event)
	case stripeGo.EventTypeInvoicePaymentSucceeded:
		return d.handleInvoicePaymentSucceeded(ctx, event)

	// CHO-1772 — SubscriptionSchedule consumed itself after the deferred
	// tier change activated. Promote ScheduledTierCode → TierCode on the
	// TAP row + clear the three schedule fields.
	case stripeGo.EventTypeSubscriptionScheduleReleased:
		return d.handleSubscriptionScheduleReleased(ctx, event)

	default:
		log.Printf("payments/dispatcher: ignored event_type=%s (not in switch)", event.Type)
		return wh.AggregateUnknown, "", nil
	}
}

// -----------------------------------------------------------------------------
// Per-event handlers
// -----------------------------------------------------------------------------

func (d *Dispatcher) handleCheckoutSessionCompleted(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	sess, err := parseSession(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	agg, pid, err := resolveTarget(sess.Metadata)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	now := d.deps.Now()
	paymentIntent := stringValue(sess.PaymentIntent)
	chargeID := chargeFromSession(sess)
	tenantID := sess.Metadata["tenant_id"]
	// Propagate tenant_id from Stripe session metadata onto ctx BEFORE any
	// repo call — even the session_id lookup is subject to the RLS
	// `tenant_isolation` policy (PG evaluates the policy for every row
	// touched, regardless of how the WHERE clause is shaped). Without a
	// tenant context the `(current_setting('chora.tenant_id', true))::uuid`
	// cast in the policy fails with `invalid input syntax for type uuid: ""`
	// and the whole webhook 500s. The metadata is set at CreateCheckoutSession
	// time (see grpc/server.go:159) so it's always present for real flows;
	// synthetic stripe-trigger events lack it and surface as `tenant_id ""`
	// which we early-return to skip rather than spin a meaningless RLS error.
	if tenantID == "" {
		log.Printf("payments/dispatcher: tenant_id missing from session metadata — skipping (synthetic event?) session=%s", sess.ID)
		return wh.AggregateUnknown, "", nil
	}
	ctx = tracing.WithTenantID(ctx, tenantID)

	switch agg {
	case wh.AggregateCoursePurchase:
		cp, err := d.deps.Course.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			cp, err = d.reconstructCoursePurchase(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("course_purchase lookup by session_id=%s: %w", sess.ID, err)
		}
		if err := cp.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, cp.PurchaseID, fmt.Errorf("course_purchase MarkPaymentCaptured: %w", err)
		}
		if err := d.deps.Course.Save(ctx, cp); err != nil {
			return agg, cp.PurchaseID, fmt.Errorf("course_purchase save: %w", err)
		}
		return agg, cp.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, cp.TenantID), agg, cp.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateApplicationPayment:
		ap, err := d.deps.Application.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			ap, err = d.reconstructApplicationPayment(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("application_payment lookup: %w", err)
		}
		if err := ap.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, ap.PurchaseID, err
		}
		if err := d.deps.Application.Save(ctx, ap); err != nil {
			return agg, ap.PurchaseID, err
		}
		return agg, ap.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, ap.TenantID), agg, ap.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateFamiliarEggPurchase:
		ep, err := d.deps.FamiliarEgg.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			ep, err = d.reconstructFamiliarEggPurchase(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("familiar_egg_purchase lookup: %w", err)
		}
		if err := ep.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, ep.PurchaseID, err
		}
		if err := d.deps.FamiliarEgg.Save(ctx, ep); err != nil {
			return agg, ep.PurchaseID, err
		}
		return agg, ep.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, ep.TenantID), agg, ep.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateTenantManaTopUp:
		t, err := d.deps.ManaTopUp.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			// ADR-188 D1 — the create-side row was never persisted; the paid
			// event is authoritative. Reconstruct from the signed metadata
			// and fulfil rather than 500-and-pray on Stripe retries.
			t, err = d.reconstructTenantManaTopUp(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("tenant_mana_topup lookup: %w", err)
		}
		if err := t.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, t.PurchaseID, err
		}
		if err := d.deps.ManaTopUp.Save(ctx, t); err != nil {
			return agg, t.PurchaseID, err
		}
		return agg, t.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, t.TenantID), agg, t.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateUserSubscription:
		u, err := d.deps.Subscription.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			u, err = d.reconstructUserSubscription(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("user_subscription lookup: %w", err)
		}
		if err := u.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, u.PurchaseID, err
		}
		u.AccumulatePaid(sess.AmountTotal)
		if err := d.deps.Subscription.Save(ctx, u); err != nil {
			return agg, u.PurchaseID, err
		}
		return agg, u.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, u.TenantID), agg, u.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateUserManaTopUp:
		um, err := d.deps.UserManaTopUp.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			um, err = d.reconstructUserManaTopUp(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("user_mana_topup lookup: %w", err)
		}
		if err := um.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, um.PurchaseID, err
		}
		if err := d.deps.UserManaTopUp.Save(ctx, um); err != nil {
			return agg, um.PurchaseID, err
		}
		return agg, um.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, um.TenantID), agg, um.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateIdentityKycFee:
		k, err := d.deps.IdentityKycFee.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			k, err = d.reconstructIdentityKycFee(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("identity_kyc_fee lookup: %w", err)
		}
		if err := k.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, k.PurchaseID, err
		}
		if err := d.deps.IdentityKycFee.Save(ctx, k); err != nil {
			return agg, k.PurchaseID, err
		}
		return agg, k.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, k.TenantID), agg, k.PurchaseID, EventPaymentCaptured, event.ID)

	case wh.AggregateTenantAddonPurchase:
		a, err := d.deps.AddonPurchase.GetByStripeSessionID(ctx, sess.ID)
		if errors.Is(err, shared.ErrNotFound) {
			// ADR-188 D1 — reconstruct the missing H+ add-on purchase row
			// from the paid event and fulfil.
			a, err = d.reconstructTenantAddonPurchase(ctx, sess, now)
		}
		if err != nil {
			return agg, pid, fmt.Errorf("tenant_addon_purchase lookup: %w", err)
		}
		if err := a.MarkPaymentCaptured(paymentIntent, chargeID, sess.AmountTotal, now); err != nil {
			return agg, a.PurchaseID, err
		}
		if err := d.deps.AddonPurchase.Save(ctx, a); err != nil {
			return agg, a.PurchaseID, err
		}
		return agg, a.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, a.TenantID), agg, a.PurchaseID, EventPaymentCaptured, event.ID)

	default:
		return wh.AggregateUnknown, pid, fmt.Errorf("unknown aggregate type %q", agg)
	}
}

func (d *Dispatcher) handleCheckoutSessionExpired(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	sess, err := parseSession(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	agg, pid, err := resolveTarget(sess.Metadata)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	now := d.deps.Now()
	tenantID := sess.Metadata["tenant_id"]
	// Propagate tenant_id onto ctx BEFORE any session-id lookup — every
	// Purchase table's tenant_isolation RLS policy is evaluated for every
	// row touched, and the repos now ApplySession (mirroring course_purchase)
	// so a pooled connection's leftover '' GUC reset-value never casts to
	// ''::uuid (22P02). Mirrors handleCheckoutSessionCompleted; synthetic
	// stripe-trigger events lack the metadata, so we skip them.
	if tenantID == "" {
		log.Printf("payments/dispatcher: tenant_id missing from expired session metadata — skipping (synthetic event?) session=%s", sess.ID)
		return wh.AggregateUnknown, "", nil
	}
	ctx = tracing.WithTenantID(ctx, tenantID)

	switch agg {
	case wh.AggregateCoursePurchase:
		cp, lerr := d.deps.Course.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := cp.MarkExpired(now); terr != nil {
			return agg, cp.PurchaseID, terr
		}
		if serr := d.deps.Course.Save(ctx, cp); serr != nil {
			return agg, cp.PurchaseID, serr
		}
		return agg, cp.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, cp.TenantID), agg, cp.PurchaseID, EventExpired, event.ID)
	case wh.AggregateApplicationPayment:
		ap, lerr := d.deps.Application.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := ap.MarkExpired(now); terr != nil {
			return agg, ap.PurchaseID, terr
		}
		if serr := d.deps.Application.Save(ctx, ap); serr != nil {
			return agg, ap.PurchaseID, serr
		}
		return agg, ap.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, ap.TenantID), agg, ap.PurchaseID, EventExpired, event.ID)
	case wh.AggregateFamiliarEggPurchase:
		ep, lerr := d.deps.FamiliarEgg.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := ep.MarkExpired(now); terr != nil {
			return agg, ep.PurchaseID, terr
		}
		if serr := d.deps.FamiliarEgg.Save(ctx, ep); serr != nil {
			return agg, ep.PurchaseID, serr
		}
		return agg, ep.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, ep.TenantID), agg, ep.PurchaseID, EventExpired, event.ID)
	case wh.AggregateTenantManaTopUp:
		t, lerr := d.deps.ManaTopUp.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := t.MarkExpired(now); terr != nil {
			return agg, t.PurchaseID, terr
		}
		if serr := d.deps.ManaTopUp.Save(ctx, t); serr != nil {
			return agg, t.PurchaseID, serr
		}
		return agg, t.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, t.TenantID), agg, t.PurchaseID, EventExpired, event.ID)
	case wh.AggregateUserSubscription:
		u, lerr := d.deps.Subscription.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := u.MarkExpired(now); terr != nil {
			return agg, u.PurchaseID, terr
		}
		if serr := d.deps.Subscription.Save(ctx, u); serr != nil {
			return agg, u.PurchaseID, serr
		}
		return agg, u.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, u.TenantID), agg, u.PurchaseID, EventExpired, event.ID)
	case wh.AggregateUserManaTopUp:
		um, lerr := d.deps.UserManaTopUp.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := um.MarkExpired(now); terr != nil {
			return agg, um.PurchaseID, terr
		}
		if serr := d.deps.UserManaTopUp.Save(ctx, um); serr != nil {
			return agg, um.PurchaseID, serr
		}
		return agg, um.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, um.TenantID), agg, um.PurchaseID, EventExpired, event.ID)
	case wh.AggregateIdentityKycFee:
		k, lerr := d.deps.IdentityKycFee.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := k.MarkExpired(now); terr != nil {
			return agg, k.PurchaseID, terr
		}
		if serr := d.deps.IdentityKycFee.Save(ctx, k); serr != nil {
			return agg, k.PurchaseID, serr
		}
		return agg, k.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, k.TenantID), agg, k.PurchaseID, EventExpired, event.ID)
	case wh.AggregateTenantAddonPurchase:
		a, lerr := d.deps.AddonPurchase.GetByStripeSessionID(ctx, sess.ID)
		if lerr != nil {
			return agg, pid, lerr
		}
		if terr := a.MarkExpired(now); terr != nil {
			return agg, a.PurchaseID, terr
		}
		if serr := d.deps.AddonPurchase.Save(ctx, a); serr != nil {
			return agg, a.PurchaseID, serr
		}
		return agg, a.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, a.TenantID), agg, a.PurchaseID, EventExpired, event.ID)
	}
	return wh.AggregateUnknown, pid, fmt.Errorf("unknown aggregate type %q", agg)
}

func (d *Dispatcher) handlePaymentIntentFailed(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	pi, err := parsePaymentIntent(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	agg, pid, err := resolveTarget(pi.Metadata)
	if err != nil {
		log.Printf("payments/dispatcher: payment_intent.payment_failed without metadata pi=%s — ack only", pi.ID)
		return wh.AggregateUnknown, "", nil
	}
	now := d.deps.Now()
	failureCode, failureMessage := failureFromPaymentIntent(pi)
	tenantID := pi.Metadata["tenant_id"]
	// GetByID below applies rls.ApplySession from ctx (the tenant_isolation
	// policy), so stamp tenant onto ctx first or it ErrNoTenantContexts. A
	// payment_intent without tenant metadata can't be tenant-scoped — ack it
	// (no aggregate transition) rather than 500-loop on Stripe retries.
	if tenantID == "" {
		log.Printf("payments/dispatcher: tenant_id missing from payment_intent metadata — ack only pi=%s", pi.ID)
		return wh.AggregateUnknown, "", nil
	}
	ctx = tracing.WithTenantID(ctx, tenantID)

	switch agg {
	case wh.AggregateCoursePurchase:
		cp, err := d.deps.Course.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := cp.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, cp.PurchaseID, err
		}
		if err := d.deps.Course.Save(ctx, cp); err != nil {
			return agg, cp.PurchaseID, err
		}
		return agg, cp.PurchaseID, d.emit(ctx, cp.TenantID, agg, cp.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateApplicationPayment:
		ap, err := d.deps.Application.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := ap.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, ap.PurchaseID, err
		}
		if err := d.deps.Application.Save(ctx, ap); err != nil {
			return agg, ap.PurchaseID, err
		}
		return agg, ap.PurchaseID, d.emit(ctx, ap.TenantID, agg, ap.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateFamiliarEggPurchase:
		ep, err := d.deps.FamiliarEgg.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := ep.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, ep.PurchaseID, err
		}
		if err := d.deps.FamiliarEgg.Save(ctx, ep); err != nil {
			return agg, ep.PurchaseID, err
		}
		return agg, ep.PurchaseID, d.emit(ctx, ep.TenantID, agg, ep.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateTenantManaTopUp:
		t, err := d.deps.ManaTopUp.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := t.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, t.PurchaseID, err
		}
		if err := d.deps.ManaTopUp.Save(ctx, t); err != nil {
			return agg, t.PurchaseID, err
		}
		return agg, t.PurchaseID, d.emit(ctx, t.TenantID, agg, t.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateUserSubscription:
		u, err := d.deps.Subscription.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := u.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, u.PurchaseID, err
		}
		if err := d.deps.Subscription.Save(ctx, u); err != nil {
			return agg, u.PurchaseID, err
		}
		return agg, u.PurchaseID, d.emit(ctx, u.TenantID, agg, u.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateUserManaTopUp:
		um, err := d.deps.UserManaTopUp.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := um.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, um.PurchaseID, err
		}
		if err := d.deps.UserManaTopUp.Save(ctx, um); err != nil {
			return agg, um.PurchaseID, err
		}
		return agg, um.PurchaseID, d.emit(ctx, um.TenantID, agg, um.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateIdentityKycFee:
		k, err := d.deps.IdentityKycFee.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := k.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, k.PurchaseID, err
		}
		if err := d.deps.IdentityKycFee.Save(ctx, k); err != nil {
			return agg, k.PurchaseID, err
		}
		return agg, k.PurchaseID, d.emit(ctx, k.TenantID, agg, k.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateTenantAddonPurchase:
		a, err := d.deps.AddonPurchase.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := a.MarkPaymentFailed(failureCode, failureMessage, now); err != nil {
			return agg, a.PurchaseID, err
		}
		if err := d.deps.AddonPurchase.Save(ctx, a); err != nil {
			return agg, a.PurchaseID, err
		}
		return agg, a.PurchaseID, d.emit(ctx, a.TenantID, agg, a.PurchaseID, EventPaymentFailed, event.ID)
	}
	return agg, pid, fmt.Errorf("unknown aggregate type %q", agg)
}

func (d *Dispatcher) handleChargeRefunded(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	ch, err := parseCharge(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	agg, pid, err := resolveTarget(ch.Metadata)
	if err != nil {
		log.Printf("payments/dispatcher: charge.refunded without metadata ch=%s — ack only", ch.ID)
		return wh.AggregateUnknown, "", nil
	}
	now := d.deps.Now()
	refundID, refundAmount := latestRefund(ch)
	tenantID := ch.Metadata["tenant_id"]
	// GetByID below applies rls.ApplySession from ctx (the tenant_isolation
	// policy), so stamp tenant onto ctx first or it ErrNoTenantContexts. A
	// charge without tenant metadata can't be tenant-scoped — ack it rather
	// than 500-loop on Stripe retries.
	if tenantID == "" {
		log.Printf("payments/dispatcher: tenant_id missing from charge metadata — ack only ch=%s", ch.ID)
		return wh.AggregateUnknown, "", nil
	}
	ctx = tracing.WithTenantID(ctx, tenantID)

	switch agg {
	case wh.AggregateCoursePurchase:
		cp, err := d.deps.Course.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := cp.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, cp.PurchaseID, err
		}
		if err := d.deps.Course.Save(ctx, cp); err != nil {
			return agg, cp.PurchaseID, err
		}
		return agg, cp.PurchaseID, d.emit(ctx, cp.TenantID, agg, cp.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateApplicationPayment:
		ap, err := d.deps.Application.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := ap.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, ap.PurchaseID, err
		}
		if err := d.deps.Application.Save(ctx, ap); err != nil {
			return agg, ap.PurchaseID, err
		}
		return agg, ap.PurchaseID, d.emit(ctx, ap.TenantID, agg, ap.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateFamiliarEggPurchase:
		ep, err := d.deps.FamiliarEgg.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := ep.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, ep.PurchaseID, err
		}
		if err := d.deps.FamiliarEgg.Save(ctx, ep); err != nil {
			return agg, ep.PurchaseID, err
		}
		return agg, ep.PurchaseID, d.emit(ctx, ep.TenantID, agg, ep.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateTenantManaTopUp:
		t, err := d.deps.ManaTopUp.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := t.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, t.PurchaseID, err
		}
		if err := d.deps.ManaTopUp.Save(ctx, t); err != nil {
			return agg, t.PurchaseID, err
		}
		return agg, t.PurchaseID, d.emit(ctx, t.TenantID, agg, t.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateUserSubscription:
		u, err := d.deps.Subscription.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := u.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, u.PurchaseID, err
		}
		if err := d.deps.Subscription.Save(ctx, u); err != nil {
			return agg, u.PurchaseID, err
		}
		return agg, u.PurchaseID, d.emit(ctx, u.TenantID, agg, u.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateUserManaTopUp:
		um, err := d.deps.UserManaTopUp.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := um.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, um.PurchaseID, err
		}
		if err := d.deps.UserManaTopUp.Save(ctx, um); err != nil {
			return agg, um.PurchaseID, err
		}
		return agg, um.PurchaseID, d.emit(ctx, um.TenantID, agg, um.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateIdentityKycFee:
		k, err := d.deps.IdentityKycFee.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := k.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, k.PurchaseID, err
		}
		if err := d.deps.IdentityKycFee.Save(ctx, k); err != nil {
			return agg, k.PurchaseID, err
		}
		return agg, k.PurchaseID, d.emit(ctx, k.TenantID, agg, k.PurchaseID, EventRefunded, event.ID)
	case wh.AggregateTenantAddonPurchase:
		a, err := d.deps.AddonPurchase.GetByID(ctx, tenantID, pid)
		if err != nil {
			return agg, pid, err
		}
		if err := a.MarkRefunded(refundID, refundAmount, shared.RefundReasonCustomerRequest, now); err != nil {
			return agg, a.PurchaseID, err
		}
		if err := d.deps.AddonPurchase.Save(ctx, a); err != nil {
			return agg, a.PurchaseID, err
		}
		return agg, a.PurchaseID, d.emit(ctx, a.TenantID, agg, a.PurchaseID, EventRefunded, event.ID)
	}
	return agg, pid, fmt.Errorf("unknown aggregate type %q", agg)
}

// -----------------------------------------------------------------------------
// ADR-164 §229.5 — async-payment + dispute handlers (Wave 1 extension).
// -----------------------------------------------------------------------------

// handleCheckoutSessionAsyncPaymentFailed handles the async sibling of
// payment_intent.payment_failed for delayed-clearing payment methods
// (ACH / SEPA / wire). The webhook payload is a Session (NOT a
// PaymentIntent) so we parse the Session, read its metadata, and walk
// the same per-aggregate MarkPaymentFailed path.
func (d *Dispatcher) handleCheckoutSessionAsyncPaymentFailed(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	sess, err := parseSession(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	agg, pid, err := resolveTarget(sess.Metadata)
	if err != nil {
		log.Printf("payments/dispatcher: checkout.session.async_payment_failed without metadata sess=%s — ack only", sess.ID)
		return wh.AggregateUnknown, "", nil
	}
	now := d.deps.Now()
	tenantID := sess.Metadata["tenant_id"]
	// Propagate tenant_id onto ctx BEFORE the session-id lookup (the repos
	// ApplySession against the tenant_isolation RLS policy — see
	// handleCheckoutSessionCompleted). Synthetic events lack metadata → skip.
	if tenantID == "" {
		log.Printf("payments/dispatcher: tenant_id missing from async-failed session metadata — skipping (synthetic event?) session=%s", sess.ID)
		return wh.AggregateUnknown, "", nil
	}
	ctx = tracing.WithTenantID(ctx, tenantID)

	// Stripe doesn't carry the failure code on the Session payload, so we
	// record a generic async-bank-decline.
	const code = "async_payment_failed"
	const message = "bank declined or returned the async payment"

	switch agg {
	case wh.AggregateCoursePurchase:
		cp, err := d.deps.Course.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := cp.MarkPaymentFailed(code, message, now); err != nil {
			return agg, cp.PurchaseID, err
		}
		if err := d.deps.Course.Save(ctx, cp); err != nil {
			return agg, cp.PurchaseID, err
		}
		return agg, cp.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, cp.TenantID), agg, cp.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateApplicationPayment:
		ap, err := d.deps.Application.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := ap.MarkPaymentFailed(code, message, now); err != nil {
			return agg, ap.PurchaseID, err
		}
		if err := d.deps.Application.Save(ctx, ap); err != nil {
			return agg, ap.PurchaseID, err
		}
		return agg, ap.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, ap.TenantID), agg, ap.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateFamiliarEggPurchase:
		ep, err := d.deps.FamiliarEgg.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := ep.MarkPaymentFailed(code, message, now); err != nil {
			return agg, ep.PurchaseID, err
		}
		if err := d.deps.FamiliarEgg.Save(ctx, ep); err != nil {
			return agg, ep.PurchaseID, err
		}
		return agg, ep.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, ep.TenantID), agg, ep.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateTenantManaTopUp:
		t, err := d.deps.ManaTopUp.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := t.MarkPaymentFailed(code, message, now); err != nil {
			return agg, t.PurchaseID, err
		}
		if err := d.deps.ManaTopUp.Save(ctx, t); err != nil {
			return agg, t.PurchaseID, err
		}
		return agg, t.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, t.TenantID), agg, t.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateUserSubscription:
		u, err := d.deps.Subscription.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := u.MarkPaymentFailed(code, message, now); err != nil {
			return agg, u.PurchaseID, err
		}
		if err := d.deps.Subscription.Save(ctx, u); err != nil {
			return agg, u.PurchaseID, err
		}
		return agg, u.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, u.TenantID), agg, u.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateUserManaTopUp:
		um, err := d.deps.UserManaTopUp.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := um.MarkPaymentFailed(code, message, now); err != nil {
			return agg, um.PurchaseID, err
		}
		if err := d.deps.UserManaTopUp.Save(ctx, um); err != nil {
			return agg, um.PurchaseID, err
		}
		return agg, um.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, um.TenantID), agg, um.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateIdentityKycFee:
		k, err := d.deps.IdentityKycFee.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := k.MarkPaymentFailed(code, message, now); err != nil {
			return agg, k.PurchaseID, err
		}
		if err := d.deps.IdentityKycFee.Save(ctx, k); err != nil {
			return agg, k.PurchaseID, err
		}
		return agg, k.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, k.TenantID), agg, k.PurchaseID, EventPaymentFailed, event.ID)
	case wh.AggregateTenantAddonPurchase:
		a, err := d.deps.AddonPurchase.GetByStripeSessionID(ctx, sess.ID)
		if err != nil {
			return agg, pid, err
		}
		if err := a.MarkPaymentFailed(code, message, now); err != nil {
			return agg, a.PurchaseID, err
		}
		if err := d.deps.AddonPurchase.Save(ctx, a); err != nil {
			return agg, a.PurchaseID, err
		}
		return agg, a.PurchaseID, d.emit(ctx, tenantIDOr(tenantID, a.TenantID), agg, a.PurchaseID, EventPaymentFailed, event.ID)
	}
	return agg, pid, fmt.Errorf("unknown aggregate type %q", agg)
}

// handleChargeDisputeCreated ingests a Stripe charge.dispute.created
// webhook, persists a new Dispute aggregate, and emits
// chora.payments.dispute.raised.v1. Looks up the parent Purchase via
// the Charge metadata (purchase_type + purchase_id) so the Dispute row
// carries (aggregate_type, purchase_id) for cross-aggregate joins.
//
// Stripe redelivery: the parent Purchase metadata is the canonical
// routing handle; if the metadata is missing the dispute can still be
// recorded against the charge (we attempt an Aggregate lookup via the
// charge handle as fallback in a future extension).
func (d *Dispatcher) handleChargeDisputeCreated(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	disp, err := parseDispute(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	if d.deps.Dispute == nil {
		log.Printf("payments/dispatcher: dispute repo not wired — dropping dispute event dp=%s", disp.ID)
		return wh.AggregateUnknown, "", nil
	}

	// Idempotent gate — if we've already persisted this Stripe dispute,
	// short-circuit. Webhook redelivery + stripe_webhook_events dedup
	// covers most cases; this is the in-aggregate safety net.
	if existing, lookupErr := d.deps.Dispute.GetByStripeDisputeID(ctx, disp.ID); lookupErr == nil && existing != nil {
		return existing.AggregateType, existing.PurchaseID, nil
	}

	aggType, purchaseID, metaErr := resolveTarget(disp.Charge.Metadata)
	if metaErr != nil {
		log.Printf("payments/dispatcher: charge.dispute.created without parent metadata dp=%s — ack only", disp.ID)
		return wh.AggregateUnknown, "", nil
	}
	tenantID := disp.Charge.Metadata["tenant_id"]
	if tenantID == "" {
		// Fall back to the per-aggregate row's tenant_id by loading the parent.
		tenantID = d.resolveTenantFromParent(ctx, aggType, purchaseID)
	}
	if tenantID == "" {
		log.Printf("payments/dispatcher: charge.dispute.created without tenant_id dp=%s — ack only", disp.ID)
		return wh.AggregateUnknown, "", nil
	}

	now := d.deps.Now()
	disputeID := newDisputeID()
	dueBy := timeOrZero(disp.EvidenceDetails)
	// Stripe returns ISO 4217 currency codes in lowercase on webhook
	// payloads; the shared.ValidateCurrency invariant expects uppercase.
	currency := strings.ToUpper(string(disp.Currency))
	dpEntity, dErr := dispute.New(
		disputeID, tenantID, aggType, purchaseID,
		disp.ID, disp.Charge.ID,
		disp.Amount, currency,
		dispute.Reason(disp.Reason),
		dueBy, now,
	)
	if dErr != nil {
		return aggType, purchaseID, fmt.Errorf("dispute.New: %w", dErr)
	}
	if insErr := d.deps.Dispute.Insert(ctx, dpEntity); insErr != nil {
		if errors.Is(insErr, dispute.ErrAlreadyExists) {
			// Race-condition replay: another goroutine inserted concurrently.
			// Read back + treat as no-op create.
			return aggType, purchaseID, nil
		}
		return aggType, purchaseID, fmt.Errorf("dispute insert: %w", insErr)
	}
	d.emitDispute(ctx, tenantID, dpEntity.DisputeID, EventDisputeRaised, event.ID)
	return aggType, purchaseID, nil
}

// handleChargeDisputeClosed transitions the Dispute aggregate to closed
// with the Stripe-reported outcome (won / lost / warning_closed) and
// emits chora.payments.dispute.closed.v1.
func (d *Dispatcher) handleChargeDisputeClosed(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	disp, err := parseDispute(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	if d.deps.Dispute == nil {
		log.Printf("payments/dispatcher: dispute repo not wired — dropping dispute close dp=%s", disp.ID)
		return wh.AggregateUnknown, "", nil
	}
	dpEntity, lookupErr := d.deps.Dispute.GetByStripeDisputeID(ctx, disp.ID)
	if lookupErr != nil {
		if errors.Is(lookupErr, dispute.ErrNotFound) {
			log.Printf("payments/dispatcher: charge.dispute.closed for unknown dispute dp=%s — ack only", disp.ID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", lookupErr
	}
	outcome := dispute.Outcome(disp.Status)
	if !outcome.IsValid() {
		// Stripe's status enum has additional values during the open
		// window — only the final 3 are valid close outcomes. Map
		// anything else to warning_closed by default so we don't
		// stall on edge cases.
		log.Printf("payments/dispatcher: charge.dispute.closed unrecognised status=%q dp=%s — defaulting to warning_closed", disp.Status, disp.ID)
		outcome = dispute.OutcomeWarningClosed
	}
	now := d.deps.Now()
	if err := dpEntity.MarkClosed(outcome, now); err != nil {
		return dpEntity.AggregateType, dpEntity.PurchaseID, fmt.Errorf("dispute MarkClosed: %w", err)
	}
	if err := d.deps.Dispute.Save(ctx, dpEntity); err != nil {
		return dpEntity.AggregateType, dpEntity.PurchaseID, fmt.Errorf("dispute save: %w", err)
	}
	d.emitDispute(ctx, dpEntity.TenantID, dpEntity.DisputeID, EventDisputeClosed, event.ID)
	return dpEntity.AggregateType, dpEntity.PurchaseID, nil
}

// handleChargeDisputeFundsWithdrawn records the Stripe debit + emits
// chora.payments.dispute.funds_withdrawn.v1. Idempotent — Stripe
// redelivery hits the Dispute aggregate's idempotency check.
func (d *Dispatcher) handleChargeDisputeFundsWithdrawn(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	return d.recordDisputeFundsMovement(ctx, event, fundsMovementWithdrawn)
}

// handleChargeDisputeFundsReinstated records the Stripe credit + emits
// chora.payments.dispute.funds_reinstated.v1. Accepts arrival without a
// prior funds_withdrawn (warning-closed disputes).
func (d *Dispatcher) handleChargeDisputeFundsReinstated(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	return d.recordDisputeFundsMovement(ctx, event, fundsMovementReinstated)
}

type fundsMovementKind int

const (
	fundsMovementWithdrawn fundsMovementKind = iota
	fundsMovementReinstated
)

func (d *Dispatcher) recordDisputeFundsMovement(ctx context.Context, event *stripeGo.Event, kind fundsMovementKind) (wh.AggregateType, string, error) {
	disp, err := parseDispute(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	if d.deps.Dispute == nil {
		log.Printf("payments/dispatcher: dispute repo not wired — dropping funds-movement dp=%s", disp.ID)
		return wh.AggregateUnknown, "", nil
	}
	dpEntity, lookupErr := d.deps.Dispute.GetByStripeDisputeID(ctx, disp.ID)
	if lookupErr != nil {
		if errors.Is(lookupErr, dispute.ErrNotFound) {
			log.Printf("payments/dispatcher: funds-movement for unknown dispute dp=%s — ack only", disp.ID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", lookupErr
	}
	now := d.deps.Now()
	var evType EventType
	switch kind {
	case fundsMovementWithdrawn:
		if err := dpEntity.RecordFundsWithdrawn(now); err != nil {
			return dpEntity.AggregateType, dpEntity.PurchaseID, err
		}
		evType = EventDisputeFundsWithdrawn
	case fundsMovementReinstated:
		if err := dpEntity.RecordFundsReinstated(now); err != nil {
			return dpEntity.AggregateType, dpEntity.PurchaseID, err
		}
		evType = EventDisputeFundsReinstated
	}
	if err := d.deps.Dispute.Save(ctx, dpEntity); err != nil {
		return dpEntity.AggregateType, dpEntity.PurchaseID, fmt.Errorf("dispute save: %w", err)
	}
	d.emitDispute(ctx, dpEntity.TenantID, dpEntity.DisputeID, evType, event.ID)
	return dpEntity.AggregateType, dpEntity.PurchaseID, nil
}

// resolveTenantFromParent looks up the parent Purchase aggregate's
// tenant_id when the Charge metadata didn't carry tenant_id explicitly.
// Returns "" if the parent cannot be resolved.
func (d *Dispatcher) resolveTenantFromParent(ctx context.Context, aggType wh.AggregateType, purchaseID string) string {
	switch aggType {
	case wh.AggregateCoursePurchase:
		// Cross-tenant GetByID requires a tenant; instead we'd need a
		// scan adapter. Skip and let the caller log the ack-only path.
		return ""
	}
	// Future extension: cross-tenant lookup via adapter-only path.
	_ = ctx
	_ = purchaseID
	return ""
}

// emitDispute hands a dispute event to the OutboxEmitter. Errors logged
// but non-fatal (same atomicity contract as emit).
func (d *Dispatcher) emitDispute(ctx context.Context, tenantID, disputeID string, evType EventType, stripeEventID string) {
	if err := d.deps.Outbox.EmitDispute(ctx, EmitDisputeInput{
		TenantID:       tenantID,
		DisputeID:      disputeID,
		EventType:      evType,
		IdempotencyKey: disputeIdempotencyKey(evType, disputeID, stripeEventID),
	}); err != nil {
		log.Printf("payments/dispatcher: outbox emit-dispute failed event=%s dispute=%s err=%v",
			evType, disputeID, err)
	}
}

func disputeIdempotencyKey(evType EventType, disputeID, stripeEventID string) string {
	return "dispute." + string(evType) + "." + disputeID + "." + stripeEventID
}

func newDisputeID() string {
	return uuid.Must(uuid.NewV7()).String()
}

func timeOrZero(ed *stripeGo.DisputeEvidenceDetails) time.Time {
	if ed == nil || ed.DueBy == 0 {
		return time.Time{}
	}
	return time.Unix(ed.DueBy, 0).UTC()
}

// parseDispute extracts the Stripe Dispute object from the event payload.
// -----------------------------------------------------------------------------
// CHO-1763 — Stripe Subscription lifecycle (tenant_addon_purchase only).
// -----------------------------------------------------------------------------

// handleCustomerSubscriptionCreated persists the Stripe Subscription's
// resolved IDs + cycle anchor onto the tenant_addon_purchase row
// identified by Subscription.metadata.purchase_id (set in the
// SubscriptionData.Metadata propagation per CHO-1762). The handler is
// idempotent under repeated delivery via the CHO-1761
// ApplyStripeSubscriptionState mutator.
//
// Synthetic events (missing tenant_id, missing purchase_id, non-TAP
// purchase_type) ack-only skip — same shape as the mana-topup
// no-tenant guard. Subscription events for user_subscription stay on
// the legacy non-Subscribe path (out of scope for CHO-1759 epic).
//
// No outbox emit — checkout.session.completed (CHO-1740 path) already
// publishes tenant_addon_purchase.payment_captured.v1 which drives the
// tenancy AddOnActivator. Double-publishing would duplicate
// downstream Activate calls; this handler's job is solely to write
// stripe_subscription_id back onto the row for CHO-1764.
func (d *Dispatcher) handleCustomerSubscriptionCreated(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	if d.deps.AddonPurchase == nil {
		return wh.AggregateUnknown, "", nil
	}
	sub, customerID, err := parseSubscription(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	metadata := sub.Metadata
	if metadata == nil {
		log.Printf("payments/dispatcher: subscription_created missing metadata — skipping (synthetic event?) sub=%s", sub.ID)
		return wh.AggregateUnknown, "", nil
	}
	purchaseType := metadata["purchase_type"]
	if purchaseType != string(wh.AggregateTenantAddonPurchase) {
		// Subscription events for other aggregates not handled by this
		// path. Skip silently — the dispatcher only owns the
		// tenant-addon Subscribe lifecycle in this PR.
		log.Printf("payments/dispatcher: subscription_created for non-TAP purchase_type=%q — skipping sub=%s", purchaseType, sub.ID)
		return wh.AggregateUnknown, "", nil
	}
	purchaseID := metadata["purchase_id"]
	if purchaseID == "" {
		log.Printf("payments/dispatcher: subscription_created missing purchase_id — skipping sub=%s", sub.ID)
		return wh.AggregateUnknown, "", nil
	}
	tenantID := metadata["tenant_id"]
	if tenantID == "" {
		log.Printf("payments/dispatcher: subscription_created missing tenant_id — skipping (synthetic event?) sub=%s", sub.ID)
		return wh.AggregateUnknown, "", nil
	}
	ctx = tracing.WithTenantID(ctx, tenantID)

	row, err := d.deps.AddonPurchase.GetByID(ctx, tenantID, purchaseID)
	if err != nil {
		return wh.AggregateTenantAddonPurchase, purchaseID, fmt.Errorf("tenant_addon_purchase lookup by purchase_id=%s: %w", purchaseID, err)
	}
	status := tap.Status(string(sub.Status))
	if !tap.IsValidStatus(status) {
		// Stripe may emit interim statuses (trialing / unpaid /
		// incomplete) we don't model. Default to pending so the row
		// at least carries the customer + subscription IDs; the next
		// status-changing event (which IS in our enum) will update.
		status = tap.StatusPending
	}
	var periodStart, periodEnd *time.Time
	if sub.CurrentPeriodStart > 0 {
		ts := time.Unix(sub.CurrentPeriodStart, 0).UTC()
		periodStart = &ts
	}
	if sub.CurrentPeriodEnd > 0 {
		ts := time.Unix(sub.CurrentPeriodEnd, 0).UTC()
		periodEnd = &ts
	}
	if err := row.ApplyStripeSubscriptionState(customerID, sub.ID, status, periodStart, periodEnd); err != nil {
		return wh.AggregateTenantAddonPurchase, purchaseID, fmt.Errorf("ApplyStripeSubscriptionState: %w", err)
	}
	if err := d.deps.AddonPurchase.Save(ctx, row); err != nil {
		return wh.AggregateTenantAddonPurchase, purchaseID, fmt.Errorf("tenant_addon_purchase save: %w", err)
	}
	return wh.AggregateTenantAddonPurchase, purchaseID, nil
}

// -----------------------------------------------------------------------------
// CHO-1771 — 4 remaining Stripe Subscription lifecycle handlers.
// -----------------------------------------------------------------------------

// handleCustomerSubscriptionUpdated catches Dashboard-side mutations +
// auto-renewal cycle-anchor bumps. Idempotent state refresh via
// ApplyStripeSubscriptionState (CHO-1761). No outbox emit — the FE
// sync change-tier path covers tier mutations; out-of-band cycle
// changes land on the next /h/addons fetch.
//
// Lookup is by stripe_subscription_id (CHO-1763 populated). Unknown
// subs (cross-account, pre-CHO-1762 row) ack-only skip.
func (d *Dispatcher) handleCustomerSubscriptionUpdated(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	if d.deps.AddonPurchase == nil {
		return wh.AggregateUnknown, "", nil
	}
	sub, customerID, err := parseSubscription(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	row, err := d.deps.AddonPurchase.GetByStripeSubscriptionID(ctx, sub.ID)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			log.Printf("payments/dispatcher: subscription_updated for unknown sub_id=%s — skipping", sub.ID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", fmt.Errorf("subscription_updated lookup sub_id=%s: %w", sub.ID, err)
	}
	ctx = tracing.WithTenantID(ctx, row.TenantID)

	status := tap.Status(string(sub.Status))
	if !tap.IsValidStatus(status) {
		status = row.Status // keep prior status on unknown Stripe values
	}
	var periodStart, periodEnd *time.Time
	if sub.CurrentPeriodStart > 0 {
		ts := time.Unix(sub.CurrentPeriodStart, 0).UTC()
		periodStart = &ts
	}
	if sub.CurrentPeriodEnd > 0 {
		ts := time.Unix(sub.CurrentPeriodEnd, 0).UTC()
		periodEnd = &ts
	}
	if err := row.ApplyStripeSubscriptionState(customerID, sub.ID, status, periodStart, periodEnd); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("ApplyStripeSubscriptionState: %w", err)
	}
	if err := d.deps.AddonPurchase.Save(ctx, row); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("subscription_updated save: %w", err)
	}
	return wh.AggregateTenantAddonPurchase, row.PurchaseID, nil
}

// handleCustomerSubscriptionDeleted sets sub_status=cancelled, emits
// `tenant_addon_purchase.cancelled.v1` so chora-tenancy can Deactivate
// the AddOn aggregate out-of-band (Stripe Dashboard cancel path —
// distinct from CHO-1731's FE-initiated deactivate flow).
//
// Idempotent: re-delivery finds the row already cancelled →
// ApplyStripeSubscriptionState no-ops on the status field; the emit
// is deduped at the outbox UNIQUE constraint via the per-event
// idempotency key.
func (d *Dispatcher) handleCustomerSubscriptionDeleted(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	if d.deps.AddonPurchase == nil {
		return wh.AggregateUnknown, "", nil
	}
	sub, customerID, err := parseSubscription(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	row, err := d.deps.AddonPurchase.GetByStripeSubscriptionID(ctx, sub.ID)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			log.Printf("payments/dispatcher: subscription_deleted for unknown sub_id=%s — skipping", sub.ID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", fmt.Errorf("subscription_deleted lookup sub_id=%s: %w", sub.ID, err)
	}
	ctx = tracing.WithTenantID(ctx, row.TenantID)

	priorStatus := row.Status
	if err := row.ApplyStripeSubscriptionState(customerID, sub.ID, tap.StatusCancelled, nil, nil); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("ApplyStripeSubscriptionState(cancelled): %w", err)
	}
	if err := d.deps.AddonPurchase.Save(ctx, row); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("subscription_deleted save: %w", err)
	}
	// CHO-1775 — emit cancelled event so chora-tenancy can Deactivate
	// the AddOn out-of-band (Stripe Dashboard cancel). Skip emit on
	// re-delivery (priorStatus already cancelled) to avoid a second
	// outbox row (the UNIQUE constraint would dedupe anyway).
	if priorStatus == tap.StatusCancelled {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, nil
	}
	return wh.AggregateTenantAddonPurchase, row.PurchaseID,
		d.emit(ctx, row.TenantID, wh.AggregateTenantAddonPurchase, row.PurchaseID, EventSubscriptionCancelled, event.ID)
}

// handleInvoicePaymentFailed sets sub_status=past_due + emits
// `tenant_addon_purchase.payment_failed.v1` for the chora-tenancy
// dunning banner + chora-notifications email.
//
// Stripe smart-retries handle most failures; access is NOT removed at
// this step. customer.subscription.deleted is the canonical removal
// signal (Stripe fires it once smart-retries exhaust).
func (d *Dispatcher) handleInvoicePaymentFailed(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	if d.deps.AddonPurchase == nil {
		return wh.AggregateUnknown, "", nil
	}
	inv, err := parseInvoice(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	if inv.subscriptionID == "" {
		log.Printf("payments/dispatcher: invoice_payment_failed missing subscription — skipping (one-shot invoice?) invoice=%s", inv.id)
		return wh.AggregateUnknown, "", nil
	}
	row, err := d.deps.AddonPurchase.GetByStripeSubscriptionID(ctx, inv.subscriptionID)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			log.Printf("payments/dispatcher: invoice_payment_failed for unknown sub_id=%s — skipping", inv.subscriptionID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", fmt.Errorf("invoice_payment_failed lookup sub_id=%s: %w", inv.subscriptionID, err)
	}
	ctx = tracing.WithTenantID(ctx, row.TenantID)

	priorStatus := row.Status
	if err := row.ApplyStripeSubscriptionState(row.StripeCustomerID, row.StripeSubscriptionID, tap.StatusPastDue, nil, nil); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("ApplyStripeSubscriptionState(past_due): %w", err)
	}
	if err := d.deps.AddonPurchase.Save(ctx, row); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("invoice_payment_failed save: %w", err)
	}
	// CHO-1775 — emit subscription_payment_failed for the chora-tenancy
	// past_due flag + chora-notifications dunning email. Skip emit on
	// re-delivery (already past_due) to avoid duplicate dunning notices.
	if priorStatus == tap.StatusPastDue {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, nil
	}
	return wh.AggregateTenantAddonPurchase, row.PurchaseID,
		d.emit(ctx, row.TenantID, wh.AggregateTenantAddonPurchase, row.PurchaseID, EventSubscriptionPaymentFailed, event.ID)
}

// handleInvoicePaymentSucceeded handles the past_due → active recovery
// path (Stripe smart-retry succeeded). Normal renewal cycles (prior
// status already active) are a no-op — Stripe's customer.subscription.updated
// covers the cycle-anchor refresh + the FE pulls the latest invoice
// list from /h/billing.
func (d *Dispatcher) handleInvoicePaymentSucceeded(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	if d.deps.AddonPurchase == nil {
		return wh.AggregateUnknown, "", nil
	}
	inv, err := parseInvoice(event)
	if err != nil {
		return wh.AggregateUnknown, "", err
	}
	if inv.subscriptionID == "" {
		log.Printf("payments/dispatcher: invoice_payment_succeeded missing subscription — skipping invoice=%s", inv.id)
		return wh.AggregateUnknown, "", nil
	}
	row, err := d.deps.AddonPurchase.GetByStripeSubscriptionID(ctx, inv.subscriptionID)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			log.Printf("payments/dispatcher: invoice_payment_succeeded for unknown sub_id=%s — skipping", inv.subscriptionID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", fmt.Errorf("invoice_payment_succeeded lookup sub_id=%s: %w", inv.subscriptionID, err)
	}
	if row.Status != tap.StatusPastDue {
		// Normal renewal — no state transition, no emit.
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, nil
	}
	ctx = tracing.WithTenantID(ctx, row.TenantID)

	if err := row.ApplyStripeSubscriptionState(row.StripeCustomerID, row.StripeSubscriptionID, tap.StatusActive, nil, nil); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("ApplyStripeSubscriptionState(active): %w", err)
	}
	if err := d.deps.AddonPurchase.Save(ctx, row); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("invoice_payment_succeeded save: %w", err)
	}
	// CHO-1775 — emit payment_recovered so the FE dunning banner clears
	// + chora-notifications sends a recovery email.
	return wh.AggregateTenantAddonPurchase, row.PurchaseID,
		d.emit(ctx, row.TenantID, wh.AggregateTenantAddonPurchase, row.PurchaseID, EventPaymentRecovered, event.ID)
}

// -----------------------------------------------------------------------------
// CHO-1772 — subscription_schedule.released handler.
// -----------------------------------------------------------------------------

// handleSubscriptionScheduleReleased fires when Stripe completes the
// deferred-tier-change schedule's second phase. End behaviour is
// `release` (set when the schedule was created in
// stripe.RealClient.ScheduleSubscriptionPriceChange) so Stripe drops the
// schedule + keeps the bare Subscription running at the new tier.
//
// Our side promotes ScheduledTierCode → TierCode on the TAP row via
// tap.TenantAddonPurchase.ReleaseSchedule which also nulls the three
// schedule columns. Lookup is by stripe_subscription_schedule_id;
// unknown schedule_ids ack-only skip (cross-account event,
// Dashboard-created schedule, or a duplicate retry of an already-cleared
// schedule).
//
// CHO-1779 — emits chora.payments.tenant_addon_purchase.
// subscription_schedule_released.v1 so chora-tenancy's payments
// subscriber can call SubscriptionRegistry.PromoteScheduledTier on its
// in-memory registry. Without this event, /h/addons keeps showing the
// stale "<scheduled> starting <past>" badge until chora-tenancy
// restarts.
func (d *Dispatcher) handleSubscriptionScheduleReleased(ctx context.Context, event *stripeGo.Event) (wh.AggregateType, string, error) {
	if d.deps.AddonPurchase == nil {
		return wh.AggregateUnknown, "", nil
	}
	var raw struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(event.Data.Raw, &raw); err != nil {
		return wh.AggregateUnknown, "", fmt.Errorf("unmarshal SubscriptionSchedule: %w", err)
	}
	if raw.ID == "" {
		log.Printf("payments/dispatcher: subscription_schedule.released missing id — skipping (synthetic event?) event_id=%s", event.ID)
		return wh.AggregateUnknown, "", nil
	}
	row, err := d.deps.AddonPurchase.GetByStripeScheduleID(ctx, raw.ID)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			log.Printf("payments/dispatcher: subscription_schedule.released for unknown schedule_id=%s — skipping", raw.ID)
			return wh.AggregateUnknown, "", nil
		}
		return wh.AggregateUnknown, "", fmt.Errorf("subscription_schedule.released lookup schedule_id=%s: %w", raw.ID, err)
	}
	ctx = tracing.WithTenantID(ctx, row.TenantID)

	// CHO-1779 — capture pre-release flag so a duplicate webhook delivery
	// (where ReleaseSchedule has nothing to promote) skips the emit.
	// Without this guard, retries would write duplicate outbox rows even
	// though the registry would no-op on the consumer side.
	hadPendingSchedule := row.HasPendingSchedule()
	if err := row.ReleaseSchedule(); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("ReleaseSchedule: %w", err)
	}
	row.UpdatedAt = d.deps.Now()
	if err := d.deps.AddonPurchase.Save(ctx, row); err != nil {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, fmt.Errorf("subscription_schedule.released save: %w", err)
	}
	if !hadPendingSchedule {
		return wh.AggregateTenantAddonPurchase, row.PurchaseID, nil
	}
	return wh.AggregateTenantAddonPurchase, row.PurchaseID,
		d.emit(ctx, row.TenantID, wh.AggregateTenantAddonPurchase, row.PurchaseID, EventSubscriptionScheduleReleased, event.ID)
}

// parsedInvoice carries the few Invoice fields the CHO-1771 handlers
// need. Stripe Invoice expansion shape varies between events + SDK
// version — we parse a minimal raw view rather than depending on
// stripe-go's Invoice struct (which moved fields between v76/77/78).
type parsedInvoice struct {
	id             string
	customerID     string
	subscriptionID string
}

func parseInvoice(event *stripeGo.Event) (parsedInvoice, error) {
	var raw struct {
		ID           string          `json:"id"`
		Customer     json.RawMessage `json:"customer"`
		Subscription json.RawMessage `json:"subscription"`
	}
	if err := json.Unmarshal(event.Data.Raw, &raw); err != nil {
		return parsedInvoice{}, fmt.Errorf("unmarshal Invoice: %w", err)
	}
	return parsedInvoice{
		id:             raw.ID,
		customerID:     stripeRawIDValue(raw.Customer),
		subscriptionID: stripeRawIDValue(raw.Subscription),
	}, nil
}

// stripeRawIDValue handles Stripe's polymorphic expansion: a referenced
// object can arrive as a bare ID string OR as a fully-expanded object
// with an `id` field. Returns "" on null / empty.
func stripeRawIDValue(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return asString
	}
	var asObj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &asObj); err == nil {
		return asObj.ID
	}
	return ""
}

// Stripe charge.dispute.* events carry a `dispute` object with an
// embedded `Charge` reference (Stripe expands the charge for these
// events by default; we accept the raw Charge identifier as a fallback).
func parseDispute(event *stripeGo.Event) (*stripeGo.Dispute, error) {
	var disp stripeGo.Dispute
	if err := json.Unmarshal(event.Data.Raw, &disp); err != nil {
		return nil, fmt.Errorf("unmarshal Dispute: %w", err)
	}
	return &disp, nil
}

// -----------------------------------------------------------------------------
// Existing helper (unchanged below).
// -----------------------------------------------------------------------------

// emit hands an event to the OutboxEmitter and returns any error so the
// caller can propagate it. Emit failures MUST be fatal to the webhook
// receive path: if the outbox INSERT itself fails (vs. the row landing and
// the async drainer publishing later), silently swallowing the error marks
// the Stripe event processed while no event was ever written — the
// downstream credit (e.g. user_mana_topup → identity CreditMana) never
// fires and the wallet is permanently stuck at the old balance with no
// retry. Propagating the error makes the webhook return 500 → Stripe
// retries → the dispatcher re-runs. Re-dispatch is safe because the
// aggregate transition is idempotent (MarkPaymentCaptured no-ops once
// captured) and the outbox row is idempotent on idempotency_key.
func (d *Dispatcher) emit(ctx context.Context, tenantID string, agg wh.AggregateType, purchaseID string, evType EventType, stripeEventID string) error {
	if err := d.deps.Outbox.Emit(ctx, EmitInput{
		TenantID:       tenantID,
		Aggregate:      agg,
		PurchaseID:     purchaseID,
		EventType:      evType,
		IdempotencyKey: outboxIdempotencyKey(agg, evType, purchaseID, stripeEventID),
	}); err != nil {
		log.Printf("payments/dispatcher: outbox emit failed agg=%s event=%s purchase=%s err=%v",
			agg, evType, purchaseID, err)
		return fmt.Errorf("outbox emit %s.%s purchase=%s: %w", agg, evType, purchaseID, err)
	}
	return nil
}

// outboxIdempotencyKey returns a stable per-event dedup key. Replays of
// the same Stripe webhook event collapse to the same outbox row via
// the UNIQUE constraint on outbox_events.idempotency_key.
func outboxIdempotencyKey(agg wh.AggregateType, evType EventType, purchaseID, stripeEventID string) string {
	return string(agg) + "." + string(evType) + "." + purchaseID + "." + stripeEventID
}

// tenantIDOr returns the first non-empty tenant_id. Stripe Session
// metadata is the canonical source; the aggregate row's tenant_id is
// the fallback.
func tenantIDOr(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

// -----------------------------------------------------------------------------
// Parsing helpers
// -----------------------------------------------------------------------------

func parseSession(event *stripeGo.Event) (*stripeGo.CheckoutSession, error) {
	var sess stripeGo.CheckoutSession
	if err := json.Unmarshal(event.Data.Raw, &sess); err != nil {
		return nil, fmt.Errorf("unmarshal Session: %w", err)
	}
	return &sess, nil
}

// parseSubscription decodes a Stripe Subscription event payload. The
// SDK type's Customer field is sometimes a string and sometimes an
// expanded *Customer object depending on the event; we lift the ID
// off either shape via a side-channel raw decode for safety.
func parseSubscription(event *stripeGo.Event) (*stripeGo.Subscription, string, error) {
	var sub stripeGo.Subscription
	if err := json.Unmarshal(event.Data.Raw, &sub); err != nil {
		return nil, "", fmt.Errorf("unmarshal Subscription: %w", err)
	}
	// stripeGo.Subscription.Customer is *stripeGo.Customer; the JSON
	// field is sometimes a plain ID string. Pull it from the raw blob
	// so we don't depend on the SDK's struct shape catching every case.
	var rawObj struct {
		Customer json.RawMessage `json:"customer"`
	}
	customerID := ""
	if err := json.Unmarshal(event.Data.Raw, &rawObj); err == nil {
		var s string
		if jerr := json.Unmarshal(rawObj.Customer, &s); jerr == nil && s != "" {
			customerID = s
		} else {
			var custObj struct {
				ID string `json:"id"`
			}
			if jerr := json.Unmarshal(rawObj.Customer, &custObj); jerr == nil {
				customerID = custObj.ID
			}
		}
	}
	if customerID == "" && sub.Customer != nil {
		customerID = sub.Customer.ID
	}
	return &sub, customerID, nil
}

func parsePaymentIntent(event *stripeGo.Event) (*stripeGo.PaymentIntent, error) {
	var pi stripeGo.PaymentIntent
	if err := json.Unmarshal(event.Data.Raw, &pi); err != nil {
		return nil, fmt.Errorf("unmarshal PaymentIntent: %w", err)
	}
	return &pi, nil
}

func parseCharge(event *stripeGo.Event) (*stripeGo.Charge, error) {
	var ch stripeGo.Charge
	if err := json.Unmarshal(event.Data.Raw, &ch); err != nil {
		return nil, fmt.Errorf("unmarshal Charge: %w", err)
	}
	return &ch, nil
}

// -----------------------------------------------------------------------------
// ADR-188 — webhook-authoritative reconstruct-on-miss
// -----------------------------------------------------------------------------

// reconstructTenantManaTopUp rebuilds a tenant_mana_topup from the verified
// Stripe session metadata when the create-side row is absent (ADR-188 D1 —
// the orphaned-paid-session bug). Amount + currency come from Stripe's
// authoritative session totals, NEVER from client-echoed metadata (D2).
// Save UPSERTs on purchase_id, so Stripe's at-least-once redelivery cannot
// duplicate.
func (d *Dispatcher) reconstructTenantManaTopUp(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*mana.TenantManaTopUp, error) {
	md := sess.Metadata
	manaUnits, perr := strconv.ParseInt(md["mana_units"], 10, 64)
	if perr != nil {
		return nil, fmt.Errorf("reconstruct tenant_mana_topup: mana_units %q: %w", md["mana_units"], perr)
	}
	t, err := mana.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"], md["sku"],
		manaUnits,
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "", // checkout URL is not carried on the webhook event
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct tenant_mana_topup: %w", err)
	}
	if err := d.deps.ManaTopUp.Save(ctx, t); err != nil {
		return nil, fmt.Errorf("reconstruct tenant_mana_topup persist: %w", err)
	}
	logReconstruction(wh.AggregateTenantManaTopUp, t.PurchaseID, t.TenantID, sess.ID)
	return t, nil
}

// reconstructTenantAddonPurchase rebuilds a tenant_addon_purchase from the
// verified Stripe session metadata when the create-side row is absent.
func (d *Dispatcher) reconstructTenantAddonPurchase(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*tap.TenantAddonPurchase, error) {
	md := sess.Metadata
	a, err := tap.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"],
		md["addon_plan_id"], md["addon_code"], md["tier_code"],
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "",
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct tenant_addon_purchase: %w", err)
	}
	if err := d.deps.AddonPurchase.Save(ctx, a); err != nil {
		return nil, fmt.Errorf("reconstruct tenant_addon_purchase persist: %w", err)
	}
	logReconstruction(wh.AggregateTenantAddonPurchase, a.PurchaseID, a.TenantID, sess.ID)
	return a, nil
}

func (d *Dispatcher) reconstructCoursePurchase(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*coursepurchase.CoursePurchase, error) {
	md := sess.Metadata
	cp, err := coursepurchase.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"], md["course_id"],
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "", now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct course_purchase: %w", err)
	}
	if err := d.deps.Course.Save(ctx, cp); err != nil {
		return nil, fmt.Errorf("reconstruct course_purchase persist: %w", err)
	}
	logReconstruction(wh.AggregateCoursePurchase, cp.PurchaseID, cp.TenantID, sess.ID)
	return cp, nil
}

func (d *Dispatcher) reconstructApplicationPayment(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*apppay.ApplicationPayment, error) {
	md := sess.Metadata
	ap, err := apppay.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"],
		md["application_id"], md["course_id"],
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "", now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct application_payment: %w", err)
	}
	if err := d.deps.Application.Save(ctx, ap); err != nil {
		return nil, fmt.Errorf("reconstruct application_payment persist: %w", err)
	}
	logReconstruction(wh.AggregateApplicationPayment, ap.PurchaseID, ap.TenantID, sess.ID)
	return ap, nil
}

func (d *Dispatcher) reconstructFamiliarEggPurchase(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*egg.FamiliarEggPurchase, error) {
	md := sess.Metadata
	// Expiry windows are not carried on the webhook event; the reconstruct
	// path (create-side row absent) restores the SKU + focal-atom hint and
	// leaves the nullable expiries nil. Fulfilment proceeds either way.
	ep, err := egg.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"], md["egg_sku"],
		md["suggested_focal_atom_id"],
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "",
		nil, nil,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct familiar_egg_purchase: %w", err)
	}
	if err := d.deps.FamiliarEgg.Save(ctx, ep); err != nil {
		return nil, fmt.Errorf("reconstruct familiar_egg_purchase persist: %w", err)
	}
	logReconstruction(wh.AggregateFamiliarEggPurchase, ep.PurchaseID, ep.TenantID, sess.ID)
	return ep, nil
}

func (d *Dispatcher) reconstructUserSubscription(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*sub.UserSubscription, error) {
	md := sess.Metadata
	u, err := sub.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"], md["plan_sku"],
		sub.BillingPeriod(md["billing_period"]),
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "", now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct user_subscription: %w", err)
	}
	if err := d.deps.Subscription.Save(ctx, u); err != nil {
		return nil, fmt.Errorf("reconstruct user_subscription persist: %w", err)
	}
	logReconstruction(wh.AggregateUserSubscription, u.PurchaseID, u.TenantID, sess.ID)
	return u, nil
}

func (d *Dispatcher) reconstructUserManaTopUp(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*umt.UserManaTopUp, error) {
	md := sess.Metadata
	manaUnits, perr := strconv.ParseInt(md["mana_units"], 10, 64)
	if perr != nil {
		return nil, fmt.Errorf("reconstruct user_mana_topup: mana_units %q: %w", md["mana_units"], perr)
	}
	u, err := umt.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"], md["sku"],
		manaUnits,
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "", now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct user_mana_topup: %w", err)
	}
	if err := d.deps.UserManaTopUp.Save(ctx, u); err != nil {
		return nil, fmt.Errorf("reconstruct user_mana_topup persist: %w", err)
	}
	logReconstruction(wh.AggregateUserManaTopUp, u.PurchaseID, u.TenantID, sess.ID)
	return u, nil
}

func (d *Dispatcher) reconstructIdentityKycFee(ctx context.Context, sess *stripeGo.CheckoutSession, now time.Time) (*kyc.IdentityKycFee, error) {
	md := sess.Metadata
	k, err := kyc.New(
		md["purchase_id"], md["tenant_id"], md["learner_gcid"], md["kyc_doc_type"],
		sess.AmountTotal, strings.ToUpper(string(sess.Currency)),
		sess.ID, "", now,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct identity_kyc_fee: %w", err)
	}
	if err := d.deps.IdentityKycFee.Save(ctx, k); err != nil {
		return nil, fmt.Errorf("reconstruct identity_kyc_fee persist: %w", err)
	}
	logReconstruction(wh.AggregateIdentityKycFee, k.PurchaseID, k.TenantID, sess.ID)
	return k, nil
}

// logReconstruction is the ADR-188 D3 fail-loud signal: taking the
// reconstruct-on-miss path means the create-side row was never persisted —
// an anomaly operators must see even though the webhook now self-heals. The
// fixed RECONSTRUCTED_PURCHASE token is a stable string for a Cloud Logging
// alert metric.
func logReconstruction(agg wh.AggregateType, purchaseID, tenantID, sessionID string) {
	log.Printf("payments/dispatcher: RECONSTRUCTED_PURCHASE agg=%s purchase=%s tenant=%s session=%s — create-side row absent; fulfilling from webhook event (ADR-188)",
		agg, purchaseID, tenantID, sessionID)
}

func resolveTarget(metadata map[string]string) (wh.AggregateType, string, error) {
	if metadata == nil {
		return wh.AggregateUnknown, "", errors.New("metadata empty (purchase_id + purchase_type required)")
	}
	pid := metadata["purchase_id"]
	pt := metadata["purchase_type"]
	if pid == "" || pt == "" {
		return wh.AggregateUnknown, "", fmt.Errorf("metadata missing purchase_id|purchase_type (got id=%q type=%q)", pid, pt)
	}
	agg := wh.AggregateType(pt)
	if !agg.IsValid() {
		return wh.AggregateUnknown, pid, fmt.Errorf("unknown purchase_type %q", pt)
	}
	return agg, pid, nil
}

func stringValue(p *stripeGo.PaymentIntent) string {
	if p == nil {
		return ""
	}
	return p.ID
}

func chargeFromSession(sess *stripeGo.CheckoutSession) string {
	if sess.PaymentIntent != nil && sess.PaymentIntent.LatestCharge != nil {
		return sess.PaymentIntent.LatestCharge.ID
	}
	return ""
}

func failureFromPaymentIntent(pi *stripeGo.PaymentIntent) (code, message string) {
	if pi == nil || pi.LastPaymentError == nil {
		return "", ""
	}
	code = string(pi.LastPaymentError.Code)
	message = pi.LastPaymentError.Msg
	return
}

func latestRefund(ch *stripeGo.Charge) (refundID string, amount int64) {
	if ch == nil || ch.Refunds == nil || len(ch.Refunds.Data) == 0 {
		return "", ch.AmountRefunded
	}
	last := ch.Refunds.Data[len(ch.Refunds.Data)-1]
	return last.ID, last.Amount
}
