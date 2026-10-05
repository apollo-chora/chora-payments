// emitter.go — outbox.Emitter satisfies dispatcher.OutboxEmitter by
// loading the matching Purchase aggregate, marshalling the proto event,
// and writing it into chora_payments.outbox_events (transactional).
//
// The Dispatcher (see dispatcher.go) drains undispatched outbox rows to
// Cloud Pub/Sub on a separate goroutine. This decouples webhook receipt
// (HTTP response time) from Pub/Sub publish reliability — a crash
// between the aggregate state mutation and the event publish no longer
// loses the event.
package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"

	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"

	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
)

// EmitterDeps wires the Emitter's per-aggregate repos + outbox store.
type EmitterDeps struct {
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

	// Outbox writes are TX-bound on chora-payments: each Emit opens a
	// fresh tx on chora_payments.outbox_events.
	Outbox *pg.OutboxRepo

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Emitter is the production OutboxEmitter implementation. Satisfies
// dispatcher.OutboxEmitter.
type Emitter struct {
	deps EmitterDeps
}

// NewEmitter constructs an Emitter. Panics on missing Outbox repo (per
// feedback_no_stubs_real_wiring).
func NewEmitter(deps EmitterDeps) *Emitter {
	if deps.Outbox == nil {
		panic("outbox.NewEmitter: Outbox repo required")
	}
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Emitter{deps: deps}
}

// Emit satisfies dispatcher.OutboxEmitter. Loads the matching Purchase
// aggregate, marshals the per-event-type proto, and INSERTs into
// chora_payments.outbox_events.
//
// Errors at the outbox-INSERT layer surface to the dispatcher caller;
// the dispatcher logs but does NOT roll back the aggregate state
// transition (per the outbox-pattern atomicity contract:
// aggregate-state already committed, the outbox is the eventual-publish
// reconciliation mechanism).
func (e *Emitter) Emit(ctx context.Context, in dispatcher.EmitInput) error {
	now := e.deps.Now()
	eventID := newEventID()
	tp := emitTraceparent(ctx)

	env := buildEnvelope(in.TenantID, eventID, in.IdempotencyKey, tp, now)
	payload, topic, err := loadAndMarshal(ctx, e.deps, env, in.Aggregate, in.PurchaseID, in.EventType)
	if err != nil {
		return err
	}

	row := pg.OutboxRow{
		EventID:        eventID,
		AggregateType:  string(in.Aggregate),
		AggregateID:    in.PurchaseID,
		TenantID:       in.TenantID,
		Topic:          topic,
		Payload:        payload,
		Traceparent:    tp,
		IdempotencyKey: in.IdempotencyKey,
		SchemaVersion:  1,
		CreatedAt:      now,
	}
	return e.deps.Outbox.InsertStandalone(ctx, row)
}

// EmitDispute satisfies dispatcher.OutboxEmitter for the cross-aggregate
// dispute event family. Loads the Dispute aggregate, marshals one of
// the 4 dispute event protos, and INSERTs into chora_payments.outbox_events.
func (e *Emitter) EmitDispute(ctx context.Context, in dispatcher.EmitDisputeInput) error {
	now := e.deps.Now()
	eventID := newEventID()
	tp := emitTraceparent(ctx)

	env := buildEnvelope(in.TenantID, eventID, in.IdempotencyKey, tp, now)
	payload, topic, err := loadAndMarshalDispute(ctx, e.deps, env, in.DisputeID, in.EventType)
	if err != nil {
		return err
	}

	row := pg.OutboxRow{
		EventID:        eventID,
		AggregateType:  "dispute",
		AggregateID:    in.DisputeID,
		TenantID:       in.TenantID,
		Topic:          topic,
		Payload:        payload,
		Traceparent:    tp,
		IdempotencyKey: in.IdempotencyKey,
		SchemaVersion:  1,
		CreatedAt:      now,
	}
	return e.deps.Outbox.InsertStandalone(ctx, row)
}

// emitTraceparent derives the W3C traceparent an outbox event is published
// under. chora-payments is the trace ROOT for Stripe webhook-originated events
// — Stripe sends no traceparent — so when the inbound emit context carries
// none a fresh W3C root is minted; when the webhook handler ran under an
// OTel-propagating middleware the inbound value is continued. A non-empty
// traceparent is MANDATORY: it flows onto both the embedded envelope proto and
// the outbox row, and the row value becomes the Pub/Sub `traceparent`
// attribute. Downstream subscribers that re-derive their own outbox events
// (chora-tenancy's egg-provision path) reject an empty value with
// `envelope: traceparent is required`.
func emitTraceparent(ctx context.Context) string {
	return tracing.EnsureTraceparent(tracing.TraceparentFromContext(ctx))
}

func newEventID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// Compile-time check that Emitter satisfies dispatcher.OutboxEmitter.
var _ dispatcher.OutboxEmitter = (*Emitter)(nil)
