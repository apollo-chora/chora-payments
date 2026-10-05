// audit_emit.go — small helpers that publish the three H+ Transaction
// History governance audit events (per ADR-165 + IMDA D1 accountability):
//
//	chora.governance.audit.tenant_admin_viewed_payments.v1
//	chora.governance.audit.cross_tenant_payments_viewed.v1
//	chora.governance.audit.refund_issued.v1
//
// These are emitted by the chora-payments admin HTTP handler on every
// privileged action (list, cross-tenant view, refund). The chora-
// governance subscriber (Agent A4 in this wave) consumes them and
// persists into chora_governance.governance_audit_events for the O+
// dashboard.
//
// Wire shape: each helper builds the matching protobuf message from
// chora-contracts/gen/go/chora/governance/v1 + writes it into the
// existing chora_payments.outbox_events table via an AuditOutbox port.
// The outbox dispatcher (dispatcher.go) drains rows to the event bus
// transactionally just like the per-aggregate event topics.
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	governancev1 "github.com/apollo-chora/chora-contracts/gen/go/chora/governance/v1"

	"github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
)

// AuditOutbox is the small write port the audit helpers depend on.
// Production: backed by *pg.OutboxRepo via the auditOutboxAdapter below.
// Tests: backed by an in-memory spy.
//
// The interface intentionally accepts a pre-serialised payload + the
// 4 envelope-adjacent fields (topic, tenant_id, aggregate_type,
// aggregate_id) so the helpers don't need to know how the underlying
// outbox table is laid out.
type AuditOutbox interface {
	InsertAudit(ctx context.Context, topic, tenantID, aggregateType, aggregateID string, payload []byte) error
}

// PurchaseFilter is the shared filter-context echo used in the audit
// payloads. Mirrors the OpenAPI query params + the proto
// PurchaseFilterContext shape, kept as a plain Go struct so HTTP
// handlers don't pull in proto types at the call site.
type PurchaseFilter struct {
	AggregateType string
	State         string
	From          *time.Time
	To            *time.Time
}

// toProto converts a PurchaseFilter to the proto envelope-companion
// representation expected by the audit messages.
func (f PurchaseFilter) toProto() *governancev1.PurchaseFilterContext {
	pf := &governancev1.PurchaseFilterContext{
		AggregateType: f.AggregateType,
		State:         f.State,
	}
	if f.From != nil {
		pf.From = timestamppb.New(f.From.UTC())
	}
	if f.To != nil {
		pf.To = timestamppb.New(f.To.UTC())
	}
	return pf
}

// EmitTenantAdminViewedPayments publishes
// chora.governance.audit.tenant_admin_viewed_payments.v1 on every
// successful GET /api/v1/admin/payments/purchases call by a TENANT_ADMIN
// / OWNER / AUDITOR.
//
// tenantID is the actor's own tenant scope (RLS-pinned for these roles).
// actorRole is the role the JWT exercised — one of TENANT_ADMIN | OWNER |
// AUDITOR. occurredAt is the server-time of the API call (UTC).
func EmitTenantAdminViewedPayments(
	ctx context.Context,
	out AuditOutbox,
	tenantID, actorGCID, actorRole string,
	filter PurchaseFilter,
	occurredAt time.Time,
) error {
	if out == nil {
		return fmt.Errorf("outbox: AuditOutbox is nil")
	}
	eventID := newAuditEventID()
	msg := &governancev1.TenantAdminViewedPayments{
		EventId:       eventID,
		TenantId:      tenantID,
		ActorGcid:     actorGCID,
		ActorRole:     actorRole,
		OccurredAt:    timestamppb.New(occurredAt.UTC()),
		FilterContext: filter.toProto(),
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal TenantAdminViewedPayments: %w", err)
	}
	return out.InsertAudit(ctx,
		"chora.governance.audit.tenant_admin_viewed_payments.v1",
		tenantID, "audit", eventID, payload,
	)
}

// EmitCrossTenantPaymentsViewed publishes
// chora.governance.audit.cross_tenant_payments_viewed.v1 — emitted in
// addition to TenantAdminViewedPayments when the actor holds the
// PLATFORM_OPERATOR role and the response touched rows from more than
// one tenant (or omitted the tenant_id filter).
//
// tenantIDsInView is the deduped set the response actually returned —
// load-bearing IMDA D1 evidence for cross-tenant access.
//
// NB the outbox row's tenant_id slot carries the sentinel "platform"
// (per envelope.proto convention for cross-tenant platform-level events).
func EmitCrossTenantPaymentsViewed(
	ctx context.Context,
	out AuditOutbox,
	actorGCID, actorRole string,
	filter PurchaseFilter,
	tenantIDsInView []string,
	occurredAt time.Time,
) error {
	if out == nil {
		return fmt.Errorf("outbox: AuditOutbox is nil")
	}
	eventID := newAuditEventID()
	msg := &governancev1.CrossTenantPaymentsViewed{
		EventId:         eventID,
		ActorGcid:       actorGCID,
		ActorRole:       actorRole,
		OccurredAt:      timestamppb.New(occurredAt.UTC()),
		FilterContext:   filter.toProto(),
		TenantIdsInView: tenantIDsInView,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal CrossTenantPaymentsViewed: %w", err)
	}
	return out.InsertAudit(ctx,
		"chora.governance.audit.cross_tenant_payments_viewed.v1",
		"platform", "audit", eventID, payload,
	)
}

// EmitRefundIssued publishes chora.governance.audit.refund_issued.v1 on
// every successful POST /api/v1/admin/payments/{purchase_id}/refund call.
// Captures the full audit fingerprint: actor + amount + currency + reason
// + Stripe refund id for cross-system reconciliation.
func EmitRefundIssued(
	ctx context.Context,
	out AuditOutbox,
	tenantID, actorGCID, actorRole string,
	purchaseID, aggregateType string,
	amountCents int64, currency, reason, stripeRefundID string,
	occurredAt time.Time,
) error {
	if out == nil {
		return fmt.Errorf("outbox: AuditOutbox is nil")
	}
	eventID := newAuditEventID()
	msg := &governancev1.RefundIssued{
		EventId:        eventID,
		TenantId:       tenantID,
		ActorGcid:      actorGCID,
		ActorRole:      actorRole,
		OccurredAt:     timestamppb.New(occurredAt.UTC()),
		PurchaseId:     purchaseID,
		AggregateType:  aggregateType,
		AmountCents:    amountCents,
		Currency:       currency,
		Reason:         reason,
		StripeRefundId: stripeRefundID,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal RefundIssued: %w", err)
	}
	return out.InsertAudit(ctx,
		"chora.governance.audit.refund_issued.v1",
		tenantID, "audit", purchaseID, payload,
	)
}

// newAuditEventID generates a fresh UUIDv7, matching emitter.go.
func newAuditEventID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// -----------------------------------------------------------------------------
// AuditOutboxAdapter — production wiring of AuditOutbox onto *pg.OutboxRepo.
// -----------------------------------------------------------------------------

// AuditOutboxAdapter adapts *pg.OutboxRepo to the small AuditOutbox port.
// Builds a free-standing outbox row + delegates to InsertStandalone (no
// in-flight transaction context required at the call site — the H+
// admin handlers fire-and-forget the audit emit after the response is
// already constructed).
type AuditOutboxAdapter struct {
	Outbox *pg.OutboxRepo
	Now    func() time.Time
}

// InsertAudit satisfies AuditOutbox. Idempotency_key is derived from the
// audit event id so repeated emits dedupe at the outbox layer.
func (a *AuditOutboxAdapter) InsertAudit(ctx context.Context, topic, tenantID, aggregateType, aggregateID string, payload []byte) error {
	if a == nil || a.Outbox == nil {
		return fmt.Errorf("outbox: AuditOutboxAdapter has no Outbox")
	}
	now := time.Now().UTC()
	if a.Now != nil {
		now = a.Now()
	}
	idempotencyKey := fmt.Sprintf("audit:%s", aggregateID)
	row := pg.OutboxRow{
		EventID:        aggregateID,
		AggregateType:  aggregateType,
		AggregateID:    aggregateID,
		TenantID:       tenantID,
		Topic:          topic,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
		SchemaVersion:  1,
		CreatedAt:      now,
	}
	return a.Outbox.InsertStandalone(ctx, row)
}
