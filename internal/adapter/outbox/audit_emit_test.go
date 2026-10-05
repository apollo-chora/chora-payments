// audit_emit_test.go — RED→GREEN tests for the 3 governance.audit.*
// emit helpers that the admin HTTP handler calls on every privileged
// op (list, refund, cross-tenant view).
package outbox

import (
	"context"
	"testing"
	"time"

	governancev1 "github.com/apollo-chora/chora-contracts/gen/go/chora/governance/v1"
	"google.golang.org/protobuf/proto"
)

// outboxSpy is a test-side OutboxWriter (the small interface the audit
// helpers depend on; smaller than the full *pg.OutboxRepo so we can stub
// it without spinning up a fake TxRunner).
type outboxSpy struct {
	rows []recordedOutbox
}

type recordedOutbox struct {
	topic        string
	tenantID     string
	aggregateID  string
	aggregateTyp string
	payload      []byte
}

func (s *outboxSpy) InsertAudit(_ context.Context, topic, tenantID, aggregateType, aggregateID string, payload []byte) error {
	s.rows = append(s.rows, recordedOutbox{
		topic:        topic,
		tenantID:     tenantID,
		aggregateID:  aggregateID,
		aggregateTyp: aggregateType,
		payload:      payload,
	})
	return nil
}

func TestEmitTenantAdminViewedPayments_WritesCanonicalTopic(t *testing.T) {
	t.Parallel()
	spy := &outboxSpy{}
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	err := EmitTenantAdminViewedPayments(
		context.Background(), spy,
		"01970000-0000-7000-8000-000000000001",
		"01970000-0000-7000-a000-000000000002",
		"TENANT_ADMIN",
		PurchaseFilter{AggregateType: "course_purchase", State: "captured"},
		now,
	)
	if err != nil {
		t.Fatalf("EmitTenantAdminViewedPayments: %v", err)
	}
	if len(spy.rows) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(spy.rows))
	}
	if got, want := spy.rows[0].topic, "chora.governance.audit.tenant_admin_viewed_payments.v1"; got != want {
		t.Errorf("topic=%q want %q", got, want)
	}

	var ev governancev1.TenantAdminViewedPayments
	if err := proto.Unmarshal(spy.rows[0].payload, &ev); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if ev.GetActorRole() != "TENANT_ADMIN" {
		t.Errorf("actor_role=%q want TENANT_ADMIN", ev.GetActorRole())
	}
	if ev.GetFilterContext().GetAggregateType() != "course_purchase" {
		t.Errorf("filter_context.aggregate_type lost")
	}
}

func TestEmitCrossTenantPaymentsViewed_CarriesTenantIDsInView(t *testing.T) {
	t.Parallel()
	spy := &outboxSpy{}
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	tenantIDs := []string{
		"01970000-0000-7000-8000-000000000001",
		"01970000-0000-7000-8000-000000000099",
	}
	err := EmitCrossTenantPaymentsViewed(
		context.Background(), spy,
		"01970000-0000-7000-a000-000000000002",
		"PLATFORM_OPERATOR",
		PurchaseFilter{},
		tenantIDs,
		now,
	)
	if err != nil {
		t.Fatalf("EmitCrossTenantPaymentsViewed: %v", err)
	}
	if len(spy.rows) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(spy.rows))
	}
	if got, want := spy.rows[0].topic, "chora.governance.audit.cross_tenant_payments_viewed.v1"; got != want {
		t.Errorf("topic=%q want %q", got, want)
	}
	if spy.rows[0].tenantID != "platform" {
		t.Errorf("tenant_id=%q want %q (cross-tenant scope = platform)", spy.rows[0].tenantID, "platform")
	}

	var ev governancev1.CrossTenantPaymentsViewed
	if err := proto.Unmarshal(spy.rows[0].payload, &ev); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := ev.GetTenantIdsInView(); len(got) != 2 {
		t.Errorf("tenant_ids_in_view len=%d want 2", len(got))
	}
}

func TestEmitRefundIssued_CarriesAggregateAndAmount(t *testing.T) {
	t.Parallel()
	spy := &outboxSpy{}
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	err := EmitRefundIssued(
		context.Background(), spy,
		"01970000-0000-7000-8000-000000000001",
		"01970000-0000-7000-a000-000000000002",
		"TENANT_ADMIN",
		"01970000-0000-7000-b000-000000000003",
		"course_purchase",
		7900, "sgd",
		"customer requested refund",
		"re_test_abc",
		now,
	)
	if err != nil {
		t.Fatalf("EmitRefundIssued: %v", err)
	}
	if len(spy.rows) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(spy.rows))
	}
	if got, want := spy.rows[0].topic, "chora.governance.audit.refund_issued.v1"; got != want {
		t.Errorf("topic=%q want %q", got, want)
	}

	var ev governancev1.RefundIssued
	if err := proto.Unmarshal(spy.rows[0].payload, &ev); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if ev.GetAmountCents() != 7900 {
		t.Errorf("amount_cents=%d want 7900", ev.GetAmountCents())
	}
	if ev.GetStripeRefundId() != "re_test_abc" {
		t.Errorf("stripe_refund_id=%q want re_test_abc", ev.GetStripeRefundId())
	}
}
