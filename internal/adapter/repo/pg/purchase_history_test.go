// purchase_history_test.go — RED→GREEN tests for the cross-aggregate
// UNION ALL projection that backs the H+ Transaction History admin
// surface (openapi/payments-admin.yaml).
package pg

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/adapter/repo/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/payments"
)

func TestPurchaseHistoryRepo_NilTx_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	repo := NewPurchaseHistoryRepo(nil, nil)
	_, _, err := repo.ListByTenant(
		withTenant(context.Background(), testTenantID),
		payments.FilterCriteria{}, "", 0,
	)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestPurchaseHistoryRepo_ListByTenant_AppliesRLS_AndUnionsAcross7Tables(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.nextN = 0
	q.rowsScan = func(_ int, _ ...any) error { return nil }
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	_, _, err := repo.ListByTenant(
		withTenant(context.Background(), testTenantID),
		payments.FilterCriteria{}, "", 20,
	)
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)

	if len(q.queries) != 1 {
		t.Fatalf("expected 1 Query call, got %d", len(q.queries))
	}
	sql := q.queries[0].sql
	for _, tbl := range []string{
		"course_purchases", "application_payments", "familiar_egg_purchases",
		"tenant_mana_topups", "user_subscriptions", "user_mana_topups",
		"identity_kyc_fees",
	} {
		if !strings.Contains(sql, tbl) {
			t.Errorf("UNION ALL sql missing source table %q; sql=%s", tbl, sql)
		}
	}
	if strings.Count(sql, "UNION ALL") < 6 {
		t.Errorf("expected >=6 UNION ALL clauses across 7 tables, got %d; sql=%s",
			strings.Count(sql, "UNION ALL"), sql)
	}
	// Must reference the normalised state CASE mapping.
	if !strings.Contains(sql, "CASE state") && !strings.Contains(sql, "CASE WHEN state") {
		t.Errorf("sql missing CASE-based state normalisation: %s", sql)
	}
}

func TestPurchaseHistoryRepo_ListAll_BypassesRLS_WhenBypassFlagSet(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowsScan = func(_ int, _ ...any) error { return nil }
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	ctx := rls.WithRLSBypass(context.Background())
	_, _, err := repo.ListAll(ctx, payments.FilterCriteria{}, "", 20)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	// No SET LOCAL chora.tenant_id should have been emitted.
	for _, e := range q.execs {
		if strings.HasPrefix(e.sql, "SET LOCAL chora.tenant_id") {
			t.Errorf("RLS bypass requested but SET LOCAL chora.tenant_id was emitted: %q", e.sql)
		}
	}
}

func TestPurchaseHistoryRepo_ListAll_WithTenantFilter_AddsWhereTenantId(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowsScan = func(_ int, _ ...any) error { return nil }
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	ctx := rls.WithRLSBypass(context.Background())
	filter := payments.FilterCriteria{TenantID: testTenantID}
	_, _, err := repo.ListAll(ctx, filter, "", 20)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(q.queries) != 1 {
		t.Fatalf("expected 1 Query, got %d", len(q.queries))
	}
	if !strings.Contains(q.queries[0].sql, "tenant_id") {
		t.Errorf("ListAll(tenant_id=…) sql missing tenant_id predicate: %q",
			q.queries[0].sql)
	}
}

func TestPurchaseHistoryRepo_PageSize_NormalisedAndCappedAt100(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowsScan = func(_ int, _ ...any) error { return nil }
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	cases := []struct {
		in   int
		want int
	}{
		{in: 0, want: payments.PageSizeDefault},
		{in: -5, want: payments.PageSizeDefault},
		{in: 50, want: 50},
		{in: 999, want: payments.PageSizeMax},
	}
	for _, c := range cases {
		q.queries = nil
		q.execs = nil
		ctx := withTenant(context.Background(), testTenantID)
		_, _, err := repo.ListByTenant(ctx, payments.FilterCriteria{}, "", c.in)
		if err != nil {
			t.Fatalf("ListByTenant pageSize=%d: %v", c.in, err)
		}
		// LIMIT is always the last query arg in our SQL convention.
		if len(q.queries[0].args) == 0 {
			t.Fatalf("expected >=1 query arg, got 0")
		}
		gotLimit := q.queries[0].args[len(q.queries[0].args)-1]
		if gotLimit != c.want+1 {
			// We always SELECT pageSize+1 to detect has_more without
			// a second round-trip; assert on the +1.
			t.Errorf("pageSize=%d → LIMIT %v, want %d (pageSize+1)",
				c.in, gotLimit, c.want+1)
		}
	}
}

func TestPurchaseHistoryRepo_GetForRefund_ReturnsRowFromRequestedAggregate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	q := newStubQuerier()
	q.nextN = 1
	q.rowsScan = func(_ int, dest ...any) error {
		// state_normalised is what the SQL CASE returns server-side.
		return scanPurchaseHistoryRow(dest, testPurchaseID, "course_purchase",
			testTenantID, testLearnerGCID, 99900, "sgd",
			"captured", "cs_test_abc", now, nil, now, `{"course_id":"x"}`)
	}
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	got, err := repo.GetForRefund(
		withTenant(context.Background(), testTenantID),
		testPurchaseID, payments.AggregateCoursePurchase,
	)
	if err != nil {
		t.Fatalf("GetForRefund: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want PurchaseHistoryItem")
	}
	if got.State != payments.StateCaptured {
		t.Errorf("got.State=%v want captured", got.State)
	}
}

func TestPurchaseHistoryRepo_GetForRefund_ReturnsErrAlreadyRefunded_WhenRowIsRefunded(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	q := newStubQuerier()
	q.nextN = 1
	q.rowsScan = func(_ int, dest ...any) error {
		return scanPurchaseHistoryRow(dest, testPurchaseID, "course_purchase",
			testTenantID, testLearnerGCID, 99900, "sgd",
			"refunded", "cs_test_abc", now, &now, now, `{}`)
	}
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	_, err := repo.GetForRefund(
		withTenant(context.Background(), testTenantID),
		testPurchaseID, payments.AggregateCoursePurchase,
	)
	if err != payments.ErrAlreadyRefunded {
		t.Errorf("err=%v want payments.ErrAlreadyRefunded", err)
	}
}

func TestPurchaseHistoryRepo_GetForRefund_ReturnsErrNotFound_WhenNoRow(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.nextN = 0
	q.rowsScan = func(_ int, _ ...any) error { return nil }
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	_, err := repo.GetForRefund(
		withTenant(context.Background(), testTenantID),
		testPurchaseID, payments.AggregateCoursePurchase,
	)
	if err != payments.ErrPurchaseNotFound {
		t.Errorf("err=%v want payments.ErrPurchaseNotFound", err)
	}
}

func TestPurchaseHistoryRepo_Cursor_Roundtrip(t *testing.T) {
	t.Parallel()
	// 21 rows for pageSize=20 → nextCursor must be non-nil.
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	q := newStubQuerier()
	q.nextN = 21
	q.rowsScan = func(idx int, dest ...any) error {
		// vary purchase_id so cursor encodes a stable last-row tuple
		pid := fmt.Sprintf("01970000-0000-7000-b000-%012d", idx)
		return scanPurchaseHistoryRow(dest, pid, "course_purchase",
			testTenantID, testLearnerGCID, 99900, "sgd",
			"payment_captured", "cs_test_abc", now.Add(-time.Duration(idx)*time.Minute),
			nil, now, `{}`)
	}
	repo := NewPurchaseHistoryRepo(newStubTxRunner(q), NewStripeCustomerRepo(newStubTxRunner(newStubQuerier())))

	items, nextCursor, err := repo.ListByTenant(
		withTenant(context.Background(), testTenantID),
		payments.FilterCriteria{}, "", 20,
	)
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(items) != 20 {
		t.Errorf("len(items)=%d want 20 (pageSize)", len(items))
	}
	if nextCursor == nil {
		t.Fatal("nextCursor is nil; want non-nil because we had >pageSize rows")
	}
	// nextCursor must decode to a JSON object with paid_at + purchase_id.
	bz, err := base64.URLEncoding.DecodeString(*nextCursor)
	if err != nil {
		t.Fatalf("cursor not base64: %v", err)
	}
	var dec map[string]any
	if err := json.Unmarshal(bz, &dec); err != nil {
		t.Fatalf("cursor not JSON: %v", err)
	}
	if dec["purchase_id"] == nil {
		t.Errorf("cursor missing purchase_id; got %v", dec)
	}
}

// scanPurchaseHistoryRow seeds a fake UNION-ALL row. 13 scan targets per
// the SELECT shape:
//
//	purchase_id, aggregate_type, tenant_id, learner_gcid,
//	amount_cents, currency, raw_state, stripe_session_id,
//	created_at, refunded_at, paid_at, metadata_json
//
// Wait — that's 12; we also pull learner_email LEFT JOIN. Mirror the
// real adapter shape: 13 cols.
func scanPurchaseHistoryRow(dest []any,
	purchaseID, aggregateType, tenantID, learnerGCID string,
	amountCents int64, currency, rawState, sessionID string,
	createdAt time.Time, refundedAt *time.Time, paidAt time.Time, metadataJSON string,
) error {
	if len(dest) != 13 {
		return fmt.Errorf("scan: dest len=%d, want 13", len(dest))
	}
	*(dest[0].(*string)) = purchaseID
	*(dest[1].(*string)) = aggregateType
	*(dest[2].(*string)) = tenantID
	*(dest[3].(*string)) = learnerGCID
	// dest[4] = learner_email (nullable LEFT JOIN; leave nil)
	*(dest[5].(*int64)) = amountCents
	*(dest[6].(*string)) = currency
	*(dest[7].(*string)) = rawState
	*(dest[8].(*string)) = sessionID
	// dest[9] = paid_at nullable
	if !paidAt.IsZero() {
		t := paidAt
		*(dest[9].(**time.Time)) = &t
	}
	// dest[10] = refunded_at nullable
	if refundedAt != nil {
		*(dest[10].(**time.Time)) = refundedAt
	}
	*(dest[11].(*time.Time)) = createdAt
	// dest[12] = metadata_json (nullable)
	if metadataJSON != "" {
		s := metadataJSON
		*(dest[12].(**string)) = &s
	}
	return nil
}
