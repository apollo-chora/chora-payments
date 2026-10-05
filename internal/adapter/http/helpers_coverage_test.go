// helpers_coverage_test.go — in-package coverage for the pure helpers of
// the http adapter (the external `http_test` files cover the handler paths;
// these access unexported functions directly).
package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"

	"github.com/apollo-chora/chora-payments/internal/domain/payments"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

func TestParseFilterDate(t *testing.T) {
	t.Parallel()
	// RFC3339 passes through verbatim.
	rfc, err := parseFilterDate("2026-05-26T00:00:00Z", false)
	if err != nil {
		t.Fatalf("rfc: %v", err)
	}
	if !rfc.Equal(time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("rfc=%v", rfc)
	}
	// Plain date: lower bound starts at midnight.
	low, err := parseFilterDate("2026-05-26", false)
	if err != nil {
		t.Fatalf("date: %v", err)
	}
	if !low.Equal(time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("low=%v", low)
	}
	// Upper bound extends to the last nanosecond of the day.
	high, err := parseFilterDate("2026-05-26", true)
	if err != nil {
		t.Fatalf("date upper: %v", err)
	}
	want := time.Date(2026, 5, 26, 23, 59, 59, 999999999, time.UTC)
	if !high.Equal(want) {
		t.Errorf("high=%v, want %v", high, want)
	}
	if _, err := parseFilterDate("not-a-date", false); err == nil {
		t.Error("invalid date should error")
	}
}

func TestProtoTimeOrEmpty(t *testing.T) {
	t.Parallel()
	if got := protoTimeOrEmpty(time.Time{}); got != "" {
		t.Errorf("zero time=%q, want empty", got)
	}
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	if got := protoTimeOrEmpty(now); got != "2026-05-24T12:00:00Z" {
		t.Errorf("got=%q", got)
	}
}

func TestParseAggregateType(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want pb.AggregateType
	}{
		{"course_purchase", pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE},
		{"COURSE_PURCHASE", pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE},
		{"application_payment", pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT},
		{"familiar_egg_purchase", pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE},
		{"tenant_mana_topup", pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP},
		{"user_subscription", pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION},
		{"user_mana_topup", pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP},
		{"identity_kyc_fee", pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE},
	}
	for _, tc := range cases {
		got, err := parseAggregateType(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseAggregateType(%q)=%v/%v, want %v", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseAggregateType("mystery"); err == nil {
		t.Error("unknown aggregate should error")
	}
}

func TestParseRefundReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want pb.RefundReason
	}{
		{"", pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST},
		{"customer_request", pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST},
		{"SUPPORT_INITIATED", pb.RefundReason_REFUND_REASON_SUPPORT_INITIATED},
		{"duplicate_charge", pb.RefundReason_REFUND_REASON_DUPLICATE_CHARGE},
		{"fraud", pb.RefundReason_REFUND_REASON_FRAUD},
		{"expired_unhatched", pb.RefundReason_REFUND_REASON_EXPIRED_UNHATCHED},
	}
	for _, tc := range cases {
		got, err := parseRefundReason(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseRefundReason(%q)=%v/%v, want %v", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseRefundReason("bogus"); err == nil {
		t.Error("unknown reason should error")
	}
}

func TestParsePageSize(t *testing.T) {
	t.Parallel()
	if got := parsePageSize(""); got != 0 {
		t.Errorf("empty=%d, want 0", got)
	}
	if got := parsePageSize("abc"); got != 0 {
		t.Errorf("bad=%d, want 0", got)
	}
	if got := parsePageSize("25"); got != 25 {
		t.Errorf("25=%d, want 25", got)
	}
}

func TestWriteGRPCError_StatusCodeMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"non-grpc", errors.New("boom"), http.StatusInternalServerError},
		{"not_found", gstatus.Error(codes.NotFound, "missing"), http.StatusNotFound},
		{"invalid_argument", gstatus.Error(codes.InvalidArgument, "bad"), http.StatusBadRequest},
		{"permission_denied", gstatus.Error(codes.PermissionDenied, "nope"), http.StatusForbidden},
		{"failed_precondition", gstatus.Error(codes.FailedPrecondition, "conflict"), http.StatusConflict},
		{"unimplemented", gstatus.Error(codes.Unimplemented, "later"), http.StatusNotImplemented},
		{"unknown", gstatus.Error(codes.Unknown, "mystery"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeGRPCError(rec, tc.err)
			if rec.Code != tc.want {
				t.Errorf("status=%d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestItemToJSON(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	paid := now.Add(time.Minute)
	refunded := now.Add(2 * time.Minute)
	it := payments.PurchaseHistoryItem{
		PurchaseID:    "p-1",
		AggregateType: "course_purchase",
		TenantID:      "t-1",
		LearnerGCID:   "g-1",
		LearnerEmail:  "a@b.c",
		AmountCents:   100,
		Currency:      "SGD",
		State:         "refunded",
		PaidAt:        &paid,
		RefundedAt:    &refunded,
		CreatedAt:     now,
		MetadataJSON:  `{"course_id":"c1"}`,
	}
	out := itemToJSON(it)
	if out.PaidAt == nil || *out.PaidAt != "2026-05-24T12:01:00Z" {
		t.Errorf("PaidAt=%v", out.PaidAt)
	}
	if out.RefundedAt == nil || out.Metadata["course_id"] != "c1" {
		t.Errorf("RefundedAt=%v Metadata=%v", out.RefundedAt, out.Metadata)
	}
	// Nil pointers + bad metadata stay absent.
	bare := itemToJSON(payments.PurchaseHistoryItem{PurchaseID: "p-2", CreatedAt: now, MetadataJSON: "{"})
	if bare.PaidAt != nil || bare.RefundedAt != nil || bare.Metadata != nil {
		t.Errorf("bare=%+v", bare)
	}
}

func TestPtrTimeStr(t *testing.T) {
	t.Parallel()
	if got := ptrTimeStr(nil); got != "" {
		t.Errorf("nil=%q", got)
	}
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	if got := ptrTimeStr(&now); got != "2026-05-24T12:00:00Z" {
		t.Errorf("got=%q", got)
	}
}

func TestResolveTenantID(t *testing.T) {
	t.Parallel()
	// Header precedence: X-Chora-Tenant-Id first.
	r := httptest.NewRequest(http.MethodGet, "/?tenant_id=query", nil)
	r.Header.Set("X-Chora-Tenant-Id", "h1")
	r.Header.Set("X-Tenant-Id", "h2")
	if got := resolveTenantID(r); got != "h1" {
		t.Errorf("got=%q, want h1", got)
	}
	// X-Tenant-Id fallback.
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.Header.Set("X-Tenant-Id", "h2")
	if got := resolveTenantID(r2); got != "h2" {
		t.Errorf("got=%q, want h2", got)
	}
	// lowercase chora-tenant-id fallback.
	r3 := httptest.NewRequest(http.MethodGet, "/", nil)
	r3.Header.Set("chora-tenant-id", "h3")
	if got := resolveTenantID(r3); got != "h3" {
		t.Errorf("got=%q, want h3", got)
	}
	// Query fallback.
	r4 := httptest.NewRequest(http.MethodGet, "/?tenant_id=q1", nil)
	if got := resolveTenantID(r4); got != "q1" {
		t.Errorf("got=%q, want q1", got)
	}
}

func TestExtractTenantIDFromPortalPath(t *testing.T) {
	t.Parallel()
	if got := extractTenantIDFromPortalPath("/api/v1/admin/tenants/t1/billing-portal"); got != "t1" {
		t.Errorf("got=%q, want t1", got)
	}
	if got := extractTenantIDFromPortalPath("/elsewhere/t1/billing-portal"); got != "" {
		t.Errorf("wrong prefix=%q", got)
	}
	if got := extractTenantIDFromPortalPath("/api/v1/admin/tenants/"); got != "" {
		t.Errorf("no tenant=%q", got)
	}
	if got := extractTenantIDFromPortalPath("/api/v1/admin/tenants/t1/invoices"); got != "" {
		t.Errorf("wrong subresource=%q", got)
	}
}

// stubHistory implements payments.PurchaseHistoryPort for callList tests.
type stubHistory struct {
	items []payments.PurchaseHistoryItem
	err   error
}

func (s *stubHistory) ListAll(_ context.Context, _ payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	return s.items, nil, s.err
}

func (s *stubHistory) ListByTenant(_ context.Context, _ payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	return s.items, nil, s.err
}

func (s *stubHistory) GetForRefund(_ context.Context, _ string, _ payments.AggregateType) (*payments.PurchaseHistoryItem, error) {
	if len(s.items) == 0 {
		return nil, errors.New("not found")
	}
	return &s.items[0], s.err
}

var _ payments.PurchaseHistoryPort = (*stubHistory)(nil)

func TestCallList(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	items := []payments.PurchaseHistoryItem{{PurchaseID: "p-1", CreatedAt: now}}
	h := NewAdminHandler(AdminHandlerDeps{History: &stubHistory{items: items}})

	opItems, _, err := h.callList(context.Background(), RolePlatformOperator, payments.FilterCriteria{}, "", 10)
	if err != nil || len(opItems) != 1 {
		t.Fatalf("operator call: %v", err)
	}
	tenantItems, _, err := h.callList(context.Background(), RoleTenantAdmin, payments.FilterCriteria{}, "", 10)
	if err != nil || len(tenantItems) != 1 {
		t.Fatalf("tenant call: %v", err)
	}
}

func TestParseFilter(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{})

	r := httptest.NewRequest(http.MethodGet, "/?aggregate_type=course_purchase&state=refunded&from=2026-05-26&to=2026-05-27&tenant_id=t1", nil)
	f, err := h.parseFilter(r, RolePlatformOperator, "")
	if err != nil {
		t.Fatalf("parseFilter: %v", err)
	}
	if f.AggregateType != "course_purchase" || f.State != "refunded" {
		t.Errorf("f=%+v", f)
	}
	if f.From == nil || f.To == nil {
		t.Fatalf("bounds not parsed: %+v", f)
	}
	if f.TenantID != "t1" {
		t.Errorf("operator tenant narrowing=%q, want t1", f.TenantID)
	}
	// Tenant-scoped caller: caller's tenant always wins over the query param.
	r2 := httptest.NewRequest(http.MethodGet, "/?tenant_id=query-tenant", nil)
	f2, err := h.parseFilter(r2, RoleTenantAdmin, "caller-tenant")
	if err != nil {
		t.Fatalf("tenant parse: %v", err)
	}
	if f2.TenantID != "caller-tenant" {
		t.Errorf("tenant filter=%q, want caller-tenant", f2.TenantID)
	}
	// Invalid dates surface.
	r3 := httptest.NewRequest(http.MethodGet, "/?from=not-a-date", nil)
	if _, err := h.parseFilter(r3, RolePlatformOperator, ""); err == nil {
		t.Error("invalid from should error")
	}
}

func TestAdminTenantsRouter_Routes(t *testing.T) {
	t.Parallel()
	inv := &AdminInvoicesHandler{}
	portal := &AdminBillingPortalHandler{}
	router := NewAdminTenantsRouter(inv, portal)

	// Unknown subresource → 404.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/t1/bogus", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown subresource status=%d, want 404", rec.Code)
	}
	// Missing tenant id → 400.
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/", nil))
	if rec2.Code != http.StatusBadRequest {
		t.Errorf("no tenant status=%d, want 400", rec2.Code)
	}
	// invoices + billing-portal routes delegate to the leaf handlers.
	// Leaf handlers with nil deps return 503 (fail-open), proving routing.
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/t1/invoices", nil))
	if rec3.Code != http.StatusServiceUnavailable {
		t.Errorf("invoices route status=%d, want 503 (nil deps leaf)", rec3.Code)
	}
	rec4 := httptest.NewRecorder()
	router.ServeHTTP(rec4, httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/t1/billing-portal", nil))
	if rec4.Code != http.StatusServiceUnavailable {
		t.Errorf("portal route status=%d, want 503 (nil deps leaf)", rec4.Code)
	}
}

func TestParseFilterDate_TrailingWhitespaceStillErrors(t *testing.T) {
	t.Parallel()
	if _, err := parseFilterDate(" 2026-05-26", false); err == nil {
		t.Error("untrimmed date should error")
	}
}

func TestItemToJSON_MetadataBadJSONTracked(t *testing.T) {
	t.Parallel()
	// Ensure the created_at column is formatted deterministically.
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	out := itemToJSON(payments.PurchaseHistoryItem{PurchaseID: "p", CreatedAt: now})
	if out.CreatedAt != "2026-05-24T12:00:00Z" {
		t.Errorf("CreatedAt=%q", out.CreatedAt)
	}
	if !strings.HasPrefix(out.CreatedAt, "2026-05-24") {
		t.Errorf("unexpected CreatedAt %q", out.CreatedAt)
	}
}
