// routing_coverage_test.go — ServeHTTP routing + H+ list/legacy handler
// coverage via the admin handler's public surface.
package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/payments"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

func testAdmin() *AdminHandler {
	return NewAdminHandler(AdminHandlerDeps{Now: time.Now})
}

// stubGetPaymentService serves GetPurchase only (legacy paths).
type stubGetPaymentService struct {
	getResp *pb.GetPurchaseResponse
	getErr  error
}

func (s *stubGetPaymentService) GetPurchase(_ context.Context, _ *pb.GetPurchaseRequest) (*pb.GetPurchaseResponse, error) {
	return s.getResp, s.getErr
}

func (s *stubGetPaymentService) RefundPurchase(_ context.Context, _ *pb.RefundPurchaseRequest) (*pb.RefundPurchaseResponse, error) {
	return nil, nil
}

func TestServeHTTP_Routing(t *testing.T) {
	t.Parallel()
	h := testAdmin()

	// Missing role → 403.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/payments/p-1", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("no role status=%d, want 403", rec.Code)
	}

	// Unknown path → 404.
	req := httptest.NewRequest(http.MethodGet, "/nonsense", nil)
	req.Header.Set("X-Chora-Role", "training-admin")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusNotFound {
		t.Errorf("unknown path status=%d", rec2.Code)
	}

	// Legacy root → 404 purchase_id required.
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/payments/", nil)
	req3.Header.Set("X-Chora-Role", "training-admin")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusNotFound {
		t.Errorf("root status=%d", rec3.Code)
	}

	// Unknown admin path → 404.
	req4 := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/bogus", nil)
	req4.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec4 := httptest.NewRecorder()
	h.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusNotFound {
		t.Errorf("unknown admin path status=%d", rec4.Code)
	}

	// Admin refund with empty purchase id → 400.
	req5 := httptest.NewRequest(http.MethodPost, "/api/v1/admin/payments//refund", strings.NewReader(`{}`))
	req5.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec5 := httptest.NewRecorder()
	h.ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusBadRequest {
		t.Errorf("empty admin refund status=%d", rec5.Code)
	}
}

func TestServeHTTP_LegacyGetHappyPath(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{
		Payments: &stubGetPaymentService{getResp: &pb.GetPurchaseResponse{
			Purchase: &pb.Purchase{PurchaseId: "p-1", State: pb.PurchaseState_PURCHASE_STATE_PAYMENT_CAPTURED},
		}},
		Now: time.Now,
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/payments/p-1?aggregate_type=course_purchase", nil)
	req.Header.Set("X-Chora-Role", "training-admin")
	req.Header.Set("X-Chora-Tenant-Id", "t-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"purchase_id":"p-1"`) {
		t.Errorf("body=%s", rec.Body.String())
	}
}

func TestServeHTTP_LegacyGetErrors(t *testing.T) {
	t.Parallel()
	h := testAdmin()
	// Wrong method.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments/p-1?aggregate_type=course_purchase", nil)
	req.Header.Set("X-Chora-Role", "training-admin")
	req.Header.Set("X-Chora-Tenant-Id", "t-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("method status=%d", rec.Code)
	}
	// Missing tenant.
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/payments/p-1?aggregate_type=course_purchase", nil)
	req2.Header.Set("X-Chora-Role", "training-admin")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status=%d", rec2.Code)
	}
	// Bad aggregate type.
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/payments/p-1?aggregate_type=mystery", nil)
	req3.Header.Set("X-Chora-Role", "training-admin")
	req3.Header.Set("X-Chora-Tenant-Id", "t-1")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusBadRequest {
		t.Errorf("bad aggregate status=%d", rec3.Code)
	}
}

func TestHandleListPurchases_WithCursor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	next := "next-page"
	cur := &next
	h2 := NewAdminHandler(AdminHandlerDeps{
		History: &cursorHistory{
			stubHistory: &stubHistory{items: []payments.PurchaseHistoryItem{{PurchaseID: "p-2", CreatedAt: now}}},
			next:        cur,
		},
		Now: func() time.Time { return now },
	})
	for _, tc := range []struct {
		name string
		role string
	}{
		{"operator", "PLATFORM_OPERATOR"},
		{"tenant", "TENANT_ADMIN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/purchases?page_token=abc&page_size=10", nil)
			req.Header.Set("X-Chora-Role", tc.role)
			req.Header.Set("X-Chora-Tenant-Id", "t-1")
			rec := httptest.NewRecorder()
			h2.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"next_page_token":"next-page"`) || !strings.Contains(rec.Body.String(), `"has_more":true`) {
				t.Errorf("body=%s", rec.Body.String())
			}
		})
	}
}

func TestHandleListPurchases_Errors(t *testing.T) {
	t.Parallel()
	h := testAdmin()
	// Wrong method.
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec := httptest.NewRecorder()
	h.handleListPurchases(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("method status=%d", rec.Code)
	}
	// Not wired.
	req2 := httptest.NewRequest(http.MethodGet, "/x", nil)
	req2.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec2 := httptest.NewRecorder()
	h.handleListPurchases(rec2, req2)
	if rec2.Code != http.StatusNotImplemented {
		t.Errorf("not wired status=%d", rec2.Code)
	}
	// List error propagates.
	h2 := NewAdminHandler(AdminHandlerDeps{
		History: &errHistory{},
		Now:     time.Now,
	})
	req3 := httptest.NewRequest(http.MethodGet, "/x", nil)
	req3.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec3 := httptest.NewRecorder()
	h2.handleListPurchases(rec3, req3)
	if rec3.Code != http.StatusInternalServerError {
		t.Errorf("list err status=%d", rec3.Code)
	}
}

func TestOutboxFilter(t *testing.T) {
	t.Parallel()
	from := time.Now()
	f := payments.FilterCriteria{AggregateType: "course_purchase", State: "refunded", From: &from}
	of := outboxFilter(f)
	if of.AggregateType != "course_purchase" || of.State != "refunded" || of.From != &from {
		t.Errorf("of=%+v", of)
	}
}

func TestNewAdminHandler_DefaultsNow(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{})
	if h.deps.Now == nil {
		t.Fatal("Now not defaulted")
	}
}

// cursorHistory decorates a history stub with an explicit next cursor.
type cursorHistory struct {
	*stubHistory
	next *string
}

func (c *cursorHistory) ListAll(ctx context.Context, f payments.FilterCriteria, cursor string, pageSize int) ([]payments.PurchaseHistoryItem, *string, error) {
	items, _, err := c.stubHistory.ListAll(ctx, f, cursor, pageSize)
	return items, c.next, err
}

func (c *cursorHistory) ListByTenant(ctx context.Context, f payments.FilterCriteria, cursor string, pageSize int) ([]payments.PurchaseHistoryItem, *string, error) {
	items, _, err := c.stubHistory.ListByTenant(ctx, f, cursor, pageSize)
	return items, c.next, err
}

// errHistory fails every list call.
type errHistory struct{}

func (e *errHistory) ListAll(_ context.Context, _ payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	return nil, nil, http.ErrServerClosed
}

func (e *errHistory) ListByTenant(_ context.Context, _ payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	return nil, nil, http.ErrServerClosed
}

func (e *errHistory) GetForRefund(_ context.Context, _ string, _ payments.AggregateType) (*payments.PurchaseHistoryItem, error) {
	return nil, nil
}
