// admin_handlers_coverage_test.go — in-package coverage for the H+
// Transaction History admin handlers (refund + export + SSE stream) and
// their helper paths, via controllable stubs.
package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-payments/internal/domain/payments"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// stubAdminPayments implements PaymentService for the post-preflight refund
// path.
type stubAdminPayments struct {
	refundResp *pb.RefundPurchaseResponse
	refundErr  error
}

func (s *stubAdminPayments) GetPurchase(_ context.Context, _ *pb.GetPurchaseRequest) (*pb.GetPurchaseResponse, error) {
	return nil, nil
}

func (s *stubAdminPayments) RefundPurchase(_ context.Context, _ *pb.RefundPurchaseRequest) (*pb.RefundPurchaseResponse, error) {
	return s.refundResp, s.refundErr
}

type refundableHistory struct {
	row *payments.PurchaseHistoryItem
	err error
}

func (s *refundableHistory) ListAll(_ context.Context, _ payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	return nil, nil, nil
}

func (s *refundableHistory) ListByTenant(_ context.Context, _ payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	return nil, nil, nil
}

func (s *refundableHistory) GetForRefund(_ context.Context, _ string, _ payments.AggregateType) (*payments.PurchaseHistoryItem, error) {
	return s.row, s.err
}

func TestHandleAdminRefund_HappyPath(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	h := NewAdminHandler(AdminHandlerDeps{
		Payments: &stubAdminPayments{refundResp: &pb.RefundPurchaseResponse{
			Purchase: &pb.Purchase{
				PurchaseId:          "p-1",
				StripeRefundId:      "re_1",
				AmountCentsRefunded: 1000,
				Currency:            "SGD",
				RefundedAt:          pbTimestamp(now),
			},
		}},
		History: &refundableHistory{row: &payments.PurchaseHistoryItem{PurchaseID: "p-1", TenantID: "t-1"}},
		Now:     func() time.Time { return now },
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/payments/p-1/refund", strings.NewReader(`{"aggregate_type":"course_purchase","reason":"customer requested"}`))
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec := httptest.NewRecorder()
	h.handleAdminRefund(rec, req, "p-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"refund_id":"re_1"`) {
		t.Errorf("body=%s", rec.Body.String())
	}
}

func TestHandleAdminRefund_Errors(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	// Wrong method.
	h := NewAdminHandler(AdminHandlerDeps{Now: func() time.Time { return now }})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.handleAdminRefund(rec, req, "p-1")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("method status=%d", rec.Code)
	}

	// Auditor cannot refund.
	h2 := NewAdminHandler(AdminHandlerDeps{Now: func() time.Time { return now }})
	req2 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{}`))
	req2.Header.Set("X-Chora-Role", "AUDITOR")
	rec2 := httptest.NewRecorder()
	h2.handleAdminRefund(rec2, req2, "p-1")
	if rec2.Code != http.StatusForbidden {
		t.Errorf("auditor status=%d", rec2.Code)
	}

	// Bad JSON body.
	h3 := NewAdminHandler(AdminHandlerDeps{Now: func() time.Time { return now }})
	req3 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{invalid`))
	req3.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec3 := httptest.NewRecorder()
	h3.handleAdminRefund(rec3, req3, "p-1")
	if rec3.Code != http.StatusBadRequest {
		t.Errorf("bad json status=%d", rec3.Code)
	}

	// Unknown aggregate type.
	h4 := NewAdminHandler(AdminHandlerDeps{Now: func() time.Time { return now }})
	req4 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"aggregate_type":"mystery"}`))
	req4.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec4 := httptest.NewRecorder()
	h4.handleAdminRefund(rec4, req4, "p-1")
	if rec4.Code != http.StatusBadRequest {
		t.Errorf("bad aggregate status=%d", rec4.Code)
	}

	// Missing tenant for tenant-scoped role.
	h5 := NewAdminHandler(AdminHandlerDeps{Now: func() time.Time { return now }})
	req5 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req5.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	rec5 := httptest.NewRecorder()
	h5.handleAdminRefund(rec5, req5, "p-1")
	if rec5.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status=%d", rec5.Code)
	}

	// Pre-flight 404.
	h6 := NewAdminHandler(AdminHandlerDeps{
		History: &refundableHistory{err: payments.ErrPurchaseNotFound},
		Now:     func() time.Time { return now },
	})
	req6 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req6.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec6 := httptest.NewRecorder()
	h6.handleAdminRefund(rec6, req6, "p-1")
	if rec6.Code != http.StatusNotFound {
		t.Errorf("404 preflight status=%d", rec6.Code)
	}

	// Pre-flight 409.
	h7 := NewAdminHandler(AdminHandlerDeps{
		History: &refundableHistory{err: payments.ErrAlreadyRefunded},
		Now:     func() time.Time { return now },
	})
	req7 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req7.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec7 := httptest.NewRecorder()
	h7.handleAdminRefund(rec7, req7, "p-1")
	if rec7.Code != http.StatusConflict {
		t.Errorf("409 preflight status=%d", rec7.Code)
	}

	// Pre-flight 500.
	h8 := NewAdminHandler(AdminHandlerDeps{
		History: &refundableHistory{err: io.ErrUnexpectedEOF},
		Now:     func() time.Time { return now },
	})
	req8 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req8.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec8 := httptest.NewRecorder()
	h8.handleAdminRefund(rec8, req8, "p-1")
	if rec8.Code != http.StatusInternalServerError {
		t.Errorf("500 preflight status=%d", rec8.Code)
	}
}

func TestHandleAdminRefund_RPCErrorPropagates(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{
		Payments: &stubAdminPayments{refundErr: gstatusErrorNotFound()},
		History:  &refundableHistory{row: &payments.PurchaseHistoryItem{PurchaseID: "p-1", TenantID: "t-1"}},
		Now:      time.Now,
	})
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec := httptest.NewRecorder()
	h.handleAdminRefund(rec, req, "p-1")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404 (grpc NotFound)", rec.Code)
	}
}

func TestHandleExportPurchases_CSVAndJSON(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	items := []payments.PurchaseHistoryItem{{
		PurchaseID: "p-1", AggregateType: "course_purchase", TenantID: "t-1",
		LearnerGCID: "g-1", AmountCents: 100, Currency: "SGD", State: "refunded",
		StripeSessionID: "cs_1", CreatedAt: now,
	}}
	deps := AdminHandlerDeps{
		History: &stubHistory{items: items},
		Now:     func() time.Time { return now },
	}

	for _, format := range []string{"csv", "json"} {
		t.Run("format_"+format, func(t *testing.T) {
			h := NewAdminHandler(deps)
			req := httptest.NewRequest(http.MethodGet, "/export?format="+format, nil)
			req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
			rec := httptest.NewRecorder()
			h.handleExportPurchases(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d", rec.Code)
			}
			if !strings.Contains(rec.Body.String(), "p-1") {
				t.Errorf("body missing purchase: %s", rec.Body.String())
			}
		})
	}
}

func TestHandleExportPurchases_Errors(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{Now: time.Now})
	// Wrong method.
	rec := httptest.NewRecorder()
	h.handleExportPurchases(rec, httptest.NewRequest(http.MethodPost, "/export", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("method status=%d", rec.Code)
	}
	// Not wired → 501.
	rec2 := httptest.NewRecorder()
	h.handleExportPurchases(rec2, httptest.NewRequest(http.MethodGet, "/export?format=csv", nil))
	if rec2.Code != http.StatusNotImplemented {
		t.Errorf("not wired status=%d", rec2.Code)
	}
	// With history: bad role → 403.
	h2 := NewAdminHandler(AdminHandlerDeps{History: &stubHistory{}, Now: time.Now})
	req := httptest.NewRequest(http.MethodGet, "/export?format=csv", nil)
	req.Header.Set("X-Chora-Role", "MYSTERY")
	rec3 := httptest.NewRecorder()
	h2.handleExportPurchases(rec3, req)
	if rec3.Code != http.StatusForbidden {
		t.Errorf("bad role status=%d", rec3.Code)
	}
	// Bad format → 400.
	h3 := NewAdminHandler(AdminHandlerDeps{History: &stubHistory{}, Now: time.Now})
	req2 := httptest.NewRequest(http.MethodGet, "/export?format=xml", nil)
	req2.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec4 := httptest.NewRecorder()
	h3.handleExportPurchases(rec4, req2)
	if rec4.Code != http.StatusBadRequest {
		t.Errorf("bad format status=%d", rec4.Code)
	}
	// Tenant role without tenant → 400.
	h4 := NewAdminHandler(AdminHandlerDeps{History: &stubHistory{}, Now: time.Now})
	req3 := httptest.NewRequest(http.MethodGet, "/export?format=csv", nil)
	req3.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	rec5 := httptest.NewRecorder()
	h4.handleExportPurchases(rec5, req3)
	if rec5.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status=%d", rec5.Code)
	}
	// Invalid filter date → 400.
	h5 := NewAdminHandler(AdminHandlerDeps{History: &stubHistory{}, Now: time.Now})
	req4 := httptest.NewRequest(http.MethodGet, "/export?format=csv&from=bad", nil)
	req4.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec6 := httptest.NewRecorder()
	h5.handleExportPurchases(rec6, req4)
	if rec6.Code != http.StatusBadRequest {
		t.Errorf("bad filter status=%d", rec6.Code)
	}
}

// stubEvents serves one PaymentEvent then closes the channel.
type stubEvents struct {
	err error
}

func (s *stubEvents) Subscribe(_ context.Context, _, _ string) (<-chan PaymentEvent, func(), error) {
	if s.err != nil {
		return nil, func() {}, s.err
	}
	ch := make(chan PaymentEvent, 1)
	ch <- PaymentEvent{PurchaseID: "p-1", AggregateType: "course_purchase", State: "refunded", OccurredAt: "2026-05-24T12:00:00Z"}
	close(ch)
	return ch, func() {}, nil
}

func TestHandleStreamPaymentEvents_NotWired(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{Now: time.Now})
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec := httptest.NewRecorder()
	h.handleStreamPaymentEvents(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "payment_event_source not wired") {
		t.Errorf("body=%s", rec.Body.String())
	}
}

func TestHandleStreamPaymentEvents_DeliversEvent(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{PaymentEvents: &stubEvents{}, Now: time.Now})
	req := httptest.NewRequest(http.MethodGet, "/stream?aggregate_type=course_purchase", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec := httptest.NewRecorder()
	h.handleStreamPaymentEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "event: payment_state_change") {
		t.Errorf("body=%s", rec.Body.String())
	}
}

func TestHandleStreamPaymentEvents_SubscribeError(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{PaymentEvents: &stubEvents{err: io.ErrUnexpectedEOF}, Now: time.Now})
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	rec := httptest.NewRecorder()
	h.handleStreamPaymentEvents(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500", rec.Code)
	}
}

func TestHandleStreamPaymentEvents_Errors(t *testing.T) {
	t.Parallel()
	h := NewAdminHandler(AdminHandlerDeps{Now: time.Now})
	// Wrong method.
	rec := httptest.NewRecorder()
	h.handleStreamPaymentEvents(rec, httptest.NewRequest(http.MethodPost, "/stream", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("method status=%d", rec.Code)
	}
	// Bad role.
	rec2 := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req.Header.Set("X-Chora-Role", "MYSTERY")
	h.handleStreamPaymentEvents(rec2, req)
	if rec2.Code != http.StatusForbidden {
		t.Errorf("bad role status=%d", rec2.Code)
	}
	// Tenant role without tenant.
	rec3 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req2.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	h.handleStreamPaymentEvents(rec3, req2)
	if rec3.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status=%d", rec3.Code)
	}
}

func TestUniqueTenantIDs(t *testing.T) {
	t.Parallel()
	items := []payments.PurchaseHistoryItem{
		{TenantID: "t1"}, {TenantID: "t1"}, {TenantID: "t2"},
	}
	got := uniqueTenantIDs(items)
	if len(got) != 2 || got[0] != "t1" || got[1] != "t2" {
		t.Errorf("got=%v", got)
	}
}

// pbTimestamp builds a timestamppb.Timestamp for test fixtures.
func pbTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

// gstatusErrorNotFound builds a gRPC NotFound error.
func gstatusErrorNotFound() error {
	return gstatus.Error(codes.NotFound, "purchase not found")
}
