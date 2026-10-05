// admin_history_handler_test.go — RED→GREEN smoke for the 4 new H+
// Transaction History endpoints (ListPurchases, AdminRefund,
// ExportPurchases, StreamPaymentEvents).
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chttp "github.com/apollo-chora/chora-payments/internal/adapter/http"
	"github.com/apollo-chora/chora-payments/internal/adapter/outbox"
	"github.com/apollo-chora/chora-payments/internal/domain/payments"
)

// stubHistory satisfies payments.PurchaseHistoryPort.
type stubHistory struct {
	items      []payments.PurchaseHistoryItem
	next       *string
	lookup     *payments.PurchaseHistoryItem
	lookupErr  error
	lastFilter payments.FilterCriteria
}

func (s *stubHistory) ListByTenant(_ context.Context, f payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	s.lastFilter = f
	return s.items, s.next, nil
}
func (s *stubHistory) ListAll(_ context.Context, f payments.FilterCriteria, _ string, _ int) ([]payments.PurchaseHistoryItem, *string, error) {
	s.lastFilter = f
	return s.items, s.next, nil
}
func (s *stubHistory) GetForRefund(_ context.Context, _ string, _ payments.AggregateType) (*payments.PurchaseHistoryItem, error) {
	return s.lookup, s.lookupErr
}

// stubAuditOutbox records audit emit calls.
type stubAuditOutbox struct {
	rows []recordedAudit
}

type recordedAudit struct {
	topic    string
	tenantID string
}

func (s *stubAuditOutbox) InsertAudit(_ context.Context, topic, tenantID, _, _ string, _ []byte) error {
	s.rows = append(s.rows, recordedAudit{topic: topic, tenantID: tenantID})
	return nil
}

func newAdminHistoryHandler(history payments.PurchaseHistoryPort, audit outbox.AuditOutbox, svc chttp.PaymentService) *chttp.AdminHandler {
	return chttp.NewAdminHandler(chttp.AdminHandlerDeps{
		Payments:    svc,
		History:     history,
		AuditOutbox: audit,
		Now:         func() time.Time { return time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC) },
	})
}

func TestListPurchases_HappyPath_TenantScoped(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	items := []payments.PurchaseHistoryItem{
		{
			PurchaseID:      "01970000-0000-7000-b000-000000000003",
			AggregateType:   payments.AggregateCoursePurchase,
			TenantID:        "01970000-0000-7000-8000-000000000001",
			LearnerGCID:     "01970000-0000-7000-a000-000000000002",
			AmountCents:     9900,
			Currency:        "sgd",
			State:           payments.StateCaptured,
			StripeSessionID: "cs_abc",
			CreatedAt:       now,
		},
	}
	hist := &stubHistory{items: items, next: nil}
	audit := &stubAuditOutbox{}
	h := newAdminHistoryHandler(hist, audit, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/purchases", nil)
	req.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	req.Header.Set("X-Chora-Tenant-Id", "01970000-0000-7000-8000-000000000001")
	req.Header.Set("X-Chora-Gcid", "01970000-0000-7000-a000-000000000002")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", w.Code, w.Body.String())
	}
	var env struct {
		Items         []map[string]any `json:"items"`
		NextPageToken *string          `json:"next_page_token"`
		HasMore       bool             `json:"has_more"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, w.Body.String())
	}
	if len(env.Items) != 1 {
		t.Errorf("len(items)=%d want 1", len(env.Items))
	}
	if env.HasMore {
		t.Errorf("has_more=true want false")
	}

	// Audit event must have been emitted.
	if len(audit.rows) < 1 {
		t.Errorf("no audit row emitted; want >=1")
	}
	if audit.rows[0].topic != "chora.governance.audit.tenant_admin_viewed_payments.v1" {
		t.Errorf("audit topic=%q want tenant_admin_viewed_payments", audit.rows[0].topic)
	}
}

func TestListPurchases_PlatformOperator_EmitsCrossTenantAudit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	items := []payments.PurchaseHistoryItem{
		{PurchaseID: "01970000-0000-7000-b000-000000000003", AggregateType: payments.AggregateCoursePurchase,
			TenantID:    "01970000-0000-7000-8000-000000000001",
			LearnerGCID: "01970000-0000-7000-a000-000000000002",
			AmountCents: 9900, Currency: "sgd", State: payments.StateCaptured,
			StripeSessionID: "cs_abc", CreatedAt: now},
		{PurchaseID: "01970000-0000-7000-b000-000000000004", AggregateType: payments.AggregateCoursePurchase,
			TenantID:    "01970000-0000-7000-8000-000000000099",
			LearnerGCID: "01970000-0000-7000-a000-000000000002",
			AmountCents: 7900, Currency: "sgd", State: payments.StateCaptured,
			StripeSessionID: "cs_def", CreatedAt: now},
	}
	hist := &stubHistory{items: items}
	audit := &stubAuditOutbox{}
	h := newAdminHistoryHandler(hist, audit, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/purchases", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")
	req.Header.Set("X-Chora-Gcid", "01970000-0000-7000-a000-000000000099")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", w.Code, w.Body.String())
	}
	// Expect both audit kinds.
	var topics []string
	for _, r := range audit.rows {
		topics = append(topics, r.topic)
	}
	wantTopics := map[string]bool{
		"chora.governance.audit.tenant_admin_viewed_payments.v1": false,
		"chora.governance.audit.cross_tenant_payments_viewed.v1": false,
	}
	for _, tt := range topics {
		wantTopics[tt] = true
	}
	for tt, seen := range wantTopics {
		if !seen {
			t.Errorf("expected audit topic %q not emitted; got=%v", tt, topics)
		}
	}
}

func TestAdminRefund_AuditorIsForbidden(t *testing.T) {
	t.Parallel()
	h := newAdminHistoryHandler(&stubHistory{}, &stubAuditOutbox{}, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/payments/01970000-0000-7000-b000-000000000003/refund",
		strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req.Header.Set("X-Chora-Role", "AUDITOR")
	req.Header.Set("X-Chora-Tenant-Id", "01970000-0000-7000-8000-000000000001")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("AUDITOR status=%d want 403; body=%s", w.Code, w.Body.String())
	}
}

func TestAdminRefund_AlreadyRefunded_Returns409(t *testing.T) {
	t.Parallel()
	hist := &stubHistory{lookupErr: payments.ErrAlreadyRefunded}
	h := newAdminHistoryHandler(hist, &stubAuditOutbox{}, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/payments/01970000-0000-7000-b000-000000000003/refund",
		strings.NewReader(`{"aggregate_type":"course_purchase"}`))
	req.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	req.Header.Set("X-Chora-Tenant-Id", "01970000-0000-7000-8000-000000000001")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("status=%d want 409; body=%s", w.Code, w.Body.String())
	}
}

func TestExportPurchases_CSV_StreamsHeaderAndRows(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	hist := &stubHistory{items: []payments.PurchaseHistoryItem{{
		PurchaseID:    "01970000-0000-7000-b000-000000000003",
		AggregateType: payments.AggregateCoursePurchase,
		TenantID:      "01970000-0000-7000-8000-000000000001",
		LearnerGCID:   "01970000-0000-7000-a000-000000000002",
		AmountCents:   9900, Currency: "sgd", State: payments.StateCaptured,
		StripeSessionID: "cs_abc", CreatedAt: now,
	}}}
	h := newAdminHistoryHandler(hist, &stubAuditOutbox{}, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/payments/export?format=csv", nil)
	req.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	req.Header.Set("X-Chora-Tenant-Id", "01970000-0000-7000-8000-000000000001")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/csv") {
		t.Errorf("Content-Type=%q want text/csv...", got)
	}
	if got := w.Header().Get("Content-Disposition"); !strings.Contains(got, "attachment") {
		t.Errorf("Content-Disposition=%q want attachment...", got)
	}
	body := w.Body.String()
	if !strings.Contains(body, "purchase_id") {
		t.Errorf("CSV header missing; body=%s", body)
	}
	if !strings.Contains(body, "01970000-0000-7000-b000-000000000003") {
		t.Errorf("CSV row missing; body=%s", body)
	}
}

func TestExportPurchases_NDJSON_StreamsOneLinePerRow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	hist := &stubHistory{items: []payments.PurchaseHistoryItem{
		{PurchaseID: "01970000-0000-7000-b000-000000000003", AggregateType: payments.AggregateCoursePurchase,
			TenantID: "t1", LearnerGCID: "g1", AmountCents: 99, Currency: "sgd",
			State: payments.StateCaptured, StripeSessionID: "cs_abc", CreatedAt: now},
		{PurchaseID: "01970000-0000-7000-b000-000000000004", AggregateType: payments.AggregateCoursePurchase,
			TenantID: "t1", LearnerGCID: "g1", AmountCents: 79, Currency: "sgd",
			State: payments.StateRefunded, StripeSessionID: "cs_def", CreatedAt: now},
	}}
	h := newAdminHistoryHandler(hist, &stubAuditOutbox{}, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/payments/export?format=json", nil)
	req.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	req.Header.Set("X-Chora-Tenant-Id", "t1")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/x-ndjson") {
		t.Errorf("Content-Type=%q want application/x-ndjson", got)
	}
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	if len(lines) != 2 {
		t.Errorf("NDJSON line count=%d want 2; body=%s", len(lines), w.Body.String())
	}
}

func TestStreamPaymentEvents_503_WhenSourceNotWired(t *testing.T) {
	t.Parallel()
	h := newAdminHistoryHandler(&stubHistory{}, &stubAuditOutbox{}, &stubPaymentService{})

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/payments/stream", nil)
	req.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	req.Header.Set("X-Chora-Tenant-Id", "01970000-0000-7000-8000-000000000001")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d want 503; body=%s", w.Code, w.Body.String())
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
		t.Errorf("Content-Type=%q want text/event-stream...", w.Header().Get("Content-Type"))
	}
}

func TestListPurchases_PlatformOperator_NoTenantHeaderRequired(t *testing.T) {
	t.Parallel()
	hist := &stubHistory{items: nil}
	h := newAdminHistoryHandler(hist, &stubAuditOutbox{}, &stubPaymentService{})

	// PLATFORM_OPERATOR may call without a tenant scope.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/purchases", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
}

// -----------------------------------------------------------------------------
// A1.1 — SSE handler tests with a wired broker stub (broker fan-out path).
// -----------------------------------------------------------------------------

// stubPaymentEventSource satisfies chttp.PaymentEventSource. Lets the test
// directly push PaymentEvents through the channel the handler receives on.
type stubPaymentEventSource struct {
	ch         chan chttp.PaymentEvent
	cleanupHit int
	lastTenant string
	lastAgg    string
	subErr     error
}

func newStubPaymentEventSource(buffer int) *stubPaymentEventSource {
	return &stubPaymentEventSource{ch: make(chan chttp.PaymentEvent, buffer)}
}

func (s *stubPaymentEventSource) Subscribe(_ context.Context, tenantID, aggregateType string) (<-chan chttp.PaymentEvent, func(), error) {
	s.lastTenant = tenantID
	s.lastAgg = aggregateType
	if s.subErr != nil {
		return nil, func() {}, s.subErr
	}
	cleanup := func() { s.cleanupHit++ }
	return s.ch, cleanup, nil
}

// TestStreamPaymentEvents_HappyPath_DeliversFrameAndCleansUp verifies the
// SSE handler writes a properly-shaped frame from the broker channel and
// invokes the cleanup hook on context cancellation.
func TestStreamPaymentEvents_HappyPath_DeliversFrameAndCleansUp(t *testing.T) {
	t.Parallel()
	src := newStubPaymentEventSource(2)
	src.ch <- chttp.PaymentEvent{
		PurchaseID:      "p1",
		AggregateType:   "course_purchase",
		State:           "captured",
		OccurredAt:      "2026-05-26T12:00:00Z",
		TenantID:        "tenant-A",
		LearnerGCID:     "gcid-1",
		AmountCents:     4999,
		Currency:        "sgd",
		StripeSessionID: "cs_test_p1",
	}

	h := chttp.NewAdminHandler(chttp.AdminHandlerDeps{
		PaymentEvents: src,
		Now:           func() time.Time { return time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC) },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/stream", nil).WithContext(ctx)
	req.Header.Set("X-Chora-Role", "TENANT_ADMIN")
	req.Header.Set("X-Chora-Tenant-Id", "tenant-A")

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, req)
		close(done)
	}()
	<-done

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q; want text/event-stream...", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, "event: payment_state_change") {
		t.Errorf("body missing SSE event line; got=%q", body)
	}
	if !strings.Contains(body, `"purchase_id":"p1"`) {
		t.Errorf("body missing purchase_id; got=%q", body)
	}
	if !strings.Contains(body, `"state":"captured"`) {
		t.Errorf("body missing state; got=%q", body)
	}
	// data: line ends with \n\n per SSE spec
	if !strings.Contains(body, "\n\n") {
		t.Errorf("body missing SSE frame terminator; got=%q", body)
	}
	if src.cleanupHit != 1 {
		t.Errorf("cleanup hit %d times; want 1 (handler must defer cleanup)", src.cleanupHit)
	}
}

// TestStreamPaymentEvents_PlatformOperator_PassesEmptyScope verifies operator
// cross-tenant request leaves the scope empty (broker's PLATFORM_OPERATOR
// signal). Optional ?tenant_id= still narrows down.
func TestStreamPaymentEvents_PlatformOperator_PassesEmptyScope(t *testing.T) {
	t.Parallel()
	src := newStubPaymentEventSource(1)
	h := chttp.NewAdminHandler(chttp.AdminHandlerDeps{PaymentEvents: src})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/stream?aggregate_type=course_purchase", nil).WithContext(ctx)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if src.lastTenant != "" {
		t.Errorf("PLATFORM_OPERATOR tenant scope = %q; want empty", src.lastTenant)
	}
	if src.lastAgg != "course_purchase" {
		t.Errorf("aggregate filter = %q; want course_purchase", src.lastAgg)
	}
}

// TestStreamPaymentEvents_OperatorWithTenantQuery narrows by tenant_id query.
func TestStreamPaymentEvents_OperatorWithTenantQuery(t *testing.T) {
	t.Parallel()
	src := newStubPaymentEventSource(1)
	h := chttp.NewAdminHandler(chttp.AdminHandlerDeps{PaymentEvents: src})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/payments/stream?tenant_id=tenant-A", nil).WithContext(ctx)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if src.lastTenant != "tenant-A" {
		t.Errorf("operator tenant_id query = %q; want tenant-A", src.lastTenant)
	}
}

// Ensure stubPaymentEventSource satisfies the interface — compile-time check.
var _ chttp.PaymentEventSource = (*stubPaymentEventSource)(nil)

// Silence "json imported and not used" if the SSE test triggers it.
var _ = json.Decoder{}
