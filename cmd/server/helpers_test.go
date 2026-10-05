// helpers_test.go — coverage for the cmd/server boot helpers that are
// testable without a live pool / event bus client: envOrDefault, the
// health/ready handlers, loggingMiddleware + statusWriter, the gRPC outbox
// adapter, and the SSE broker wiring helpers.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	pgrpc "github.com/apollo-chora/chora-payments/internal/adapter/grpc"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("CHORA_TEST_HELPER_VAR", "  set-value  ")
	if got := envOrDefault("CHORA_TEST_HELPER_VAR", "default"); got != "set-value" {
		t.Errorf("env set: got %q, want set-value (trimmed)", got)
	}
	t.Setenv("CHORA_TEST_HELPER_VAR", "   ")
	if got := envOrDefault("CHORA_TEST_HELPER_VAR", "default"); got != "default" {
		t.Errorf("blank env: got %q, want default", got)
	}
	t.Setenv("CHORA_TEST_HELPER_VAR", "")
	if got := envOrDefault("CHORA_TEST_HELPER_VAR", "default"); got != "default" {
		t.Errorf("empty env: got %q, want default", got)
	}
}

func TestHealthHandler(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	healthHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type=%q", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body=%v", body)
	}
	if body["service"] != serviceName || body["version"] != serviceVersion {
		t.Errorf("body=%v", body)
	}
}

func TestReadyHandler(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	readyHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
}

func TestLoggingMiddleware(t *testing.T) {
	t.Parallel()
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/webhooks/stripe", nil)
	rec := httptest.NewRecorder()
	loggingMiddleware(inner).ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d, want 201", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

func TestStatusWriter_WriteDefaults200(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	n, err := sw.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if sw.status != http.StatusOK {
		t.Errorf("status=%d, want 200 (default on bare Write)", sw.status)
	}
}

func TestStatusWriter_WriteHeaderRecordsStatus(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	sw.WriteHeader(http.StatusUnauthorized)
	if sw.status != http.StatusUnauthorized {
		t.Errorf("status=%d, want 401", sw.status)
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("recorder status=%d, want 401 (delegated)", rec.Code)
	}
}

func TestGRPCOutboxAdapter_Emit(t *testing.T) {
	t.Parallel()
	em := &recordingBootEmitter{}
	a := grpcOutboxAdapter{emit: em}
	err := a.Emit(context.Background(), pgrpc.OutboxEmitInput{
		TenantID:       "t1",
		Aggregate:      wh.AggregateCoursePurchase,
		PurchaseID:     "p1",
		EventType:      "payment_captured",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(em.emits) != 1 {
		t.Fatalf("emits=%d, want 1", len(em.emits))
	}
	if em.emits[0].PurchaseID != "p1" || em.emits[0].EventType != "payment_captured" {
		t.Errorf("emit=%+v", em.emits[0])
	}
	if em.emits[0].Aggregate != wh.AggregateCoursePurchase {
		t.Errorf("aggregate=%s", em.emits[0].Aggregate)
	}
}

func TestGRPCOutboxAdapter_Emit_NilEmitterIsNoOp(t *testing.T) {
	t.Parallel()
	a := grpcOutboxAdapter{} // emit nil
	if err := a.Emit(context.Background(), pgrpc.OutboxEmitInput{TenantID: "t", PurchaseID: "p"}); err != nil {
		t.Fatalf("Emit with nil emitter: %v", err)
	}
}

func TestPaymentSseSubEnvKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		topic string
		want  string
	}{
		{"chora.payments.course_purchase.payment_captured.v1", "CHORA_PAYMENTS_SSE_SUB__COURSE_PURCHASE_PAYMENT_CAPTURED"},
		{"chora.payments.user_subscription.cancelled.v1", "CHORA_PAYMENTS_SSE_SUB__USER_SUBSCRIPTION_CANCELLED"},
		{"chora.payments.dispute.raised.v1", "CHORA_PAYMENTS_SSE_SUB__DISPUTE_RAISED"},
		{"short", ""},
	}
	for _, tc := range cases {
		if got := paymentSseSubEnvKey(tc.topic); got != tc.want {
			t.Errorf("paymentSseSubEnvKey(%q)=%q, want %q", tc.topic, got, tc.want)
		}
	}
}

func TestPaymentBrokerSubscriptionMap_HonoursEnvOverrides(t *testing.T) {
	// Override one specific topic; the rest must fall back to defaults.
	t.Setenv("CHORA_PAYMENTS_SSE_SUB__COURSE_PURCHASE_PAYMENT_CAPTURED", "custom-sub-course-captured")
	m := paymentBrokerSubscriptionMap()
	if len(m) == 0 {
		t.Fatal("empty subscription map")
	}
	if got := m["chora.payments.course_purchase.payment_captured.v1"]; got != "custom-sub-course-captured" {
		t.Errorf("overridden sub=%q", got)
	}
	foundDefault := false
	for topic, sub := range m {
		if !strings.HasPrefix(topic, "chora.payments.") {
			t.Errorf("unexpected topic key %q", topic)
			continue
		}
		if strings.TrimSpace(sub) == "" {
			t.Errorf("empty subscription for %q", topic)
		}
		if topic != "chora.payments.course_purchase.payment_captured.v1" {
			foundDefault = true
		}
	}
	if !foundDefault {
		t.Error("expected at least one default subscription beyond the override")
	}
}

// recordingBootEmitter lets grpcOutboxAdapter tests spy on Emit calls.
// Implements dispatcher.OutboxEmitter (the adapter's delegate port).
type recordingBootEmitter struct {
	emits []dispatcher.EmitInput
}

func (r *recordingBootEmitter) Emit(_ context.Context, in dispatcher.EmitInput) error {
	r.emits = append(r.emits, in)
	return nil
}

func (r *recordingBootEmitter) EmitDispute(_ context.Context, in dispatcher.EmitDisputeInput) error {
	return nil
}

var _ dispatcher.OutboxEmitter = (*recordingBootEmitter)(nil)

// TestBootstrapPriceCatalogue_UnsetEnvReturnsPlaceholder — the no-env path
// must produce the placeholder catalogue with a nil shutdown func (no
// secret resolution).
func TestBootstrapPriceCatalogue_UnsetEnvReturnsPlaceholder(t *testing.T) {
	t.Setenv("STRIPE_PRICE_CATALOGUE_SECRET_ID", "")
	t.Setenv("CHORA_DB_PROJECT", "")
	cat, shutdown := bootstrapPriceCatalogue(context.Background())
	if cat == nil {
		t.Fatal("expected placeholder catalogue")
	}
	if shutdown != nil {
		t.Error("shutdown func should be nil when no secret env is set")
	}
}

// TestOutboxWorkerID_FallsBackToHostnameThenLocal covers the env fallback
// chain of the outbox worker id.
func TestOutboxWorkerID_FallsBackToHostnameThenLocal(t *testing.T) {
	t.Setenv("CHORA_OUTBOX_WORKER_ID", "")
	t.Setenv("HOSTNAME", "")
	if got := outboxWorkerID(); got != "chora-payments-local" {
		t.Errorf("no env: got %q, want chora-payments-local", got)
	}
	t.Setenv("HOSTNAME", "payments-pod-123")
	if got := outboxWorkerID(); got != "payments-pod-123" {
		t.Errorf("hostname: got %q, want payments-pod-123", got)
	}
	t.Setenv("CHORA_OUTBOX_WORKER_ID", "worker-9")
	if got := outboxWorkerID(); got != "worker-9" {
		t.Errorf("explicit: got %q, want worker-9", got)
	}
}

// TestBootstrapDBPool_UnsetEnvReturnsNil — no DSN env means in-memory
// repos; the pool helper must return (nil, nil) without connecting.
func TestBootstrapDBPool_UnsetEnvReturnsNil(t *testing.T) {
	t.Setenv("CHORA_DB_DSN", "")
	t.Setenv("CHORA_DB_DSN_SECRET_ID", "")
	t.Setenv("CHORA_DB_PROJECT", "")
	pool, shutdown := bootstrapDBPool(context.Background())
	if pool != nil {
		t.Errorf("pool=%v, want nil", pool)
	}
	if shutdown != nil {
		t.Error("shutdown func should be nil when no secret env is set")
	}
}

// TestNewPgxTxRunner_NilPoolIsNil — the runner constructor must return nil
// for a nil pool (so main falls back to in-memory repos).
func TestNewPgxTxRunner_NilPoolIsNil(t *testing.T) {
	if got := newPgxTxRunner(nil); got != nil {
		t.Errorf("newPgxTxRunner(nil)=%v, want nil", got)
	}
}
