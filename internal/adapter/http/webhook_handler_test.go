// webhook_handler_test.go — RED→GREEN tests for the Stripe webhook receive
// path's dedup + retry semantics.
//
// The dedup gate INSERTs the event up-front, then dispatches. The bug this
// suite pins: when a dispatch FAILS, the event row already exists, so
// Stripe's retry hit the Insert UNIQUE-violation and got an idempotent skip
// (200) — the failed dispatch was NEVER retried, stranding the payment. The
// fix: re-dispatch when the existing row is unprocessed (processed_at NULL);
// only genuinely-processed events are idempotent-skipped.
package http

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const testWebhookSecret = "whsec_test_secret_for_unit_tests"

// fakeWebhookEvents is an in-memory wh.Repo. Insert returns ErrAlreadyProcessed
// when the event_id already exists (mirrors the UNIQUE-violation gate).
type fakeWebhookEvents struct {
	mu     sync.Mutex
	stored map[string]*wh.WebhookEvent
}

func newFakeWebhookEvents() *fakeWebhookEvents {
	return &fakeWebhookEvents{stored: map[string]*wh.WebhookEvent{}}
}

func (f *fakeWebhookEvents) seed(w *wh.WebhookEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *w
	f.stored[w.EventID] = &cp
}

func (f *fakeWebhookEvents) Insert(_ context.Context, w *wh.WebhookEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.stored[w.EventID]; ok {
		return wh.ErrAlreadyProcessed
	}
	cp := *w
	f.stored[w.EventID] = &cp
	return nil
}

func (f *fakeWebhookEvents) MarkProcessed(_ context.Context, eventID string, agg wh.AggregateType, purchaseID string, processedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.stored[eventID]; ok {
		t := processedAt
		e.ProcessedAt = &t
		e.TargetAggregateType = agg
		e.TargetPurchaseID = purchaseID
	}
	return nil
}

func (f *fakeWebhookEvents) MarkFailed(_ context.Context, eventID, processingError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.stored[eventID]; ok {
		e.ProcessingError = processingError
	}
	return nil
}

func (f *fakeWebhookEvents) GetByEventID(_ context.Context, eventID string) (*wh.WebhookEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.stored[eventID]; ok {
		cp := *e
		return &cp, nil
	}
	return nil, shared.ErrNotFound
}

var _ wh.Repo = (*fakeWebhookEvents)(nil)

// fakeDispatcher records Dispatch invocations and returns a canned result.
type fakeDispatcher struct {
	calls int
	agg   wh.AggregateType
	pid   string
	err   error
}

func (d *fakeDispatcher) Dispatch(_ interface{}, _ *stripeGo.Event) (wh.AggregateType, string, error) {
	d.calls++
	return d.agg, d.pid, d.err
}

// signedRequest builds a POST with a valid Stripe-Signature header for body.
func signedRequest(t *testing.T, secret string, body []byte) *http.Request {
	t.Helper()
	ts := time.Now().Unix()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.%s", ts, body)))
	sig := hex.EncodeToString(mac.Sum(nil))
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/stripe", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", fmt.Sprintf("t=%d,v1=%s", ts, sig))
	return req
}

func eventBody(eventID string) []byte {
	return []byte(fmt.Sprintf(
		`{"id":%q,"type":"checkout.session.completed","data":{"object":{"id":"cs_test_x","object":"checkout_session","metadata":{"tenant_id":"01970000-0000-7000-8000-000000000001","purchase_type":"user_mana_topup","purchase_id":"01970000-0000-7000-b000-000000000003"}}}}`,
		eventID))
}

func newWebhookTestHandler(events *fakeWebhookEvents, disp *fakeDispatcher) http.HandlerFunc {
	return NewWebhookHandler(WebhookDeps{
		WebhookSecret: testWebhookSecret,
		WebhookEvents: events,
		Dispatcher:    disp,
		Now:           func() time.Time { return time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC) },
	})
}

func TestWebhook_FreshEvent_Dispatches(t *testing.T) {
	events := newFakeWebhookEvents()
	disp := &fakeDispatcher{agg: wh.AggregateUserManaTopUp, pid: "01970000-0000-7000-b000-000000000003"}
	h := newWebhookTestHandler(events, disp)

	rec := httptest.NewRecorder()
	h(rec, signedRequest(t, testWebhookSecret, eventBody("evt_fresh_1")))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if disp.calls != 1 {
		t.Errorf("dispatch calls=%d, want 1", disp.calls)
	}
	got, _ := events.GetByEventID(context.Background(), "evt_fresh_1")
	if got == nil || !got.IsProcessed() {
		t.Errorf("event not marked processed after successful dispatch: %+v", got)
	}
}

func TestWebhook_DuplicateProcessedEvent_SkipsWithoutRedispatch(t *testing.T) {
	events := newFakeWebhookEvents()
	processedAt := time.Date(2026, 6, 4, 11, 0, 0, 0, time.UTC)
	w, _ := wh.New("evt_done_1", "checkout.session.completed", processedAt)
	w.ProcessedAt = &processedAt // genuinely processed
	events.seed(w)

	disp := &fakeDispatcher{agg: wh.AggregateUserManaTopUp, pid: "p1"}
	h := newWebhookTestHandler(events, disp)

	rec := httptest.NewRecorder()
	h(rec, signedRequest(t, testWebhookSecret, eventBody("evt_done_1")))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 (idempotent skip)", rec.Code)
	}
	if disp.calls != 0 {
		t.Errorf("dispatch calls=%d, want 0 (already processed must NOT re-dispatch)", disp.calls)
	}
}

// TestWebhook_DuplicateUnprocessedEvent_Redispatches is the RED test for the
// secondary bug: a prior delivery INSERTed the row then its dispatch FAILED
// (processed_at left NULL). Stripe's retry MUST re-dispatch, not skip.
func TestWebhook_DuplicateUnprocessedEvent_Redispatches(t *testing.T) {
	events := newFakeWebhookEvents()
	w, _ := wh.New("evt_retry_1", "checkout.session.completed", time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC))
	w.ProcessingError = "user_mana_topup lookup: 22P02" // prior dispatch failed
	events.seed(w)                                      // ProcessedAt stays nil

	disp := &fakeDispatcher{agg: wh.AggregateUserManaTopUp, pid: "p1"} // now succeeds
	h := newWebhookTestHandler(events, disp)

	rec := httptest.NewRecorder()
	h(rec, signedRequest(t, testWebhookSecret, eventBody("evt_retry_1")))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200 after successful re-dispatch", rec.Code, rec.Body.String())
	}
	if disp.calls != 1 {
		t.Errorf("dispatch calls=%d, want 1 (unprocessed event MUST be re-dispatched on retry)", disp.calls)
	}
	got, _ := events.GetByEventID(context.Background(), "evt_retry_1")
	if got == nil || !got.IsProcessed() {
		t.Errorf("event not marked processed after successful re-dispatch: %+v", got)
	}
}

func TestWebhook_DispatchFailure_MarksFailedAnd500(t *testing.T) {
	events := newFakeWebhookEvents()
	disp := &fakeDispatcher{err: fmt.Errorf("downstream blew up")}
	h := newWebhookTestHandler(events, disp)

	rec := httptest.NewRecorder()
	h(rec, signedRequest(t, testWebhookSecret, eventBody("evt_fail_1")))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 so Stripe retries", rec.Code)
	}
	got, _ := events.GetByEventID(context.Background(), "evt_fail_1")
	if got == nil || got.IsProcessed() {
		t.Errorf("event must NOT be processed on dispatch failure: %+v", got)
	}
	if got.ProcessingError == "" {
		t.Errorf("expected ProcessingError recorded on failed dispatch")
	}
}
