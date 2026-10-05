// broker_test.go — RED tests for the in-process event-bus fan-out broker.
//
// Tests validate:
//   - Subscribe returns a channel + cleanup closure
//   - Tenant-scoped subscriber receives only matching tenant's events
//   - Cross-tenant subscriber (empty tenant) receives all events
//   - Aggregate filter (when set) drops non-matching aggregates
//   - Bounded channel: slow client drops oldest event when buffer full
//   - cleanup() unregisters the client + closes the channel
//   - Idempotent dedupe: replayed message (same idempotency_key) emits once
//   - Concurrent Subscribe + Publish is race-free
//   - Ack on success, Nack on decoder error (eventbus settlement)
//
// Per [[feedback-d6-resilience-first-class]] Pillar 2 + the coordinator
// directive: dedup via idempotency_key (5min TTL), ack-after-fan-out.
package event_broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
)

// -----------------------------------------------------------------------------
// stubBus is a controllable eventbus.Bus stub. Lets the test inject
// envelopes into the broker via Subscribe-registered handlers.
// -----------------------------------------------------------------------------

type stubBus struct {
	mu       sync.Mutex
	handlers map[string]eventbus.Handler
	wg       sync.WaitGroup

	acked  atomic.Int32
	nacked atomic.Int32
}

func newStubBus() *stubBus {
	return &stubBus{
		handlers: make(map[string]eventbus.Handler),
	}
}

func (s *stubBus) Publish(_ context.Context, _ string, _ envelope.Envelope, _ []byte) error {
	return nil
}

func (s *stubBus) Subscribe(ctx context.Context, cfg eventbus.ConsumerConfig, handler eventbus.Handler) error {
	s.mu.Lock()
	s.handlers[cfg.Subject] = handler
	s.mu.Unlock()
	s.wg.Add(1)
	defer s.wg.Done()
	<-ctx.Done()
	return ctx.Err()
}

func (s *stubBus) Close() error { return nil }

// deliver invokes the registered handler for the subject, recording the
// settlement (ack on nil, nack on error) the way the real eventbus does.
// Blocks until the broker has registered the subscriber.
func (s *stubBus) deliver(subject string, msg eventbus.Message) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		h, ok := s.handlers[subject]
		s.mu.Unlock()
		if ok {
			if err := h(context.Background(), msg); err != nil {
				s.nacked.Add(1)
			} else {
				s.acked.Add(1)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("stubBus.deliver: subscriber not registered in time: " + subject)
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// buildBusMessageCoursePaymentCaptured builds an eventbus.Message carrying a
// serialised CoursePurchasePaymentCaptured proto + transport envelope.
func buildBusMessageCoursePaymentCaptured(t *testing.T, tenantID, purchaseID, idempotencyKey string) eventbus.Message {
	t.Helper()
	now := time.Now().UTC()
	payload := &paymentsv1.CoursePurchasePaymentCaptured{
		Envelope: &commonv1.EventEnvelope{
			EventId:        "evt-" + purchaseID,
			IdempotencyKey: idempotencyKey,
			TenantId:       tenantID,
			OccurredAt:     timestamppb.New(now),
		},
		PurchaseId:      purchaseID,
		LearnerGcid:     "gcid-learner-1",
		CourseId:        "course-1",
		StripeSessionId: "cs_test_" + purchaseID,
		AmountCentsPaid: 4999,
		Currency:        "sgd",
		PaidAt:          timestamppb.New(now),
	}
	bz, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return eventbus.Message{
		Subject: "chora.payments.course_purchase.payment_captured.v1",
		Envelope: envelope.Envelope{
			EventID:        "evt-" + purchaseID,
			IdempotencyKey: idempotencyKey,
			TenantID:       tenantID,
			OccurredAt:     now,
			PublishedAt:    now,
			SourceProject:  "chora-local",
			SourceService:  "chora-payments",
			SchemaVersion:  1,
		},
		Payload: bz,
	}
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

// TestBroker_Subscribe_DeliversEventToMatchingTenant verifies a tenant-scoped
// subscriber receives an event published on its tenant.
func TestBroker_Subscribe_DeliversEventToMatchingTenant(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-1",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-1"))

	select {
	case ev := <-ch:
		if ev.TenantID != "tenant-A" || ev.PurchaseID != "p1" {
			t.Fatalf("unexpected event %+v", ev)
		}
		if ev.AggregateType != "course_purchase" {
			t.Fatalf("aggregate_type = %q; want course_purchase", ev.AggregateType)
		}
		if ev.State != "captured" {
			t.Fatalf("state = %q; want captured", ev.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for event")
	}
}

// TestBroker_Subscribe_TenantIsolation rejects cross-tenant leakage.
func TestBroker_Subscribe_TenantIsolation(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-2",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	chA, cleanupA, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	defer cleanupA()
	chB, cleanupB, err := b.Subscribe(ctx, "tenant-B", "")
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}
	defer cleanupB()

	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-2"))

	select {
	case ev := <-chA:
		if ev.TenantID != "tenant-A" {
			t.Fatalf("tenant A got event for tenant %q", ev.TenantID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("tenant A timed out")
	}

	select {
	case ev := <-chB:
		t.Fatalf("tenant B leaked event %+v", ev)
	case <-time.After(200 * time.Millisecond):
		// expected
	}
}

// TestBroker_Subscribe_CrossTenantOperator empty tenant scope sees all.
func TestBroker_Subscribe_CrossTenantOperator(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-3",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	chOp, cleanup, err := b.Subscribe(ctx, "", "") // operator scope
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-3a"))
	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-B", "p2", "idem-3b"))

	seen := make(map[string]struct{})
	deadline := time.Now().Add(2 * time.Second)
	for len(seen) < 2 && time.Now().Before(deadline) {
		select {
		case ev := <-chOp:
			seen[ev.TenantID] = struct{}{}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, ok := seen["tenant-A"]; !ok {
		t.Errorf("operator missed tenant-A event")
	}
	if _, ok := seen["tenant-B"]; !ok {
		t.Errorf("operator missed tenant-B event")
	}
}

// TestBroker_Subscribe_AggregateFilter narrows by aggregate_type.
func TestBroker_Subscribe_AggregateFilter(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1":   "test-sub-4a",
			"chora.payments.user_subscription.payment_captured.v1": "test-sub-4b",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "course_purchase")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	// course_purchase — should arrive
	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-4a"))

	// user_subscription — should NOT arrive (filtered)
	sub := &paymentsv1.UserSubscriptionPaymentCaptured{
		Envelope: &commonv1.EventEnvelope{
			EventId:        "evt-sub",
			IdempotencyKey: "idem-4b",
			TenantId:       "tenant-A",
			OccurredAt:     timestamppb.New(time.Now().UTC()),
		},
		PurchaseId:  "sub-1",
		LearnerGcid: "gcid-1",
		Currency:    "sgd",
		PaidAt:      timestamppb.New(time.Now().UTC()),
	}
	subBz, _ := proto.Marshal(sub)
	stub.deliver("chora.payments.user_subscription.payment_captured.v1", eventbus.Message{
		Subject: "chora.payments.user_subscription.payment_captured.v1",
		Envelope: envelope.Envelope{
			EventID:        "evt-sub",
			IdempotencyKey: "idem-4b",
			TenantID:       "tenant-A",
			OccurredAt:     time.Now().UTC(),
			PublishedAt:    time.Now().UTC(),
			SourceProject:  "chora-local",
			SourceService:  "chora-payments",
			SchemaVersion:  1,
		},
		Payload: subBz,
	})

	got := []PaymentEvent{}
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 event (filtered to course_purchase), got %d: %+v", len(got), got)
	}
	if got[0].AggregateType != "course_purchase" {
		t.Fatalf("aggregate_type = %q; want course_purchase", got[0].AggregateType)
	}
}

// TestBroker_Subscribe_BoundedChannelDropsOldest verifies backpressure
// behaviour — slow client drops oldest events.
func TestBroker_Subscribe_BoundedChannelDropsOldest(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-5",
		},
		ChannelBuffer: 2,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	// Fill the buffer + overflow by 2 (without consuming).
	for i := 0; i < 4; i++ {
		stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", fmt.Sprintf("p%d", i), fmt.Sprintf("idem-5-%d", i)))
	}
	time.Sleep(200 * time.Millisecond) // let broker process

	count := 0
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-ch:
			count++
		case <-timer.C:
			// Buffer was 2, so client should see ≤ 2 events (oldest dropped).
			if count == 0 {
				t.Fatalf("expected client to receive at least 1 event after backpressure; got 0")
			}
			if count > 2 {
				t.Fatalf("expected client to receive ≤ 2 events (buffer=2); got %d", count)
			}
			return
		}
	}
}

// TestBroker_Cleanup_DropsSubscriber.
func TestBroker_Cleanup_DropsSubscriber(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-6",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cleanup()
	cleanup() // double-call must not panic

	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-6"))
	time.Sleep(100 * time.Millisecond)

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("post-cleanup channel still delivered events")
		}
	default:
		// also acceptable — channel closed
	}
	if got := b.SubscriberCount(); got != 0 {
		t.Fatalf("SubscriberCount = %d; want 0", got)
	}
}

// TestBroker_Idempotent_DedupesReplay drops a re-delivered message with
// the same idempotency_key.
func TestBroker_Idempotent_DedupesReplay(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-7",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	// Same idempotency_key delivered twice — broker should fan out exactly once.
	msg := buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-7-dup")
	stub.deliver("chora.payments.course_purchase.payment_captured.v1", msg)
	stub.deliver("chora.payments.course_purchase.payment_captured.v1", msg)

	count := 0
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-ch:
			count++
		case <-time.After(50 * time.Millisecond):
		}
	}
	if count != 1 {
		t.Fatalf("expected dedup → 1 fan-out, got %d", count)
	}
}

// TestBroker_Concurrent_SubscribeAndDeliver — race-detector friendly.
func TestBroker_Concurrent_SubscribeAndDeliver(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-8",
		},
		ChannelBuffer: 16,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	var wg sync.WaitGroup
	var received atomic.Int32
	// 10 subscribers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
			if err != nil {
				t.Errorf("Subscribe[%d]: %v", idx, err)
				return
			}
			defer cleanup()
			to := time.NewTimer(1500 * time.Millisecond)
			defer to.Stop()
			for {
				select {
				case <-ch:
					received.Add(1)
				case <-to.C:
					return
				}
			}
		}(i)
	}

	// Wait briefly for subscribers to settle, then publish 5 unique events.
	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 5; i++ {
		stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", fmt.Sprintf("p%d", i), fmt.Sprintf("idem-8-%d", i)))
	}

	wg.Wait()
	// 10 subscribers × 5 events = 50 max (no drops, buffer=16).
	if got := received.Load(); got < 5 {
		t.Fatalf("received = %d; want ≥ 5 across all subscribers", got)
	}
}

// TestBroker_NewBroker_Validation rejects bad config.
func TestBroker_NewBroker_Validation(t *testing.T) {
	t.Parallel()
	if _, err := NewBroker(BrokerConfig{}); err == nil {
		t.Errorf("empty config should error")
	}
	if _, err := NewBroker(BrokerConfig{Bus: newStubBus()}); err == nil {
		t.Errorf("empty subject map should error")
	}
}

// TestBroker_Ack_OnSuccess verifies ack-after-processing: a decoded +
// fanned-out message settles nil (ack).
func TestBroker_Ack_OnSuccess(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-ack",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	_, cleanup, _ := b.Subscribe(ctx, "tenant-A", "")
	defer cleanup()

	stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p1", "idem-ack-1"))
	time.Sleep(200 * time.Millisecond)
	if stub.acked.Load() != 1 {
		t.Errorf("expected 1 Ack; got %d", stub.acked.Load())
	}
	if stub.nacked.Load() != 0 {
		t.Errorf("expected 0 Nacks; got %d", stub.nacked.Load())
	}
}

// TestBroker_DroppedTotal_Increments verifies the ops counter ticks up on
// backpressure drops.
func TestBroker_DroppedTotal_Increments(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-drop",
		},
		ChannelBuffer: 1,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	_, cleanup, _ := b.Subscribe(ctx, "tenant-A", "")
	defer cleanup()

	for i := 0; i < 8; i++ {
		stub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", fmt.Sprintf("p%d", i), fmt.Sprintf("idem-drop-%d", i)))
	}
	time.Sleep(250 * time.Millisecond)
	// Buffer=1; we sent 8 unique events → at least 6 should drop.
	if b.DroppedTotal() < 5 {
		t.Errorf("DroppedTotal = %d; want ≥ 5", b.DroppedTotal())
	}
}

// TestBroker_Subscribe_AfterClose returns an error.
func TestBroker_Subscribe_AfterClose(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-closed",
		},
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		_ = b.Run(ctx)
		close(runDone)
	}()
	cancel()
	<-runDone

	_, _, err = b.Subscribe(context.Background(), "tenant-A", "")
	if err == nil {
		t.Errorf("Subscribe after broker close should error")
	}
}

// -----------------------------------------------------------------------------
// Resilience tests — subscription error self-heal (ADR-168 / 2026-05-29)
// -----------------------------------------------------------------------------

// errorBus is a stub whose Subscribe always returns a configurable
// non-cancel error. Tracks call count per consumer so tests can assert
// retry behaviour.
type errorBus struct {
	mu        sync.Mutex
	callCount map[string]*atomic.Int32
	err       error
}

func newErrorBus(err error) *errorBus {
	return &errorBus{
		callCount: make(map[string]*atomic.Int32),
		err:       err,
	}
}

func (e *errorBus) Publish(_ context.Context, _ string, _ envelope.Envelope, _ []byte) error {
	return nil
}

func (e *errorBus) Subscribe(_ context.Context, cfg eventbus.ConsumerConfig, _ eventbus.Handler) error {
	e.mu.Lock()
	if _, ok := e.callCount[cfg.Name]; !ok {
		var c atomic.Int32
		e.callCount[cfg.Name] = &c
	}
	ctr := e.callCount[cfg.Name]
	e.mu.Unlock()
	ctr.Add(1)

	// Simulate a fast-return error (e.g. consumer creation failure).
	return e.err
}

func (e *errorBus) Close() error { return nil }

func (e *errorBus) getCallCount(name string) int32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.callCount[name]; ok {
		return c.Load()
	}
	return 0
}

// TestBroker_Run_RetriesOnSubscriptionError verifies that a subscription whose
// Subscribe always returns a non-cancel error (simulating a consumer
// creation failure) does NOT cause Run to return while ctx is live. Run
// returns only after ctx is cancelled, with nil or ctx.Err() — never the
// error. The test also asserts the broker retried (Subscribe called > 1).
func TestBroker_Run_RetriesOnSubscriptionError(t *testing.T) {
	t.Parallel()

	consumerErr := fmt.Errorf("nats: invalid consumer name (resource=chora-payments-fanout-test)")
	bus := newErrorBus(consumerErr)

	cfg := BrokerConfig{
		Bus: bus,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "error-sub",
		},
		ChannelBuffer: 4,
		// Millisecond backoff so the retry assertion does not wait real
		// seconds. Per-broker, so it cannot race another parallel test.
		BackoffBase: 5 * time.Millisecond,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- b.Run(ctx)
	}()

	// Wait for Run to finish (ctx timeout triggers it).
	select {
	case err := <-runErr:
		// Run must not return the consumer error.
		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Fatalf("Run returned unexpected error: %v (want nil or context error)", err)
		}
		// Broker must have retried: Subscribe should have been called
		// more than once.
		count := bus.getCallCount("error-sub")
		if count <= 1 {
			t.Errorf("expected Subscribe to be called >1 time (retries); got %d", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after ctx was cancelled")
	}
}

// TestBroker_Run_HealthySubStillDeliversDuringPeerError verifies that a healthy
// subscription continues to deliver messages even when another subscription in
// the same broker is repeatedly erroring (simulating mixed provisioning state).
func TestBroker_Run_HealthySubStillDeliversDuringPeerError(t *testing.T) {
	t.Parallel()

	// We need a bus that can serve two consumers differently:
	// "error-sub-peer" → always errors, "healthy-sub" → normal delivery.
	healthyStub := newStubBus()
	consumerErr := fmt.Errorf("nats: invalid consumer name (resource=chora-payments-fanout-peer)")

	mixed := &mixedBus{
		errName: "error-sub-peer",
		err:     consumerErr,
		happy:   healthyStub,
	}

	cfg := BrokerConfig{
		Bus: mixed,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1":   "healthy-sub",
			"chora.payments.user_subscription.payment_captured.v1": "error-sub-peer",
		},
		ChannelBuffer: 4,
		BackoffBase:   5 * time.Millisecond,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()

	// Subscribe and expect delivery from the healthy subscription.
	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	healthyStub.deliver("chora.payments.course_purchase.payment_captured.v1", buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p-mixed", "idem-mixed-1"))

	select {
	case ev := <-ch:
		if ev.PurchaseID != "p-mixed" {
			t.Fatalf("unexpected event %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout: healthy sub did not deliver event while peer sub was erroring")
	}

	// Cancel and confirm Run exits cleanly.
	cancel()
	select {
	case runErr := <-runDone:
		if runErr != nil && runErr != context.Canceled && runErr != context.DeadlineExceeded {
			t.Fatalf("Run returned unexpected error on cancel: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after cancel")
	}
}

// mixedBus routes one consumer name to always-error, all others to a
// healthy stubBus.
type mixedBus struct {
	errName string
	err     error
	happy   *stubBus

	mu       sync.Mutex
	errCalls atomic.Int32
}

func (m *mixedBus) Publish(_ context.Context, _ string, _ envelope.Envelope, _ []byte) error {
	return nil
}

func (m *mixedBus) Subscribe(ctx context.Context, cfg eventbus.ConsumerConfig, handler eventbus.Handler) error {
	if cfg.Name == m.errName {
		m.errCalls.Add(1)
		return m.err
	}
	return m.happy.Subscribe(ctx, cfg, handler)
}

func (m *mixedBus) Close() error { return nil }

// TestBroker_Nack_OnDecodeFailure verifies a decode failure settles as an
// error (nack → redelivery → DLQ at MaxDeliver on the real bus).
func TestBroker_Nack_OnDecodeFailure(t *testing.T) {
	t.Parallel()
	stub := newStubBus()
	cfg := BrokerConfig{
		Bus: stub,
		SubscriptionsBySubject: map[string]string{
			"chora.payments.course_purchase.payment_captured.v1": "test-sub-nack",
		},
		ChannelBuffer: 4,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	// Bogus payload — decoder will return error.
	msg := eventbus.Message{
		Subject: "chora.payments.course_purchase.payment_captured.v1",
		Envelope: envelope.Envelope{
			EventID:        "evt-bad",
			IdempotencyKey: "idem-bad",
			TenantID:       "tenant-A",
			OccurredAt:     time.Now().UTC(),
			PublishedAt:    time.Now().UTC(),
			SourceProject:  "chora-local",
			SourceService:  "chora-payments",
			SchemaVersion:  1,
		},
		Payload: []byte("not-a-proto"),
	}
	stub.deliver("chora.payments.course_purchase.payment_captured.v1", msg)
	time.Sleep(200 * time.Millisecond)
	if stub.nacked.Load() != 1 {
		t.Errorf("expected 1 Nack; got %d", stub.nacked.Load())
	}
	if stub.acked.Load() != 0 {
		t.Errorf("expected 0 Acks; got %d", stub.acked.Load())
	}
}
