// Package event_broker implements an in-process event-bus fan-out broker
// for the SSE /api/v1/admin/payments/stream endpoint.
//
// Design (per the A1.1 spec + coordinator §1-5 directives):
//
//   - **Shared subscription per subject**: the broker subscribes ONCE per
//     chora.payments.* subject on boot (28 subjects for the 7 Purchase
//     aggregates × 4 state events). Each pod runs its own broker + its own
//     durable consumer; pods load-balance via the stream's natural
//     consumer load-distribution. NOT one subscription per HTTP client —
//     that would create a broker resource per FE EventSource open, which
//     doesn't scale.
//
//   - **Per-HTTP-client in-memory channel**: each call to Subscribe(...)
//     returns a fresh bounded channel. The fan-out goroutine routes each
//     decoded PaymentEvent into the channels whose (tenant, aggregate)
//     filter matches.
//
//   - **Tenant isolation**: tenant-scoped subscribers (BFF caller is
//     TENANT_ADMIN / OWNER / AUDITOR) see only their tenant's events.
//     Empty tenant scope means PLATFORM_OPERATOR cross-tenant fan-out.
//
//   - **Backpressure**: bounded per-client channel (default buffer 64).
//     When full, the broker drops the oldest event (non-blocking send,
//     then receive one + retry once). This keeps a slow FE from blocking
//     the global fan-out loop.
//
//   - **Idempotent dedup**: subscriber tracks recently-seen
//     idempotency_keys (5 min TTL via chora-common/idempotent
//     in-memory store) and drops replays. The bus's at-least-once
//     delivery would otherwise fan out duplicate events to FEs on
//     redelivery.
//
//   - **Ack-after-fan-out**: the per-message handler returns nil only
//     AFTER decoding + fan-out succeed (the eventbus acks on nil and
//     nacks on error). Decode errors return an error so the bus
//     redelivers; persistent decode failures route to the DLQ via the
//     consumer's MaxDeliver + dead-letter policy.
//
//   - **Multi-pod safe**: each pod sees only the events that the stream
//     routed to ITS consumer. Clients reconnect after EventSource
//     disconnect (A3's payments-realtime.service.ts has built-in
//     auto-reconnect) so the load-balancing eventually converges.
//
// Hexagonal: ADAPTER. Depends on chora-common/eventbus (a Chora platform
// library, NOT a cloud SDK directly). The chora-payments domain doesn't
// know this broker exists.
package event_broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"
)

// -----------------------------------------------------------------------------
// Defaults
// -----------------------------------------------------------------------------

const (
	// DefaultChannelBuffer is the per-client bounded-channel size. 64 is a
	// reasonable middle ground: ~1 second of capture at 64 ev/s peak before
	// backpressure kicks in.
	DefaultChannelBuffer = 64

	// DefaultDedupTTL is the inbox-key retention window. 5 minutes is
	// shorter than the AgentDecision / closure subscribers' 24h because
	// SSE is ephemeral — a 5-minute redelivery dedup is enough to absorb
	// typical bus redelivery latency. Coordinator directive §2.
	DefaultDedupTTL = 5 * time.Minute

	// backoffCap is the maximum per-subscription retry sleep duration.
	backoffCap = 30 * time.Second
)

// DefaultBackoffBase is the starting duration for per-subscription retry
// backoff when BrokerConfig.BackoffBase is unset.
//
// This was a package-level `var backoffBase` that tests shrank to milliseconds
// to avoid waiting real seconds. That made it MUTABLE GLOBAL STATE read from
// every subscription goroutine, and the tests that shrank it are t.Parallel():
// one test's write raced another test's running broker, which the detector
// caught and which then failed every parallel test in flight. The seam was the
// right idea in the wrong place; it now lives on the config, alongside the
// ChannelBuffer and DedupTTL zero-means-default fields.
const DefaultBackoffBase = 1 * time.Second

// -----------------------------------------------------------------------------
// BrokerConfig — input to NewBroker.
// -----------------------------------------------------------------------------

// BrokerConfig parameterises a PaymentEventBroker instance.
type BrokerConfig struct {
	// Bus is the eventbus the broker subscribes through. Must be non-nil.
	Bus eventbus.Bus

	// SubscriptionsBySubject maps a chora.payments.* subject name to the
	// durable consumer name the broker should bind to. Each map entry
	// results in one persistent consumer via Bus.Subscribe.
	//
	// In production, the standard 28-entry map produced by
	// DefaultSubscriptionMap() is what main.go wires. Override only for
	// tests.
	SubscriptionsBySubject map[string]string

	// ChannelBuffer is the per-client channel size. 0 = DefaultChannelBuffer.
	ChannelBuffer int

	// DedupTTL is the idempotency_key retention window. 0 = DefaultDedupTTL.
	DedupTTL time.Duration

	// Inbox is the idempotency store. nil = in-memory.
	Inbox idempotent.Store

	// BackoffBase is the starting per-subscription retry backoff, doubling to
	// backoffCap. 0 = DefaultBackoffBase. Tests shrink it to milliseconds here
	// rather than by writing a package global, which raced.
	BackoffBase time.Duration
}

// consumerConfig is the shared durable-consumer tuning for every
// chora-payments SSE fan-out consumer: at-least-once with a 30s ack
// window, five delivery attempts, and the canonical _dlq.<subject>
// dead-letter routing.
//
// The dotted historical subscription id is safe as Name — eventbus
// sanitises it to a NATS-legal durable name internally.
func consumerConfig(name, subject string) eventbus.ConsumerConfig {
	return eventbus.ConsumerConfig{
		Name:       name,
		Subject:    subject,
		MaxDeliver: 5,
		AckWait:    30 * time.Second,
		Backoff: []time.Duration{
			1 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
		},
		DLQSubject: eventbus.DLQSubject(subject),
	}
}

// DefaultSubscriptionMap returns the canonical 28-subject → consumer map
// using DefaultSubscriptionName for the value. main.go calls this when
// per-subject env-var overrides are absent.
func DefaultSubscriptionMap() map[string]string {
	out := make(map[string]string, 28)
	for _, t := range SupportedSubjects() {
		out[t] = DefaultSubscriptionName(t)
	}
	return out
}

// -----------------------------------------------------------------------------
// PaymentEventBroker — public interface consumed by admin_handler.go.
// -----------------------------------------------------------------------------

// PaymentEventBroker is the hex port the SSE handler depends on. The
// concrete implementation is *Broker; the port keeps the http adapter from
// importing the eventbus directly.
type PaymentEventBroker interface {
	// Subscribe opens a per-HTTP-client feed scoped by tenant (empty
	// tenant = PLATFORM_OPERATOR cross-tenant) and aggregate (empty = all).
	// Returns the channel + cleanup closure the handler MUST call on
	// disconnect.
	Subscribe(ctx context.Context, tenantID, aggregateType string) (<-chan PaymentEvent, func(), error)
}

// -----------------------------------------------------------------------------
// Broker — concrete implementation.
// -----------------------------------------------------------------------------

// Broker is the in-process fan-out implementation.
type Broker struct {
	cfg BrokerConfig

	mu          sync.RWMutex
	subscribers map[string]*clientSub // client_id → sub
	closed      atomic.Bool

	// Counter of dropped messages per client (cumulative, for ops).
	droppedTotal atomic.Int64
}

// clientSub is one subscriber's filter + channel.
type clientSub struct {
	id            string
	tenantID      string // empty = all (PLATFORM_OPERATOR)
	aggregateType string // empty = all aggregates
	ch            chan PaymentEvent
	cleanedUp     atomic.Bool

	// closeMu serialises the broker fan-out send with the client's
	// cleanup-time channel close. Sender takes the read lock; closer
	// takes the write lock — so the channel is never observed in a
	// concurrent close + send state.
	closeMu sync.RWMutex
	// closed is true after cleanup() ran the close(ch). Sender checks
	// this under closeMu read lock before each send.
	closed bool
}

// NewBroker constructs a Broker. Returns error on missing required config.
func NewBroker(cfg BrokerConfig) (*Broker, error) {
	if cfg.Bus == nil {
		return nil, errors.New("event_broker: Bus required")
	}
	if len(cfg.SubscriptionsBySubject) == 0 {
		return nil, errors.New("event_broker: SubscriptionsBySubject must list at least one (subject, consumer)")
	}
	if cfg.ChannelBuffer <= 0 {
		cfg.ChannelBuffer = DefaultChannelBuffer
	}
	if cfg.DedupTTL <= 0 {
		cfg.DedupTTL = DefaultDedupTTL
	}
	if cfg.Inbox == nil {
		cfg.Inbox = idempotent.NewMemoryStore()
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = DefaultBackoffBase
	}
	return &Broker{
		cfg:         cfg,
		subscribers: make(map[string]*clientSub),
	}, nil
}

// -----------------------------------------------------------------------------
// Subscribe — opens a per-client channel + returns its cleanup closure.
// -----------------------------------------------------------------------------

// Subscribe satisfies PaymentEventBroker. Tenant-scoped fan-out + bounded
// channel + cleanup hook.
func (b *Broker) Subscribe(_ context.Context, tenantID, aggregateType string) (<-chan PaymentEvent, func(), error) {
	if b.closed.Load() {
		return nil, func() {}, errors.New("event_broker: broker shutting down")
	}
	id := newClientID()
	sub := &clientSub{
		id:            id,
		tenantID:      tenantID,
		aggregateType: aggregateType,
		ch:            make(chan PaymentEvent, b.cfg.ChannelBuffer),
	}
	b.mu.Lock()
	b.subscribers[id] = sub
	b.mu.Unlock()

	cleanup := func() {
		if sub.cleanedUp.Swap(true) {
			return
		}
		b.mu.Lock()
		delete(b.subscribers, id)
		b.mu.Unlock()
		// Close under closeMu, which is what the field was declared for.
		// b.mu alone is NOT enough: fanOut snapshots its matches under
		// b.mu.RLock and then RELEASES it before sending, so deleting the
		// subscriber here does not retract a pointer a fan-out already holds.
		// Closing outside closeMu let that fan-out send on a closed channel,
		// which panics the receiving goroutine rather than dropping an event.
		sub.closeMu.Lock()
		sub.closed = true
		close(sub.ch)
		sub.closeMu.Unlock()
	}

	return sub.ch, cleanup, nil
}

// SubscriberCount returns the current number of attached HTTP clients.
// Exposed for ops + tests.
func (b *Broker) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

// DroppedTotal returns the cumulative number of events that were dropped
// due to slow-client backpressure. Exposed for ops dashboards.
func (b *Broker) DroppedTotal() int64 {
	return b.droppedTotal.Load()
}

// -----------------------------------------------------------------------------
// Run — subscribes one durable consumer per (subject, consumer) and blocks
// until ctx is cancelled.
// -----------------------------------------------------------------------------

// Run drives the fan-out. Blocks until ctx is cancelled. Each (subject,
// consumer) pair gets one durable consumer on the bus. If Subscribe returns
// a non-cancel error (e.g. the consumer cannot be created), the pair logs
// the error and retries with bounded exponential backoff (backoffBase →
// 2× → … → backoffCap) while ctx is live.
//
// Run returns nil (or ctx.Err()) ONLY when ctx is cancelled — never because an
// individual subscription failed. This prevents a single missing/transient
// subscription from taking down the entire SSE feature.
func (b *Broker) Run(ctx context.Context) error {
	defer b.closed.Store(true)

	var wg sync.WaitGroup
	for subject, consumer := range b.cfg.SubscriptionsBySubject {
		subject := subject
		consumer := consumer
		wg.Add(1)
		go func() {
			defer wg.Done()
			delay := b.cfg.BackoffBase
			for {
				err := b.cfg.Bus.Subscribe(ctx, consumerConfig(consumer, subject), func(handlerCtx context.Context, msg eventbus.Message) error {
					return b.handleMessage(handlerCtx, subject, msg)
				})

				// Context cancelled or deadline exceeded — clean shutdown; stop retrying.
				if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				// Also stop if the parent context is done (ctx check is
				// authoritative; the returned error may be wrapped).
				if ctx.Err() != nil {
					return
				}

				// Transient / provisioning error — log and retry after backoff.
				log.Printf("payments/event_broker: consumer=%s subject=%s transient err=%v; retrying in %s", consumer, subject, err, delay)

				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}

				// Exponential backoff with cap.
				delay *= 2
				if delay > backoffCap {
					delay = backoffCap
				}
			}
		}()
	}
	wg.Wait()

	// Close all client channels on broker shutdown so SSE handlers wake up.
	b.mu.Lock()
	for _, sub := range b.subscribers {
		if !sub.cleanedUp.Swap(true) {
			// Same discipline as cleanup(): an in-flight fanOut may still
			// hold this pointer from a snapshot taken before b.mu was
			// acquired here.
			sub.closeMu.Lock()
			sub.closed = true
			close(sub.ch)
			sub.closeMu.Unlock()
		}
	}
	b.subscribers = make(map[string]*clientSub)
	b.mu.Unlock()

	return ctx.Err()
}

// -----------------------------------------------------------------------------
// handleMessage — per-bus-message handler. Decodes + fans out.
// -----------------------------------------------------------------------------

// handleMessage runs inside the eventbus consume loop. Returning nil
// acknowledges the message; returning an error triggers redelivery (and
// DLQ routing at MaxDeliver).
func (b *Broker) handleMessage(ctx context.Context, subject string, msg eventbus.Message) error {
	// Decode.
	ev, err := decodePaymentEvent(subject, msg.Payload, msg.Envelope)
	if err != nil {
		// Decode failures return an error → the bus redelivers → DLQ at
		// MaxDeliver. Don't loop forever in-process.
		log.Printf("payments/event_broker: decode failed subject=%s id=%s err=%v", subject, msg.Envelope.EventID, err)
		return err
	}

	// Idempotent dedup — drop replayed messages within DedupTTL.
	dedupeKey := ev.IdempotencyKey
	if dedupeKey == "" {
		dedupeKey = ev.EventID
	}
	if dedupeKey == "" {
		// Defensive: no dedup key at all → fan out anyway (replays will
		// double-emit but that's better than losing the event).
		b.fanOut(ev)
		return nil
	}

	// Process(key, ttl, fn) skips fn on replay → fanOut is the side effect
	// we want to dedupe.
	if err := b.cfg.Inbox.Process(ctx, dedupeKey, b.cfg.DedupTTL, func() error {
		b.fanOut(ev)
		return nil
	}); err != nil {
		// Inbox failure (memory-store can't return error on Process; pg
		// backend might) — return an error so the message redelivers.
		log.Printf("payments/event_broker: inbox process failed key=%s err=%v", dedupeKey, err)
		return err
	}

	return nil
}

// fanOut delivers ev to every matching subscriber. Non-blocking sends with
// drop-oldest backpressure semantics for slow clients.
func (b *Broker) fanOut(ev PaymentEvent) {
	b.mu.RLock()
	// Copy the matching subs into a local slice so we can release the
	// read lock before sending (sends may take time on slow clients).
	matches := make([]*clientSub, 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		if !subscriberMatches(sub, ev) {
			continue
		}
		matches = append(matches, sub)
	}
	b.mu.RUnlock()

	for _, sub := range matches {
		b.deliver(sub, ev)
	}
}

// deliver performs the non-blocking send to one subscriber, under that
// subscriber's closeMu read lock.
//
// The lock is the point. b.mu is released above so a slow client cannot stall
// the whole fan-out, which means `matches` can outlive the subscriber: cleanup
// may delete and close it while this loop still holds the pointer. Sending on
// a closed channel PANICS, so the failure mode was a crashed broker goroutine
// rather than a dropped event. closeMu + closed were declared and documented
// for exactly this and were never wired to anything.
//
// Read lock, so concurrent fan-outs to different subscribers still proceed in
// parallel; only a close contends. Every touch of sub.ch, including the
// drop-oldest receive, is inside it.
func (b *Broker) deliver(sub *clientSub, ev PaymentEvent) {
	sub.closeMu.RLock()
	defer sub.closeMu.RUnlock()
	if sub.closed {
		// Unsubscribed between the snapshot and now. Not a drop worth
		// counting: nobody is listening.
		return
	}
	select {
	case sub.ch <- ev:
		// delivered
	default:
		// Channel full → drop oldest + try once more.
		select {
		case <-sub.ch:
			b.droppedTotal.Add(1)
		default:
		}
		select {
		case sub.ch <- ev:
			// delivered after drop-oldest
		default:
			// Still full (slow client races) — give up.
			b.droppedTotal.Add(1)
		}
	}
}

// subscriberMatches returns true iff the subscriber's filter accepts ev.
func subscriberMatches(sub *clientSub, ev PaymentEvent) bool {
	if sub.tenantID != "" && sub.tenantID != ev.TenantID {
		return false
	}
	if sub.aggregateType != "" && sub.aggregateType != ev.AggregateType {
		return false
	}
	return true
}

// newClientID returns a UUIDv7 (sortable + globally unique).
func newClientID() string {
	return fmt.Sprintf("brkclient-%s", uuid.Must(uuid.NewV7()).String())
}

// Compile-time interface check.
var _ PaymentEventBroker = (*Broker)(nil)
