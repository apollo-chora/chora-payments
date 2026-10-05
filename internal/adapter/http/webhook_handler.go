// Package http carries the chora-payments HTTP handlers: Stripe webhook
// ingress + admin queries + health probes.
//
// Per ADR-164 §2.2, the receive path is in-process:
//
//	Stripe HTTPS POST → chora-gateway proxy → chora-payments
//	/v1/webhooks/stripe → HMAC verify → dedup (stripe_webhook_events) →
//	aggregate transition + outbox publish → 200 OK to Stripe.
//
// No separate Cloud Function bridge. Cloud Armor priority-102 allow rule
// (Infra-side, c947f817) admits the request to the GCLB; the
// Stripe-Signature HMAC is the only trust anchor inside the handler.
package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"
	stripeWebhook "github.com/stripe/stripe-go/v78/webhook"

	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const (
	// MaxWebhookBodyBytes caps the inbound webhook body size. Stripe webhook
	// payloads are typically <50KB; 1MB is a generous safety cap.
	MaxWebhookBodyBytes = 1 << 20 // 1 MiB

	// SignatureToleranceSeconds is the maximum allowed clock skew between
	// Stripe's signature timestamp and chora-payments' clock. Matches
	// Stripe's recommended default (300s).
	SignatureToleranceSeconds = 300
)

// WebhookDeps wires the webhook handler.
type WebhookDeps struct {
	// WebhookSecret is the Stripe webhook signing secret (whsec_…). Empty
	// secret short-circuits the handler to 503 (no HMAC trust anchor).
	WebhookSecret string

	// WebhookEvents is the global dedup gate for inbound Stripe events.
	WebhookEvents wh.Repo

	// Dispatcher routes a verified+dedupes webhook to the matching
	// aggregate handler. Set by cmd/server boot.
	Dispatcher WebhookDispatcher

	// Now overrides time.Now for deterministic tests.
	Now func() time.Time
}

// WebhookDispatcher is the hexagonal port for routing a verified Stripe
// event to the matching aggregate handler. cmd/server wires the
// production dispatcher (PaymentService internal); tests wire a stub.
type WebhookDispatcher interface {
	// Dispatch processes a verified Stripe event. Returns the resolved
	// (aggregate, purchase_id) for audit-trail bookkeeping in
	// stripe_webhook_events. Errors propagate as 5xx so Stripe retries.
	Dispatch(ctx interface{}, event *stripeGo.Event) (wh.AggregateType, string, error)
}

// NewWebhookHandler returns the HTTP handler for POST /v1/webhooks/stripe.
func NewWebhookHandler(deps WebhookDeps) http.HandlerFunc {
	now := deps.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if strings.TrimSpace(deps.WebhookSecret) == "" {
			log.Printf("payments: webhook 503 — STRIPE_WEBHOOK_SECRET unset")
			writeError(w, http.StatusServiceUnavailable, "webhook secret not configured")
			return
		}
		if deps.WebhookEvents == nil || deps.Dispatcher == nil {
			log.Printf("payments: webhook 503 — dependencies not wired (events=%v dispatcher=%v)",
				deps.WebhookEvents != nil, deps.Dispatcher != nil)
			writeError(w, http.StatusServiceUnavailable, "webhook handler not fully wired")
			return
		}

		// Read body (capped).
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxWebhookBodyBytes))
		if err != nil {
			writeError(w, http.StatusBadRequest, "read body: "+err.Error())
			return
		}

		// Verify Stripe-Signature HMAC.
		sig := r.Header.Get("Stripe-Signature")
		if sig == "" {
			writeError(w, http.StatusUnauthorized, "Stripe-Signature header required")
			return
		}
		// IgnoreAPIVersionMismatch: account default API version is 2020-03-02
		// (set at account creation, immutable); stripe-go 78.12.0 expects
		// 2024-04-10. The webhook endpoint's api_version setting translates the
		// rendered body for webhook deliveries, but the validator inside
		// stripe-go cross-checks the api_version field embedded in the event
		// payload — that field reflects the account default and triggers a
		// hard reject. Ignoring the mismatch is the documented workaround
		// (Stripe's own error message recommends it) and the HMAC signature
		// remains the trust anchor. The webhook endpoint api_version pin
		// remains in place for forward compatibility — re-evaluate when the
		// account default is upgraded.
		event, err := stripeWebhook.ConstructEventWithOptions(body, sig, deps.WebhookSecret, stripeWebhook.ConstructEventOptions{
			Tolerance:                SignatureToleranceSeconds * time.Second,
			IgnoreAPIVersionMismatch: true,
		})
		if err != nil {
			// Forensics: the signature failed BEFORE the event was trusted, but
			// the unverified id/type are still useful — Stripe RETRIES a non-2xx
			// (test mode: 3× over a few hours, then DROPS the event), so a
			// persistent signature failure means a captured payment is silently
			// lost. The usual cause is a stale/duplicate Stripe webhook endpoint
			// signing with a different whsec_ than STRIPE_WEBHOOK_SECRET. Logging
			// the id/type makes a stranded event traceable.
			id, typ := bestEffortEventMeta(body)
			log.Printf("payments: webhook signature verify failed (unverified event_id=%q type=%q): %v — Stripe will retry then drop; check for a stale/duplicate webhook endpoint secret",
				id, typ, err)
			writeError(w, http.StatusBadRequest, "signature verification failed")
			return
		}

		// Dedup gate — INSERT stripe_webhook_events; UNIQUE violation means
		// the event_id was seen before. Distinguish a genuinely-PROCESSED
		// event (idempotent skip, 200) from a prior FAILED/incomplete dispatch
		// (the up-front Insert recorded the row, then dispatch errored and
		// MarkFailed left processed_at NULL). A failed event MUST be
		// re-dispatched on Stripe's retry — silently skipping it strands the
		// payment forever (the original WS-2 mana-credit bug: a transient
		// downstream RLS error meant the wallet was never credited and the
		// retry got "already processed"). Re-dispatch is safe because the
		// aggregate transition (UPSERT) + downstream subscriber are idempotent.
		whEvent, _ := wh.New(event.ID, string(event.Type), now())
		if err := deps.WebhookEvents.Insert(r.Context(), whEvent); err != nil {
			if !errors.Is(err, wh.ErrAlreadyProcessed) {
				log.Printf("payments: webhook insert failed event=%s: %v", event.ID, err)
				writeError(w, http.StatusInternalServerError, "dedup insert failed")
				return
			}
			existing, gerr := deps.WebhookEvents.GetByEventID(r.Context(), event.ID)
			if gerr != nil {
				// Can't tell processed vs failed — default to skip (the prior
				// delivery may have succeeded; re-dispatch on an unknown state
				// is riskier than a one-off skip). Log loudly for forensics.
				log.Printf("payments: webhook event=%s exists, state lookup failed (%v) — idempotent skip", event.ID, gerr)
				writeOK(w, map[string]any{"received": true, "skipped": "duplicate"})
				return
			}
			if existing.IsProcessed() {
				log.Printf("payments: webhook event=%s already processed — idempotent skip", event.ID)
				writeOK(w, map[string]any{"received": true, "skipped": "duplicate"})
				return
			}
			// Row exists but unprocessed → a prior dispatch failed. Fall
			// through to re-dispatch (the MarkProcessed below records success).
			log.Printf("payments: webhook event=%s exists but unprocessed (prior dispatch failed: %q) — re-dispatching",
				event.ID, existing.ProcessingError)
		}

		// Dispatch to the aggregate handler.
		agg, purchaseID, derr := deps.Dispatcher.Dispatch(r.Context(), &event)
		if derr != nil {
			log.Printf("payments: webhook dispatch failed event=%s type=%s err=%v",
				event.ID, event.Type, derr)
			_ = deps.WebhookEvents.MarkFailed(r.Context(), event.ID, derr.Error())
			writeError(w, http.StatusInternalServerError, "dispatch failed")
			return
		}

		// Mark processed.
		if err := deps.WebhookEvents.MarkProcessed(r.Context(), event.ID, agg, purchaseID, now()); err != nil {
			log.Printf("payments: webhook mark-processed failed event=%s: %v", event.ID, err)
			// Already dispatched; don't 500 (Stripe would retry). Log + 200.
		}
		writeOK(w, map[string]any{"received": true, "aggregate": agg, "purchase_id": purchaseID})
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func writeOK(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

// bestEffortEventMeta extracts the Stripe event id + type from a raw (and
// possibly UNVERIFIED / malformed) webhook body for forensic logging only.
// Never returns an error — a malformed body yields empty strings. This MUST
// NOT be used for any trust decision (the HMAC signature is the only trust
// anchor); it exists purely so a signature-rejected delivery is traceable.
func bestEffortEventMeta(body []byte) (id, eventType string) {
	var meta struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	_ = json.Unmarshal(body, &meta)
	return meta.ID, meta.Type
}

// Compile-time guard — the writeOK/writeError helpers are unused externally
// (yet); referenced via fmt.Stringer below to satisfy goimports.
var _ = fmt.Sprintf
