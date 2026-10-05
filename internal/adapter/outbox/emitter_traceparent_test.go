package outbox

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
)

// assertValidW3CTraceparent fails the test unless s is a structurally valid
// W3C traceparent: 4 dash-separated hex chunks of lengths 2/32/16/2 with
// non-zero trace_id + span_id.
func assertValidW3CTraceparent(t *testing.T, s string) {
	t.Helper()
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		t.Fatalf("traceparent %q: want 4 dash-separated parts, got %d", s, len(parts))
	}
	for i, want := range []int{2, 32, 16, 2} {
		if len(parts[i]) != want {
			t.Fatalf("traceparent %q: part %d len=%d want %d", s, i, len(parts[i]), want)
		}
		if _, err := hex.DecodeString(parts[i]); err != nil {
			t.Fatalf("traceparent %q: part %d not hex: %v", s, i, err)
		}
	}
	if strings.Trim(parts[1], "0") == "" {
		t.Fatalf("traceparent %q: trace_id is all-zero (W3C invalid)", s)
	}
	if strings.Trim(parts[2], "0") == "" {
		t.Fatalf("traceparent %q: span_id is all-zero (W3C invalid)", s)
	}
}

// TestEmitTraceparent_MintsRootWhenContextEmpty pins the ROOT-at-payments
// contract: a Stripe webhook carries no upstream traceparent, so when the
// emit context has none the Emitter MUST mint a fresh W3C root. Without it
// the outbox row (and thus the Pub/Sub `traceparent` attribute) is empty and
// the downstream chora-tenancy egg-provision emit rejects the derived event
// with `envelope: traceparent is required` (the egg purchase→provision chain
// blocker, 2026-07-03).
func TestEmitTraceparent_MintsRootWhenContextEmpty(t *testing.T) {
	t.Parallel()
	got := emitTraceparent(context.Background())
	if got == "" {
		t.Fatal("emitTraceparent(background) = \"\"; want a minted W3C root")
	}
	assertValidW3CTraceparent(t, got)
}

// TestEmitTraceparent_ContinuesInboundContextTrace verifies that when the
// webhook handler ran under an OTel-propagating middleware (context carries a
// valid traceparent) the Emitter continues that trace verbatim rather than
// minting a disconnected root.
func TestEmitTraceparent_ContinuesInboundContextTrace(t *testing.T) {
	t.Parallel()
	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := tracing.WithTraceparent(context.Background(), inbound)
	if got := emitTraceparent(ctx); got != inbound {
		t.Fatalf("emitTraceparent(ctx) = %q; want inbound %q", got, inbound)
	}
}

// TestBuildEnvelope_StampsTraceparent locks the embedded envelope proto — the
// published payload's Envelope.Traceparent must carry the same value the
// Pub/Sub attribute does (contract honesty per the "traceparent mandatory in
// the event envelope" invariant).
func TestBuildEnvelope_StampsTraceparent(t *testing.T) {
	t.Parallel()
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	env := buildEnvelope(
		"11111111-1111-7111-8111-111111111111",
		"01970000-0000-7000-e000-000000000001",
		"idem-1",
		tp,
		time.Date(2026, 7, 3, 1, 0, 0, 0, time.UTC),
	)
	if env.GetTraceparent() != tp {
		t.Fatalf("env.Traceparent = %q; want %q", env.GetTraceparent(), tp)
	}
}
