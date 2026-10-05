// tier_handlers_coverage_test.go — residual branches of the change-tier /
// preview-tier handler helpers (constructor Now default, status clamping,
// first-non-empty).
package http

import (
	"testing"

	"github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

func TestNewChangeTierHandler_DefaultsNow(t *testing.T) {
	t.Parallel()
	h := NewChangeTierHandler(ChangeTierHandlerDeps{})
	if h.deps.Now == nil {
		t.Fatal("Now not defaulted")
	}
	if got := h.deps.Now(); got.IsZero() {
		t.Fatal("default Now returns zero time")
	}
}

func TestNewPreviewTierHandler_DefaultsNow(t *testing.T) {
	t.Parallel()
	h := NewPreviewTierHandler(PreviewTierHandlerDeps{})
	if h.deps.Now == nil {
		t.Fatal("Now not defaulted")
	}
}

func TestMapStripeStatus(t *testing.T) {
	t.Parallel()
	if got := mapStripeStatus(string(tenant_addon_purchase.StatusActive)); got != tenant_addon_purchase.StatusActive {
		t.Errorf("valid status=%q", got)
	}
	if got := mapStripeStatus("past_due_friendly"); got != tenant_addon_purchase.StatusPending {
		t.Errorf("unknown status=%q, want pending clamp", got)
	}
	if got := mapStripeStatus(""); got != tenant_addon_purchase.StatusPending {
		t.Errorf("empty status=%q, want pending clamp", got)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()
	if got := firstNonEmpty("a", "b"); got != "a" {
		t.Errorf("got=%q", got)
	}
	if got := firstNonEmpty("", "b"); got != "b" {
		t.Errorf("fallback=%q", got)
	}
}
