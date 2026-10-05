// bypass_test.go — RED→GREEN tests for the RLS-bypass context helper.
//
// Per ADR-165 (PROPOSED) the PLATFORM_OPERATOR role is permitted to read
// across tenants on chora-payments' admin-only purchase_history surface.
// WithRLSBypass marks the context; IsRLSBypassed reports membership; the
// purchase_history pg adapter consults the flag before calling rls.ApplySession.
package rls

import (
	"context"
	"testing"
)

func TestIsRLSBypassed_NoFlag_ReturnsFalse(t *testing.T) {
	t.Parallel()
	if IsRLSBypassed(context.Background()) {
		t.Errorf("IsRLSBypassed(empty ctx) = true, want false")
	}
}

func TestWithRLSBypass_FlagsTheContext(t *testing.T) {
	t.Parallel()
	ctx := WithRLSBypass(context.Background())
	if !IsRLSBypassed(ctx) {
		t.Errorf("IsRLSBypassed(WithRLSBypass(ctx)) = false, want true")
	}
}

func TestWithRLSBypass_DoesNotMutateParent(t *testing.T) {
	t.Parallel()
	parent := context.Background()
	child := WithRLSBypass(parent)
	if IsRLSBypassed(parent) {
		t.Errorf("WithRLSBypass leaked to parent ctx")
	}
	if !IsRLSBypassed(child) {
		t.Errorf("child ctx missing the bypass flag")
	}
}

func TestWithRLSBypass_NilCtxIsHandledGracefully(t *testing.T) {
	t.Parallel()
	// IsRLSBypassed(nil) must not panic; defaults to false.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("IsRLSBypassed(nil) panicked: %v", r)
		}
	}()
	if IsRLSBypassed(nil) {
		t.Errorf("IsRLSBypassed(nil) = true, want false")
	}
}

func TestWithRLSBypass_NilCtxDefaultsToBackground(t *testing.T) {
	t.Parallel()
	ctx := WithRLSBypass(nil)
	if ctx == nil {
		t.Fatal("WithRLSBypass(nil) returned nil ctx")
	}
	if !IsRLSBypassed(ctx) {
		t.Error("flag not set on defaulted ctx")
	}
	// Nil ctx is safe to inspect.
	if IsRLSBypassed(nil) {
		t.Error("IsRLSBypassed(nil)=true, want false")
	}
	// Non-bypassed value type falls back to false.
	plain := context.WithValue(context.Background(), "other-key", true)
	if IsRLSBypassed(plain) {
		t.Error("IsRLSBypassed with unrelated value=true")
	}
}
