// Package rls (chora-payments local) carries the context-key plumbing that
// marks a request as "permitted to read across tenants" — i.e. RLS-bypass.
//
// This is DISTINCT from libs/chora-go-common/rls (which owns the canonical
// SET LOCAL chora.tenant_id session-var emission). The shared lib stays
// unchanged; the bypass decision is local to chora-payments because
// chora-payments owns the ONE permitted single-domain bypass on the
// platform (per ADR-165 PROPOSED 2026-05-26 — H+ Transaction History
// PLATFORM_OPERATOR cross-tenant read).
//
// Invariants enforced by callers:
//
//   - WithRLSBypass(ctx) MUST only be set after the HTTP layer has verified
//     the caller's role is PLATFORM_OPERATOR (see internal/adapter/http/
//     role_gate.go).
//   - The flag is RLS-bypass-on-read ONLY. Writes still go through the
//     normal tenant-scoped path. The purchase_history pg adapter is the
//     single intended consumer of IsRLSBypassed today.
//
// Per .claude/rules/ddd-enforcement.md: cross-DB queries remain FORBIDDEN.
// This bypass is INTRA-domain (chora_payments only) — it skips the
// SET LOCAL chora.tenant_id step so the operator can SELECT across
// every tenant row in the 7 Purchase tables. No cross-database access
// occurs and the DDD invariant is preserved.
package rls

import "context"

// rlsBypassKey is the unexported context-key sentinel. Unexported so
// callers cannot reach in and flip the flag without going through the
// WithRLSBypass entry-point (which the role-gate is the only intended
// caller of).
type rlsBypassKey struct{}

// WithRLSBypass returns a child context flagged as RLS-bypass-permitted.
//
// MUST only be called after the HTTP role-gate has verified the caller
// holds PLATFORM_OPERATOR. The pg layer asserts on this invariant.
func WithRLSBypass(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, rlsBypassKey{}, true)
}

// IsRLSBypassed reports whether the supplied context has been flagged
// via WithRLSBypass. Safe for nil ctx (returns false).
func IsRLSBypassed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(rlsBypassKey{}).(bool)
	return v
}
