// role_gate.go — admin-role recognition + ctx flagging for the H+
// Transaction History endpoints.
//
// 4 admin roles per ADR-165 (PROPOSED 2026-05-26) + openapi/payments-
// admin.yaml security schema:
//
//	TENANT_ADMIN       — full read + refund within own tenant.
//	OWNER              — full read + refund within own tenant (same caps).
//	AUDITOR            — read-only across list / export / stream within
//	                     own tenant; CANNOT refund.
//	PLATFORM_OPERATOR  — cross-tenant read + refund. The single role
//	                     permitted to bypass RLS on chora-payments.
//
// Legacy roles (training-admin, platform-admin) accepted for backward
// compat with the existing admin_handler.go shape until the gateway
// completes role coining (handled by Agent A5 in this wave).
package http

import (
	"context"
	"errors"
	"strings"

	"github.com/apollo-chora/chora-payments/internal/adapter/repo/rls"
)

// AdminRole is the canonical role string the admin handlers recognise.
type AdminRole string

const (
	RoleTenantAdmin      AdminRole = "TENANT_ADMIN"
	RoleOwner            AdminRole = "OWNER"
	RoleAuditor          AdminRole = "AUDITOR"
	RolePlatformOperator AdminRole = "PLATFORM_OPERATOR"

	// Legacy roles (pre-ADR-165) still accepted on input. Treated as
	// TENANT_ADMIN-equivalent capability surface.
	roleLegacyTrainingAdmin AdminRole = "training-admin"
	roleLegacyPlatformAdmin AdminRole = "platform-admin"
)

// ErrUnknownRole is returned by ParseAdminRole when the supplied role
// string is not one of the 4 canonical + 2 legacy values.
var ErrUnknownRole = errors.New("role not in admin allow-list")

// ParseAdminRole canonicalises the X-Chora-Role header value into one
// of the AdminRole constants. Comparison is case-sensitive for the
// canonical 4 (per the IdP coining) + case-insensitive for the legacy
// 2 (pre-coining).
//
// chora-gateway stampRoleHeader comma-joins multi-role principals
// (e.g. "TENANT_ADMIN,INSTRUCTOR" — see Y3 contract-alignment audit
// 2026-05-26 sub-drift 6.A). When multiple roles are present, the
// highest-privilege match wins: PLATFORM_OPERATOR > OWNER >
// TENANT_ADMIN > legacy > AUDITOR. Unknown roles are silently
// ignored — at least one must canonicalise or ErrUnknownRole returns.
func ParseAdminRole(s string) (AdminRole, error) {
	seen := make(map[AdminRole]bool, 6)
	for _, candidate := range strings.Split(s, ",") {
		c := strings.TrimSpace(candidate)
		if c == "" {
			continue
		}
		switch c {
		case "TENANT_ADMIN":
			seen[RoleTenantAdmin] = true
			continue
		case "OWNER":
			seen[RoleOwner] = true
			continue
		case "AUDITOR":
			seen[RoleAuditor] = true
			continue
		case "PLATFORM_OPERATOR":
			seen[RolePlatformOperator] = true
			continue
		}
		switch strings.ToLower(c) {
		case "training-admin":
			seen[roleLegacyTrainingAdmin] = true
		case "platform-admin":
			seen[roleLegacyPlatformAdmin] = true
		}
	}
	priority := []AdminRole{
		RolePlatformOperator,
		RoleOwner,
		RoleTenantAdmin,
		roleLegacyTrainingAdmin,
		roleLegacyPlatformAdmin,
		RoleAuditor,
	}
	for _, r := range priority {
		if seen[r] {
			return r, nil
		}
	}
	return "", ErrUnknownRole
}

// ApplyOperatorContext flags the request context as RLS-bypass-permitted
// IFF the role is PLATFORM_OPERATOR. Tenant-scoped roles get the ctx
// back unchanged.
//
// Caller MUST have parsed the role via ParseAdminRole + verified it is
// in the allow-list BEFORE calling this function.
func ApplyOperatorContext(ctx context.Context, role AdminRole) context.Context {
	if role == RolePlatformOperator {
		return rls.WithRLSBypass(ctx)
	}
	return ctx
}

// CanRefund reports whether the role is permitted to issue refunds.
// AUDITOR is the only canonical role explicitly forbidden; PLATFORM_OPERATOR
// + TENANT_ADMIN + OWNER are all permitted.
func CanRefund(role AdminRole) bool {
	switch role {
	case RoleTenantAdmin, RoleOwner, RolePlatformOperator,
		roleLegacyTrainingAdmin, roleLegacyPlatformAdmin:
		return true
	}
	return false
}
