// role_gate_test.go — RED→GREEN tests for the admin role gate. Recognises
// PLATFORM_OPERATOR + sets rls.WithRLSBypass on ctx so the
// purchase_history pg adapter skips the SET LOCAL chora.tenant_id step.
package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/apollo-chora/chora-payments/internal/adapter/repo/rls"
)

func TestParseAdminRole_Allows4CanonicalRoles(t *testing.T) {
	t.Parallel()
	for _, role := range []string{
		"TENANT_ADMIN", "OWNER", "AUDITOR", "PLATFORM_OPERATOR",
	} {
		got, err := ParseAdminRole(role)
		if err != nil {
			t.Errorf("ParseAdminRole(%q) err=%v want nil", role, err)
		}
		if string(got) != role {
			t.Errorf("ParseAdminRole(%q) = %q want %q", role, got, role)
		}
	}
}

func TestParseAdminRole_AlsoAcceptsLegacyRoles(t *testing.T) {
	t.Parallel()
	for _, role := range []string{"training-admin", "platform-admin"} {
		_, err := ParseAdminRole(role)
		if err != nil {
			t.Errorf("ParseAdminRole(%q) err=%v want nil (legacy compat)", role, err)
		}
	}
}

func TestParseAdminRole_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if _, err := ParseAdminRole("learner"); err == nil {
		t.Errorf("ParseAdminRole(learner) returned nil; want error")
	}
}

func TestApplyOperatorContext_SetsRLSBypass_OnPlatformOperator(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/v1/admin/payments/purchases", nil)
	req.Header.Set("X-Chora-Role", "PLATFORM_OPERATOR")

	ctx := ApplyOperatorContext(req.Context(), RolePlatformOperator)
	if !rls.IsRLSBypassed(ctx) {
		t.Errorf("PLATFORM_OPERATOR ctx missing RLS bypass flag")
	}
}

func TestApplyOperatorContext_DoesNotBypass_OnTenantAdmin(t *testing.T) {
	t.Parallel()
	ctx := ApplyOperatorContext(context.Background(), RoleTenantAdmin)
	if rls.IsRLSBypassed(ctx) {
		t.Errorf("TENANT_ADMIN ctx unexpectedly has RLS bypass")
	}
}
