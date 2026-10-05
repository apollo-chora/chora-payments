// admin_tenants_router.go — multiplexes the tenant-scoped admin paths:
//
//	GET  /api/v1/admin/tenants/{tenantId}/invoices       → invoices handler
//	POST /api/v1/admin/tenants/{tenantId}/billing-portal → portal handler
//
// Mounted at the trailing-slash prefix `/api/v1/admin/tenants/`. Unknown
// suffixes return 404 with `tenant_subresource_unknown` so a typo on the
// gateway surfaces immediately instead of silently 5xx'ing.
package http

import (
	"net/http"
	"strings"
)

// AdminTenantsRouter dispatches /api/v1/admin/tenants/{tenantId}/{subresource}.
type AdminTenantsRouter struct {
	invoices *AdminInvoicesHandler
	portal   *AdminBillingPortalHandler
}

// NewAdminTenantsRouter constructs the dispatcher. Both handlers are
// REQUIRED — nil deps short-circuit to 503 via the leaf handlers.
func NewAdminTenantsRouter(invoices *AdminInvoicesHandler, portal *AdminBillingPortalHandler) *AdminTenantsRouter {
	return &AdminTenantsRouter{invoices: invoices, portal: portal}
}

// ServeHTTP routes by the trailing subresource. Path shape:
//
//	/api/v1/admin/tenants/{tenantId}/{subresource}
func (r *AdminTenantsRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	const prefix = "/api/v1/admin/tenants/"
	tail := strings.TrimPrefix(req.URL.Path, prefix)
	idx := strings.IndexByte(tail, '/')
	if idx <= 0 {
		writeInvoicesError(w, http.StatusBadRequest, "validation_failed", "tenant_id required in path")
		return
	}
	subresource := tail[idx+1:]
	// Strip any trailing query string (mux doesn't but be defensive).
	if q := strings.IndexByte(subresource, '?'); q >= 0 {
		subresource = subresource[:q]
	}
	switch subresource {
	case "invoices":
		r.invoices.ServeHTTP(w, req)
	case "billing-portal":
		r.portal.ServeHTTP(w, req)
	default:
		writeInvoicesError(w, http.StatusNotFound, "tenant_subresource_unknown",
			"unrecognised subresource '/"+subresource+"' on /api/v1/admin/tenants/{tenantId}")
	}
}

var _ http.Handler = (*AdminTenantsRouter)(nil)
