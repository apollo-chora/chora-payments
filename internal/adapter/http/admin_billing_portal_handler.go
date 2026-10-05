// admin_billing_portal_handler.go — H+ Billing tenant-scoped Stripe
// Customer Portal session mint.
//
//	POST /api/v1/admin/tenants/{tenantId}/billing-portal
//	Body: { "return_url": "https://hplus.chora.site/h/billing" }
//	Resp: { "url": "https://billing.stripe.com/p/session/..." }
//
// FE flow: click "Manage billing in Stripe" → call this endpoint → set
// `window.location.href = resp.url` → Stripe-hosted page handles
// payment-method management + invoice downloads + subscription cancel
// (cancel events return via the customer.subscription.deleted webhook).
//
// Resolves the tenant's Stripe Customer ID via the TenantAddonPurchase
// repo. 422 `no_stripe_customer` when the tenant has no active
// subscriptions — the FE renders "Subscribe to an add-on first" rather
// than 500ing.
package http

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-common/tracing"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// AdminBillingPortalHandlerDeps wires the dependencies.
type AdminBillingPortalHandlerDeps struct {
	AddonPurchase tap.Repo
	Stripe        stripeadapter.Client
}

// AdminBillingPortalHandler serves POST /api/v1/admin/tenants/{tenantId}/billing-portal.
type AdminBillingPortalHandler struct {
	deps AdminBillingPortalHandlerDeps
}

// NewAdminBillingPortalHandler constructs the handler.
func NewAdminBillingPortalHandler(deps AdminBillingPortalHandlerDeps) *AdminBillingPortalHandler {
	return &AdminBillingPortalHandler{deps: deps}
}

// BillingPortalRequest is the JSON body.
type BillingPortalRequest struct {
	ReturnURL string `json:"return_url"`
}

// BillingPortalResponse is the JSON response.
type BillingPortalResponse struct {
	URL string `json:"url"`
}

// ServeHTTP routes POST → handle; everything else → 405.
func (h *AdminBillingPortalHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeInvoicesError(w, http.StatusMethodNotAllowed, "method_not_allowed", "POST only")
		return
	}
	h.handle(w, r)
}

func (h *AdminBillingPortalHandler) handle(w http.ResponseWriter, r *http.Request) {
	if h.deps.AddonPurchase == nil || h.deps.Stripe == nil {
		writeInvoicesError(w, http.StatusServiceUnavailable, "unwired", "billing-portal handler not wired")
		return
	}
	tenantID := extractTenantIDFromPortalPath(r.URL.Path)
	if tenantID == "" {
		writeInvoicesError(w, http.StatusBadRequest, "validation_failed", "tenant_id required in path")
		return
	}
	var req BillingPortalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeInvoicesError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	req.ReturnURL = strings.TrimSpace(req.ReturnURL)
	if req.ReturnURL == "" {
		writeInvoicesError(w, http.StatusBadRequest, "validation_failed", "return_url required")
		return
	}
	ctx := tracing.WithTenantID(r.Context(), tenantID)

	customerID, err := h.deps.AddonPurchase.GetStripeCustomerIDByTenant(ctx, tenantID)
	if err != nil {
		log.Printf("payments/admin/billing-portal: GetStripeCustomerIDByTenant err: %v", err)
		writeInvoicesError(w, http.StatusInternalServerError, "lookup_failed", "internal lookup error")
		return
	}
	if customerID == "" {
		writeInvoicesError(w, http.StatusUnprocessableEntity, "no_stripe_customer",
			"tenant has no Stripe Customer yet; subscribe to an add-on first")
		return
	}

	out, err := h.deps.Stripe.CreateBillingPortalSession(ctx, stripeadapter.CreateBillingPortalSessionInput{
		StripeCustomerID: customerID,
		ReturnURL:        req.ReturnURL,
	})
	if err != nil {
		log.Printf("payments/admin/billing-portal: Stripe CreateBillingPortalSession err: %v", err)
		writeInvoicesError(w, http.StatusBadGateway, "stripe_error", "Stripe portal session failed: "+err.Error())
		return
	}
	writeInvoicesJSON(w, http.StatusOK, BillingPortalResponse{URL: out.URL})
}

// extractTenantIDFromPortalPath pulls the {tenantId} segment out of
// `/api/v1/admin/tenants/{tenantId}/billing-portal`.
func extractTenantIDFromPortalPath(path string) string {
	const prefix = "/api/v1/admin/tenants/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	tail := path[len(prefix):]
	idx := strings.IndexByte(tail, '/')
	if idx <= 0 {
		return ""
	}
	tenantID := tail[:idx]
	if tail[idx+1:] != "billing-portal" {
		return ""
	}
	return tenantID
}

var _ http.Handler = (*AdminBillingPortalHandler)(nil)
