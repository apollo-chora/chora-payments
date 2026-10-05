// admin_invoices_handler.go — H+ Billing tenant-scoped invoice list.
//
//	GET /api/v1/admin/tenants/{tenantId}/invoices?limit=&starting_after=
//
// The gateway aggregator forwards here after rewriting the `me` alias
// to the resolved tenant ID. This handler:
//
//  1. Resolves the tenant's Stripe Customer ID via the TenantAddonPurchase
//     repo (1 Customer per tenant by CHO-1762 contract; lookup returns
//     "" when the tenant has no purchases yet).
//  2. Empty case → 200 with empty `items` list (NOT 404 — new tenants
//     legitimately have no invoices and the FE renders an empty state).
//  3. Calls Stripe invoice.List paginated.
//  4. For each invoice's stripe_subscription_id, looks up the matching
//     TenantAddonPurchase row to synthesise a description field as
//     `{addon_code}:{tier_code}`. FE i18n resolves to human labels.
//
// Auth is by network — chora-payments is internal-only; the originating
// admin token was verified at the gateway. The gateway stamps the
// resolved tenant_id into the path, so this handler does NOT re-auth
// against a body field.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/apollo-chora/chora-common/tracing"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// AdminInvoicesHandlerDeps wires the dependencies.
type AdminInvoicesHandlerDeps struct {
	AddonPurchase tap.Repo
	Stripe        stripeadapter.Client
}

// AdminInvoicesHandler serves GET /api/v1/admin/tenants/{tenantId}/invoices.
type AdminInvoicesHandler struct {
	deps AdminInvoicesHandlerDeps
}

// NewAdminInvoicesHandler constructs the handler.
func NewAdminInvoicesHandler(deps AdminInvoicesHandlerDeps) *AdminInvoicesHandler {
	return &AdminInvoicesHandler{deps: deps}
}

// InvoiceRow is one row in the response.
type InvoiceRowDTO struct {
	StripeInvoiceID  string `json:"stripe_invoice_id"`
	Number           string `json:"number"`
	PeriodStart      string `json:"period_start"` // RFC3339
	PeriodEnd        string `json:"period_end"`
	Status           string `json:"status"`
	TotalCents       int64  `json:"total_cents"`
	Currency         string `json:"currency"`
	HostedInvoiceURL string `json:"hosted_invoice_url"`
	InvoicePDFURL    string `json:"invoice_pdf_url"`
	// Description is the synthesised "{addon_code}:{tier_code}" label
	// the FE i18n resolves to a human string. Empty when the invoice's
	// subscription ID doesn't match any local TAP row (e.g. data drift,
	// closure-bound row purged before its final invoice landed).
	Description string `json:"description"`
}

// InvoicesListResponse is the wire shape.
type InvoicesListResponse struct {
	Items      []InvoiceRowDTO `json:"items"`
	NextCursor *string         `json:"next_cursor"`
}

// ServeHTTP routes GET → handle; everything else → 405.
func (h *AdminInvoicesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeInvoicesError(w, http.StatusMethodNotAllowed, "method_not_allowed", "GET only")
		return
	}
	h.handle(w, r)
}

func (h *AdminInvoicesHandler) handle(w http.ResponseWriter, r *http.Request) {
	if h.deps.AddonPurchase == nil || h.deps.Stripe == nil {
		writeInvoicesError(w, http.StatusServiceUnavailable, "unwired", "invoices handler not wired")
		return
	}
	tenantID := extractTenantIDFromInvoicesPath(r.URL.Path)
	if tenantID == "" {
		writeInvoicesError(w, http.StatusBadRequest, "validation_failed", "tenant_id required in path")
		return
	}
	ctx := tracing.WithTenantID(r.Context(), tenantID)

	// Resolve the tenant's Stripe Customer ID.
	customerID, err := h.deps.AddonPurchase.GetStripeCustomerIDByTenant(ctx, tenantID)
	if err != nil {
		log.Printf("payments/admin/invoices: GetStripeCustomerIDByTenant err: %v", err)
		writeInvoicesError(w, http.StatusInternalServerError, "lookup_failed", "internal lookup error")
		return
	}
	if customerID == "" {
		// New tenant — empty list, NOT 404.
		writeInvoicesJSON(w, http.StatusOK, InvoicesListResponse{Items: []InvoiceRowDTO{}, NextCursor: nil})
		return
	}

	limit, startingAfter := parseInvoicesPagination(r.URL.Query())

	out, err := h.deps.Stripe.ListInvoices(ctx, stripeadapter.ListInvoicesInput{
		StripeCustomerID: customerID,
		Limit:            limit,
		StartingAfter:    startingAfter,
	})
	if err != nil {
		log.Printf("payments/admin/invoices: Stripe ListInvoices err: %v", err)
		writeInvoicesError(w, http.StatusBadGateway, "stripe_error", "Stripe invoice list failed: "+err.Error())
		return
	}

	items := make([]InvoiceRowDTO, 0, len(out.Items))
	for _, row := range out.Items {
		desc := lookupAddonTierLabel(ctx, h.deps.AddonPurchase, row.StripeSubscriptionID)
		items = append(items, InvoiceRowDTO{
			StripeInvoiceID:  row.StripeInvoiceID,
			Number:           row.Number,
			PeriodStart:      row.PeriodStart.UTC().Format("2006-01-02T15:04:05Z"),
			PeriodEnd:        row.PeriodEnd.UTC().Format("2006-01-02T15:04:05Z"),
			Status:           row.Status,
			TotalCents:       row.TotalCents,
			Currency:         row.Currency,
			HostedInvoiceURL: row.HostedInvoiceURL,
			InvoicePDFURL:    row.InvoicePDFURL,
			Description:      desc,
		})
	}
	var nextCursor *string
	if out.NextCursor != "" {
		c := out.NextCursor
		nextCursor = &c
	}
	writeInvoicesJSON(w, http.StatusOK, InvoicesListResponse{Items: items, NextCursor: nextCursor})
}

// lookupAddonTierLabel joins a Stripe subscription ID back to the local
// TenantAddonPurchase row to build "{addon_code}:{tier_code}". Returns
// empty string on lookup failure (the FE renders the row without a
// label — better than 500ing the entire page).
func lookupAddonTierLabel(ctx context.Context, repo tap.Repo, subID string) string {
	if strings.TrimSpace(subID) == "" {
		return ""
	}
	row, err := repo.GetByStripeSubscriptionID(ctx, subID)
	if err != nil {
		if !errors.Is(err, shared.ErrNotFound) {
			log.Printf("payments/admin/invoices: GetByStripeSubscriptionID(%s) err: %v", subID, err)
		}
		return ""
	}
	return row.AddonCode + ":" + row.TierCode
}

// extractTenantIDFromInvoicesPath pulls the {tenantId} segment out of
// `/api/v1/admin/tenants/{tenantId}/invoices`. Returns empty string on
// malformed paths.
func extractTenantIDFromInvoicesPath(path string) string {
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
	suffix := tail[idx+1:]
	if suffix != "invoices" && !strings.HasPrefix(suffix, "invoices?") {
		return ""
	}
	return tenantID
}

// parseInvoicesPagination reads limit + starting_after query params.
// limit defaults to 20 and is clamped to [1, 100].
func parseInvoicesPagination(q map[string][]string) (limit int64, startingAfter string) {
	limit = 20
	if vals, ok := q["limit"]; ok && len(vals) > 0 {
		if n, err := strconv.ParseInt(vals[0], 10, 64); err == nil {
			if n > 0 && n <= 100 {
				limit = n
			}
		}
	}
	if vals, ok := q["starting_after"]; ok && len(vals) > 0 {
		startingAfter = strings.TrimSpace(vals[0])
	}
	return
}

func writeInvoicesJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeInvoicesError(w http.ResponseWriter, status int, code, message string) {
	writeInvoicesJSON(w, status, map[string]string{"error": code, "message": message})
}

var _ http.Handler = (*AdminInvoicesHandler)(nil)
