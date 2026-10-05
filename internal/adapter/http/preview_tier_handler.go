// preview_tier_handler.go — CHO-1765. HTTP endpoint for the H+
// Marketplace preview-tier-change flow:
//
//	POST /api/v1/admin/tenant-addons:preview-tier-change
//
// chora-tenancy's existing preview endpoint dispatches here (CHO-1767
// follow-up). Read-only — calls Stripe Invoice.upcoming with the
// proposed price change and sums proration line items into the
// billing-delta. Does NOT mutate the live Subscription.
package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	"github.com/apollo-chora/chora-payments/internal/adapter/stripe_catalogue"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// PreviewTierHandlerDeps wires the preview-tier-change handler. Shares
// the same shape as ChangeTierHandlerDeps so cmd/server can pass the
// same wiring objects.
type PreviewTierHandlerDeps struct {
	AddonPurchase  tap.Repo
	Stripe         stripeadapter.Client
	PriceCatalogue stripe_catalogue.PriceCatalogue
	Now            func() time.Time
}

// PreviewTierHandler serves POST /api/v1/admin/tenant-addons:preview-tier-change.
type PreviewTierHandler struct {
	deps PreviewTierHandlerDeps
}

// NewPreviewTierHandler constructs the handler.
func NewPreviewTierHandler(deps PreviewTierHandlerDeps) *PreviewTierHandler {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	return &PreviewTierHandler{deps: deps}
}

// PreviewTierRequest is the JSON body. Same shape as ChangeTierRequest;
// admin_gcid is optional here (preview is read-only, audit doesn't need
// the actor).
type PreviewTierRequest struct {
	TenantID       string `json:"tenant_id"`
	AddonCode      string `json:"addon_code"`
	TargetTierCode string `json:"target_tier_code"`
	Currency       string `json:"currency"`
	ProrationMode  string `json:"proration_mode"`
	// EffectiveAt mirrors the change-tier request — "" / "immediate" use
	// the proration-style preview; "end_of_cycle" returns billing_delta=0
	// + next_invoice = the new tier's full monthly cost via Stripe.GetPrice.
	EffectiveAt string `json:"effective_at"`
}

// PreviewTierResponse mirrors the existing chora-tenancy preview shape
// so CHO-1767 can pass the wire payload through verbatim.
//
// CHO-1772 extension: when EffectiveAt=end_of_cycle drove the preview,
// DeferredToCycleEnd is true and EffectiveAt carries the cycle anchor.
// On the immediate path EffectiveAt is empty + DeferredToCycleEnd false
// (existing behaviour unchanged).
type PreviewTierResponse struct {
	PurchaseID            string `json:"purchase_id"`
	FromTier              string `json:"from_tier"`
	ToTier                string `json:"to_tier"`
	BillingDeltaCents     int64  `json:"billing_delta_cents"`
	NextInvoiceTotalCents int64  `json:"next_invoice_total_cents"`
	Currency              string `json:"currency"`
	ProrationMode         string `json:"proration_mode"`
	// CHO-1772 fields.
	DeferredToCycleEnd bool   `json:"deferred_to_cycle_end"`
	EffectiveAt        string `json:"effective_at,omitempty"`
}

// ServeHTTP routes POST → handle; everything else → 405.
func (h *PreviewTierHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writePreviewError(w, http.StatusMethodNotAllowed, "method_not_allowed", "POST only")
		return
	}
	h.handle(w, r)
}

func (h *PreviewTierHandler) handle(w http.ResponseWriter, r *http.Request) {
	if h.deps.AddonPurchase == nil || h.deps.Stripe == nil {
		writePreviewError(w, http.StatusServiceUnavailable, "unwired", "preview-tier-change handler not wired")
		return
	}
	var req PreviewTierRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writePreviewError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.AddonCode = strings.TrimSpace(req.AddonCode)
	req.TargetTierCode = strings.TrimSpace(req.TargetTierCode)
	req.Currency = strings.TrimSpace(req.Currency)
	if req.TenantID == "" {
		writePreviewError(w, http.StatusBadRequest, "validation_failed", "tenant_id required")
		return
	}
	if req.AddonCode == "" {
		writePreviewError(w, http.StatusBadRequest, "validation_failed", "addon_code required")
		return
	}
	if req.TargetTierCode == "" {
		writePreviewError(w, http.StatusBadRequest, "validation_failed", "target_tier_code required")
		return
	}
	if req.Currency == "" {
		writePreviewError(w, http.StatusBadRequest, "validation_failed", "currency required")
		return
	}
	prorationBehavior := req.ProrationMode
	if prorationBehavior == "" {
		prorationBehavior = "create_prorations"
	}
	// CHO-1786 — preview MUST accept always_invoice so the FE
	// "Immediately" path doesn't 400 before the user confirms.
	if prorationBehavior != "create_prorations" && prorationBehavior != "none" && prorationBehavior != "always_invoice" {
		writePreviewError(w, http.StatusBadRequest, "validation_failed", "proration_mode must be create_prorations, none, or always_invoice")
		return
	}

	// CHO-1772 — mirror change_tier_handler's effective_at validation.
	effectiveAt := strings.TrimSpace(req.EffectiveAt)
	if effectiveAt != "" && effectiveAt != "immediate" && effectiveAt != "end_of_cycle" {
		writePreviewError(w, http.StatusBadRequest, "validation_failed",
			"effective_at must be empty, immediate, or end_of_cycle")
		return
	}

	ctx := tracing.WithTenantID(r.Context(), req.TenantID)

	// 1. Look up active TAP row.
	row, err := h.deps.AddonPurchase.GetActiveByTenantAndAddonCode(ctx, req.TenantID, req.AddonCode)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			writePreviewError(w, http.StatusNotFound, "addon_not_found", fmt.Sprintf("no active subscription for tenant=%s addon=%s", req.TenantID, req.AddonCode))
			return
		}
		log.Printf("payments/preview-tier: GetActiveByTenantAndAddonCode err: %v", err)
		writePreviewError(w, http.StatusInternalServerError, "lookup_failed", "internal lookup error")
		return
	}

	// 2. Guard legacy rows.
	if row.StripeSubscriptionID == "" {
		writePreviewError(w, http.StatusUnprocessableEntity, "no_stripe_subscription",
			"this add-on was activated before subscription billing rolled out (CHO-1762); please deactivate and re-subscribe to change tier")
		return
	}

	// 3. Same-tier no-op preview returns zeroes synchronously.
	if row.TierCode == req.TargetTierCode {
		writePreviewJSON(w, http.StatusOK, PreviewTierResponse{
			PurchaseID:    row.PurchaseID,
			FromTier:      row.TierCode,
			ToTier:        req.TargetTierCode,
			Currency:      strings.ToLower(row.Currency),
			ProrationMode: prorationBehavior,
		})
		return
	}

	// 4. Resolve target Price ID.
	if h.deps.PriceCatalogue == nil {
		writePreviewError(w, http.StatusServiceUnavailable, "catalogue_unwired", "Stripe Price catalogue not wired")
		return
	}
	priceID, ok := h.deps.PriceCatalogue.Resolve(req.AddonCode, req.TargetTierCode, req.Currency)
	if !ok {
		writePreviewError(w, http.StatusUnprocessableEntity, "stripe_price_missing",
			fmt.Sprintf("no Stripe Price configured for (addon=%s tier=%s currency=%s)", req.AddonCode, req.TargetTierCode, req.Currency))
		return
	}
	if stripe_catalogue.IsPlaceholder(priceID) {
		writePreviewError(w, http.StatusUnprocessableEntity, "stripe_price_missing",
			fmt.Sprintf("placeholder Price ID for (addon=%s tier=%s currency=%s) — run scripts/stripe-seed-addon-prices.sh + set STRIPE_PRICE_CATALOGUE_SECRET_ID", req.AddonCode, req.TargetTierCode, req.Currency))
		return
	}

	// 5a. CHO-1772 end-of-cycle preview — no proration today; the next
	// invoice is the new tier's full monthly price via Stripe.GetPrice.
	// When the row's current_period_end is nil (the
	// customer.subscription.created webhook may not have populated it
	// on rows created before the CHO-1763 dispatcher landed), fall back
	// to a live Stripe.GetSubscription lookup so the preview still
	// renders rather than 422-ing the user.
	if effectiveAt == "end_of_cycle" {
		var cycleEnd time.Time
		if row.CurrentPeriodEnd != nil {
			cycleEnd = *row.CurrentPeriodEnd
		} else {
			subOut, subErr := h.deps.Stripe.GetSubscription(ctx, row.StripeSubscriptionID)
			if subErr != nil {
				log.Printf("payments/preview-tier: Stripe GetSubscription fallback err: %v", subErr)
				// Stay lenient: emit the preview without the anchor; the
				// FE renders the cents totals and skips the date copy.
			} else {
				cycleEnd = subOut.CurrentPeriodEnd
			}
		}
		priceOut, err := h.deps.Stripe.GetPrice(ctx, priceID)
		if err != nil {
			log.Printf("payments/preview-tier: Stripe GetPrice err: %v", err)
			writePreviewError(w, http.StatusBadGateway, "stripe_error", "Stripe Price lookup failed: "+err.Error())
			return
		}
		effectiveAtStr := ""
		if !cycleEnd.IsZero() {
			effectiveAtStr = cycleEnd.Format(time.RFC3339Nano)
		}
		resp := PreviewTierResponse{
			PurchaseID:            row.PurchaseID,
			FromTier:              row.TierCode,
			ToTier:                req.TargetTierCode,
			BillingDeltaCents:     0,
			NextInvoiceTotalCents: priceOut.UnitAmountCents,
			Currency:              firstNonEmpty(priceOut.Currency, strings.ToLower(row.Currency)),
			ProrationMode:         prorationBehavior,
			DeferredToCycleEnd:    true,
			EffectiveAt:           effectiveAtStr,
		}
		writePreviewJSON(w, http.StatusOK, resp)
		return
	}

	// 5b. Stripe Invoice.upcoming preview (immediate / proration-style).
	out, err := h.deps.Stripe.PreviewSubscriptionPriceChange(ctx, stripeadapter.PreviewSubscriptionPriceChangeInput{
		StripeSubscriptionID: row.StripeSubscriptionID,
		NewPriceID:           priceID,
		ProrationBehavior:    prorationBehavior,
	})
	if err != nil {
		log.Printf("payments/preview-tier: Stripe PreviewSubscriptionPriceChange err: %v", err)
		writePreviewError(w, http.StatusBadGateway, "stripe_error", "Stripe Invoice.upcoming failed: "+err.Error())
		return
	}

	// CHO-1789 follow-up: Stripe's invoice.Upcoming with
	// subscription_proration_behavior=always_invoice returns the
	// SEPARATE today-issued proration invoice as the "upcoming" total,
	// not the next renewal invoice. Threading upcoming.Total into the
	// FE's "Next renewal total" label rendered nonsense like
	// "Next renewal total: SGD 34.03" on a 49→14.90 downgrade (the 34.03
	// is the credit amount, not the next-cycle bill). Look up the new
	// tier's monthly price directly — that IS the next renewal total
	// regardless of proration behavior. Same call the end-of-cycle path
	// already makes.
	nextRenewalCents := out.NextInvoiceTotalCents
	nextRenewalCurrency := out.Currency
	priceOut, perr := h.deps.Stripe.GetPrice(ctx, priceID)
	if perr != nil {
		log.Printf("payments/preview-tier: Stripe GetPrice (next-renewal) err: %v — falling back to upcoming.Total", perr)
	} else {
		nextRenewalCents = priceOut.UnitAmountCents
		if priceOut.Currency != "" {
			nextRenewalCurrency = priceOut.Currency
		}
	}

	resp := PreviewTierResponse{
		PurchaseID:            row.PurchaseID,
		FromTier:              row.TierCode,
		ToTier:                req.TargetTierCode,
		BillingDeltaCents:     out.BillingDeltaCents,
		NextInvoiceTotalCents: nextRenewalCents,
		Currency:              firstNonEmpty(nextRenewalCurrency, strings.ToLower(row.Currency)),
		ProrationMode:         prorationBehavior,
	}
	writePreviewJSON(w, http.StatusOK, resp)
}

func writePreviewJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writePreviewError(w http.ResponseWriter, status int, code, message string) {
	writePreviewJSON(w, status, map[string]string{"error": code, "message": message})
}

var _ http.Handler = (*PreviewTierHandler)(nil)
