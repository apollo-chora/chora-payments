// change_tier_handler.go — CHO-1764. HTTP endpoint for the H+
// Marketplace change-tier flow:
//
//	POST /api/v1/admin/tenant-addons:change-tier
//
// chora-tenancy's admin PATCH dispatches here after validation; this
// handler resolves the tenant_addon_purchase row, calls Stripe
// Subscription.update via the CHO-1760 Price catalogue, persists the
// new tier on the row, and returns the proration delta for FE display.
//
// Auth is by network — chora-payments is internal-only; the originating
// admin token was verified at the gateway. There's no role gate here
// for now; CHO-1764b (tenancy refactor) wires an mTLS-or-shared-secret
// gate in a follow-up.
package http

import (
	"context"
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

// ChangeTierHandlerDeps wires the change-tier handler.
type ChangeTierHandlerDeps struct {
	AddonPurchase  tap.Repo
	Stripe         stripeadapter.Client
	PriceCatalogue stripe_catalogue.PriceCatalogue
	Now            func() time.Time
}

// ChangeTierHandler serves POST /api/v1/admin/tenant-addons:change-tier.
type ChangeTierHandler struct {
	deps ChangeTierHandlerDeps
}

// NewChangeTierHandler constructs the handler.
func NewChangeTierHandler(deps ChangeTierHandlerDeps) *ChangeTierHandler {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	return &ChangeTierHandler{deps: deps}
}

// ChangeTierRequest is the JSON body.
type ChangeTierRequest struct {
	TenantID       string `json:"tenant_id"`
	AdminGCID      string `json:"admin_gcid"`
	AddonCode      string `json:"addon_code"`
	TargetTierCode string `json:"target_tier_code"`
	Currency       string `json:"currency"`
	// ProrationMode — "create_prorations" (default) or "none".
	ProrationMode string `json:"proration_mode"`
	// EffectiveAt — "" / "immediate" → CHO-1764 immediate path (proration
	// billed today, new tier active right away).
	// "end_of_cycle" → CHO-1772 SubscriptionSchedule path (defer the change
	// to the next billing anchor; no proration today; new tier active on
	// the cycle Stripe activates the schedule).
	EffectiveAt string `json:"effective_at"`
}

// ChangeTierResponse is the JSON response. Mirrors the existing
// chora-tenancy `changeTier` response shape so CHO-1764b's refactor
// can pass it through verbatim.
//
// CHO-1772 extension: when EffectiveAt=end_of_cycle drove the dispatch,
// the schedule fields are populated + DeferredToCycleEnd is true. On the
// immediate path the schedule fields are empty/zero + DeferredToCycleEnd
// is false (existing CHO-1764 behaviour unchanged).
type ChangeTierResponse struct {
	PurchaseID           string `json:"purchase_id"`
	StripeSubscriptionID string `json:"stripe_subscription_id"`
	FromTier             string `json:"from_tier"`
	ToTier               string `json:"to_tier"`
	BillingDeltaCents    int64  `json:"billing_delta_cents"`
	Currency             string `json:"currency"`
	EffectiveAt          string `json:"effective_at"`
	SubscriptionStatus   string `json:"subscription_status"`
	// CHO-1772 fields (omitted on the immediate path via omitempty +
	// DeferredToCycleEnd = false).
	DeferredToCycleEnd   bool   `json:"deferred_to_cycle_end"`
	ScheduleID           string `json:"schedule_id,omitempty"`
	ScheduledTierCode    string `json:"scheduled_tier_code,omitempty"`
	ScheduledEffectiveAt string `json:"scheduled_effective_at,omitempty"`
}

// ServeHTTP routes POST → handle; everything else → 405.
func (h *ChangeTierHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeChangeTierError(w, http.StatusMethodNotAllowed, "method_not_allowed", "POST only")
		return
	}
	h.handle(w, r)
}

func (h *ChangeTierHandler) handle(w http.ResponseWriter, r *http.Request) {
	if h.deps.AddonPurchase == nil || h.deps.Stripe == nil {
		writeChangeTierError(w, http.StatusServiceUnavailable, "unwired", "change-tier handler not wired")
		return
	}
	var req ChangeTierRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeChangeTierError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.AddonCode = strings.TrimSpace(req.AddonCode)
	req.TargetTierCode = strings.TrimSpace(req.TargetTierCode)
	req.Currency = strings.TrimSpace(req.Currency)
	if req.TenantID == "" {
		writeChangeTierError(w, http.StatusBadRequest, "validation_failed", "tenant_id required")
		return
	}
	if req.AddonCode == "" {
		writeChangeTierError(w, http.StatusBadRequest, "validation_failed", "addon_code required")
		return
	}
	if req.TargetTierCode == "" {
		writeChangeTierError(w, http.StatusBadRequest, "validation_failed", "target_tier_code required")
		return
	}
	if req.Currency == "" {
		writeChangeTierError(w, http.StatusBadRequest, "validation_failed", "currency required")
		return
	}
	prorationBehavior := req.ProrationMode
	if prorationBehavior == "" {
		prorationBehavior = "create_prorations"
	}
	// CHO-1786 — accept always_invoice so the FE "Immediately" path can tell
	// Stripe to create a separate invoice today + bill the prorated delta
	// now (matches the result card's "Additional charge today" label).
	if prorationBehavior != "create_prorations" && prorationBehavior != "none" && prorationBehavior != "always_invoice" {
		writeChangeTierError(w, http.StatusBadRequest, "validation_failed", "proration_mode must be create_prorations, none, or always_invoice")
		return
	}

	// CHO-1772 — only 3 effective_at values are accepted: empty (legacy /
	// CHO-1764 default), "immediate" (explicit version of the same), or
	// "end_of_cycle" (new SubscriptionSchedule path).
	effectiveAt := strings.TrimSpace(req.EffectiveAt)
	if effectiveAt != "" && effectiveAt != "immediate" && effectiveAt != "end_of_cycle" {
		writeChangeTierError(w, http.StatusBadRequest, "validation_failed",
			"effective_at must be empty, immediate, or end_of_cycle")
		return
	}

	ctx := tracing.WithTenantID(r.Context(), req.TenantID)

	// 1. Look up the active TAP row for (tenant, addon_code).
	row, err := h.deps.AddonPurchase.GetActiveByTenantAndAddonCode(ctx, req.TenantID, req.AddonCode)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			writeChangeTierError(w, http.StatusNotFound, "addon_not_found", fmt.Sprintf("no active subscription for tenant=%s addon=%s", req.TenantID, req.AddonCode))
			return
		}
		log.Printf("payments/change-tier: GetActiveByTenantAndAddonCode err: %v", err)
		writeChangeTierError(w, http.StatusInternalServerError, "lookup_failed", "internal lookup error")
		return
	}

	// 2. Guard legacy rows: stripe_subscription_id MUST be populated.
	if row.StripeSubscriptionID == "" {
		writeChangeTierError(w, http.StatusUnprocessableEntity, "no_stripe_subscription",
			"this add-on was activated before subscription billing rolled out (CHO-1762); please deactivate and re-subscribe to change tier")
		return
	}

	// 3. Resolve the target Price ID via CHO-1760 catalogue.
	if h.deps.PriceCatalogue == nil {
		writeChangeTierError(w, http.StatusServiceUnavailable, "catalogue_unwired", "Stripe Price catalogue not wired")
		return
	}
	priceID, ok := h.deps.PriceCatalogue.Resolve(req.AddonCode, req.TargetTierCode, req.Currency)
	if !ok {
		writeChangeTierError(w, http.StatusUnprocessableEntity, "stripe_price_missing",
			fmt.Sprintf("no Stripe Price configured for (addon=%s tier=%s currency=%s)", req.AddonCode, req.TargetTierCode, req.Currency))
		return
	}
	if stripe_catalogue.IsPlaceholder(priceID) {
		writeChangeTierError(w, http.StatusUnprocessableEntity, "stripe_price_missing",
			fmt.Sprintf("placeholder Price ID for (addon=%s tier=%s currency=%s) — run scripts/stripe-seed-addon-prices.sh + set STRIPE_PRICE_CATALOGUE_SECRET_ID", req.AddonCode, req.TargetTierCode, req.Currency))
		return
	}

	// 4. Refuse same-tier no-op early so Stripe doesn't bill anything.
	fromTier := row.TierCode
	if fromTier == req.TargetTierCode {
		writeChangeTierError(w, http.StatusConflict, "tier_unchanged",
			fmt.Sprintf("already on tier=%s", req.TargetTierCode))
		return
	}

	now := h.deps.Now()

	// 5. CHO-1772 — end-of-cycle dispatch path. The row's CurrentPeriodEnd
	// is the cycle anchor Stripe will use; fall back to a live
	// Stripe.GetSubscription lookup when the row's anchor is nil (the
	// customer.subscription.created webhook may not have populated it on
	// rows created before the CHO-1763 dispatcher landed).
	if effectiveAt == "end_of_cycle" {
		var startDate time.Time
		if row.CurrentPeriodEnd != nil {
			startDate = *row.CurrentPeriodEnd
		} else {
			subOut, subErr := h.deps.Stripe.GetSubscription(ctx, row.StripeSubscriptionID)
			if subErr != nil {
				log.Printf("payments/change-tier: GetSubscription fallback err: %v", subErr)
				writeChangeTierError(w, http.StatusBadGateway, "stripe_error",
					"Stripe subscription lookup failed: "+subErr.Error())
				return
			}
			if subOut.CurrentPeriodEnd.IsZero() {
				writeChangeTierError(w, http.StatusUnprocessableEntity, "no_current_period_end",
					"Stripe reports no current_period_end for this subscription; immediate change required")
				return
			}
			startDate = subOut.CurrentPeriodEnd
		}
		schedOut, err := h.deps.Stripe.ScheduleSubscriptionPriceChange(ctx, stripeadapter.ScheduleSubscriptionPriceChangeInput{
			StripeSubscriptionID: row.StripeSubscriptionID,
			NewPriceID:           priceID,
			StartDate:            startDate,
		})
		if err != nil {
			log.Printf("payments/change-tier: Stripe ScheduleSubscriptionPriceChange err: %v", err)
			writeChangeTierError(w, http.StatusBadGateway, "stripe_error", "Stripe SubscriptionSchedule failed: "+err.Error())
			return
		}
		// TierCode stays at the CURRENT paid-for tier. ScheduledTierCode
		// holds the deferred target; release fires on the Stripe-driven
		// subscription_schedule.released webhook.
		if applyErr := row.ApplySchedule(schedOut.StripeSubscriptionScheduleID, req.TargetTierCode, schedOut.EffectiveAt); applyErr != nil {
			log.Printf("payments/change-tier: ApplySchedule err: %v", applyErr)
			writeChangeTierError(w, http.StatusInternalServerError, "schedule_apply_failed", "failed to record schedule on row")
			return
		}
		row.UpdatedAt = now
		if err := h.deps.AddonPurchase.Save(ctx, row); err != nil {
			log.Printf("payments/change-tier: Save (schedule) err: %v", err)
			writeChangeTierError(w, http.StatusInternalServerError, "save_failed", "failed to persist row")
			return
		}
		resp := ChangeTierResponse{
			PurchaseID:           row.PurchaseID,
			StripeSubscriptionID: row.StripeSubscriptionID,
			FromTier:             fromTier,
			ToTier:               req.TargetTierCode,
			BillingDeltaCents:    0,
			Currency:             strings.ToLower(row.Currency),
			EffectiveAt:          schedOut.EffectiveAt.Format(time.RFC3339Nano),
			SubscriptionStatus:   string(row.Status),
			DeferredToCycleEnd:   true,
			ScheduleID:           schedOut.StripeSubscriptionScheduleID,
			ScheduledTierCode:    req.TargetTierCode,
			ScheduledEffectiveAt: schedOut.EffectiveAt.Format(time.RFC3339Nano),
		}
		writeChangeTierJSON(w, http.StatusOK, resp)
		return
	}

	// 6. Immediate path (CHO-1764, unchanged) — call Stripe Subscription.update.
	out, err := h.deps.Stripe.UpdateSubscriptionPrice(ctx, stripeadapter.UpdateSubscriptionPriceInput{
		StripeSubscriptionID: row.StripeSubscriptionID,
		NewPriceID:           priceID,
		ProrationBehavior:    prorationBehavior,
	})
	if err != nil {
		log.Printf("payments/change-tier: Stripe UpdateSubscriptionPrice err: %v", err)
		writeChangeTierError(w, http.StatusBadGateway, "stripe_error", "Stripe Subscription update failed: "+err.Error())
		return
	}

	// 7. Persist the new tier + Stripe state on the row. The webhook
	// (customer.subscription.updated, deferred to CHO-1767) will
	// double-confirm later; for now the synchronous write keeps the
	// state coherent with what the user just paid for.
	row.TierCode = req.TargetTierCode
	if applyErr := row.ApplyStripeSubscriptionState(
		row.StripeCustomerID,
		row.StripeSubscriptionID,
		mapStripeStatus(out.Status),
		&out.CurrentPeriodStart, &out.CurrentPeriodEnd,
	); applyErr != nil {
		log.Printf("payments/change-tier: ApplyStripeSubscriptionState err: %v", applyErr)
	}
	row.UpdatedAt = now
	if err := h.deps.AddonPurchase.Save(ctx, row); err != nil {
		log.Printf("payments/change-tier: Save err: %v", err)
		writeChangeTierError(w, http.StatusInternalServerError, "save_failed", "failed to persist row")
		return
	}

	resp := ChangeTierResponse{
		PurchaseID:           row.PurchaseID,
		StripeSubscriptionID: row.StripeSubscriptionID,
		FromTier:             fromTier,
		ToTier:               req.TargetTierCode,
		BillingDeltaCents:    out.BillingDeltaCents,
		Currency:             firstNonEmpty(out.Currency, row.Currency),
		EffectiveAt:          now.Format(time.RFC3339Nano),
		SubscriptionStatus:   out.Status,
	}
	writeChangeTierJSON(w, http.StatusOK, resp)
}

// mapStripeStatus clamps an unknown Stripe status to tap.StatusPending
// so the row keeps the customer + subscription IDs even on interim
// states. Mirrors handleCustomerSubscriptionCreated in the dispatcher.
func mapStripeStatus(stripeStatus string) tap.Status {
	s := tap.Status(stripeStatus)
	if tap.IsValidStatus(s) {
		return s
	}
	return tap.StatusPending
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func writeChangeTierJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeChangeTierError(w http.ResponseWriter, status int, code, message string) {
	writeChangeTierJSON(w, status, map[string]string{"error": code, "message": message})
}

// Compile-time guard that the handler still satisfies http.Handler.
var _ http.Handler = (*ChangeTierHandler)(nil)

// silence unused import in rare go builds where errors gets folded
var _ = context.Context(nil)
