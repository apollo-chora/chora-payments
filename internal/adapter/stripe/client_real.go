// client_real.go — Stripe SDK adapter using github.com/stripe/stripe-go/v78.
//
// Wires CreateCheckoutSession + RefundCharge to the Stripe REST API. The
// constructor takes the API key + the test-mode flag (purely advisory —
// stripe-go's behaviour depends on the prefix of the key, sk_test_ vs
// sk_live_).
package stripe

import (
	"context"
	"errors"
	"fmt"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"
	billingportalsession "github.com/stripe/stripe-go/v78/billingportal/session"
	"github.com/stripe/stripe-go/v78/checkout/session"
	"github.com/stripe/stripe-go/v78/customer"
	"github.com/stripe/stripe-go/v78/invoice"
	"github.com/stripe/stripe-go/v78/price"
	"github.com/stripe/stripe-go/v78/refund"
	"github.com/stripe/stripe-go/v78/subscription"
	"github.com/stripe/stripe-go/v78/subscriptionschedule"
)

// RealClient is the production Stripe SDK adapter.
type RealClient struct {
	apiKey   string
	testMode bool
}

// NewRealClient constructs a RealClient. apiKey is the Stripe API key
// (sk_test_… for dev, sk_live_… for prod). testMode is purely advisory.
// Returns an error if apiKey is empty.
func NewRealClient(apiKey string, testMode bool) (*RealClient, error) {
	if apiKey == "" {
		return nil, errors.New("stripe/real: api_key required (set STRIPE_API_KEY)")
	}
	return &RealClient{apiKey: apiKey, testMode: testMode}, nil
}

func (r *RealClient) CreateCheckoutSession(ctx context.Context, in CreateCheckoutSessionInput) (CreateCheckoutSessionOutput, error) {
	if err := validateInput(in); err != nil {
		return CreateCheckoutSessionOutput{}, err
	}
	// stripe-go uses the package-level key — set it lazily per call to
	// keep the adapter goroutine-safe in the face of key rotation.
	stripeGo.Key = r.apiKey

	sess, err := session.New(buildCheckoutSessionParams(in))
	if err != nil {
		return CreateCheckoutSessionOutput{}, fmt.Errorf("stripe/real: create checkout session: %w", err)
	}
	return CreateCheckoutSessionOutput{
		StripeSessionID:   sess.ID,
		StripeCheckoutURL: sess.URL,
	}, nil
}

func (r *RealClient) RefundCharge(ctx context.Context, in RefundChargeInput) (RefundChargeOutput, error) {
	if in.StripeChargeID == "" {
		return RefundChargeOutput{}, errors.New("stripe/real: stripe_charge_id required")
	}
	stripeGo.Key = r.apiKey

	params := &stripeGo.RefundParams{
		Charge: stripeGo.String(in.StripeChargeID),
		Reason: stripeGo.String(in.Reason),
	}
	if in.AmountCents > 0 {
		params.Amount = stripeGo.Int64(in.AmountCents)
	}
	rf, err := refund.New(params)
	if err != nil {
		return RefundChargeOutput{}, fmt.Errorf("stripe/real: refund charge: %w", err)
	}
	return RefundChargeOutput{
		StripeRefundID:      rf.ID,
		AmountCentsRefunded: rf.Amount,
	}, nil
}

// EnsureCustomer creates a Stripe Customer via the SDK. The caller is
// responsible for caching the returned cus_xxx locally — Stripe does
// NOT dedupe customer.New on metadata; repeated calls without a local
// cache create duplicate Customers and orphan saved cards.
func (r *RealClient) EnsureCustomer(ctx context.Context, in EnsureCustomerInput) (EnsureCustomerOutput, error) {
	if in.TenantID == "" {
		return EnsureCustomerOutput{}, errors.New("stripe/real: tenant_id required")
	}
	if in.LearnerGCID == "" {
		return EnsureCustomerOutput{}, errors.New("stripe/real: learner_gcid required")
	}
	stripeGo.Key = r.apiKey

	params := &stripeGo.CustomerParams{
		Metadata: customerMetadataWithDefaults(in),
	}
	if in.Email != "" {
		params.Email = stripeGo.String(in.Email)
	}
	// Stripe's Customer create has no idempotency on metadata — the
	// chora-payments service must lookup the local registry first.
	c, err := customer.New(params)
	if err != nil {
		return EnsureCustomerOutput{}, fmt.Errorf("stripe/real: create customer: %w", err)
	}
	return EnsureCustomerOutput{
		StripeCustomerID: c.ID,
	}, nil
}

// CancelSubscription cancels (or schedules cancellation of) a Stripe
// Subscription via the SDK.
//
// CancelAtPeriodEnd=true → subscription.Update with cancel_at_period_end=true.
//
//	Stripe schedules cancellation at period end;
//	Status stays "active" until rollover.
//
// CancelAtPeriodEnd=false → subscription.Cancel. Immediate; Stripe sets
//
//	Status="canceled" + CanceledAt=now.
//
// Per ADR-164 Stage A.5 — closes FE-initiated cancel debt.
func (r *RealClient) CancelSubscription(ctx context.Context, in CancelSubscriptionInput) (CancelSubscriptionOutput, error) {
	if in.StripeSubscriptionID == "" {
		return CancelSubscriptionOutput{}, errors.New("stripe/real: stripe_subscription_id required")
	}
	stripeGo.Key = r.apiKey

	var sub *stripeGo.Subscription
	var err error
	if in.CancelAtPeriodEnd {
		params := &stripeGo.SubscriptionParams{
			CancelAtPeriodEnd: stripeGo.Bool(true),
		}
		if in.Reason != "" {
			params.CancellationDetails = &stripeGo.SubscriptionCancellationDetailsParams{
				Comment: stripeGo.String(in.Reason),
			}
		}
		sub, err = subscription.Update(in.StripeSubscriptionID, params)
	} else {
		params := &stripeGo.SubscriptionCancelParams{}
		if in.Reason != "" {
			params.CancellationDetails = &stripeGo.SubscriptionCancelCancellationDetailsParams{
				Comment: stripeGo.String(in.Reason),
			}
		}
		sub, err = subscription.Cancel(in.StripeSubscriptionID, params)
	}
	if err != nil {
		return CancelSubscriptionOutput{}, fmt.Errorf("stripe/real: cancel subscription: %w", err)
	}

	out := CancelSubscriptionOutput{
		StripeSubscriptionID: sub.ID,
		CancelAtPeriodEnd:    sub.CancelAtPeriodEnd,
		Status:               string(sub.Status),
	}
	if sub.CanceledAt > 0 {
		out.CancelledAt = time.Unix(sub.CanceledAt, 0).UTC()
	}
	// EffectiveAt picks Stripe's current_period_end for graceful cancels;
	// canceled_at for immediate.
	if in.CancelAtPeriodEnd && sub.CurrentPeriodEnd > 0 {
		out.EffectiveAt = time.Unix(sub.CurrentPeriodEnd, 0).UTC()
	} else if sub.CanceledAt > 0 {
		out.EffectiveAt = time.Unix(sub.CanceledAt, 0).UTC()
	}
	return out, nil
}

// UpdateSubscriptionPrice (CHO-1764) switches the Stripe Subscription's
// first SubscriptionItem to a different recurring Price + records the
// prorated delta from the upcoming invoice.
//
// Stripe wire flow:
//  1. subscription.Get(sub_id, expand=["items"]) → resolve the live
//     SubscriptionItem ID.
//  2. subscription.Update(sub_id, items=[{id: <item_id>, price: <new_price_id>}],
//     proration_behavior=<from input>).
//  3. invoice.Upcoming(customer=<sub.customer>, subscription=<sub_id>) →
//     sum the proration line items for the billing delta.
//
// The delta lookup is best-effort; on failure we return the update
// outcome with delta=0 (the FE shows "Updated; check your next invoice"
// rather than 5xx-ing). Stripe also bills the delta on the next invoice
// regardless of whether we read it back.
func (r *RealClient) UpdateSubscriptionPrice(ctx context.Context, in UpdateSubscriptionPriceInput) (UpdateSubscriptionPriceOutput, error) {
	if in.StripeSubscriptionID == "" {
		return UpdateSubscriptionPriceOutput{}, errors.New("stripe/real: stripe_subscription_id required")
	}
	if in.NewPriceID == "" {
		return UpdateSubscriptionPriceOutput{}, errors.New("stripe/real: new_price_id required")
	}
	stripeGo.Key = r.apiKey

	prorationBehavior := in.ProrationBehavior
	if prorationBehavior == "" {
		prorationBehavior = "create_prorations"
	}

	// 1) Resolve live SubscriptionItem ID.
	sub, err := subscription.Get(in.StripeSubscriptionID, nil)
	if err != nil {
		return UpdateSubscriptionPriceOutput{}, fmt.Errorf("stripe/real: subscription.Get: %w", err)
	}
	if sub.Items == nil || len(sub.Items.Data) == 0 {
		return UpdateSubscriptionPriceOutput{}, errors.New("stripe/real: subscription has no items")
	}
	itemID := sub.Items.Data[0].ID

	// 2) Update with new Price + proration policy.
	updParams := &stripeGo.SubscriptionParams{
		Items: []*stripeGo.SubscriptionItemsParams{
			{
				ID:    stripeGo.String(itemID),
				Price: stripeGo.String(in.NewPriceID),
			},
		},
		ProrationBehavior: stripeGo.String(prorationBehavior),
	}
	updated, err := subscription.Update(in.StripeSubscriptionID, updParams)
	if err != nil {
		return UpdateSubscriptionPriceOutput{}, fmt.Errorf("stripe/real: subscription.Update: %w", err)
	}

	out := UpdateSubscriptionPriceOutput{
		StripeSubscriptionID: updated.ID,
		NewPriceID:           in.NewPriceID,
		Status:               string(updated.Status),
		CurrentPeriodStart:   time.Unix(updated.CurrentPeriodStart, 0).UTC(),
		CurrentPeriodEnd:     time.Unix(updated.CurrentPeriodEnd, 0).UTC(),
		Currency:             string(updated.Currency),
	}

	// 3) Best-effort delta. Two source paths depending on proration policy:
	//
	//   - always_invoice: Stripe finalised a SEPARATE subscription_update
	//     invoice today; its total IS the delta (positive on upgrade, negative
	//     on downgrade credit). invoice.Upcoming on the next-cycle invoice
	//     has zero proration lines in this mode, so reading it returns 0
	//     and the FE result card showed "$0.00" — CHO-1789.
	//   - create_prorations / "": defer to next renewal — sum the proration
	//     line items off invoice.Upcoming.
	//   - none: zero, by definition.
	var latestInvoiceTotal, upcomingProrationSum int64
	if prorationBehavior == "always_invoice" && updated.LatestInvoice != nil && updated.LatestInvoice.ID != "" {
		latestInv, lerr := invoice.Get(updated.LatestInvoice.ID, nil)
		if lerr == nil && latestInv != nil {
			latestInvoiceTotal = latestInv.Total
			if out.Currency == "" {
				out.Currency = string(latestInv.Currency)
			}
		}
	} else if prorationBehavior != "none" && prorationBehavior != "always_invoice" && updated.Customer != nil {
		invParams := &stripeGo.InvoiceUpcomingParams{
			Customer:     stripeGo.String(updated.Customer.ID),
			Subscription: stripeGo.String(in.StripeSubscriptionID),
		}
		upcoming, ierr := invoice.Upcoming(invParams)
		if ierr == nil && upcoming != nil {
			if upcoming.Lines != nil {
				for _, line := range upcoming.Lines.Data {
					if line.Proration {
						upcomingProrationSum += line.Amount
					}
				}
			}
			if out.Currency == "" {
				out.Currency = string(upcoming.Currency)
			}
		}
	}
	out.BillingDeltaCents = computeBillingDelta(prorationBehavior, latestInvoiceTotal, upcomingProrationSum)

	return out, nil
}

// computeBillingDelta selects the delta source based on Stripe's proration
// behavior (CHO-1789):
//   - always_invoice → the just-finalised today-invoice total
//     (subscription_update billing_reason; negative for downgrade credits)
//   - create_prorations / "" → the next renewal invoice's proration line sum
//   - none → 0
//
// Extracted as a pure helper so the branch logic is unit-testable without
// a live Stripe backend.
func computeBillingDelta(prorationBehavior string, latestInvoiceTotal, upcomingProrationSum int64) int64 {
	switch prorationBehavior {
	case "always_invoice":
		return latestInvoiceTotal
	case "none":
		return 0
	default:
		return upcomingProrationSum
	}
}

// PreviewSubscriptionPriceChange (CHO-1765) simulates the upcoming
// invoice Stripe WOULD generate if the subscription's first item were
// updated to the supplied Price. Non-mutating; uses Invoice.upcoming
// with the SubscriptionItems override.
func (r *RealClient) PreviewSubscriptionPriceChange(ctx context.Context, in PreviewSubscriptionPriceChangeInput) (PreviewSubscriptionPriceChangeOutput, error) {
	if in.StripeSubscriptionID == "" {
		return PreviewSubscriptionPriceChangeOutput{}, errors.New("stripe/real: stripe_subscription_id required")
	}
	if in.NewPriceID == "" {
		return PreviewSubscriptionPriceChangeOutput{}, errors.New("stripe/real: new_price_id required")
	}
	stripeGo.Key = r.apiKey

	prorationBehavior := in.ProrationBehavior
	if prorationBehavior == "" {
		prorationBehavior = "create_prorations"
	}

	// 1) Resolve live SubscriptionItem ID + Customer.
	sub, err := subscription.Get(in.StripeSubscriptionID, nil)
	if err != nil {
		return PreviewSubscriptionPriceChangeOutput{}, fmt.Errorf("stripe/real: subscription.Get: %w", err)
	}
	if sub.Items == nil || len(sub.Items.Data) == 0 {
		return PreviewSubscriptionPriceChangeOutput{}, errors.New("stripe/real: subscription has no items")
	}
	itemID := sub.Items.Data[0].ID
	if sub.Customer == nil {
		return PreviewSubscriptionPriceChangeOutput{}, errors.New("stripe/real: subscription has no customer")
	}

	// 2) Simulate the upcoming invoice WITH the proposed price change.
	invParams := &stripeGo.InvoiceUpcomingParams{
		Customer:                      stripeGo.String(sub.Customer.ID),
		Subscription:                  stripeGo.String(in.StripeSubscriptionID),
		SubscriptionProrationBehavior: stripeGo.String(prorationBehavior),
		SubscriptionItems: []*stripeGo.SubscriptionItemsParams{
			{
				ID:    stripeGo.String(itemID),
				Price: stripeGo.String(in.NewPriceID),
			},
		},
	}
	upcoming, err := invoice.Upcoming(invParams)
	if err != nil {
		return PreviewSubscriptionPriceChangeOutput{}, fmt.Errorf("stripe/real: invoice.Upcoming: %w", err)
	}

	out := PreviewSubscriptionPriceChangeOutput{
		NextInvoiceTotalCents: upcoming.Total,
		Currency:              string(upcoming.Currency),
	}
	if upcoming.Lines != nil {
		for _, line := range upcoming.Lines.Data {
			if line.Proration {
				out.BillingDeltaCents += line.Amount
			}
		}
	}
	return out, nil
}

// ScheduleSubscriptionPriceChange (CHO-1772) creates a Stripe
// SubscriptionSchedule that defers the tier change to the next billing
// anchor. End behaviour is `release` — once Phase 1 begins, the
// schedule discards itself + the underlying Subscription keeps running
// at the new tier.
//
// Stripe wire flow:
//  1. subscription.Get(sub_id) → resolve the live SubscriptionItem ID +
//     the current Price ID for Phase 0 echo-back. Stripe rejects
//     subscriptionschedule.Update with phases that don't carry items.
//  2. subscriptionschedule.New(FromSubscription=sub_id) → seeds Phase 0
//     from the live subscription. The returned schedule has one phase
//     whose Items mirror the current SubscriptionItem.
//  3. subscriptionschedule.Update(sched_id, Phases=[<seeded phase 0>,
//     {StartDate: in.StartDate, Items: [{Price: new_price}]}],
//     EndBehavior="release") — appends Phase 1.
//
// On Stripe-side completion of Phase 1, Stripe fires
// `subscription_schedule.released`; the dispatcher invokes
// TenantAddonPurchase.ReleaseSchedule which promotes ScheduledTierCode
// to TierCode and clears the schedule fields on the row.
func (r *RealClient) ScheduleSubscriptionPriceChange(ctx context.Context, in ScheduleSubscriptionPriceChangeInput) (ScheduleSubscriptionPriceChangeOutput, error) {
	if in.StripeSubscriptionID == "" {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/real: stripe_subscription_id required")
	}
	if in.NewPriceID == "" {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/real: new_price_id required")
	}
	if in.StartDate.IsZero() {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/real: start_date required")
	}
	stripeGo.Key = r.apiKey

	// 1) Check if the subscription already has a schedule attached
	// (recovery path: a prior change-tier attempt may have created the
	// schedule shell but failed before Update; Stripe will reject any
	// new subscriptionschedule.New on that sub). When found, reuse the
	// existing schedule_id for Update so we never leak orphan schedules.
	subGetParams := &stripeGo.SubscriptionParams{}
	subGetParams.AddExpand("schedule")
	sub, err := subscription.Get(in.StripeSubscriptionID, subGetParams)
	if err != nil {
		return ScheduleSubscriptionPriceChangeOutput{}, fmt.Errorf("stripe/real: subscription.Get: %w", err)
	}

	var scheduleID string
	var phase0 *stripeGo.SubscriptionSchedulePhase
	if sub != nil && sub.Schedule != nil && sub.Schedule.ID != "" {
		// Reuse existing schedule; the seeded Phase 0 is already there.
		scheduleID = sub.Schedule.ID
		if len(sub.Schedule.Phases) > 0 {
			phase0 = sub.Schedule.Phases[0]
		}
	} else {
		// Fresh path: seed schedule from the live Subscription.
		created, cerr := subscriptionschedule.New(&stripeGo.SubscriptionScheduleParams{
			FromSubscription: stripeGo.String(in.StripeSubscriptionID),
		})
		if cerr != nil {
			return ScheduleSubscriptionPriceChangeOutput{}, fmt.Errorf("stripe/real: subscriptionschedule.New: %w", cerr)
		}
		if created == nil || len(created.Phases) == 0 {
			return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/real: subscriptionschedule.New returned no phases")
		}
		scheduleID = created.ID
		phase0 = created.Phases[0]
	}
	if phase0 == nil {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/real: schedule has no phase 0")
	}

	// 2) Build Phase 0 echo (current SubscriptionItem from the seeded
	// schedule) + Phase 1 (new Price at StartDate).
	phase0Items := make([]*stripeGo.SubscriptionSchedulePhaseItemParams, 0, len(phase0.Items))
	for _, item := range phase0.Items {
		if item == nil || item.Price == nil {
			continue
		}
		phase0Items = append(phase0Items, &stripeGo.SubscriptionSchedulePhaseItemParams{
			Price:    stripeGo.String(item.Price.ID),
			Quantity: stripeGo.Int64(item.Quantity),
		})
	}
	if len(phase0Items) == 0 {
		return ScheduleSubscriptionPriceChangeOutput{}, errors.New("stripe/real: seeded phase 0 has no items")
	}

	startUnix := in.StartDate.UTC().Unix()
	// Phase 0 MUST carry a StartDate to anchor the schedule. The seeded
	// schedule from FromSubscription has phase0.StartDate populated
	// (mirrors the live subscription's current_period_start) — echo it
	// back. Without this, Stripe returns 400: "missing at least one
	// phase with a start_date to anchor end dates to".
	phase0Start := phase0.StartDate
	if phase0Start == 0 {
		// Defensive fallback: anchor at the input StartDate, although
		// in practice Stripe always returns a populated start_date on
		// the seeded phase.
		phase0Start = startUnix
	}
	updateParams := &stripeGo.SubscriptionScheduleParams{
		EndBehavior: stripeGo.String(string(stripeGo.SubscriptionScheduleEndBehaviorRelease)),
		Phases: []*stripeGo.SubscriptionSchedulePhaseParams{
			{
				Items:     phase0Items,
				StartDate: stripeGo.Int64(phase0Start),
				EndDate:   stripeGo.Int64(startUnix),
			},
			{
				StartDate: stripeGo.Int64(startUnix),
				Items: []*stripeGo.SubscriptionSchedulePhaseItemParams{
					{Price: stripeGo.String(in.NewPriceID)},
				},
			},
		},
	}
	updated, err := subscriptionschedule.Update(scheduleID, updateParams)
	if err != nil {
		return ScheduleSubscriptionPriceChangeOutput{}, fmt.Errorf("stripe/real: subscriptionschedule.Update: %w", err)
	}
	return ScheduleSubscriptionPriceChangeOutput{
		StripeSubscriptionScheduleID: updated.ID,
		EffectiveAt:                  in.StartDate.UTC(),
	}, nil
}

// GetPrice (CHO-1772) reads the recurring monthly UnitAmount + currency
// for a Stripe Price ID. Used by the preview-tier-change handler on the
// end_of_cycle path so the FE can render "next invoice = USD X.XX"
// without simulating a proration computation.
func (r *RealClient) GetPrice(ctx context.Context, priceID string) (GetPriceOutput, error) {
	if priceID == "" {
		return GetPriceOutput{}, errors.New("stripe/real: price_id required")
	}
	stripeGo.Key = r.apiKey
	p, err := price.Get(priceID, nil)
	if err != nil {
		return GetPriceOutput{}, fmt.Errorf("stripe/real: price.Get: %w", err)
	}
	return GetPriceOutput{
		UnitAmountCents: p.UnitAmount,
		Currency:        string(p.Currency),
	}, nil
}

// GetSubscription (CHO-1772 follow-up) reads the live cycle anchor +
// status for a Stripe Subscription. Used by both end_of_cycle handlers
// as a fallback when the TAP row's current_period_end is nil.
func (r *RealClient) GetSubscription(ctx context.Context, subID string) (GetSubscriptionOutput, error) {
	if subID == "" {
		return GetSubscriptionOutput{}, errors.New("stripe/real: subscription_id required")
	}
	stripeGo.Key = r.apiKey
	sub, err := subscription.Get(subID, nil)
	if err != nil {
		return GetSubscriptionOutput{}, fmt.Errorf("stripe/real: subscription.Get: %w", err)
	}
	return GetSubscriptionOutput{
		CurrentPeriodStart: time.Unix(sub.CurrentPeriodStart, 0).UTC(),
		CurrentPeriodEnd:   time.Unix(sub.CurrentPeriodEnd, 0).UTC(),
		Status:             string(sub.Status),
	}, nil
}

var _ Client = (*RealClient)(nil)

// buildCheckoutSessionParams is the pure (network-free) construction of the
// Stripe Checkout Session params. Extracted so the param shape — in
// particular the saved-card display config — is unit-testable without a
// live Stripe backend (session.New).
//
// CHO-1762 — the tenant-addon Subscribe flow (PurchaseTypeTenantAddonPurchase)
// switches from mode=payment + inline PriceData to mode=subscription +
// LineItems[].Price referencing a Stripe Price ID resolved from the
// CHO-1760 catalogue. The gRPC server is responsible for rejecting
// empty / placeholder Price IDs before they reach this builder; an
// empty StripePriceID here falls back to the legacy mode=payment path
// for backwards compatibility with existing tests + non-TAP callers.
func buildCheckoutSessionParams(in CreateCheckoutSessionInput) *stripeGo.CheckoutSessionParams {
	mode := stripeGo.String(string(stripeGo.CheckoutSessionModePayment))
	useSubscriptionMode := false
	// user_subscription stays on its legacy Subscription-mode path
	// (inline PriceData; CHO-1762 doesn't touch the mana flow).
	if in.PurchaseType == PurchaseTypeUserSubscription {
		mode = stripeGo.String(string(stripeGo.CheckoutSessionModeSubscription))
		useSubscriptionMode = true
	}
	// CHO-1762 — tenant-addon Subscribe gets Subscription mode + Price
	// reference when the gRPC layer resolved a real Price ID.
	useTAPSubscription := in.PurchaseType == PurchaseTypeTenantAddonPurchase && in.StripePriceID != ""
	if useTAPSubscription {
		mode = stripeGo.String(string(stripeGo.CheckoutSessionModeSubscription))
		useSubscriptionMode = true
	}

	var lineItems []*stripeGo.CheckoutSessionLineItemParams
	if useTAPSubscription {
		lineItems = []*stripeGo.CheckoutSessionLineItemParams{
			{
				Price:    stripeGo.String(in.StripePriceID),
				Quantity: stripeGo.Int64(1),
			},
		}
	} else {
		lineItems = []*stripeGo.CheckoutSessionLineItemParams{
			{
				PriceData: &stripeGo.CheckoutSessionLineItemPriceDataParams{
					Currency: stripeGo.String(in.Currency),
					ProductData: &stripeGo.CheckoutSessionLineItemPriceDataProductDataParams{
						Name: stripeGo.String(productNameOrDefault(in)),
					},
					UnitAmount: stripeGo.Int64(in.AmountCents),
				},
				Quantity: stripeGo.Int64(1),
			},
		}
	}

	params := &stripeGo.CheckoutSessionParams{
		Mode:              mode,
		LineItems:         lineItems,
		SuccessURL:        stripeGo.String(in.SuccessURL),
		CancelURL:         stripeGo.String(in.CancelURL),
		ClientReferenceID: stripeGo.String(in.PurchaseID),
		Metadata:          metadataWithDefaults(in),
	}

	// CHO-1762 — Stripe propagates Session.metadata to the Subscription
	// only when SubscriptionData.metadata is explicitly set. The CHO-1763
	// webhook handler reads Subscription.metadata to resolve back to the
	// chora purchase, so this attachment is mandatory.
	if useTAPSubscription {
		params.SubscriptionData = &stripeGo.CheckoutSessionSubscriptionDataParams{
			Metadata: metadataWithDefaults(in),
		}
	}
	// When a Customer is wired, attach the Session to it so Stripe
	// persists the PaymentMethod on first checkout and offers the
	// saved card on subsequent Sessions for the same Customer.
	// SetupFutureUsage = "off_session" tells Stripe to save the PM for
	// future merchant-initiated charges (refunds, follow-on purchases
	// without the learner re-entering the card). Per Stripe Checkout
	// "Save Card for Future Use" docs.
	if in.StripeCustomerID != "" {
		params.Customer = stripeGo.String(in.StripeCustomerID)
		if !useSubscriptionMode {
			// Subscription mode auto-saves; only Payment mode needs the
			// explicit setup_future_usage flag. The guard MUST match the
			// effective `mode` (not PurchaseType), otherwise any future
			// subscription-mode PurchaseType added past CHO-1762 (e.g.
			// TenantAddonPurchase) silently keeps PaymentIntentData and
			// Stripe 400s with "You can not pass payment_intent_data in
			// subscription mode" — surfacing to the FE as a misleading
			// 409 on /api/v1/checkout/tenant-addon.
			params.PaymentIntentData = &stripeGo.CheckoutSessionPaymentIntentDataParams{
				SetupFutureUsage: stripeGo.String(string(stripeGo.PaymentIntentSetupFutureUsageOffSession)),
			}
		}
		// Surface the returning learner's saved card on the hosted Checkout
		// page. By default Stripe only shows saved PaymentMethods whose
		// allow_redisplay == "always"; cards saved via setup_future_usage=
		// off_session in `payment` mode get allow_redisplay="limited", so
		// without widening allow_redisplay_filters the page renders a blank
		// card form (the learner re-types the card). We do NOT set
		// payment_method_save here — it governs offering a save checkbox for
		// NEW cards and conflicts with the setup_future_usage save above;
		// the filter alone makes the EXISTING saved card reusable.
		// (saved_payment_method_options requires a customer on the session,
		// satisfied in this branch.)
		params.SavedPaymentMethodOptions = &stripeGo.CheckoutSessionSavedPaymentMethodOptionsParams{
			AllowRedisplayFilters: stripeGo.StringSlice([]string{
				string(stripeGo.CheckoutSessionSavedPaymentMethodOptionsAllowRedisplayFilterAlways),
				string(stripeGo.CheckoutSessionSavedPaymentMethodOptionsAllowRedisplayFilterLimited),
				string(stripeGo.CheckoutSessionSavedPaymentMethodOptionsAllowRedisplayFilterUnspecified),
			}),
		}
	}

	return params
}

func productNameOrDefault(in CreateCheckoutSessionInput) string {
	if in.ProductName != "" {
		return in.ProductName
	}
	return string(in.PurchaseType) + " — " + in.PurchaseID
}

// ListInvoices (H+ Billing) lists Stripe Invoices for a Customer. Cursor
// pagination follows Stripe's StartingAfter convention; the last item's
// ID is the next cursor when HasMore.
func (r *RealClient) ListInvoices(_ context.Context, in ListInvoicesInput) (ListInvoicesOutput, error) {
	if in.StripeCustomerID == "" {
		return ListInvoicesOutput{}, errors.New("stripe/real: stripe_customer_id required")
	}
	stripeGo.Key = r.apiKey

	params := &stripeGo.InvoiceListParams{
		Customer: stripeGo.String(in.StripeCustomerID),
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 20
	}
	params.Limit = stripeGo.Int64(limit)
	if in.StartingAfter != "" {
		params.StartingAfter = stripeGo.String(in.StartingAfter)
	}

	items := make([]InvoiceRow, 0, limit)
	iter := invoice.List(params)
	hasMore := false
	for iter.Next() {
		inv := iter.Invoice()
		row := InvoiceRow{
			StripeInvoiceID:  inv.ID,
			Number:           inv.Number,
			Status:           string(inv.Status),
			TotalCents:       inv.Total,
			Currency:         string(inv.Currency),
			HostedInvoiceURL: inv.HostedInvoiceURL,
			InvoicePDFURL:    inv.InvoicePDF,
		}
		if inv.PeriodStart > 0 {
			row.PeriodStart = time.Unix(inv.PeriodStart, 0).UTC()
		}
		if inv.PeriodEnd > 0 {
			row.PeriodEnd = time.Unix(inv.PeriodEnd, 0).UTC()
		}
		if inv.Subscription != nil {
			row.StripeSubscriptionID = inv.Subscription.ID
		}
		items = append(items, row)
		hasMore = iter.InvoiceList().ListMeta.HasMore
	}
	if err := iter.Err(); err != nil {
		return ListInvoicesOutput{}, fmt.Errorf("stripe/real: invoice.List: %w", err)
	}
	cursor := ""
	if hasMore && len(items) > 0 {
		cursor = items[len(items)-1].StripeInvoiceID
	}
	return ListInvoicesOutput{Items: items, NextCursor: cursor}, nil
}

// CreateBillingPortalSession (H+ Billing) mints a one-shot Stripe
// Customer Portal session URL. The FE redirects the user; Stripe hosts
// payment-method management, invoice downloads, and subscription cancel
// (which we receive back via the customer.subscription.deleted webhook).
func (r *RealClient) CreateBillingPortalSession(_ context.Context, in CreateBillingPortalSessionInput) (CreateBillingPortalSessionOutput, error) {
	if in.StripeCustomerID == "" {
		return CreateBillingPortalSessionOutput{}, errors.New("stripe/real: stripe_customer_id required")
	}
	if in.ReturnURL == "" {
		return CreateBillingPortalSessionOutput{}, errors.New("stripe/real: return_url required")
	}
	stripeGo.Key = r.apiKey

	sess, err := billingportalsession.New(&stripeGo.BillingPortalSessionParams{
		Customer:  stripeGo.String(in.StripeCustomerID),
		ReturnURL: stripeGo.String(in.ReturnURL),
	})
	if err != nil {
		return CreateBillingPortalSessionOutput{}, fmt.Errorf("stripe/real: billing_portal session.New: %w", err)
	}
	return CreateBillingPortalSessionOutput{URL: sess.URL}, nil
}
