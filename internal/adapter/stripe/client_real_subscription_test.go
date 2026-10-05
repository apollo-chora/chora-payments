// client_real_subscription_test.go — CHO-1762.
//
// Pins the new Stripe Checkout flow for tenant-addon Subscribe:
// mode=subscription + LineItems[].Price referencing a pre-existing
// Stripe Price ID (resolved from the CHO-1760 catalogue at the gRPC
// layer) + SubscriptionData.Metadata so the Subscription receives the
// chora_purchase_id correlation keys needed by CHO-1763 webhooks.
package stripe

import (
	"testing"

	stripeGo "github.com/stripe/stripe-go/v78"
)

func subscribeTAPInput() CreateCheckoutSessionInput {
	return CreateCheckoutSessionInput{
		PurchaseType:     PurchaseTypeTenantAddonPurchase,
		PurchaseID:       "0190aaaa-0000-7000-8000-000000000001",
		TenantID:         "11111111-1111-7111-8111-111111111111",
		LearnerGCID:      "00000000-0000-7000-8000-000000001999",
		AmountCents:      4900,
		Currency:         "usd",
		StripePriceID:    "price_1Tabcde",
		StripeCustomerID: "cus_test",
		SuccessURL:       "https://chora.site/h/marketplace/x?checkout=success",
		CancelURL:        "https://chora.site/h/marketplace/x?checkout=cancel",
		Metadata: map[string]string{
			"addon_plan_id": "0190bbbb-0000-7000-8000-000000000002",
			"addon_code":    "tms",
			"tier_code":     "pro",
		},
	}
}

// TenantAddonPurchase + StripePriceID set → mode=subscription, Price
// referenced (not PriceData), no UnitAmount fields.
func TestBuildCheckoutSessionParams_TenantAddon_PriceID_UsesSubscriptionMode(t *testing.T) {
	in := subscribeTAPInput()
	params := buildCheckoutSessionParams(in)
	if params.Mode == nil || *params.Mode != string(stripeGo.CheckoutSessionModeSubscription) {
		t.Fatalf("mode not subscription: %v", params.Mode)
	}
	if len(params.LineItems) != 1 {
		t.Fatalf("expected 1 line item, got %d", len(params.LineItems))
	}
	li := params.LineItems[0]
	if li.PriceData != nil {
		t.Errorf("subscription line item must not carry inline PriceData: %+v", li.PriceData)
	}
	if li.Price == nil || *li.Price != "price_1Tabcde" {
		t.Errorf("expected Price=price_1Tabcde, got %v", li.Price)
	}
	if li.Quantity == nil || *li.Quantity != 1 {
		t.Errorf("expected quantity=1, got %v", li.Quantity)
	}
}

// SubscriptionData.Metadata is REQUIRED so customer.subscription.* webhooks
// (which carry Subscription.metadata, NOT Session.metadata) can resolve back
// to the chora_purchase_id correlation keys.
func TestBuildCheckoutSessionParams_TenantAddon_PriceID_AttachesSubscriptionDataMetadata(t *testing.T) {
	in := subscribeTAPInput()
	params := buildCheckoutSessionParams(in)
	if params.SubscriptionData == nil {
		t.Fatalf("SubscriptionData not set on mode=subscription tenant-addon flow")
	}
	md := params.SubscriptionData.Metadata
	if md == nil {
		t.Fatalf("SubscriptionData.Metadata nil")
	}
	// The CHO-1763 webhook handler looks up by these keys, so they MUST
	// flow into the Subscription's metadata.
	for k, want := range map[string]string{
		"purchase_id":   in.PurchaseID,
		"tenant_id":     in.TenantID,
		"learner_gcid":  in.LearnerGCID,
		"purchase_type": string(PurchaseTypeTenantAddonPurchase),
		"addon_code":    "tms",
		"tier_code":     "pro",
	} {
		if md[k] != want {
			t.Errorf("SubscriptionData.Metadata[%q] = %q, want %q", k, md[k], want)
		}
	}
}

// TenantAddon + StripePriceID empty → fall through to the existing
// mode=payment + ad-hoc PriceData path. CHO-1762's runtime guard lives in
// the gRPC server (refuses with stripe_price_missing); buildCheckoutSessionParams
// is best-effort and doesn't enforce. This keeps the builder pure +
// allows the legacy mode=payment path to remain testable.
func TestBuildCheckoutSessionParams_TenantAddon_EmptyPriceID_FallsBackToPaymentMode(t *testing.T) {
	in := subscribeTAPInput()
	in.StripePriceID = ""
	params := buildCheckoutSessionParams(in)
	if params.Mode == nil || *params.Mode != string(stripeGo.CheckoutSessionModePayment) {
		t.Errorf("expected mode=payment fallback when PriceID empty; got %v", params.Mode)
	}
	if len(params.LineItems) != 1 {
		t.Fatalf("expected 1 line item, got %d", len(params.LineItems))
	}
	if params.LineItems[0].Price != nil {
		t.Errorf("Price must be nil on fallback path; got %v", params.LineItems[0].Price)
	}
	if params.LineItems[0].PriceData == nil {
		t.Errorf("PriceData must be set on fallback path")
	}
	if params.SubscriptionData != nil {
		t.Errorf("SubscriptionData must be nil on fallback path; got %+v", params.SubscriptionData)
	}
}

// Existing non-TAP cases — Setting StripePriceID on a non-TAP PurchaseType
// is ignored (we only opt in to mode=subscription via the explicit
// TenantAddon discriminator). UserSubscription is the legacy path and
// stays on its existing branch.
func TestBuildCheckoutSessionParams_NonTAPPurchaseType_IgnoresPriceID(t *testing.T) {
	in := subscribeTAPInput()
	in.PurchaseType = PurchaseTypeUserManaTopUp
	in.StripePriceID = "price_should_be_ignored"
	params := buildCheckoutSessionParams(in)
	if params.LineItems[0].Price != nil {
		t.Errorf("non-TAP must ignore StripePriceID; Price=%v", params.LineItems[0].Price)
	}
	if params.SubscriptionData != nil {
		t.Errorf("non-TAP must not set SubscriptionData; got %+v", params.SubscriptionData)
	}
}
