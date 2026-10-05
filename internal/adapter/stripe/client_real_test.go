// client_real_test.go — unit tests for the pure Stripe Checkout Session
// param builder. These assert the param object WITHOUT touching Stripe's
// network API (CreateCheckoutSession's only side effect beyond
// buildCheckoutSessionParams is session.New, which needs a live backend).
//
// The headline coverage here is the "render the returning learner's saved
// card" gap (2026-06-04): when a Customer is attached, Stripe by default
// only surfaces saved PaymentMethods whose allow_redisplay == "always".
// Cards saved via setup_future_usage=off_session in `payment` mode get
// allow_redisplay="limited", so without an explicit
// saved_payment_method_options.allow_redisplay_filters the hosted Checkout
// page shows a BLANK card form. We widen the filter to surface limited +
// unspecified PMs too.
package stripe

import (
	"testing"

	stripeGo "github.com/stripe/stripe-go/v78"
)

func baseInput() CreateCheckoutSessionInput {
	return CreateCheckoutSessionInput{
		PurchaseType: PurchaseTypeUserManaTopUp,
		PurchaseID:   "0190aaaa-0000-7000-8000-000000000001",
		TenantID:     "11111111-1111-7111-8111-111111111111",
		LearnerGCID:  "00000000-0000-7000-8000-000000001999",
		AmountCents:  500,
		Currency:     "sgd",
		SuccessURL:   "https://chora.site/a/wallet?mana_topup=success",
		CancelURL:    "https://chora.site/a/wallet?mana_topup=cancel",
	}
}

func filterSet(filters []*string) map[string]bool {
	out := make(map[string]bool, len(filters))
	for _, f := range filters {
		if f != nil {
			out[*f] = true
		}
	}
	return out
}

// With a Customer attached on a payment-mode session, the builder must
// (a) attach the Customer, (b) keep setup_future_usage=off_session, and
// (c) set saved_payment_method_options.allow_redisplay_filters so the
// previously-saved (allow_redisplay="limited") card renders for reuse.
func TestBuildCheckoutSessionParams_PaymentMode_WithCustomer_SurfacesSavedCard(t *testing.T) {
	in := baseInput()
	in.StripeCustomerID = "cus_UaCiFpMAqNGQD7"

	params := buildCheckoutSessionParams(in)

	if params.Customer == nil || *params.Customer != "cus_UaCiFpMAqNGQD7" {
		t.Fatalf("Customer not attached: %v", params.Customer)
	}
	if params.PaymentIntentData == nil || params.PaymentIntentData.SetupFutureUsage == nil ||
		*params.PaymentIntentData.SetupFutureUsage != string(stripeGo.PaymentIntentSetupFutureUsageOffSession) {
		t.Fatalf("setup_future_usage not off_session: %+v", params.PaymentIntentData)
	}
	if params.SavedPaymentMethodOptions == nil {
		t.Fatal("SavedPaymentMethodOptions is nil — saved card will not render on hosted Checkout")
	}
	got := filterSet(params.SavedPaymentMethodOptions.AllowRedisplayFilters)
	for _, want := range []string{
		string(stripeGo.CheckoutSessionSavedPaymentMethodOptionsAllowRedisplayFilterAlways),
		string(stripeGo.CheckoutSessionSavedPaymentMethodOptionsAllowRedisplayFilterLimited),
		string(stripeGo.CheckoutSessionSavedPaymentMethodOptionsAllowRedisplayFilterUnspecified),
	} {
		if !got[want] {
			t.Errorf("allow_redisplay_filters missing %q (got %v)", want, got)
		}
	}
}

// Subscription mode also attaches the Customer and surfaces saved cards,
// but does NOT set PaymentIntentData (Stripe auto-saves the PM for the
// subscription).
func TestBuildCheckoutSessionParams_SubscriptionMode_WithCustomer_SurfacesSavedCard(t *testing.T) {
	in := baseInput()
	in.PurchaseType = PurchaseTypeUserSubscription
	in.StripeCustomerID = "cus_UaCiFpMAqNGQD7"

	params := buildCheckoutSessionParams(in)

	if params.Mode == nil || *params.Mode != string(stripeGo.CheckoutSessionModeSubscription) {
		t.Fatalf("mode not subscription: %v", params.Mode)
	}
	if params.PaymentIntentData != nil {
		t.Errorf("subscription mode must not set PaymentIntentData: %+v", params.PaymentIntentData)
	}
	if params.SavedPaymentMethodOptions == nil {
		t.Fatal("SavedPaymentMethodOptions is nil for returning subscriber")
	}
	if !filterSet(params.SavedPaymentMethodOptions.AllowRedisplayFilters)["limited"] {
		t.Error("allow_redisplay_filters must include 'limited' so off_session-saved cards render")
	}
}

// CHO-1762 added a SECOND subscription-mode branch (PurchaseTypeTenantAddonPurchase
// with a resolved Stripe Price ID) without extending the earlier
// PurchaseTypeUserSubscription test. Without this case the
// `PaymentIntentData != nil` guard at the bottom of buildCheckoutSessionParams
// only caught the legacy UserSubscription path — the TAP subscription
// path silently kept setting PaymentIntentData, which Stripe rejects at
// session.New time with HTTP 400 "You can not pass `payment_intent_data`
// in `subscription` mode." That 400 surfaced as a 409 to the FE on
// `/api/v1/checkout/tenant-addon` because the gRPC error classifier maps
// stripe.invalid_request_error to AlreadyExists for the Subscribe path.
// Regression caught on local pre-flight smoke 2026-06-16.
func TestBuildCheckoutSessionParams_TAPSubscription_WithCustomer_OmitsPaymentIntentData(t *testing.T) {
	in := baseInput()
	in.PurchaseType = PurchaseTypeTenantAddonPurchase
	in.StripePriceID = "price_1TixRpBWYn4OgbxK5hsTN1J4"
	in.StripeCustomerID = "cus_UaCiFpMAqNGQD7"

	params := buildCheckoutSessionParams(in)

	if params.Mode == nil || *params.Mode != string(stripeGo.CheckoutSessionModeSubscription) {
		t.Fatalf("mode not subscription: %v", params.Mode)
	}
	if params.PaymentIntentData != nil {
		t.Fatalf("subscription mode MUST NOT set PaymentIntentData (Stripe 400s on it): %+v", params.PaymentIntentData)
	}
	// SubscriptionData.Metadata still required so the webhook can resolve
	// Subscription → purchase (CHO-1762 / CHO-1763 contract).
	if params.SubscriptionData == nil || params.SubscriptionData.Metadata == nil {
		t.Errorf("SubscriptionData.Metadata MUST be set so CHO-1763 webhook can resolve the purchase")
	}
}

// Without a Customer there is nothing to display — saved_payment_method_options
// MUST be omitted (Stripe rejects it without a customer on the session).
func TestBuildCheckoutSessionParams_NoCustomer_OmitsSavedPaymentMethodOptions(t *testing.T) {
	in := baseInput() // StripeCustomerID == ""

	params := buildCheckoutSessionParams(in)

	if params.Customer != nil {
		t.Errorf("Customer must be nil when StripeCustomerID empty: %v", *params.Customer)
	}
	if params.PaymentIntentData != nil {
		t.Errorf("PaymentIntentData must be nil without a customer: %+v", params.PaymentIntentData)
	}
	if params.SavedPaymentMethodOptions != nil {
		t.Errorf("SavedPaymentMethodOptions must be nil without a customer: %+v", params.SavedPaymentMethodOptions)
	}
}

// Sanity: the existing line-item + metadata wiring is preserved by the
// extraction (guards against the refactor dropping fields).
func TestBuildCheckoutSessionParams_PreservesLineItemAndMetadata(t *testing.T) {
	in := baseInput()

	params := buildCheckoutSessionParams(in)

	if len(params.LineItems) != 1 || params.LineItems[0].PriceData == nil ||
		*params.LineItems[0].PriceData.UnitAmount != 500 {
		t.Fatalf("line item amount wrong: %+v", params.LineItems)
	}
	if params.Metadata["purchase_type"] != string(PurchaseTypeUserManaTopUp) ||
		params.Metadata["learner_gcid"] != in.LearnerGCID {
		t.Errorf("metadata defaults missing: %v", params.Metadata)
	}
	if params.ClientReferenceID == nil || *params.ClientReferenceID != in.PurchaseID {
		t.Errorf("client_reference_id not set to purchase_id: %v", params.ClientReferenceID)
	}
}
