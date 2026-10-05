// client_stub_test.go — coverage for the deterministic StubClient used in
// local dev + the standalone smoke gate. Every method is side-effect free,
// so exact-assert tests are possible.
package stripe

import (
	"context"
	"strings"
	"testing"
	"time"
)

func stubCheckoutInput() CreateCheckoutSessionInput {
	return CreateCheckoutSessionInput{
		PurchaseType:     PurchaseTypeCoursePurchase,
		PurchaseID:       "0190aaaa-0000-7000-8000-000000000001",
		TenantID:         "11111111-1111-7111-8111-111111111111",
		LearnerGCID:      "00000000-0000-7000-8000-000000001999",
		AmountCents:      1990,
		Currency:         "sgd",
		ProductName:      "CSPO Course",
		SuccessURL:       "https://chora.site/course/success",
		CancelURL:        "https://chora.site/course/cancel",
		StripeCustomerID: "cus_stub_test",
		Metadata:         map[string]string{"course_id": "course_0001"},
	}
}

func TestStubClient_CreateCheckoutSession_Happy(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.CreateCheckoutSession(context.Background(), stubCheckoutInput())
	if err != nil {
		t.Fatalf("CreateCheckoutSession: %v", err)
	}
	if !strings.HasPrefix(out.StripeSessionID, "cs_stub_") {
		t.Errorf("session id=%q, want cs_stub_ prefix", out.StripeSessionID)
	}
	if !strings.Contains(out.StripeCheckoutURL, out.StripeSessionID) {
		t.Errorf("checkout URL=%q, want to embed session id", out.StripeCheckoutURL)
	}
}

func TestStubClient_CreateCheckoutSession_Deterministic(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	in := stubCheckoutInput()
	one, err := c.CreateCheckoutSession(context.Background(), in)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	two, err := c.CreateCheckoutSession(context.Background(), in)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if one.StripeSessionID != two.StripeSessionID {
		t.Errorf("session ids differ: %q vs %q", one.StripeSessionID, two.StripeSessionID)
	}
}

func TestStubClient_CreateCheckoutSession_Validation(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	base := stubCheckoutInput()
	bad := []struct {
		name   string
		mutate func(*CreateCheckoutSessionInput)
		want   string
	}{
		{"purchase_id", func(i *CreateCheckoutSessionInput) { i.PurchaseID = "" }, "purchase_id required"},
		{"tenant_id", func(i *CreateCheckoutSessionInput) { i.TenantID = "" }, "tenant_id required"},
		{"learner_gcid", func(i *CreateCheckoutSessionInput) { i.LearnerGCID = "" }, "learner_gcid required"},
		{"negative amount", func(i *CreateCheckoutSessionInput) { i.AmountCents = -1 }, "amount_cents"},
		{"currency len", func(i *CreateCheckoutSessionInput) { i.Currency = "S" }, "currency"},
		{"no urls", func(i *CreateCheckoutSessionInput) { i.SuccessURL = "" }, "success_url"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			tc.mutate(&in)
			_, err := c.CreateCheckoutSession(context.Background(), in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err=%v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestStubClient_EnsureCustomer(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "t1", LearnerGCID: "g1", Email: "a@b.c"})
	if err != nil {
		t.Fatalf("EnsureCustomer: %v", err)
	}
	if !strings.HasPrefix(out.StripeCustomerID, "cus_stub_") {
		t.Errorf("customer id=%q, want cus_stub_ prefix", out.StripeCustomerID)
	}
	// Deterministic per (tenant, gcid).
	again, _ := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "t1", LearnerGCID: "g1"})
	if again.StripeCustomerID != out.StripeCustomerID {
		t.Errorf("not deterministic: %q vs %q", again.StripeCustomerID, out.StripeCustomerID)
	}
	if _, err := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "", LearnerGCID: "g1"}); err == nil {
		t.Error("missing tenant should error")
	}
	if _, err := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "t1", LearnerGCID: ""}); err == nil {
		t.Error("missing gcid should error")
	}
}

func TestStubClient_RefundCharge(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.RefundCharge(context.Background(), RefundChargeInput{StripeChargeID: "ch_test_1", AmountCents: 500, Reason: "customer_request"})
	if err != nil {
		t.Fatalf("RefundCharge: %v", err)
	}
	if !strings.HasPrefix(out.StripeRefundID, "re_stub_") {
		t.Errorf("refund id=%q", out.StripeRefundID)
	}
	if out.AmountCentsRefunded != 500 {
		t.Errorf("AmountCentsRefunded=%d, want 500", out.AmountCentsRefunded)
	}
	if _, err := c.RefundCharge(context.Background(), RefundChargeInput{StripeChargeID: "  "}); err == nil {
		t.Error("blank charge id should error")
	}
}

func TestStubClient_CancelSubscription(t *testing.T) {
	t.Parallel()
	c := NewStubClient()

	graceful, err := c.CancelSubscription(context.Background(), CancelSubscriptionInput{StripeSubscriptionID: "sub_1", CancelAtPeriodEnd: true})
	if err != nil {
		t.Fatalf("graceful: %v", err)
	}
	if graceful.Status != "active" || !graceful.CancelAtPeriodEnd {
		t.Errorf("graceful out=%+v", graceful)
	}
	if !graceful.EffectiveAt.After(time.Now()) {
		t.Errorf("graceful EffectiveAt=%v, want future", graceful.EffectiveAt)
	}
	if !graceful.CancelledAt.IsZero() {
		t.Errorf("graceful CancelledAt=%v, want zero", graceful.CancelledAt)
	}

	immediate, err := c.CancelSubscription(context.Background(), CancelSubscriptionInput{StripeSubscriptionID: "sub_1", CancelAtPeriodEnd: false})
	if err != nil {
		t.Fatalf("immediate: %v", err)
	}
	if immediate.Status != "canceled" {
		t.Errorf("immediate Status=%s, want canceled", immediate.Status)
	}
	if immediate.CancelledAt.IsZero() {
		t.Errorf("immediate CancelledAt zero, want set")
	}
	if _, err := c.CancelSubscription(context.Background(), CancelSubscriptionInput{StripeSubscriptionID: "  "}); err == nil {
		t.Error("blank sub id should error")
	}
}

func TestStubClient_UpdateSubscriptionPrice(t *testing.T) {
	t.Parallel()
	c := NewStubClient()

	prorated, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "create_prorations"})
	if err != nil {
		t.Fatalf("prorated: %v", err)
	}
	if prorated.Status != "active" || prorated.BillingDeltaCents <= 0 {
		t.Errorf("prorated out=%+v", prorated)
	}
	if prorated.Currency != "usd" {
		t.Errorf("Currency=%s", prorated.Currency)
	}

	none, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "none"})
	if err != nil {
		t.Fatalf("none: %v", err)
	}
	if none.BillingDeltaCents != 0 {
		t.Errorf("none delta=%d, want 0", none.BillingDeltaCents)
	}

	def, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1"})
	if err != nil {
		t.Fatalf("default behavior: %v", err)
	}
	if def.BillingDeltaCents <= 0 {
		t.Errorf("default delta=%d, want > 0", def.BillingDeltaCents)
	}

	if _, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "", NewPriceID: "price_1"}); err == nil {
		t.Error("blank sub id should error")
	}
	if _, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: ""}); err == nil {
		t.Error("blank price id should error")
	}
}

func TestStubClient_PreviewSubscriptionPriceChange(t *testing.T) {
	t.Parallel()
	c := NewStubClient()

	prorated, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "create_prorations"})
	if err != nil {
		t.Fatalf("prorated: %v", err)
	}
	if prorated.BillingDeltaCents <= 0 || prorated.NextInvoiceTotalCents <= prorated.BillingDeltaCents {
		t.Errorf("prorated out=%+v", prorated)
	}
	if prorated.Currency != "usd" {
		t.Errorf("Currency=%s", prorated.Currency)
	}

	none, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "none"})
	if err != nil {
		t.Fatalf("none: %v", err)
	}
	if none.BillingDeltaCents != 0 || none.NextInvoiceTotalCents != 0 {
		t.Errorf("none out=%+v", none)
	}

	if _, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "", NewPriceID: "p"}); err == nil {
		t.Error("blank sub id should error")
	}
	if _, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "s", NewPriceID: ""}); err == nil {
		t.Error("blank price id should error")
	}
}

func TestStubClient_GetSubscription(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.GetSubscription(context.Background(), "sub_1")
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if out.Status != "active" {
		t.Errorf("Status=%s, want active", out.Status)
	}
	if !out.CurrentPeriodEnd.After(out.CurrentPeriodStart) {
		t.Errorf("period end %v not after start %v", out.CurrentPeriodEnd, out.CurrentPeriodStart)
	}
	if _, err := c.GetSubscription(context.Background(), "  "); err == nil {
		t.Error("blank sub id should error")
	}
}

func TestStubClient_GetPrice(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.GetPrice(context.Background(), "price_1")
	if err != nil {
		t.Fatalf("GetPrice: %v", err)
	}
	if out.UnitAmountCents <= 0 || out.Currency != "usd" {
		t.Errorf("GetPrice out=%+v", out)
	}
	if _, err := c.GetPrice(context.Background(), "  "); err == nil {
		t.Error("blank price id should error")
	}
}

func TestStubClient_ListInvoices(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.ListInvoices(context.Background(), ListInvoicesInput{StripeCustomerID: "cus_1", Limit: 10, StartingAfter: "in_prev"})
	if err != nil {
		t.Fatalf("ListInvoices: %v", err)
	}
	if len(out.Items) != 2 {
		t.Errorf("items=%d, want 2", len(out.Items))
	}
	for _, inv := range out.Items {
		if inv.Status != "paid" || inv.TotalCents != 4900 {
			t.Errorf("invoice row=%+v", inv)
		}
	}
	if out.NextCursor != "" {
		t.Errorf("NextCursor=%q, want empty for stub", out.NextCursor)
	}
	if _, err := c.ListInvoices(context.Background(), ListInvoicesInput{StripeCustomerID: "  "}); err == nil {
		t.Error("blank customer id should error")
	}
}

func TestStubClient_CreateBillingPortalSession(t *testing.T) {
	t.Parallel()
	c := NewStubClient()
	out, err := c.CreateBillingPortalSession(context.Background(), CreateBillingPortalSessionInput{StripeCustomerID: "cus_1", ReturnURL: "https://chora.site/billing"})
	if err != nil {
		t.Fatalf("CreateBillingPortalSession: %v", err)
	}
	if !strings.Contains(out.URL, "billing.stripe.com") {
		t.Errorf("URL=%q", out.URL)
	}
	if _, err := c.CreateBillingPortalSession(context.Background(), CreateBillingPortalSessionInput{StripeCustomerID: "  "}); err == nil {
		t.Error("blank customer id should error")
	}
}

func TestValidateInput(t *testing.T) {
	t.Parallel()
	if err := validateInput(stubCheckoutInput()); err != nil {
		t.Errorf("valid input rejected: %v", err)
	}
}

func TestMetadataWithDefaults(t *testing.T) {
	t.Parallel()
	in := stubCheckoutInput()
	in.Metadata = map[string]string{"course_id": "course_0001"}
	m := metadataWithDefaults(in)
	for _, k := range []string{"purchase_id", "tenant_id", "learner_gcid", "purchase_type", "course_id"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q", k)
		}
	}
	if m["purchase_type"] != string(PurchaseTypeCoursePurchase) {
		t.Errorf("purchase_type=%q", m["purchase_type"])
	}

	// nil input metadata must not panic.
	if m2 := metadataWithDefaults(CreateCheckoutSessionInput{PurchaseID: "p", TenantID: "t", LearnerGCID: "g"}); m2["purchase_id"] != "p" {
		t.Errorf("nil-metadata defaults: %v", m2)
	}
}

func TestCustomerMetadataWithDefaults(t *testing.T) {
	t.Parallel()
	m := customerMetadataWithDefaults(EnsureCustomerInput{TenantID: "t1", LearnerGCID: "g1", Metadata: map[string]string{"email": "a@b.c"}})
	if m["tenant_id"] != "t1" || m["learner_gcid"] != "g1" || m["email"] != "a@b.c" {
		t.Errorf("metadata=%v", m)
	}
}
