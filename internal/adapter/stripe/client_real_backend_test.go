// client_real_backend_test.go — RealClient coverage via a hermetic mock
// stripe.Backend installed with stripe.SetBackend. No live Stripe network
// calls; each test asserts the output mapping + validation branches.
package stripe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"
	"github.com/stripe/stripe-go/v78/form"
)

// mockBackend implements stripeGo.Backend, returning canned objects keyed
// on the request path.
type mockBackend struct {
	calls []string

	// subscription Get (GET /v1/subscriptions/{id}).
	subGet *stripeGo.Subscription
	// subscription Update/Cancel result (POST/DELETE /v1/subscriptions/{id}).
	subPost *stripeGo.Subscription
	// invoice.Get single result.
	invoice *stripeGo.Invoice
	// invoice.Upcoming result.
	upcoming     *stripeGo.Invoice
	invoiceList  []*stripeGo.Invoice
	listHasMore  bool
	scheduleResp *stripeGo.SubscriptionSchedule
	// err returns instead of the canned response when set.
	err error
	// listCallCount counts invoice-list pages served.
	listCallCount int
}

func (m *mockBackend) record(method, path string) {
	m.calls = append(m.calls, method+" "+path)
}

func (m *mockBackend) Call(method, path, key string, params stripeGo.ParamsContainer, v stripeGo.LastResponseSetter) error {
	m.record(method, path)
	if m.err != nil {
		return m.err
	}
	switch {
	case method == http.MethodPost && path == "/v1/checkout/sessions":
		s := v.(*stripeGo.CheckoutSession)
		s.ID = "cs_test_mock"
		s.URL = "https://checkout.stripe.com/c/pay/cs_test_mock"
	case method == http.MethodPost && path == "/v1/refunds":
		r := v.(*stripeGo.Refund)
		r.ID = "re_test_mock"
		r.Amount = 500
	case method == http.MethodPost && path == "/v1/customers":
		c := v.(*stripeGo.Customer)
		c.ID = "cus_test_mock"
	case method == http.MethodPost && path == "/v1/billing_portal/sessions":
		s := v.(*stripeGo.BillingPortalSession)
		s.URL = "https://billing.stripe.com/p/session/test_mock"
	case method == http.MethodPost && path == "/v1/subscription_schedules":
		if m.scheduleResp != nil {
			*v.(*stripeGo.SubscriptionSchedule) = *m.scheduleResp
		}
	case strings.HasPrefix(path, "/v1/subscription_schedules/"):
		if m.scheduleResp != nil {
			*v.(*stripeGo.SubscriptionSchedule) = *m.scheduleResp
		}
	case strings.HasPrefix(path, "/v1/invoices/upcoming"):
		if m.upcoming != nil {
			*v.(*stripeGo.Invoice) = *m.upcoming
		}
	case strings.HasPrefix(path, "/v1/invoices/"):
		if m.invoice != nil {
			*v.(*stripeGo.Invoice) = *m.invoice
		}
	case strings.HasPrefix(path, "/v1/subscriptions/"):
		if m.subPost != nil {
			*v.(*stripeGo.Subscription) = *m.subPost
		} else if m.subGet != nil {
			*v.(*stripeGo.Subscription) = *m.subGet
		}
	case strings.HasPrefix(path, "/v1/prices/"):
		p := v.(*stripeGo.Price)
		p.ID = "price_test_mock"
		p.UnitAmount = 7900
		p.Currency = stripeGo.CurrencyUSD
	default:
		return fmt.Errorf("mockBackend: unexpected Call %s %s", method, path)
	}
	return nil
}

func (m *mockBackend) CallRaw(method, path, key string, body *form.Values, params *stripeGo.Params, v stripeGo.LastResponseSetter) error {
	m.record(method, path)
	if m.err != nil {
		return m.err
	}
	switch {
	case strings.HasPrefix(path, "/v1/invoices"):
		list := v.(*stripeGo.InvoiceList)
		// Serve the canned page once; subsequent pages are empty so the
		// iterator terminates (a never-ending HasMore=true would loop).
		if m.listCallCount == 0 {
			list.Data = m.invoiceList
			list.HasMore = m.listHasMore
		}
		m.listCallCount++
	default:
		return fmt.Errorf("mockBackend: unexpected CallRaw %s %s", method, path)
	}
	return nil
}

func (m *mockBackend) CallStreaming(method, path, key string, params stripeGo.ParamsContainer, v stripeGo.StreamingLastResponseSetter) error {
	return errors.New("mockBackend: CallStreaming not supported")
}

func (m *mockBackend) CallMultipart(method, path, key, boundary string, body *bytes.Buffer, params *stripeGo.Params, v stripeGo.LastResponseSetter) error {
	return errors.New("mockBackend: CallMultipart not supported")
}

func (m *mockBackend) SetMaxNetworkRetries(max int64) {}

// withMockClient installs the mock backend for the duration of the test and
// returns a RealClient wired to it.
func withMockClient(t *testing.T, mock *mockBackend) *RealClient {
	t.Helper()
	prev := stripeGo.GetBackend(stripeGo.APIBackend)
	stripeGo.SetBackend(stripeGo.APIBackend, mock)
	t.Cleanup(func() { stripeGo.SetBackend(stripeGo.APIBackend, prev) })
	c, err := NewRealClient("sk_test_mock", true)
	if err != nil {
		t.Fatalf("NewRealClient: %v", err)
	}
	return c
}

func TestNewRealClient_RequiresKey(t *testing.T) {
	if _, err := NewRealClient("", true); err == nil {
		t.Fatal("NewRealClient with empty key should error")
	}
	c, err := NewRealClient("sk_test_abc", false)
	if err != nil {
		t.Fatalf("NewRealClient: %v", err)
	}
	if c.apiKey != "sk_test_abc" || c.testMode {
		t.Errorf("client state: key=%s testMode=%v", c.apiKey, c.testMode)
	}
}

func TestRealClient_CreateCheckoutSession_Happy(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	out, err := c.CreateCheckoutSession(context.Background(), stubCheckoutInput())
	if err != nil {
		t.Fatalf("CreateCheckoutSession: %v", err)
	}
	if out.StripeSessionID != "cs_test_mock" {
		t.Errorf("session id=%s", out.StripeSessionID)
	}
	if out.StripeCheckoutURL != "https://checkout.stripe.com/c/pay/cs_test_mock" {
		t.Errorf("url=%s", out.StripeCheckoutURL)
	}
	if len(mock.calls) != 1 || mock.calls[0] != "POST /v1/checkout/sessions" {
		t.Errorf("calls=%v", mock.calls)
	}
}

func TestRealClient_CreateCheckoutSession_ValidationError(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	in := stubCheckoutInput()
	in.PurchaseID = ""
	if _, err := c.CreateCheckoutSession(context.Background(), in); err == nil {
		t.Fatal("expected validation error")
	}
	if len(mock.calls) != 0 {
		t.Errorf("no backend call expected, got %v", mock.calls)
	}
}

func TestRealClient_RefundCharge(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	out, err := c.RefundCharge(context.Background(), RefundChargeInput{StripeChargeID: "ch_1", AmountCents: 500, Reason: "fraud"})
	if err != nil {
		t.Fatalf("RefundCharge: %v", err)
	}
	if out.StripeRefundID != "re_test_mock" || out.AmountCentsRefunded != 500 {
		t.Errorf("out=%+v", out)
	}
	if _, err := c.RefundCharge(context.Background(), RefundChargeInput{StripeChargeID: ""}); err == nil {
		t.Error("blank charge id should error")
	}
}

func TestRealClient_RefundCharge_FullRefundNoAmount(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	if _, err := c.RefundCharge(context.Background(), RefundChargeInput{StripeChargeID: "ch_1"}); err != nil {
		t.Fatalf("full refund: %v", err)
	}
}

func TestRealClient_EnsureCustomer(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	out, err := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "t1", LearnerGCID: "g1", Email: "a@b.c"})
	if err != nil {
		t.Fatalf("EnsureCustomer: %v", err)
	}
	if out.StripeCustomerID != "cus_test_mock" {
		t.Errorf("customer id=%s", out.StripeCustomerID)
	}
	c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "t1", LearnerGCID: "g1"})
	if len(mock.calls) != 2 {
		t.Errorf("calls=%v, want 2 customer creates", mock.calls)
	}
	if _, err := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "", LearnerGCID: "g1"}); err == nil {
		t.Error("missing tenant should error")
	}
	if _, err := c.EnsureCustomer(context.Background(), EnsureCustomerInput{TenantID: "t1", LearnerGCID: ""}); err == nil {
		t.Error("missing gcid should error")
	}
}

func subscriptionResult(status stripeGo.SubscriptionStatus, cancelAtPeriodEnd bool) *stripeGo.Subscription {
	now := time.Now().UTC().Unix()
	s := &stripeGo.Subscription{
		ID:                 "sub_test_mock",
		Status:             status,
		CancelAtPeriodEnd:  cancelAtPeriodEnd,
		CurrentPeriodStart: now - 86400,
		CurrentPeriodEnd:   now + 86400,
		CanceledAt:         0,
		Currency:           stripeGo.CurrencyUSD,
		Customer:           &stripeGo.Customer{ID: "cus_test_mock"},
		Items: &stripeGo.SubscriptionItemList{
			Data: []*stripeGo.SubscriptionItem{{ID: "si_test_mock"}},
		},
	}
	if !cancelAtPeriodEnd {
		s.CanceledAt = now
	}
	return s
}

func TestRealClient_CancelSubscription_Graceful(t *testing.T) {
	mock := &mockBackend{subPost: subscriptionResult(stripeGo.SubscriptionStatusActive, true)}
	c := withMockClient(t, mock)
	out, err := c.CancelSubscription(context.Background(), CancelSubscriptionInput{StripeSubscriptionID: "sub_1", CancelAtPeriodEnd: true, Reason: "too expensive"})
	if err != nil {
		t.Fatalf("CancelSubscription: %v", err)
	}
	if !out.CancelAtPeriodEnd || out.Status != "active" {
		t.Errorf("out=%+v", out)
	}
	if out.EffectiveAt.IsZero() {
		t.Error("graceful EffectiveAt should be set from current_period_end")
	}
	if len(mock.calls) != 1 || !strings.HasPrefix(mock.calls[0], "POST /v1/subscriptions/") {
		t.Errorf("calls=%v", mock.calls)
	}
}

func TestRealClient_CancelSubscription_Immediate(t *testing.T) {
	mock := &mockBackend{subPost: subscriptionResult(stripeGo.SubscriptionStatusCanceled, false)}
	c := withMockClient(t, mock)
	out, err := c.CancelSubscription(context.Background(), CancelSubscriptionInput{StripeSubscriptionID: "sub_1", CancelAtPeriodEnd: false})
	if err != nil {
		t.Fatalf("CancelSubscription: %v", err)
	}
	if out.Status != "canceled" || out.CancelledAt.IsZero() {
		t.Errorf("out=%+v", out)
	}
	if !strings.HasPrefix(mock.calls[0], "DELETE /v1/subscriptions/") {
		t.Errorf("calls=%v", mock.calls)
	}
	if _, err := c.CancelSubscription(context.Background(), CancelSubscriptionInput{StripeSubscriptionID: ""}); err == nil {
		t.Error("blank sub id should error")
	}
}

func TestRealClient_UpdateSubscriptionPrice_CreateProrations(t *testing.T) {
	mock := &mockBackend{
		subGet:  subscriptionResult(stripeGo.SubscriptionStatusActive, false),
		subPost: subscriptionResult(stripeGo.SubscriptionStatusActive, false),
		upcoming: &stripeGo.Invoice{
			ID:       "in_upcoming",
			Currency: stripeGo.CurrencyUSD,
			Lines: &stripeGo.InvoiceLineItemList{
				Data: []*stripeGo.InvoiceLineItem{
					{Proration: true, Amount: 1200},
					{Proration: false, Amount: 4900},
				},
			},
		},
	}
	c := withMockClient(t, mock)
	out, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "create_prorations"})
	if err != nil {
		t.Fatalf("UpdateSubscriptionPrice: %v", err)
	}
	if out.BillingDeltaCents != 1200 {
		t.Errorf("delta=%d, want 1200 (proration lines only)", out.BillingDeltaCents)
	}
	if out.NewPriceID != "price_1" || out.Status != "active" || out.Currency != "usd" {
		t.Errorf("out=%+v", out)
	}
	// subscription.Get → subscription.Update → invoice.Upcoming.
	if len(mock.calls) != 3 {
		t.Errorf("calls=%v", mock.calls)
	}
}

func TestRealClient_UpdateSubscriptionPrice_AlwaysInvoice(t *testing.T) {
	latest := &stripeGo.Invoice{ID: "in_latest", Total: -1500, Currency: stripeGo.CurrencyUSD}
	mock := &mockBackend{
		subGet: subscriptionResult(stripeGo.SubscriptionStatusActive, false),
		subPost: func() *stripeGo.Subscription {
			s := subscriptionResult(stripeGo.SubscriptionStatusActive, false)
			s.LatestInvoice = &stripeGo.Invoice{ID: "in_latest"}
			return s
		}(),
		invoice: latest,
	}
	c := withMockClient(t, mock)
	out, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "always_invoice"})
	if err != nil {
		t.Fatalf("UpdateSubscriptionPrice: %v", err)
	}
	if out.BillingDeltaCents != -1500 {
		t.Errorf("delta=%d, want -1500 (latest invoice total)", out.BillingDeltaCents)
	}
}

func TestRealClient_UpdateSubscriptionPrice_None(t *testing.T) {
	mock := &mockBackend{
		subGet:  subscriptionResult(stripeGo.SubscriptionStatusActive, false),
		subPost: subscriptionResult(stripeGo.SubscriptionStatusActive, false),
	}
	c := withMockClient(t, mock)
	out, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "none"})
	if err != nil {
		t.Fatalf("UpdateSubscriptionPrice: %v", err)
	}
	if out.BillingDeltaCents != 0 {
		t.Errorf("delta=%d, want 0", out.BillingDeltaCents)
	}
}

func TestRealClient_UpdateSubscriptionPrice_DefaultsToCreateProrations(t *testing.T) {
	mock := &mockBackend{
		subGet: subscriptionResult(stripeGo.SubscriptionStatusActive, false),
		subPost: func() *stripeGo.Subscription {
			s := subscriptionResult(stripeGo.SubscriptionStatusActive, false)
			s.LatestInvoice = nil
			return s
		}(),
		upcoming: &stripeGo.Invoice{
			ID: "in_upcoming", Currency: stripeGo.CurrencyUSD,
			Lines: &stripeGo.InvoiceLineItemList{Data: []*stripeGo.InvoiceLineItem{{Proration: true, Amount: 800}}},
		},
	}
	c := withMockClient(t, mock)
	out, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1"})
	if err != nil {
		t.Fatalf("UpdateSubscriptionPrice: %v", err)
	}
	if out.BillingDeltaCents != 800 {
		t.Errorf("delta=%d, want 800", out.BillingDeltaCents)
	}
}

func TestRealClient_UpdateSubscriptionPrice_Errors(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	if _, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "", NewPriceID: "p"}); err == nil {
		t.Error("blank sub should error")
	}
	if _, err := c.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "s", NewPriceID: ""}); err == nil {
		t.Error("blank price should error")
	}
	// Subscription with no items.
	mock2 := &mockBackend{subGet: &stripeGo.Subscription{ID: "sub_1"}}
	c2 := withMockClient(t, mock2)
	if _, err := c2.UpdateSubscriptionPrice(context.Background(), UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1"}); err == nil {
		t.Error("no-items subscription should error")
	}
}

func TestRealClient_PreviewSubscriptionPriceChange_Happy(t *testing.T) {
	mock := &mockBackend{
		subGet: subscriptionResult(stripeGo.SubscriptionStatusActive, false),
		upcoming: &stripeGo.Invoice{
			ID: "in_upcoming", Total: 6100, Currency: stripeGo.CurrencyUSD,
			Lines: &stripeGo.InvoiceLineItemList{
				Data: []*stripeGo.InvoiceLineItem{{Proration: true, Amount: 4300}},
			},
		},
	}
	c := withMockClient(t, mock)
	out, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "price_1", ProrationBehavior: "create_prorations"})
	if err != nil {
		t.Fatalf("PreviewSubscriptionPriceChange: %v", err)
	}
	if out.NextInvoiceTotalCents != 6100 || out.BillingDeltaCents != 4300 || out.Currency != "usd" {
		t.Errorf("out=%+v", out)
	}
}

func TestRealClient_PreviewSubscriptionPriceChange_Errors(t *testing.T) {
	mock := &mockBackend{}
	c := withMockClient(t, mock)
	if _, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "", NewPriceID: "p"}); err == nil {
		t.Error("blank sub should error")
	}
	if _, err := c.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "s", NewPriceID: ""}); err == nil {
		t.Error("blank price should error")
	}
	mock2 := &mockBackend{subGet: &stripeGo.Subscription{ID: "sub_1"}} // no items
	c2 := withMockClient(t, mock2)
	if _, err := c2.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "p"}); err == nil {
		t.Error("no-items subscription should error")
	}
	// No customer on subscription.
	mock3 := &mockBackend{subGet: &stripeGo.Subscription{ID: "sub_1", Items: &stripeGo.SubscriptionItemList{Data: []*stripeGo.SubscriptionItem{{ID: "si_1"}}}}}
	c3 := withMockClient(t, mock3)
	if _, err := c3.PreviewSubscriptionPriceChange(context.Background(), PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "p"}); err == nil {
		t.Error("no-customer subscription should error")
	}
}

func scheduleWithPhases() *stripeGo.SubscriptionSchedule {
	return &stripeGo.SubscriptionSchedule{
		ID: "sub_sched_test",
		Phases: []*stripeGo.SubscriptionSchedulePhase{
			{
				StartDate: time.Now().UTC().Unix(),
				Items: []*stripeGo.SubscriptionSchedulePhaseItem{
					{Price: &stripeGo.Price{ID: "price_0"}, Quantity: 1},
				},
			},
		},
	}
}

func TestRealClient_ScheduleSubscriptionPriceChange_FreshPath(t *testing.T) {
	mock := &mockBackend{
		subGet:       &stripeGo.Subscription{ID: "sub_1"}, // no schedule attached
		scheduleResp: scheduleWithPhases(),
	}
	c := withMockClient(t, mock)
	out, err := c.ScheduleSubscriptionPriceChange(context.Background(), ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_1",
		NewPriceID:           "price_1",
		StartDate:            time.Now().UTC().Add(30 * 24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("ScheduleSubscriptionPriceChange: %v", err)
	}
	if out.StripeSubscriptionScheduleID != "sub_sched_test" {
		t.Errorf("schedule id=%s", out.StripeSubscriptionScheduleID)
	}
	if out.EffectiveAt.IsZero() {
		t.Error("EffectiveAt should be set")
	}
	if len(mock.calls) != 3 {
		t.Errorf("calls=%v, want get+new+update", mock.calls)
	}
}

func TestRealClient_ScheduleSubscriptionPriceChange_ReusePath(t *testing.T) {
	mock := &mockBackend{
		subGet: &stripeGo.Subscription{
			ID:       "sub_1",
			Schedule: scheduleWithPhases(),
		},
		scheduleResp: scheduleWithPhases(),
	}
	c := withMockClient(t, mock)
	out, err := c.ScheduleSubscriptionPriceChange(context.Background(), ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_1",
		NewPriceID:           "price_1",
		StartDate:            time.Now().UTC().Add(30 * 24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("ScheduleSubscriptionPriceChange: %v", err)
	}
	if out.StripeSubscriptionScheduleID != "sub_sched_test" {
		t.Errorf("schedule id=%s", out.StripeSubscriptionScheduleID)
	}
	if len(mock.calls) != 2 {
		t.Errorf("calls=%v, want get+update (no create)", mock.calls)
	}
}

func TestRealClient_ScheduleSubscriptionPriceChange_Errors(t *testing.T) {
	c := withMockClient(t, &mockBackend{})
	if _, err := c.ScheduleSubscriptionPriceChange(context.Background(), ScheduleSubscriptionPriceChangeInput{}); err == nil {
		t.Error("empty input should error")
	}
	// Seeded schedule without phases → phase0 nil.
	mock2 := &mockBackend{subGet: &stripeGo.Subscription{ID: "sub_1"}}
	c2 := withMockClient(t, mock2)
	if _, err := c2.ScheduleSubscriptionPriceChange(context.Background(), ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_1", NewPriceID: "price_1", StartDate: time.Now().UTC(),
	}); err == nil {
		t.Error("no-phases schedule should error")
	}
	// Phase 1 without items → phase0Items empty.
	mock3 := &mockBackend{
		subGet: &stripeGo.Subscription{ID: "sub_1"},
		scheduleResp: &stripeGo.SubscriptionSchedule{
			ID:     "sub_sched_test",
			Phases: []*stripeGo.SubscriptionSchedulePhase{{StartDate: time.Now().UTC().Unix()}},
		},
	}
	c3 := withMockClient(t, mock3)
	if _, err := c3.ScheduleSubscriptionPriceChange(context.Background(), ScheduleSubscriptionPriceChangeInput{
		StripeSubscriptionID: "sub_1", NewPriceID: "price_1", StartDate: time.Now().UTC(),
	}); err == nil {
		t.Error("seeded phase without items should error")
	}
}

func TestRealClient_GetPrice(t *testing.T) {
	c := withMockClient(t, &mockBackend{})
	out, err := c.GetPrice(context.Background(), "price_1")
	if err != nil {
		t.Fatalf("GetPrice: %v", err)
	}
	if out.UnitAmountCents != 7900 || out.Currency != "usd" {
		t.Errorf("out=%+v", out)
	}
	if _, err := c.GetPrice(context.Background(), ""); err == nil {
		t.Error("blank price id should error")
	}
}

func TestRealClient_GetSubscription(t *testing.T) {
	c := withMockClient(t, &mockBackend{subGet: subscriptionResult(stripeGo.SubscriptionStatusActive, false)})
	out, err := c.GetSubscription(context.Background(), "sub_1")
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if out.Status != "active" || out.CurrentPeriodStart.IsZero() || out.CurrentPeriodEnd.IsZero() {
		t.Errorf("out=%+v", out)
	}
	if _, err := c.GetSubscription(context.Background(), ""); err == nil {
		t.Error("blank sub id should error")
	}
}

func TestRealClient_ListInvoices(t *testing.T) {
	now := time.Now().UTC()
	mock := &mockBackend{
		invoiceList: []*stripeGo.Invoice{
			{
				ID: "in_1", Number: "INV-001", Status: stripeGo.InvoiceStatusPaid,
				Total: 4900, Currency: stripeGo.CurrencyUSD,
				PeriodStart: now.Add(-30 * 24 * time.Hour).Unix(), PeriodEnd: now.Unix(),
				HostedInvoiceURL: "https://invoice.stripe.com/i/in_1", InvoicePDF: "https://invoice.stripe.com/i/in_1/pdf",
				Subscription: &stripeGo.Subscription{ID: "sub_1"},
			},
		},
		listHasMore: true,
	}
	c := withMockClient(t, mock)
	out, err := c.ListInvoices(context.Background(), ListInvoicesInput{StripeCustomerID: "cus_1", Limit: 5, StartingAfter: "in_prev"})
	if err != nil {
		t.Fatalf("ListInvoices: %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("items=%d, want 1", len(out.Items))
	}
	row := out.Items[0]
	if row.StripeInvoiceID != "in_1" || row.Status != "paid" || row.StripeSubscriptionID != "sub_1" {
		t.Errorf("row=%+v", row)
	}
	if row.PeriodStart.IsZero() || row.PeriodEnd.IsZero() {
		t.Errorf("period bounds not mapped: %+v", row)
	}
	if out.NextCursor != "in_1" {
		t.Errorf("cursor=%q, want in_1 (HasMore)", out.NextCursor)
	}
	if _, err := c.ListInvoices(context.Background(), ListInvoicesInput{StripeCustomerID: ""}); err == nil {
		t.Error("blank customer should error")
	}
}

func TestRealClient_ListInvoices_Empty(t *testing.T) {
	c := withMockClient(t, &mockBackend{})
	out, err := c.ListInvoices(context.Background(), ListInvoicesInput{StripeCustomerID: "cus_1"})
	if err != nil {
		t.Fatalf("ListInvoices: %v", err)
	}
	if len(out.Items) != 0 || out.NextCursor != "" {
		t.Errorf("out=%+v", out)
	}
}

func TestRealClient_CreateBillingPortalSession(t *testing.T) {
	c := withMockClient(t, &mockBackend{})
	out, err := c.CreateBillingPortalSession(context.Background(), CreateBillingPortalSessionInput{StripeCustomerID: "cus_1", ReturnURL: "https://chora.site/billing"})
	if err != nil {
		t.Fatalf("CreateBillingPortalSession: %v", err)
	}
	if out.URL != "https://billing.stripe.com/p/session/test_mock" {
		t.Errorf("url=%s", out.URL)
	}
	if _, err := c.CreateBillingPortalSession(context.Background(), CreateBillingPortalSessionInput{StripeCustomerID: ""}); err == nil {
		t.Error("blank customer should error")
	}
	if _, err := c.CreateBillingPortalSession(context.Background(), CreateBillingPortalSessionInput{StripeCustomerID: "cus_1", ReturnURL: ""}); err == nil {
		t.Error("blank return url should error")
	}
}

// TestRealClient_BackendErrors wraps every RealClient method's network-call
// error branch: each call must surface the wrapped backend failure.
func TestRealClient_BackendErrors(t *testing.T) {
	boom := errors.New("backend down")
	ctx := context.Background()

	// subscription flows need the canned objects present even when the
	// backend fails (validation passes first).
	subRes := subscriptionResult(stripeGo.SubscriptionStatusActive, false)
	withErr := func(mock *mockBackend) *RealClient {
		mock.err = boom
		mock.subGet = subRes
		mock.subPost = subRes
		mock.upcoming = &stripeGo.Invoice{ID: "in_1"}
		mock.scheduleResp = scheduleWithPhases()
		mock.invoiceList = []*stripeGo.Invoice{{ID: "in_1"}}
		return withMockClient(t, mock)
	}

	cases := []struct {
		name string
		fn   func(c *RealClient) error
	}{
		{"create_checkout_session", func(c *RealClient) error { _, err := c.CreateCheckoutSession(ctx, stubCheckoutInput()); return err }},
		{"refund", func(c *RealClient) error {
			_, err := c.RefundCharge(ctx, RefundChargeInput{StripeChargeID: "ch_1"})
			return err
		}},
		{"ensure_customer", func(c *RealClient) error {
			_, err := c.EnsureCustomer(ctx, EnsureCustomerInput{TenantID: "t", LearnerGCID: "g"})
			return err
		}},
		{"cancel_subscription", func(c *RealClient) error {
			_, err := c.CancelSubscription(ctx, CancelSubscriptionInput{StripeSubscriptionID: "sub_1", CancelAtPeriodEnd: false})
			return err
		}},
		{"update_subscription_price", func(c *RealClient) error {
			_, err := c.UpdateSubscriptionPrice(ctx, UpdateSubscriptionPriceInput{StripeSubscriptionID: "sub_1", NewPriceID: "p"})
			return err
		}},
		{"preview_subscription_price_change", func(c *RealClient) error {
			_, err := c.PreviewSubscriptionPriceChange(ctx, PreviewSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "p"})
			return err
		}},
		{"schedule_subscription_price_change", func(c *RealClient) error {
			_, err := c.ScheduleSubscriptionPriceChange(ctx, ScheduleSubscriptionPriceChangeInput{StripeSubscriptionID: "sub_1", NewPriceID: "p", StartDate: time.Now().UTC()})
			return err
		}},
		{"get_price", func(c *RealClient) error { _, err := c.GetPrice(ctx, "price_1"); return err }},
		{"get_subscription", func(c *RealClient) error { _, err := c.GetSubscription(ctx, "sub_1"); return err }},
		{"list_invoices", func(c *RealClient) error {
			_, err := c.ListInvoices(ctx, ListInvoicesInput{StripeCustomerID: "cus_1"})
			return err
		}},
		{"billing_portal_session", func(c *RealClient) error {
			_, err := c.CreateBillingPortalSession(ctx, CreateBillingPortalSessionInput{StripeCustomerID: "cus_1", ReturnURL: "https://x"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := withErr(&mockBackend{})
			if err := tc.fn(c); err == nil {
				t.Fatalf("expected wrapped backend error")
			} else if !strings.Contains(err.Error(), "backend down") {
				t.Errorf("err=%v, want wrapped 'backend down'", err)
			}
		})
	}
}
