// admin_invoices_handler_test.go — H+ Billing tenant-scoped invoice
// list. RED → GREEN per [[strict-tdd-red-first]].
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chttp "github.com/apollo-chora/chora-payments/internal/adapter/http"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

const (
	billingTenantID    = "0197bbbb-bbbb-7000-8000-bbbbbbbbbbbb"
	billingAdminGCID   = "0197cccc-cccc-7000-8000-cccccccccccc"
	billingAddonPlanID = "0197dddd-dddd-7000-8000-dddddddddddd"
	billingAddonCode   = "tms"
	billingTierCode    = "starter"
	billingCustomerID  = "cus_billing_handler_test"
	billingSubID       = "sub_billing_handler_test"
)

// fakeInvoicesStripe is a minimal stripe.Client implementing only what
// the invoices handler exercises. ListInvoices returns a fixed two-row
// page derived from the input; everything else returns "not implemented".
type fakeInvoicesStripe struct {
	stripeadapter.Client
	called       int
	lastInput    stripeadapter.ListInvoicesInput
	wantCustomer string
	rows         []stripeadapter.InvoiceRow
	nextCursor   string
	err          error
}

func (f *fakeInvoicesStripe) ListInvoices(_ context.Context, in stripeadapter.ListInvoicesInput) (stripeadapter.ListInvoicesOutput, error) {
	f.called++
	f.lastInput = in
	if f.err != nil {
		return stripeadapter.ListInvoicesOutput{}, f.err
	}
	return stripeadapter.ListInvoicesOutput{Items: f.rows, NextCursor: f.nextCursor}, nil
}

func newBillingRepoWithTAP(t *testing.T) *inmem.TenantAddonPurchaseRepo {
	t.Helper()
	repo := inmem.NewTenantAddonPurchaseRepo()
	a, err := tap.New(
		"0197aaaa-aaaa-7000-8000-aaaaaaaaaaa1",
		billingTenantID, billingAdminGCID,
		billingAddonPlanID, billingAddonCode, billingTierCode,
		4900, "USD",
		"cs_test_billing", "https://checkout.stripe.com/c/pay/cs_test_billing",
		time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	if err := a.ApplyStripeSubscriptionState(billingCustomerID, billingSubID, tap.StatusActive, nil, nil); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return repo
}

// Happy path: tenant has an active TAP with stripe_customer_id; handler
// resolves the customer, forwards to Stripe, joins each invoice's
// subscription_id back to the TAP row to synthesise the description
// field as "addon_code:tier_code".
func TestAdminInvoicesHandler_HappyPath_ResolvesCustomerAndJoinsDescription(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakeInvoicesStripe{
		Client:       stripeadapter.NewStubClient(),
		wantCustomer: billingCustomerID,
		rows: []stripeadapter.InvoiceRow{
			{
				StripeInvoiceID:      "in_test_001",
				Number:               "INV-001",
				PeriodStart:          time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC),
				PeriodEnd:            time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC),
				Status:               "paid",
				TotalCents:           4900,
				Currency:             "usd",
				HostedInvoiceURL:     "https://invoice.stripe.com/i/test_001",
				InvoicePDFURL:        "https://invoice.stripe.com/i/test_001/pdf",
				StripeSubscriptionID: billingSubID,
			},
		},
	}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices?limit=10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200, body=%s", rec.Code, rec.Body.String())
	}
	if fake.called != 1 {
		t.Errorf("Stripe ListInvoices called %d times; want 1", fake.called)
	}
	if fake.lastInput.StripeCustomerID != billingCustomerID {
		t.Errorf("Stripe got customer = %q; want %q", fake.lastInput.StripeCustomerID, billingCustomerID)
	}
	if fake.lastInput.Limit != 10 {
		t.Errorf("Stripe got limit = %d; want 10", fake.lastInput.Limit)
	}
	var body struct {
		Items []struct {
			StripeInvoiceID string `json:"stripe_invoice_id"`
			Description     string `json:"description"`
			TotalCents      int64  `json:"total_cents"`
		} `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].StripeInvoiceID != "in_test_001" {
		t.Fatalf("items = %+v", body.Items)
	}
	if body.Items[0].Description != billingAddonCode+":"+billingTierCode {
		t.Errorf("description = %q; want %q (addon_code:tier_code joined from subscription_id)",
			body.Items[0].Description, billingAddonCode+":"+billingTierCode)
	}
	if body.Items[0].TotalCents != 4900 {
		t.Errorf("total_cents = %d; want 4900", body.Items[0].TotalCents)
	}
}

// Tenant has no TAP rows yet (new tenant, never subscribed) — handler
// MUST return 200 with empty list, NOT 404. The FE renders an empty
// state.
func TestAdminInvoicesHandler_EmptyTenant_ReturnsEmptyListNot404(t *testing.T) {
	repo := inmem.NewTenantAddonPurchaseRepo()
	fake := &fakeInvoicesStripe{Client: stripeadapter.NewStubClient()}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (empty), body=%s", rec.Code, rec.Body.String())
	}
	if fake.called != 0 {
		t.Errorf("Stripe ListInvoices called %d times; want 0 (no customer = no upstream call)", fake.called)
	}
	if !strings.Contains(rec.Body.String(), `"items":[]`) && !strings.Contains(rec.Body.String(), `"items":null`) {
		t.Errorf("body must carry empty items: %s", rec.Body.String())
	}
}

// Pagination — starting_after query param forwarded to Stripe verbatim.
func TestAdminInvoicesHandler_StartingAfterCursor_ForwardedToStripe(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakeInvoicesStripe{
		Client:     stripeadapter.NewStubClient(),
		nextCursor: "in_test_last",
	}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices?starting_after=in_test_prev&limit=5", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	if fake.lastInput.StartingAfter != "in_test_prev" {
		t.Errorf("StartingAfter forwarded = %q; want in_test_prev", fake.lastInput.StartingAfter)
	}
	if fake.lastInput.Limit != 5 {
		t.Errorf("Limit forwarded = %d; want 5", fake.lastInput.Limit)
	}
}

// Missing tenant_id in the path → 400.
func TestAdminInvoicesHandler_EmptyTenantIDInPath_400(t *testing.T) {
	repo := inmem.NewTenantAddonPurchaseRepo()
	fake := &fakeInvoicesStripe{Client: stripeadapter.NewStubClient()}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants//invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400, body=%s", rec.Code, rec.Body.String())
	}
}

// Non-GET → 405.
func TestAdminInvoicesHandler_NonGET_405(t *testing.T) {
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: inmem.NewTenantAddonPurchaseRepo(),
		Stripe:        &fakeInvoicesStripe{Client: stripeadapter.NewStubClient()},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", rec.Code)
	}
}

// Stripe error → 502 bad gateway.
func TestAdminInvoicesHandler_StripeError_502(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakeInvoicesStripe{
		Client: stripeadapter.NewStubClient(),
		err:    stripeAPIError("boom"),
	}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d; want 502", rec.Code)
	}
}

type stripeAPIError string

func (e stripeAPIError) Error() string { return string(e) }
