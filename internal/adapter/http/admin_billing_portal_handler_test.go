// admin_billing_portal_handler_test.go — H+ Billing tenant-scoped
// Stripe Customer Portal session mint. RED → GREEN.
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	chttp "github.com/apollo-chora/chora-payments/internal/adapter/http"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
)

type fakePortalStripe struct {
	stripeadapter.Client
	called    int
	lastInput stripeadapter.CreateBillingPortalSessionInput
	out       stripeadapter.CreateBillingPortalSessionOutput
	err       error
}

func (f *fakePortalStripe) CreateBillingPortalSession(_ context.Context, in stripeadapter.CreateBillingPortalSessionInput) (stripeadapter.CreateBillingPortalSessionOutput, error) {
	f.called++
	f.lastInput = in
	if f.err != nil {
		return stripeadapter.CreateBillingPortalSessionOutput{}, f.err
	}
	return f.out, nil
}

func TestAdminBillingPortalHandler_HappyPath_ReturnsURL(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakePortalStripe{
		Client: stripeadapter.NewStubClient(),
		out:    stripeadapter.CreateBillingPortalSessionOutput{URL: "https://billing.stripe.com/p/session/test_xyz"},
	}
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})
	body := strings.NewReader(`{"return_url":"http://localhost:4200/h/billing"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200, body=%s", rec.Code, rec.Body.String())
	}
	if fake.called != 1 {
		t.Errorf("CreateBillingPortalSession called %d times; want 1", fake.called)
	}
	if fake.lastInput.StripeCustomerID != billingCustomerID {
		t.Errorf("customer = %q; want %q", fake.lastInput.StripeCustomerID, billingCustomerID)
	}
	if fake.lastInput.ReturnURL != "http://localhost:4200/h/billing" {
		t.Errorf("return_url = %q; want http://localhost:4200/h/billing", fake.lastInput.ReturnURL)
	}
	var resp struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.URL != "https://billing.stripe.com/p/session/test_xyz" {
		t.Errorf("url = %q; want the Stripe portal session URL", resp.URL)
	}
}

// Tenant with no Stripe Customer yet → 422 "no_stripe_customer".
// The FE renders an error CTA ("Subscribe to an add-on first") rather
// than silently 500ing.
func TestAdminBillingPortalHandler_NoCustomer_422(t *testing.T) {
	repo := inmem.NewTenantAddonPurchaseRepo()
	fake := &fakePortalStripe{Client: stripeadapter.NewStubClient()}
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})
	body := strings.NewReader(`{"return_url":"http://localhost:4200/h/billing"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d; want 422, body=%s", rec.Code, rec.Body.String())
	}
	if fake.called != 0 {
		t.Errorf("Stripe MUST NOT be called when customer is unresolved (%d)", fake.called)
	}
}

func TestAdminBillingPortalHandler_MissingReturnURL_400(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakePortalStripe{Client: stripeadapter.NewStubClient()}
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: repo,
		Stripe:        fake,
	})
	body := strings.NewReader(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", rec.Code)
	}
}

func TestAdminBillingPortalHandler_NonPOST_405(t *testing.T) {
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: inmem.NewTenantAddonPurchaseRepo(),
		Stripe:        &fakePortalStripe{Client: stripeadapter.NewStubClient()},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", rec.Code)
	}
}
