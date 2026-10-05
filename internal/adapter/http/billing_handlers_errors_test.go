// billing_handlers_errors_test.go — error + edge branches of the H+
// Billing handlers (no-customer, lookup failures, Stripe failures,
// description-join miss paths).
package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	chttp "github.com/apollo-chora/chora-payments/internal/adapter/http"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
)

// errAddonRepo wraps the inmem addon repo but fails customer lookups.
type errAddonRepo struct {
	*inmem.TenantAddonPurchaseRepo
	err error
}

func (e *errAddonRepo) GetStripeCustomerIDByTenant(_ context.Context, _ string) (string, error) {
	return "", e.err
}

func TestAdminInvoicesHandler_NoCustomer_ReturnsEmptyList(t *testing.T) {
	repo := inmem.NewTenantAddonPurchaseRepo() // no rows → no customer
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        &fakeInvoicesStripe{Client: stripeadapter.NewStubClient()},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 (empty list, not 404)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Errorf("body=%s", rec.Body.String())
	}
}

func TestAdminInvoicesHandler_LookupError_Returns500(t *testing.T) {
	repo := &errAddonRepo{TenantAddonPurchaseRepo: inmem.NewTenantAddonPurchaseRepo(), err: errors.New("db down")}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{
		AddonPurchase: repo,
		Stripe:        &fakeInvoicesStripe{Client: stripeadapter.NewStubClient()},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500", rec.Code)
	}
}

func TestAdminInvoicesHandler_StripeError_Returns502(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakeInvoicesStripe{Client: stripeadapter.NewStubClient(), err: errors.New("stripe down")}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{AddonPurchase: repo, Stripe: fake})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status=%d, want 502", rec.Code)
	}
}

func TestAdminInvoicesHandler_UnjoinableSubscription(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakeInvoicesStripe{
		Client: stripeadapter.NewStubClient(),
		rows: []stripeadapter.InvoiceRow{
			{
				StripeInvoiceID:      "in_1",
				Number:               "INV-001",
				StripeSubscriptionID: "sub_unknown", // no matching TAP row
			},
			{
				StripeInvoiceID:      "in_2",
				Number:               "INV-002",
				StripeSubscriptionID: "", // blank → label skipped
			},
		},
	}
	h := chttp.NewAdminInvoicesHandler(chttp.AdminInvoicesHandlerDeps{AddonPurchase: repo, Stripe: fake})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/"+billingTenantID+"/invoices", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "knowledge_graph:pro") {
		t.Errorf("unexpected label joined for unknown sub: %s", rec.Body.String())
	}
}

func TestAdminBillingPortalHandler_NoCustomer_Returns422(t *testing.T) {
	repo := inmem.NewTenantAddonPurchaseRepo()
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: repo,
		Stripe:        &fakePortalStripe{Client: stripeadapter.NewStubClient()},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", strings.NewReader(`{"return_url":"https://x"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status=%d, want 422", rec.Code)
	}
}

func TestAdminBillingPortalHandler_LookupError_Returns500(t *testing.T) {
	repo := &errAddonRepo{TenantAddonPurchaseRepo: inmem.NewTenantAddonPurchaseRepo(), err: errors.New("db down")}
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: repo,
		Stripe:        &fakePortalStripe{Client: stripeadapter.NewStubClient()},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", strings.NewReader(`{"return_url":"https://x"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500", rec.Code)
	}
}

func TestAdminBillingPortalHandler_StripeError_Returns502(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	fake := &fakePortalStripe{Client: stripeadapter.NewStubClient(), err: errors.New("stripe down")}
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{AddonPurchase: repo, Stripe: fake})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", strings.NewReader(`{"return_url":"https://x"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status=%d, want 502", rec.Code)
	}
}

func TestAdminBillingPortalHandler_BadBodyAndMissingTenant(t *testing.T) {
	repo := newBillingRepoWithTAP(t)
	h := chttp.NewAdminBillingPortalHandler(chttp.AdminBillingPortalHandlerDeps{
		AddonPurchase: repo,
		Stripe:        &fakePortalStripe{Client: stripeadapter.NewStubClient()},
	})
	// Malformed JSON.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", strings.NewReader(`{bad`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad body status=%d", rec.Code)
	}
	// Missing return_url.
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/"+billingTenantID+"/billing-portal", strings.NewReader(`{}`))
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Errorf("missing return_url status=%d", rec2.Code)
	}
	// Path without tenant id.
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants//billing-portal", strings.NewReader(`{"return_url":"https://x"}`))
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusBadRequest {
		t.Errorf("missing tenant path status=%d", rec3.Code)
	}
}
