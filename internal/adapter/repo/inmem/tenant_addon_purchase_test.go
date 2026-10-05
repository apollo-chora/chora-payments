package inmem

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

const (
	tapTestPurchaseID      = "0197aaaa-aaaa-7000-8000-aaaaaaaaaaaa"
	tapTestTenantID        = "0197bbbb-bbbb-7000-8000-bbbbbbbbbbbb"
	tapTestAdminGCID       = "0197cccc-cccc-7000-8000-cccccccccccc"
	tapTestAddonPlanID     = "0197dddd-dddd-7000-8000-dddddddddddd"
	tapTestAddonCode       = "knowledge_graph"
	tapTestTierCode        = "pro"
	tapTestStripeSessionID = "cs_test_tap"
	tapTestStripeCheckout  = "https://checkout.stripe.com/c/pay/cs_test_tap"
)

func tapFixture(t *testing.T, purchaseSuffix string) *tap.TenantAddonPurchase {
	t.Helper()
	purchaseID := tapTestPurchaseID
	sessionID := tapTestStripeSessionID
	if purchaseSuffix != "" {
		// Override last byte for uniqueness across tests.
		purchaseID = purchaseID[:len(purchaseID)-1] + purchaseSuffix
		sessionID = sessionID + "_" + purchaseSuffix
	}
	a, err := tap.New(
		purchaseID, tapTestTenantID, tapTestAdminGCID,
		tapTestAddonPlanID, tapTestAddonCode, tapTestTierCode,
		4900, "USD",
		sessionID, tapTestStripeCheckout,
		time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	return a
}

// -----------------------------------------------------------------------------
// CHO-1761 — Subscription fields round-trip + GetByStripeSubscriptionID
// -----------------------------------------------------------------------------

func TestTenantAddonPurchaseRepo_Save_AndGet_RoundTripsSubscriptionFields(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	a := tapFixture(t, "")
	periodStart := time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)
	periodEnd := periodStart.AddDate(0, 1, 0)
	if err := a.ApplyStripeSubscriptionState(
		"cus_subscription_round_trip", "sub_subscription_round_trip",
		tap.StatusActive, &periodStart, &periodEnd,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(context.Background(), tapTestTenantID, a.PurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.StripeCustomerID != "cus_subscription_round_trip" {
		t.Errorf("StripeCustomerID dropped: %q", got.StripeCustomerID)
	}
	if got.StripeSubscriptionID != "sub_subscription_round_trip" {
		t.Errorf("StripeSubscriptionID dropped: %q", got.StripeSubscriptionID)
	}
	if got.Status != tap.StatusActive {
		t.Errorf("Status dropped: %q", got.Status)
	}
	if got.CurrentPeriodStart == nil || !got.CurrentPeriodStart.Equal(periodStart) {
		t.Errorf("CurrentPeriodStart dropped: %v", got.CurrentPeriodStart)
	}
	if got.CurrentPeriodEnd == nil || !got.CurrentPeriodEnd.Equal(periodEnd) {
		t.Errorf("CurrentPeriodEnd dropped: %v", got.CurrentPeriodEnd)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_FindsMatchingRow(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	a := tapFixture(t, "")
	periodStart := time.Now().UTC()
	if err := a.ApplyStripeSubscriptionState(
		"cus_lookup_test", "sub_lookup_test",
		tap.StatusActive, &periodStart, nil,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_lookup_test")
	if err != nil {
		t.Fatalf("GetByStripeSubscriptionID: %v", err)
	}
	if got.PurchaseID != a.PurchaseID {
		t.Errorf("wrong purchase: %q, want %q", got.PurchaseID, a.PurchaseID)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_ReturnsErrNotFoundOnMiss(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	if _, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_does_not_exist"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("expected ErrNotFound; got %v", err)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_RejectsEmpty(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	// Empty must fail-loud with ErrStripeSubscriptionRequired —
	// otherwise an aggregate with no Stripe sub yet (legacy mode=payment
	// row) would match the empty-string lookup, leaking cross-tenant.
	if _, err := repo.GetByStripeSubscriptionID(context.Background(), ""); !errors.Is(err, tap.ErrStripeSubscriptionRequired) {
		t.Errorf("expected ErrStripeSubscriptionRequired; got %v", err)
	}
}

// Save updates the bySubID index when the sub_id is set; later lookups
// of the new ID must find the row. Re-Save with a new sub_id (e.g. a
// row that gets re-keyed by a webhook) updates the index in-place.
func TestTenantAddonPurchaseRepo_Save_UpdatesBySubIDIndex(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	a := tapFixture(t, "")
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	// Initial Save: no sub_id yet → GetByStripeSubscriptionID should miss.
	if _, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_anything"); !errors.Is(err, shared.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before sub_id applied; got %v", err)
	}
	// Apply subscription state + re-Save.
	if err := a.ApplyStripeSubscriptionState(
		"cus_update_index", "sub_update_index",
		tap.StatusActive, nil, nil,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	got, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_update_index")
	if err != nil {
		t.Fatalf("GetByStripeSubscriptionID after re-Save: %v", err)
	}
	if got.PurchaseID != a.PurchaseID {
		t.Errorf("wrong purchase: %q, want %q", got.PurchaseID, a.PurchaseID)
	}
}

// -----------------------------------------------------------------------------
// CHO-1759-followup billing — GetStripeCustomerIDByTenant
// -----------------------------------------------------------------------------

// Tenant with multiple active TAP rows shares ONE Stripe Customer per the
// CHO-1762 contract; the lookup returns that customer ID. Used by the
// billing-invoices + billing-portal handlers to resolve the tenant's
// Stripe Customer without requiring the caller to pass it.
func TestTenantAddonPurchaseRepo_GetStripeCustomerIDByTenant_FindsCustomer(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	a := tapFixture(t, "a")
	if err := a.ApplyStripeSubscriptionState(
		"cus_billing_one", "sub_billing_one",
		tap.StatusActive, nil, nil,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetStripeCustomerIDByTenant(context.Background(), tapTestTenantID)
	if err != nil {
		t.Fatalf("GetStripeCustomerIDByTenant: %v", err)
	}
	if got != "cus_billing_one" {
		t.Errorf("customer_id = %q; want cus_billing_one", got)
	}
}

// Tenant with NO TAP rows yet returns empty string + nil error — billing
// handlers treat that as "render empty invoice list", NOT a 404 or 5xx.
func TestTenantAddonPurchaseRepo_GetStripeCustomerIDByTenant_EmptyOnNoRows(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	got, err := repo.GetStripeCustomerIDByTenant(context.Background(), tapTestTenantID)
	if err != nil {
		t.Fatalf("GetStripeCustomerIDByTenant: %v", err)
	}
	if got != "" {
		t.Errorf("customer_id = %q; want empty string", got)
	}
}

// Rows for the tenant that haven't reached payment_captured (no Stripe
// Customer yet) are skipped. Only consider rows with a non-empty
// stripe_customer_id. This avoids returning "" from a half-bootstrapped
// row when a sibling row IS fully wired.
func TestTenantAddonPurchaseRepo_GetStripeCustomerIDByTenant_SkipsRowsWithoutCustomer(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	bare := tapFixture(t, "x") // no ApplyStripeSubscriptionState → empty customer
	if err := repo.Save(context.Background(), bare); err != nil {
		t.Fatalf("Save bare: %v", err)
	}
	full := tapFixture(t, "y")
	if err := full.ApplyStripeSubscriptionState(
		"cus_billing_full", "sub_billing_full",
		tap.StatusActive, nil, nil,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := repo.Save(context.Background(), full); err != nil {
		t.Fatalf("Save full: %v", err)
	}
	got, err := repo.GetStripeCustomerIDByTenant(context.Background(), tapTestTenantID)
	if err != nil {
		t.Fatalf("GetStripeCustomerIDByTenant: %v", err)
	}
	if got != "cus_billing_full" {
		t.Errorf("customer_id = %q; want cus_billing_full (bare row must not shadow full row)", got)
	}
}

// Empty tenantID fail-loud per tap.ErrTenantRequired — otherwise a
// caller passing the empty string would silently match any tenant's
// first half-bootstrapped row.
func TestTenantAddonPurchaseRepo_GetStripeCustomerIDByTenant_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	if _, err := repo.GetStripeCustomerIDByTenant(context.Background(), ""); !errors.Is(err, tap.ErrTenantRequired) {
		t.Errorf("expected ErrTenantRequired; got %v", err)
	}
}
