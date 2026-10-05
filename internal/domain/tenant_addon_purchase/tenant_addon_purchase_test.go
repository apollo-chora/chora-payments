package tenant_addon_purchase_test

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// CHO-1738 — H+ Marketplace tenant-add-on Purchase aggregate.
//
// Domain validation tests. The shared FSM transitions
// (MarkPaymentCaptured / Failed / Refunded / Expired) are covered by
// shared/purchase_test.go; this file pins the aggregate-specific
// constructor invariants + the AdminGCID alias.

const (
	testPurchaseID      = "0197aaaa-aaaa-7000-8000-aaaaaaaaaaaa"
	testTenantID        = "0197bbbb-bbbb-7000-8000-bbbbbbbbbbbb"
	testAdminGCID       = "0197cccc-cccc-7000-8000-cccccccccccc"
	testAddonPlanID     = "0197dddd-dddd-7000-8000-dddddddddddd"
	testAddonCode       = "knowledge_graph"
	testTierCode        = "pro"
	testStripeSessionID = "cs_test_marketplace_session"
	testStripeCheckout  = "https://checkout.stripe.com/c/pay/cs_test"
	testAmountCents     = int64(4900)
	testCurrency        = "SGD"
)

func now() time.Time {
	return time.Date(2026, 6, 14, 18, 30, 0, 0, time.UTC)
}

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	got, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, testAddonCode, testTierCode,
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID = %q, want %q", got.PurchaseID, testPurchaseID)
	}
	if got.TenantID != testTenantID {
		t.Errorf("TenantID = %q, want %q", got.TenantID, testTenantID)
	}
	if got.AdminGCID() != testAdminGCID {
		t.Errorf("AdminGCID() = %q, want %q", got.AdminGCID(), testAdminGCID)
	}
	// AdminGCID is stored as LearnerGCID under the hood per the
	// tenant_mana_topup sibling pattern (shared.Purchase has no
	// admin-specific column; we alias for API clarity).
	if got.LearnerGCID != testAdminGCID {
		t.Errorf("LearnerGCID (underlying) = %q, want %q", got.LearnerGCID, testAdminGCID)
	}
	if got.AddonPlanID != testAddonPlanID {
		t.Errorf("AddonPlanID = %q, want %q", got.AddonPlanID, testAddonPlanID)
	}
	if got.AddonCode != testAddonCode {
		t.Errorf("AddonCode = %q, want %q", got.AddonCode, testAddonCode)
	}
	if got.TierCode != testTierCode {
		t.Errorf("TierCode = %q, want %q", got.TierCode, testTierCode)
	}
	if got.State != shared.StateCheckoutStarted {
		t.Errorf("State = %v, want StateCheckoutStarted", got.State)
	}
	if got.AmountCents != testAmountCents {
		t.Errorf("AmountCents = %d, want %d", got.AmountCents, testAmountCents)
	}
	if got.Currency != testCurrency {
		t.Errorf("Currency = %q, want %q", got.Currency, testCurrency)
	}
	if got.StripeSessionID != testStripeSessionID {
		t.Errorf("StripeSessionID = %q, want %q", got.StripeSessionID, testStripeSessionID)
	}
	if !got.CheckoutStartedAt.Equal(now()) {
		t.Errorf("CheckoutStartedAt = %v, want %v", got.CheckoutStartedAt, now())
	}
}

func TestNew_RequiresAddonPlanID(t *testing.T) {
	t.Parallel()
	_, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		"", testAddonCode, testTierCode,
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if !errors.Is(err, tap.ErrAddonPlanRequired) {
		t.Errorf("err = %v, want ErrAddonPlanRequired", err)
	}
}

func TestNew_RequiresAddonCode(t *testing.T) {
	t.Parallel()
	_, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, "", testTierCode,
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if !errors.Is(err, tap.ErrAddonCodeRequired) {
		t.Errorf("err = %v, want ErrAddonCodeRequired", err)
	}
}

func TestNew_RequiresTierCode(t *testing.T) {
	t.Parallel()
	_, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, testAddonCode, "",
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if !errors.Is(err, tap.ErrTierCodeRequired) {
		t.Errorf("err = %v, want ErrTierCodeRequired", err)
	}
}

func TestNew_DelegatesAmountValidationToShared(t *testing.T) {
	t.Parallel()
	_, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, testAddonCode, testTierCode,
		-100, testCurrency, // negative amount → shared rejects
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if !errors.Is(err, shared.ErrAmountNegative) {
		t.Errorf("err = %v, want shared.ErrAmountNegative", err)
	}
}

func TestNew_DelegatesTenantValidationToShared(t *testing.T) {
	t.Parallel()
	_, err := tap.New(
		testPurchaseID, "", testAdminGCID,
		testAddonPlanID, testAddonCode, testTierCode,
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err = %v, want shared.ErrTenantRequired", err)
	}
}

func TestMarkPaymentCaptured_PropagatesToSharedFSM(t *testing.T) {
	t.Parallel()
	a, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, testAddonCode, testTierCode,
		testAmountCents, testCurrency,
		testStripeSessionID, testStripeCheckout,
		now(),
	)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	captured := now().Add(time.Minute)
	if err := a.MarkPaymentCaptured("pi_test", "ch_test", testAmountCents, captured); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if a.State != shared.StatePaymentCaptured {
		t.Errorf("State = %v, want StatePaymentCaptured", a.State)
	}
	if a.AmountCentsPaid != testAmountCents {
		t.Errorf("AmountCentsPaid = %d, want %d", a.AmountCentsPaid, testAmountCents)
	}
}
