package user_mana_topup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_xyz"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_xyz"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, err := umt.New(
		purchaseID, tenantID, learnerGCID, "mana.user_topup.standard_v1",
		2500,
		1990, "SGD",
		sessionID, checkoutURL,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if u.SKU != "mana.user_topup.standard_v1" {
		t.Errorf("SKU=%s", u.SKU)
	}
	if u.ManaUnits != 2500 {
		t.Errorf("ManaUnits=%d, want 2500", u.ManaUnits)
	}
	if u.LearnerGCID != learnerGCID {
		t.Errorf("LearnerGCID=%s, want %s", u.LearnerGCID, learnerGCID)
	}
	if u.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", u.State)
	}
}

func TestNew_ValidatesSKU(t *testing.T) {
	t.Parallel()
	_, err := umt.New(
		purchaseID, tenantID, learnerGCID, "",
		2500, 1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, umt.ErrSKURequired) {
		t.Errorf("err=%v, want ErrSKURequired", err)
	}
}

func TestNew_ValidatesManaUnits(t *testing.T) {
	t.Parallel()
	for _, units := range []int64{0, -1} {
		_, err := umt.New(
			purchaseID, tenantID, learnerGCID, "mana.user_topup.standard_v1",
			units, 1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
		)
		if !errors.Is(err, umt.ErrManaUnitsRequired) {
			t.Errorf("units=%d err=%v, want ErrManaUnitsRequired", units, err)
		}
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := umt.New(
		purchaseID, "", learnerGCID, "mana.user_topup.standard_v1",
		2500, 1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err=%v, want shared.ErrTenantRequired", err)
	}
}

func TestMarkPaymentCaptured_TransitionsState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	u, _ := umt.New(
		purchaseID, tenantID, learnerGCID, "mana.user_topup.standard_v1",
		2500, 1990, "SGD", sessionID, checkoutURL, now,
	)
	if err := u.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute)); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if u.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", u.State)
	}
}
