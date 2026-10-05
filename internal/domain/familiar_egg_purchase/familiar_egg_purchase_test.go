package familiar_egg_purchase_test

import (
	"errors"
	"testing"
	"time"

	fep "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_egg"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_egg"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	soft := now.Add(30 * 24 * time.Hour)
	hard := now.Add(60 * 24 * time.Hour)
	e, err := fep.New(
		purchaseID, tenantID, learnerGCID, "egg.standard.v1",
		"focal_atom_0001",
		1990, "SGD",
		sessionID, checkoutURL,
		&soft, &hard,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if e.EggSKU != "egg.standard.v1" {
		t.Errorf("EggSKU=%s, want egg.standard.v1", e.EggSKU)
	}
	if e.SuggestedFocalAtomID != "focal_atom_0001" {
		t.Errorf("SuggestedFocalAtomID=%s, want focal_atom_0001", e.SuggestedFocalAtomID)
	}
	if e.SoftExpiryAt == nil || !e.SoftExpiryAt.Equal(soft) {
		t.Errorf("SoftExpiryAt=%v, want %v", e.SoftExpiryAt, soft)
	}
	if e.HardExpiryAt == nil || !e.HardExpiryAt.Equal(hard) {
		t.Errorf("HardExpiryAt=%v, want %v", e.HardExpiryAt, hard)
	}
	if e.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", e.State)
	}
}

func TestNew_ValidatesEggSKU(t *testing.T) {
	t.Parallel()
	_, err := fep.New(
		purchaseID, tenantID, learnerGCID, "",
		"focal_atom_0001",
		1990, "SGD", sessionID, checkoutURL, nil, nil, time.Now().UTC(),
	)
	if !errors.Is(err, fep.ErrEggSKURequired) {
		t.Errorf("err=%v, want ErrEggSKURequired", err)
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := fep.New(
		purchaseID, "", learnerGCID, "egg.standard.v1",
		"focal_atom_0001",
		1990, "SGD", sessionID, checkoutURL, nil, nil, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err=%v, want shared.ErrTenantRequired", err)
	}
}

func TestMarkPaymentCaptured_TransitionsState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	e, _ := fep.New(
		purchaseID, tenantID, learnerGCID, "egg.standard.v1",
		"focal_atom_0001",
		1990, "SGD", sessionID, checkoutURL, nil, nil, now,
	)
	if err := e.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute)); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if e.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", e.State)
	}
}
