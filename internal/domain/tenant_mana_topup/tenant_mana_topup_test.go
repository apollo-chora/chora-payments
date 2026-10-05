package tenant_mana_topup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tmt "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
)

const (
	tenantID  = "01970000-0000-7000-8000-000000000001"
	adminGCID = "01970000-0000-7000-a000-000000000002"
	purchase  = "01970000-0000-7000-b000-000000000003"
	sessionID = "cs_test_topup"
	checkout  = "https://checkout.stripe.com/c/pay/cs_test_topup"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	top, err := tmt.New(
		purchase, tenantID, adminGCID, "mana.tenant_topup.standard_v1",
		5000,
		4990, "SGD",
		sessionID, checkout,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if top.SKU != "mana.tenant_topup.standard_v1" {
		t.Errorf("SKU=%s, want mana.tenant_topup.standard_v1", top.SKU)
	}
	if top.ManaUnits != 5000 {
		t.Errorf("ManaUnits=%d, want 5000", top.ManaUnits)
	}
	if top.AdminGCID() != adminGCID {
		t.Errorf("AdminGCID=%s, want %s", top.AdminGCID(), adminGCID)
	}
	if top.LearnerGCID != adminGCID {
		t.Errorf("LearnerGCID=%s, want adminGCID", top.LearnerGCID)
	}
	if top.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", top.State)
	}
}

func TestNew_ValidatesSKU(t *testing.T) {
	t.Parallel()
	_, err := tmt.New(
		purchase, tenantID, adminGCID, "",
		5000, 4990, "SGD", sessionID, checkout, time.Now().UTC(),
	)
	if !errors.Is(err, tmt.ErrSKURequired) {
		t.Errorf("err=%v, want ErrSKURequired", err)
	}
}

func TestNew_ValidatesManaUnits(t *testing.T) {
	t.Parallel()
	for _, units := range []int64{0, -1} {
		_, err := tmt.New(
			purchase, tenantID, adminGCID, "mana.tenant_topup.standard_v1",
			units, 4990, "SGD", sessionID, checkout, time.Now().UTC(),
		)
		if !errors.Is(err, tmt.ErrManaUnitsRequired) {
			t.Errorf("units=%d err=%v, want ErrManaUnitsRequired", units, err)
		}
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := tmt.New(
		purchase, "", adminGCID, "mana.tenant_topup.standard_v1",
		5000, 4990, "SGD", sessionID, checkout, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err=%v, want shared.ErrTenantRequired", err)
	}
}

func TestMarkPaymentCaptured_TransitionsState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	top, _ := tmt.New(
		purchase, tenantID, adminGCID, "mana.tenant_topup.standard_v1",
		5000, 4990, "SGD", sessionID, checkout, now,
	)
	if err := top.MarkPaymentCaptured("pi_test", "ch_test", 4990, now.Add(time.Minute)); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if top.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", top.State)
	}
}
