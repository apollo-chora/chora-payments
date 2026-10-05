package identity_kyc_fee_test

import (
	"errors"
	"testing"
	"time"

	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_kyc"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_kyc"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	k, err := kyc.New(
		purchaseID, tenantID, learnerGCID, "manual_id_doc",
		999, "USD",
		sessionID, checkoutURL,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if k.KYCDocType != "manual_id_doc" {
		t.Errorf("KYCDocType=%s, want manual_id_doc", k.KYCDocType)
	}
	if k.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", k.State)
	}
	if k.AmountCents != 999 {
		t.Errorf("AmountCents=%d, want 999", k.AmountCents)
	}
	if k.LearnerGCID != learnerGCID {
		t.Errorf("LearnerGCID mismatch")
	}
}

func TestNew_ValidatesKYCDocType(t *testing.T) {
	t.Parallel()
	_, err := kyc.New(
		purchaseID, tenantID, learnerGCID, "",
		999, "USD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, kyc.ErrKYCDocTypeRequired) {
		t.Errorf("err=%v, want ErrKYCDocTypeRequired", err)
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := kyc.New(
		purchaseID, tenantID, "", "manual_id_doc",
		999, "USD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrLearnerRequired) {
		t.Errorf("err=%v, want shared.ErrLearnerRequired", err)
	}
}

func TestMarkPaymentCaptured_TransitionsState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	k, _ := kyc.New(
		purchaseID, tenantID, learnerGCID, "manual_id_doc",
		999, "USD", sessionID, checkoutURL, now,
	)
	if err := k.MarkPaymentCaptured("pi_test", "ch_test", 999, now.Add(time.Minute)); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if k.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", k.State)
	}
}

func TestMarkRefunded_FromPaymentCaptured(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	k, _ := kyc.New(
		purchaseID, tenantID, learnerGCID, "manual_id_doc",
		999, "USD", sessionID, checkoutURL, now,
	)
	_ = k.MarkPaymentCaptured("pi", "ch", 999, now.Add(time.Minute))
	if err := k.MarkRefunded("re_test", 999, shared.RefundReasonDuplicateCharge, now.Add(time.Hour)); err != nil {
		t.Fatalf("MarkRefunded: %v", err)
	}
	if k.State != shared.StateRefunded {
		t.Errorf("State=%s, want refunded", k.State)
	}
}
