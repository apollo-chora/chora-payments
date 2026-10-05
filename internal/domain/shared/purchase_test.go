package shared_test

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_abc123"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_abc123"
)

func mkPurchase(t *testing.T) shared.Purchase {
	t.Helper()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	p, err := shared.NewPurchase(
		purchaseID, tenantID, learnerGCID,
		99900, "SGD", sessionID, checkoutURL, now,
	)
	if err != nil {
		t.Fatalf("NewPurchase: %v", err)
	}
	return p
}

func TestNewPurchase_Happy(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	if p.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", p.State)
	}
	if p.AmountCents != 99900 || p.Currency != "SGD" {
		t.Errorf("AmountCents=%d Currency=%s, want 99900 SGD", p.AmountCents, p.Currency)
	}
	if p.StripeSessionID != sessionID || p.StripeCheckoutURL != checkoutURL {
		t.Errorf("Stripe handles not populated")
	}
}

func TestNewPurchase_Validation(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	tests := []struct {
		name      string
		tenant    string
		learner   string
		amount    int64
		currency  string
		sessionID string
		wantErr   error
	}{
		{"empty tenant", "", learnerGCID, 100, "SGD", sessionID, shared.ErrTenantRequired},
		{"empty learner", tenantID, "", 100, "SGD", sessionID, shared.ErrLearnerRequired},
		{"negative amount", tenantID, learnerGCID, -1, "SGD", sessionID, shared.ErrAmountNegative},
		{"bad currency length", tenantID, learnerGCID, 100, "SG", sessionID, shared.ErrCurrencyInvalid},
		{"bad currency lowercase", tenantID, learnerGCID, 100, "sgd", sessionID, shared.ErrCurrencyInvalid},
		{"empty session", tenantID, learnerGCID, 100, "SGD", "", shared.ErrStripeSessionRequired},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := shared.NewPurchase(purchaseID, tc.tenant, tc.learner, tc.amount, tc.currency, tc.sessionID, checkoutURL, now)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("err=%v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestMarkPaymentCaptured_FromCheckoutStarted(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	now := time.Date(2026, 5, 24, 12, 5, 0, 0, time.UTC)
	if err := p.MarkPaymentCaptured("pi_test_xyz", "ch_test_xyz", 99900, now); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if p.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", p.State)
	}
	if p.PaidAt == nil || !p.PaidAt.Equal(now) {
		t.Errorf("PaidAt not set to now")
	}
	if p.StripePaymentIntentID != "pi_test_xyz" || p.StripeChargeID != "ch_test_xyz" {
		t.Errorf("Stripe IDs not captured")
	}
}

func TestMarkPaymentCaptured_Idempotent(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	now := time.Date(2026, 5, 24, 12, 5, 0, 0, time.UTC)
	_ = p.MarkPaymentCaptured("pi", "ch", 99900, now)
	if err := p.MarkPaymentCaptured("pi", "ch", 99900, now); err != nil {
		t.Errorf("second MarkPaymentCaptured should be idempotent: %v", err)
	}
}

func TestMarkPaymentCaptured_InvalidFromFailed(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	now := time.Now().UTC()
	_ = p.MarkPaymentFailed("card_declined", "Card declined", now)
	err := p.MarkPaymentCaptured("pi", "ch", 99900, now)
	if !errors.Is(err, shared.ErrInvalidTransition) {
		t.Errorf("err=%v, want ErrInvalidTransition", err)
	}
}

func TestMarkPaymentFailed_FromCheckoutStarted(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	now := time.Now().UTC()
	if err := p.MarkPaymentFailed("card_declined", "Card declined", now); err != nil {
		t.Fatalf("MarkPaymentFailed: %v", err)
	}
	if p.State != shared.StatePaymentFailed {
		t.Errorf("State=%s, want payment_failed", p.State)
	}
}

func TestMarkRefunded_FromPaymentCaptured(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	tt := time.Now().UTC()
	_ = p.MarkPaymentCaptured("pi", "ch", 99900, tt)
	if err := p.MarkRefunded("re_test_aaa", 99900, shared.RefundReasonCustomerRequest, tt.Add(time.Hour)); err != nil {
		t.Fatalf("MarkRefunded: %v", err)
	}
	if p.State != shared.StateRefunded {
		t.Errorf("State=%s, want refunded", p.State)
	}
	if p.AmountCentsRefunded != 99900 {
		t.Errorf("AmountCentsRefunded=%d, want 99900", p.AmountCentsRefunded)
	}
}

func TestMarkRefunded_PartialAccumulates(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	tt := time.Now().UTC()
	_ = p.MarkPaymentCaptured("pi", "ch", 99900, tt)
	_ = p.MarkRefunded("re_aaa", 30000, shared.RefundReasonCustomerRequest, tt.Add(time.Hour))
	_ = p.MarkRefunded("re_bbb", 20000, shared.RefundReasonSupportInitiated, tt.Add(2*time.Hour))
	if p.AmountCentsRefunded != 50000 {
		t.Errorf("AmountCentsRefunded=%d, want 50000 (accumulated)", p.AmountCentsRefunded)
	}
}

func TestMarkRefunded_InvalidReason(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	tt := time.Now().UTC()
	_ = p.MarkPaymentCaptured("pi", "ch", 99900, tt)
	err := p.MarkRefunded("re_aaa", 100, shared.RefundReason("bogus"), tt.Add(time.Hour))
	if !errors.Is(err, shared.ErrRefundReasonInvalid) {
		t.Errorf("err=%v, want ErrRefundReasonInvalid", err)
	}
}

func TestMarkExpired_FromCheckoutStarted(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	now := time.Now().UTC()
	if err := p.MarkExpired(now); err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}
	if p.State != shared.StateExpired {
		t.Errorf("State=%s, want expired", p.State)
	}
}

func TestMarkExpired_InvalidFromPaymentCaptured(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	tt := time.Now().UTC()
	_ = p.MarkPaymentCaptured("pi", "ch", 99900, tt)
	err := p.MarkExpired(tt.Add(time.Hour))
	if !errors.Is(err, shared.ErrInvalidTransition) {
		t.Errorf("err=%v, want ErrInvalidTransition", err)
	}
}

func TestState_IsTerminal(t *testing.T) {
	t.Parallel()
	cases := map[shared.State]bool{
		shared.StateCheckoutStarted: false,
		shared.StatePaymentCaptured: false,
		shared.StatePaymentFailed:   false,
		shared.StateRefunded:        true,
		shared.StateExpired:         true,
	}
	for s, want := range cases {
		if got := s.IsTerminal(); got != want {
			t.Errorf("State(%s).IsTerminal()=%v, want %v", s, got, want)
		}
	}
}
