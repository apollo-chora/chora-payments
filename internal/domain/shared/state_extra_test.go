// state_extra_test.go — residual branches in shared.State helpers not
// covered by purchase_test.go.
package shared_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

func TestStateIsValid(t *testing.T) {
	t.Parallel()
	for _, s := range []shared.State{
		shared.StateCheckoutStarted,
		shared.StatePaymentCaptured,
		shared.StatePaymentFailed,
		shared.StateRefunded,
		shared.StateExpired,
	} {
		if !s.IsValid() {
			t.Errorf("State %q should be valid", s)
		}
	}
	if shared.State("mystery").IsValid() {
		t.Error("mystery state should be invalid")
	}
}

func TestStateIsTerminal(t *testing.T) {
	t.Parallel()
	if !shared.StateRefunded.IsTerminal() || !shared.StateExpired.IsTerminal() {
		t.Error("refunded + expired should be terminal")
	}
	if shared.StateCheckoutStarted.IsTerminal() || shared.StatePaymentCaptured.IsTerminal() || shared.StatePaymentFailed.IsTerminal() {
		t.Error("non-terminal states reported terminal")
	}
}

func TestTransitionError_ErrorAndUnwrap(t *testing.T) {
	t.Parallel()
	te := shared.TransitionError{From: shared.StateCheckoutStarted, To: shared.StatePaymentCaptured}
	if !strings.Contains(te.Error(), "checkout_started") || !strings.Contains(te.Error(), "payment_captured") {
		t.Errorf("Error()=%q", te.Error())
	}
	if !errors.Is(te, shared.ErrInvalidTransition) {
		t.Errorf("errors.Is(te, ErrInvalidTransition) = false")
	}
}

func TestMarkPaymentCaptured_RejectsNegativeAmount(t *testing.T) {
	t.Parallel()
	p := mkPurchase(t)
	err := p.MarkPaymentCaptured("pi_1", "ch_1", -5, time.Now().UTC())
	if !errors.Is(err, shared.ErrAmountNegative) {
		t.Errorf("err=%v, want ErrAmountNegative", err)
	}
}

func TestMarkPaymentFailed_IdempotentAndInvalidTransition(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	p := mkPurchase(t)
	if err := p.MarkPaymentFailed("card_declined", "declined", now); err != nil {
		t.Fatalf("first failed: %v", err)
	}
	if err := p.MarkPaymentFailed("again", "again", now); err != nil {
		t.Fatalf("idempotent repeat: %v", err)
	}
	if p.StripeFailureCode != "card_declined" {
		t.Errorf("failure code overwritten on idempotent repeat: %q", p.StripeFailureCode)
	}
	// From captured → invalid.
	q := mkPurchase(t)
	_ = q.MarkPaymentCaptured("pi", "ch", 100, now)
	if err := q.MarkPaymentFailed("code", "msg", now); !errors.Is(err, shared.ErrInvalidTransition) {
		t.Errorf("err=%v, want ErrInvalidTransition", err)
	}
}

func TestMarkRefunded_RejectsNegativeAmount(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	p := mkPurchase(t)
	_ = p.MarkPaymentCaptured("pi", "ch", 100, now)
	if err := p.MarkRefunded("re_1", -1, shared.RefundReasonCustomerRequest, now); !errors.Is(err, shared.ErrAmountNegative) {
		t.Errorf("err=%v, want ErrAmountNegative", err)
	}
}

func TestMarkExpired_IdempotentAndInvalidTransition(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	p := mkPurchase(t)
	if err := p.MarkExpired(now); err != nil {
		t.Fatalf("first expired: %v", err)
	}
	if err := p.MarkExpired(now); err != nil {
		t.Fatalf("idempotent repeat: %v", err)
	}
	// From captured → invalid.
	q := mkPurchase(t)
	_ = q.MarkPaymentCaptured("pi", "ch", 100, now)
	if err := q.MarkExpired(now); !errors.Is(err, shared.ErrInvalidTransition) {
		t.Errorf("err=%v, want ErrInvalidTransition", err)
	}
}

func TestValidateCurrency_RejectsLowercaseAndShort(t *testing.T) {
	t.Parallel()
	if err := shared.ValidateCurrency("sgd"); !errors.Is(err, shared.ErrCurrencyInvalid) {
		t.Errorf("lowercase currency err=%v", err)
	}
	if err := shared.ValidateCurrency("SG"); !errors.Is(err, shared.ErrCurrencyInvalid) {
		t.Errorf("short currency err=%v", err)
	}
}
