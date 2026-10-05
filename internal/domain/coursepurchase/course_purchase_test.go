package coursepurchase_test

import (
	"errors"
	"testing"
	"time"

	cp "github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_course"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_course"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	c, err := cp.New(
		purchaseID, tenantID, learnerGCID, "course_0001",
		1990, "SGD",
		sessionID, checkoutURL,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.CourseID != "course_0001" {
		t.Errorf("CourseID=%s, want course_0001", c.CourseID)
	}
	if c.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", c.State)
	}
	if c.StripeSessionID != sessionID {
		t.Errorf("StripeSessionID=%s, want %s", c.StripeSessionID, sessionID)
	}
}

func TestNew_ValidatesCourseID(t *testing.T) {
	t.Parallel()
	_, err := cp.New(
		purchaseID, tenantID, learnerGCID, "",
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, cp.ErrCourseRequired) {
		t.Errorf("err=%v, want ErrCourseRequired", err)
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := cp.New(
		purchaseID, "", learnerGCID, "course_0001",
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err=%v, want shared.ErrTenantRequired", err)
	}
}

func TestMarkPaymentCaptured_TransitionsState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	c, _ := cp.New(
		purchaseID, tenantID, learnerGCID, "course_0001",
		1990, "SGD", sessionID, checkoutURL, now,
	)
	if err := c.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute)); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if c.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", c.State)
	}
}
