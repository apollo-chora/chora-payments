package application_payment_test

import (
	"errors"
	"testing"
	"time"

	ap "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	purchaseID  = "01970000-0000-7000-b000-000000000003"
	sessionID   = "cs_test_app"
	checkoutURL = "https://checkout.stripe.com/c/pay/cs_test_app"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	a, err := ap.New(
		purchaseID, tenantID, learnerGCID, "app_0001", "course_0001",
		1990, "SGD",
		sessionID, checkoutURL,
		now,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.ApplicationID != "app_0001" {
		t.Errorf("ApplicationID=%s, want app_0001", a.ApplicationID)
	}
	if a.CourseID != "course_0001" {
		t.Errorf("CourseID=%s, want course_0001", a.CourseID)
	}
	if a.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want checkout_started", a.State)
	}
	if a.LearnerGCID != learnerGCID {
		t.Errorf("LearnerGCID mismatch")
	}
}

func TestNew_ValidatesApplicationID(t *testing.T) {
	t.Parallel()
	_, err := ap.New(
		purchaseID, tenantID, learnerGCID, "", "course_0001",
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, ap.ErrApplicationRequired) {
		t.Errorf("err=%v, want ErrApplicationRequired", err)
	}
}

func TestNew_ValidatesCourseID(t *testing.T) {
	t.Parallel()
	_, err := ap.New(
		purchaseID, tenantID, learnerGCID, "app_0001", "",
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, ap.ErrCourseRequired) {
		t.Errorf("err=%v, want ErrCourseRequired", err)
	}
}

func TestNew_PropagatesSharedValidation(t *testing.T) {
	t.Parallel()
	_, err := ap.New(
		purchaseID, tenantID, "", "app_0001", "course_0001",
		1990, "SGD", sessionID, checkoutURL, time.Now().UTC(),
	)
	if !errors.Is(err, shared.ErrLearnerRequired) {
		t.Errorf("err=%v, want shared.ErrLearnerRequired", err)
	}
}

func TestMarkPaymentCaptured_TransitionsState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	a, _ := ap.New(
		purchaseID, tenantID, learnerGCID, "app_0001", "course_0001",
		1990, "SGD", sessionID, checkoutURL, now,
	)
	if err := a.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute)); err != nil {
		t.Fatalf("MarkPaymentCaptured: %v", err)
	}
	if a.State != shared.StatePaymentCaptured {
		t.Errorf("State=%s, want payment_captured", a.State)
	}
}
