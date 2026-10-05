package stripe_customer_test

import (
	"errors"
	"testing"
	"time"

	sc "github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
)

const (
	tenantID    = "01970000-0000-7000-8000-000000000001"
	learnerGCID = "01970000-0000-7000-a000-000000000002"
	customerID  = "cus_test_0001"
)

func TestNew_Happy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	c, err := sc.New(tenantID, learnerGCID, customerID, "learner@example.com", now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.TenantID != tenantID {
		t.Errorf("TenantID=%s, want %s", c.TenantID, tenantID)
	}
	if c.LearnerGCID != learnerGCID {
		t.Errorf("LearnerGCID=%s, want %s", c.LearnerGCID, learnerGCID)
	}
	if c.StripeCustomerID != customerID {
		t.Errorf("StripeCustomerID=%s, want %s", c.StripeCustomerID, customerID)
	}
	if c.Email != "learner@example.com" {
		t.Errorf("Email=%s, want learner@example.com", c.Email)
	}
	if !c.CreatedAt.Equal(now) || !c.UpdatedAt.Equal(now) {
		t.Errorf("CreatedAt/UpdatedAt not set to now")
	}
}

func TestNew_ValidatesTenantID(t *testing.T) {
	t.Parallel()
	_, err := sc.New("", learnerGCID, customerID, "", time.Now().UTC())
	if !errors.Is(err, sc.ErrTenantRequired) {
		t.Errorf("err=%v, want ErrTenantRequired", err)
	}
}

func TestNew_ValidatesLearnerGCID(t *testing.T) {
	t.Parallel()
	_, err := sc.New(tenantID, "", customerID, "", time.Now().UTC())
	if !errors.Is(err, sc.ErrLearnerRequired) {
		t.Errorf("err=%v, want ErrLearnerRequired", err)
	}
}

func TestNew_ValidatesStripeCustomerID(t *testing.T) {
	t.Parallel()
	_, err := sc.New(tenantID, learnerGCID, "", "", time.Now().UTC())
	if !errors.Is(err, sc.ErrStripeCustomerEmpty) {
		t.Errorf("err=%v, want ErrStripeCustomerEmpty", err)
	}
}
