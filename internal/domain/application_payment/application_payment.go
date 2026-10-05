// Package application_payment is the Course-Application Singpass+SkillsFuture
// Stripe Checkout aggregate (replaces chora.tenancy.payment.captured.v1 at
// Wave 1 Stage C cutover).
//
// Topic family: chora.payments.application_payment.*.v1
// Originating service: chora-delivery
package application_payment

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// ApplicationPayment is the aggregate root for a course-application payment.
type ApplicationPayment struct {
	shared.Purchase

	// ApplicationID is the chora_delivery.applications row this payment
	// is for. Soft FK; chora-payments does NOT validate existence.
	ApplicationID string

	// CourseID — the course the application is for (denormalised for
	// audit + downstream subscriber convenience).
	CourseID string
}

var (
	ErrApplicationRequired = errors.New("payments/application_payment: application_id required")
	ErrCourseRequired      = errors.New("payments/application_payment: course_id required")
)

// New constructs an ApplicationPayment in checkout_started state.
func New(
	purchaseID, tenantID, learnerGCID, applicationID, courseID string,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*ApplicationPayment, error) {
	if applicationID == "" {
		return nil, ErrApplicationRequired
	}
	if courseID == "" {
		return nil, ErrCourseRequired
	}
	base, err := shared.NewPurchase(
		purchaseID, tenantID, learnerGCID,
		amountCents, currency,
		stripeSessionID, stripeCheckoutURL,
		now,
	)
	if err != nil {
		return nil, err
	}
	return &ApplicationPayment{
		Purchase:      base,
		ApplicationID: applicationID,
		CourseID:      courseID,
	}, nil
}

type Repo interface {
	Save(ctx context.Context, ap *ApplicationPayment) error
	GetByID(ctx context.Context, tenantID, purchaseID string) (*ApplicationPayment, error)
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*ApplicationPayment, error)
}
