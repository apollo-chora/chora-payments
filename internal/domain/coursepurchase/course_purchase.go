// Package coursepurchase is the CJ#2 paid-course Stripe Checkout aggregate.
//
// Topic family: chora.payments.course_purchase.*.v1
// Originating service: chora-delivery
package coursepurchase

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// CoursePurchase is the aggregate root for a single CJ#2 paid course
// enrolment Checkout flow.
type CoursePurchase struct {
	shared.Purchase

	// CourseID is the paid course this purchase enrolls the learner into.
	// Soft FK to chora_delivery.courses; chora-payments does NOT validate
	// course existence (cross-DB queries forbidden).
	CourseID string
}

// ErrCourseRequired is returned when a CoursePurchase has no CourseID.
var ErrCourseRequired = errors.New("payments/coursepurchase: course_id required")

// New constructs a CoursePurchase in checkout_started state.
func New(
	purchaseID, tenantID, learnerGCID, courseID string,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*CoursePurchase, error) {
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
	return &CoursePurchase{
		Purchase: base,
		CourseID: courseID,
	}, nil
}

// -----------------------------------------------------------------------------
// Repo port
// -----------------------------------------------------------------------------

// Repo is the hexagonal port for persisting + retrieving CoursePurchase
// aggregates. Production wires the pg adapter; unit tests wire the
// inmem adapter.
type Repo interface {
	// Save UPSERTs the aggregate by PurchaseID.
	Save(ctx context.Context, cp *CoursePurchase) error
	// GetByID looks up a CoursePurchase by PurchaseID inside the RLS scope
	// of `tenantID`. Returns shared.ErrNotFound when no row matches.
	GetByID(ctx context.Context, tenantID, purchaseID string) (*CoursePurchase, error)
	// GetByStripeSessionID — webhook dispatch lookup. RLS-bypass safe
	// because session_id is UNIQUE + Stripe-issued (no cross-tenant
	// collision risk).
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*CoursePurchase, error)
}
