// Package identity_kyc_fee is the one-time KYC manual-doc verification fee
// Stripe Checkout aggregate. Per ADR-142: Singpass-verified GCID is free,
// manual KYC is $9.99.
//
// Topic family: chora.payments.identity_kyc_fee.*.v1
// Originating service: chora-identity (marks KYC manual-review pipeline
//
//	as paid on payment_captured).
//
// Closes Stage E debt: chora-identity used to charge the $9.99 manual KYC
// fee inline (direct Stripe-go import on chora-identity's HTTP handler).
// Stage A.5 extracts that charge to chora-payments as a 7th aggregate.
//
// Simpler than the per-aggregate FSMs of user_subscription — no recurring
// lifecycle states. Just the canonical shared 5-state payment FSM.
package identity_kyc_fee

import (
	"context"
	"errors"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

// IdentityKycFee is the aggregate root for a one-time KYC manual-doc
// verification charge.
type IdentityKycFee struct {
	shared.Purchase

	// KYCDocType — the canonical KYC document type the learner is
	// verifying via this fee (e.g., "manual_id_doc", "manual_passport",
	// "manual_proof_of_address").
	KYCDocType string
}

var (
	ErrKYCDocTypeRequired = errors.New("payments/identity_kyc_fee: kyc_doc_type required")
)

// New constructs an IdentityKycFee in checkout_started state.
func New(
	purchaseID, tenantID, learnerGCID, kycDocType string,
	amountCents int64, currency string,
	stripeSessionID, stripeCheckoutURL string,
	now time.Time,
) (*IdentityKycFee, error) {
	if kycDocType == "" {
		return nil, ErrKYCDocTypeRequired
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
	return &IdentityKycFee{
		Purchase:   base,
		KYCDocType: kycDocType,
	}, nil
}

type Repo interface {
	Save(ctx context.Context, k *IdentityKycFee) error
	GetByID(ctx context.Context, tenantID, purchaseID string) (*IdentityKycFee, error)
	GetByStripeSessionID(ctx context.Context, stripeSessionID string) (*IdentityKycFee, error)
}
