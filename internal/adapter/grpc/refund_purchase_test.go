// refund_purchase_test.go — coverage for the gRPC RefundPurchase RPC across
// every supported aggregate + the refundAmount / mapRefundReason helpers it
// exercises.
package grpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pgrpc "github.com/apollo-chora/chora-payments/internal/adapter/grpc"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"
	"github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	"github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	"github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	"github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// refundTestServer builds a server plus handles to its inmem repos so tests
// can seed payment_captured aggregates before calling RefundPurchase.
type refundTestServer struct {
	server *pgrpc.Server

	course    *inmem.CoursePurchaseRepo
	app       *inmem.ApplicationPaymentRepo
	egg       *inmem.FamiliarEggPurchaseRepo
	mana      *inmem.TenantManaTopUpRepo
	sub       *inmem.UserSubscriptionRepo
	userMana  *inmem.UserManaTopUpRepo
	kyc       *inmem.IdentityKycFeeRepo
	addon     *inmem.TenantAddonPurchaseRepo
	customers *inmem.StripeCustomerRepo
}

func newRefundTestServer(t *testing.T) *refundTestServer {
	t.Helper()
	ts := &refundTestServer{
		course:    inmem.NewCoursePurchaseRepo(),
		app:       inmem.NewApplicationPaymentRepo(),
		egg:       inmem.NewFamiliarEggPurchaseRepo(),
		mana:      inmem.NewTenantManaTopUpRepo(),
		sub:       inmem.NewUserSubscriptionRepo(),
		userMana:  inmem.NewUserManaTopUpRepo(),
		kyc:       inmem.NewIdentityKycFeeRepo(),
		addon:     inmem.NewTenantAddonPurchaseRepo(),
		customers: inmem.NewStripeCustomerRepo(),
	}
	ts.server = pgrpc.New(pgrpc.Deps{
		Course:            ts.course,
		Application:       ts.app,
		FamiliarEgg:       ts.egg,
		ManaTopUp:         ts.mana,
		Subscription:      ts.sub,
		UserManaTopUp:     ts.userMana,
		IdentityKycFee:    ts.kyc,
		AddonPurchase:     ts.addon,
		StripeCustomers:   ts.customers,
		Stripe:            stripeadapter.NewStubClient(),
		DefaultSuccessURL: "https://chora.site/payment/success",
		DefaultCancelURL:  "https://chora.site/payment/cancel",
		Now:               func() time.Time { return time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC) },
	})
	return ts
}

const refundPurchaseID = "01970000-0000-7000-b000-000000000081"

// seedCaptured inserts a payment_captured aggregate for the given type with
// the Stripe charge id populated (required by the stub refund client).
func (ts *refundTestServer) seedCaptured(t *testing.T, aggType pb.AggregateType) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	switch aggType {
	case pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE:
		a, _ := coursepurchase.New(refundPurchaseID, tTenantID, tLearnerGCID, tCourseID, 99900, "SGD", "cs_refund", "https://x", now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.course.Save(ctx, a)
	case pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT:
		a, _ := application_payment.New(refundPurchaseID, tTenantID, tLearnerGCID, "app_1", tCourseID, 99900, "SGD", "cs_refund", "https://x", now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.app.Save(ctx, a)
	case pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE:
		a, _ := familiar_egg_purchase.New(refundPurchaseID, tTenantID, tLearnerGCID, "egg.standard.v1", "focal", 99900, "SGD", "cs_refund", "https://x", nil, nil, now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.egg.Save(ctx, a)
	case pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP:
		a, _ := tenant_mana_topup.New(refundPurchaseID, tTenantID, tLearnerGCID, "mana.topup.v1", 5000, 99900, "SGD", "cs_refund", "https://x", now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.mana.Save(ctx, a)
	case pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION:
		a, _ := user_subscription.New(refundPurchaseID, tTenantID, tLearnerGCID, "sub.v1", user_subscription.BillingMonthly, 99900, "SGD", "cs_refund", "https://x", now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.sub.Save(ctx, a)
	case pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP:
		a, _ := user_mana_topup.New(refundPurchaseID, tTenantID, tLearnerGCID, "mana.user.v1", 2500, 99900, "SGD", "cs_refund", "https://x", now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.userMana.Save(ctx, a)
	case pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE:
		a, _ := identity_kyc_fee.New(refundPurchaseID, tTenantID, tLearnerGCID, "manual_id_doc", 99900, "SGD", "cs_refund", "https://x", now)
		_ = a.MarkPaymentCaptured("pi_refund", "ch_refund", 99900, now)
		_ = ts.kyc.Save(ctx, a)
	default:
		t.Fatalf("unhandled aggregate %v", aggType)
	}
}

func refundAggregateCases() []struct {
	aggType pb.AggregateType
	reason  pb.RefundReason
	state   pb.PurchaseState
} {
	return []struct {
		aggType pb.AggregateType
		reason  pb.RefundReason
		state   pb.PurchaseState
	}{
		{pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE, pb.RefundReason_REFUND_REASON_SUPPORT_INITIATED, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
		{pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT, pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
		{pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE, pb.RefundReason_REFUND_REASON_DUPLICATE_CHARGE, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
		{pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP, pb.RefundReason_REFUND_REASON_FRAUD, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
		{pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION, pb.RefundReason_REFUND_REASON_EXPIRED_UNHATCHED, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
		{pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP, pb.RefundReason_REFUND_REASON_UNSPECIFIED, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
		{pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE, pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST, pb.PurchaseState_PURCHASE_STATE_REFUNDED},
	}
}

func TestRefundPurchase_AllAggregates_HappyPath(t *testing.T) {
	for _, tc := range refundAggregateCases() {
		t.Run(strings.ToLower(tc.aggType.String()), func(t *testing.T) {
			ts := newRefundTestServer(t)
			ts.seedCaptured(t, tc.aggType)
			resp, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{
				PurchaseId:    refundPurchaseID,
				TenantId:      tTenantID,
				AggregateType: tc.aggType,
				AmountCents:   5000,
				Reason:        tc.reason,
			})
			if err != nil {
				t.Fatalf("RefundPurchase: %v", err)
			}
			if resp.Purchase == nil || resp.Purchase.State != tc.state {
				t.Errorf("Purchase=%+v", resp.Purchase)
			}
			if !strings.HasPrefix(resp.Purchase.StripeRefundId, "re_stub_") {
				t.Errorf("StripeRefundId=%q", resp.Purchase.StripeRefundId)
			}
		})
	}
}

func TestRefundPurchase_RequestedAmountZeroFallsBackToRemaining(t *testing.T) {
	ts := newRefundTestServer(t)
	ts.seedCaptured(t, pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE)
	resp, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{
		PurchaseId:    refundPurchaseID,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
		AmountCents:   0, // full remaining
		Reason:        pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST,
	})
	if err != nil {
		t.Fatalf("RefundPurchase: %v", err)
	}
	if resp.Purchase.AmountCentsRefunded != 99900 {
		t.Errorf("AmountCentsRefunded=%d, want 99900 (full remaining)", resp.Purchase.AmountCentsRefunded)
	}
}

func TestRefundPurchase_RequiresPurchaseAndTenant(t *testing.T) {
	ts := newRefundTestServer(t)
	if _, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{TenantId: tTenantID}); err == nil {
		t.Error("missing purchase_id should error")
	}
	if _, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{PurchaseId: refundPurchaseID}); err == nil {
		t.Error("missing tenant_id should error")
	}
}

func TestRefundPurchase_RepoMissNotFound(t *testing.T) {
	ts := newRefundTestServer(t)
	_, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{
		PurchaseId:    refundPurchaseID,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
	})
	if err == nil || !strings.Contains(err.Error(), "NotFound") {
		t.Errorf("err=%v, want NotFound", err)
	}
}

func TestRefundPurchase_UnknownAggregateType(t *testing.T) {
	ts := newRefundTestServer(t)
	_, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{
		PurchaseId:    refundPurchaseID,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_UNSPECIFIED,
	})
	if err == nil || !strings.Contains(err.Error(), "unknown aggregate_type") {
		t.Errorf("err=%v, want unknown aggregate_type", err)
	}
}

func TestRefundPurchaseResponse_StateQueries(t *testing.T) {
	// GetPurchase round-trip through loadPurchaseProto for the refunded row —
	// exercises the mapState projection already flagged by the refund path.
	ts := newRefundTestServer(t)
	ts.seedCaptured(t, pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE)
	if _, err := ts.server.RefundPurchase(context.Background(), &pb.RefundPurchaseRequest{
		PurchaseId:    refundPurchaseID,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
		AmountCents:   100,
	}); err != nil {
		t.Fatalf("RefundPurchase: %v", err)
	}
	got, err := ts.server.GetPurchase(context.Background(), &pb.GetPurchaseRequest{
		PurchaseId:    refundPurchaseID,
		TenantId:      tTenantID,
		AggregateType: pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE,
	})
	if err != nil {
		t.Fatalf("GetPurchase: %v", err)
	}
	if got.Purchase.State != pb.PurchaseState_PURCHASE_STATE_REFUNDED {
		t.Errorf("state=%v, want refunded", got.Purchase.State)
	}
	if got.Purchase.RefundReason != pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST {
		t.Errorf("refund reason=%v", got.Purchase.RefundReason)
	}
}

// compile-time guard against the wh import being dropped by refactors.
var _ = wh.AggregateCoursePurchase
var _ = shared.StateRefunded
