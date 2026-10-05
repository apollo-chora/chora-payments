// payload_test.go — RED→GREEN marshaling coverage for the per-event
// proto marshallers introduced in Stage A.5 (user_mana_topup +
// identity_kyc_fee + user_subscription.cancelled).
package outbox

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"

	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const (
	testTenantID    = "01970000-0000-7000-8000-000000000001"
	testLearnerGCID = "01970000-0000-7000-a000-000000000002"
	testPurchaseID  = "01970000-0000-7000-b000-000000000003"
	testSessionID   = "cs_test_payload"
	testCheckoutURL = "https://checkout.stripe.com/c/pay/cs_test_payload"
)

func fixedTime() time.Time {
	return time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
}

func testEnvelope() *commonv1.EventEnvelope {
	return &commonv1.EventEnvelope{
		EventId:        "evt-1",
		IdempotencyKey: "k-1",
		TenantId:       testTenantID,
		OccurredAt:     timestamppb.New(fixedTime()),
		PublishedAt:    timestamppb.New(fixedTime()),
		SourceService:  "chora-payments",
	}
}

// -----------------------------------------------------------------------------
// UserManaTopUp — 5 events
// -----------------------------------------------------------------------------

func TestMarshalUserManaTopUp_AllFiveEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	u, err := umt.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.user_topup.standard_v1",
		2500, 1990, "SGD",
		testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("umt.New: %v", err)
	}
	// Advance to payment_captured for events that require it.
	paid := u
	_ = paid.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute))

	cases := []struct {
		name    string
		evType  dispatcher.EventType
		decode  proto.Message
		wantTop string
	}{
		{"checkout_started", dispatcher.EventCheckoutStarted, &paymentsv1.UserManaTopUpCheckoutStarted{}, "chora.payments.user_mana_topup.checkout_started.v1"},
		{"payment_captured", dispatcher.EventPaymentCaptured, &paymentsv1.UserManaTopUpPaymentCaptured{}, "chora.payments.user_mana_topup.payment_captured.v1"},
		{"payment_failed", dispatcher.EventPaymentFailed, &paymentsv1.UserManaTopUpPaymentFailed{}, "chora.payments.user_mana_topup.payment_failed.v1"},
		{"refunded", dispatcher.EventRefunded, &paymentsv1.UserManaTopUpRefunded{}, "chora.payments.user_mana_topup.refunded.v1"},
		{"expired", dispatcher.EventExpired, &paymentsv1.UserManaTopUpExpired{}, "chora.payments.user_mana_topup.expired.v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, topic, err := marshalUserManaTopUp(testEnvelope(), paid, tc.evType)
			if err != nil {
				t.Fatalf("marshalUserManaTopUp: %v", err)
			}
			if topic != tc.wantTop {
				t.Errorf("topic=%q, want %q", topic, tc.wantTop)
			}
			if err := proto.Unmarshal(payload, tc.decode); err != nil {
				t.Errorf("unmarshal: %v", err)
			}
		})
	}
}

func TestMarshalUserManaTopUp_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	u, _ := umt.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.user_topup.standard_v1",
		2500, 1990, "SGD", testSessionID, testCheckoutURL, fixedTime(),
	)
	_, _, err := marshalUserManaTopUp(testEnvelope(), u, dispatcher.EventType("bogus"))
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// IdentityKycFee — 5 events
// -----------------------------------------------------------------------------

func TestMarshalIdentityKycFee_AllFiveEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	k, err := kyc.New(
		testPurchaseID, testTenantID, testLearnerGCID, "manual_id_doc",
		999, "USD",
		testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("kyc.New: %v", err)
	}
	_ = k.MarkPaymentCaptured("pi_test", "ch_test", 999, now.Add(time.Minute))

	cases := []struct {
		name    string
		evType  dispatcher.EventType
		decode  proto.Message
		wantTop string
	}{
		{"checkout_started", dispatcher.EventCheckoutStarted, &paymentsv1.IdentityKycFeeCheckoutStarted{}, "chora.payments.identity_kyc_fee.checkout_started.v1"},
		{"payment_captured", dispatcher.EventPaymentCaptured, &paymentsv1.IdentityKycFeePaymentCaptured{}, "chora.payments.identity_kyc_fee.payment_captured.v1"},
		{"payment_failed", dispatcher.EventPaymentFailed, &paymentsv1.IdentityKycFeePaymentFailed{}, "chora.payments.identity_kyc_fee.payment_failed.v1"},
		{"refunded", dispatcher.EventRefunded, &paymentsv1.IdentityKycFeeRefunded{}, "chora.payments.identity_kyc_fee.refunded.v1"},
		{"expired", dispatcher.EventExpired, &paymentsv1.IdentityKycFeeExpired{}, "chora.payments.identity_kyc_fee.expired.v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, topic, err := marshalIdentityKycFee(testEnvelope(), k, tc.evType)
			if err != nil {
				t.Fatalf("marshalIdentityKycFee: %v", err)
			}
			if topic != tc.wantTop {
				t.Errorf("topic=%q, want %q", topic, tc.wantTop)
			}
			if err := proto.Unmarshal(payload, tc.decode); err != nil {
				t.Errorf("unmarshal: %v", err)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// UserSubscription.cancelled — new lifecycle event introduced by Stage A.5
// -----------------------------------------------------------------------------

func TestMarshalUserSubscription_Cancelled(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	u, err := sub.New(
		testPurchaseID, testTenantID, testLearnerGCID, "familiar.standard.monthly.v1",
		sub.BillingMonthly,
		999, "SGD",
		testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("sub.New: %v", err)
	}
	u.StripeSubscriptionID = "sub_test_xyz"
	u.MarkSubscriptionCancelled("user requested cancel", true, now.AddDate(0, 0, 30), now)

	payload, topic, err := marshalUserSubscription(testEnvelope(), u, dispatcher.EventSubscriptionCancelled)
	if err != nil {
		t.Fatalf("marshal cancelled: %v", err)
	}
	if topic != "chora.payments.user_subscription.cancelled.v1" {
		t.Errorf("topic=%q", topic)
	}
	got := &paymentsv1.UserSubscriptionCancelled{}
	if err := proto.Unmarshal(payload, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.PurchaseId != testPurchaseID {
		t.Errorf("PurchaseId=%s, want %s", got.PurchaseId, testPurchaseID)
	}
	if got.StripeSubscriptionId != "sub_test_xyz" {
		t.Errorf("StripeSubscriptionId=%s, want sub_test_xyz", got.StripeSubscriptionId)
	}
	if !got.AtPeriodEnd {
		t.Errorf("AtPeriodEnd=false, want true")
	}
	if got.Reason != "user requested cancel" {
		t.Errorf("Reason=%s", got.Reason)
	}
}

// -----------------------------------------------------------------------------
// loadAndMarshal dispatch — wires the right marshaller per aggregate.
// -----------------------------------------------------------------------------

func TestLoadAndMarshal_DispatchesNewAggregates(t *testing.T) {
	t.Parallel()
	now := fixedTime()

	courseRepo := inmem.NewCoursePurchaseRepo()
	appRepo := inmem.NewApplicationPaymentRepo()
	eggRepo := inmem.NewFamiliarEggPurchaseRepo()
	manaRepo := inmem.NewTenantManaTopUpRepo()
	subRepo := inmem.NewUserSubscriptionRepo()
	userManaRepo := inmem.NewUserManaTopUpRepo()
	kycRepo := inmem.NewIdentityKycFeeRepo()

	// Seed user_mana_topup + identity_kyc_fee.
	umtAgg, _ := umt.New(testPurchaseID, testTenantID, testLearnerGCID, "mana.user_topup.standard_v1", 2500, 1990, "SGD", testSessionID, testCheckoutURL, now)
	_ = userManaRepo.Save(context.Background(), umtAgg)

	kycAgg, _ := kyc.New("01970000-0000-7000-b000-000000000007", testTenantID, testLearnerGCID, "manual_id_doc", 999, "USD", "cs_test_kyc", testCheckoutURL, now)
	_ = kycRepo.Save(context.Background(), kycAgg)

	deps := EmitterDeps{
		Course: courseRepo, Application: appRepo, FamiliarEgg: eggRepo,
		ManaTopUp: manaRepo, Subscription: subRepo,
		UserManaTopUp: userManaRepo, IdentityKycFee: kycRepo,
	}

	// user_mana_topup.
	payload, topic, err := loadAndMarshal(context.Background(), deps, testEnvelope(), wh.AggregateUserManaTopUp, testPurchaseID, dispatcher.EventCheckoutStarted)
	if err != nil {
		t.Fatalf("loadAndMarshal user_mana_topup: %v", err)
	}
	if topic != "chora.payments.user_mana_topup.checkout_started.v1" {
		t.Errorf("topic=%q", topic)
	}
	if len(payload) == 0 {
		t.Errorf("payload empty")
	}

	// identity_kyc_fee.
	payload, topic, err = loadAndMarshal(context.Background(), deps, testEnvelope(), wh.AggregateIdentityKycFee, "01970000-0000-7000-b000-000000000007", dispatcher.EventCheckoutStarted)
	if err != nil {
		t.Fatalf("loadAndMarshal identity_kyc_fee: %v", err)
	}
	if topic != "chora.payments.identity_kyc_fee.checkout_started.v1" {
		t.Errorf("topic=%q", topic)
	}
	if len(payload) == 0 {
		t.Errorf("payload empty")
	}
}

// silence unused-variable lint warning on shared.
var _ = shared.StateCheckoutStarted
