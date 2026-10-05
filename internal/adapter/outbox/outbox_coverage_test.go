// outbox_coverage_test.go — coverage for the outbox package surfaces not
// exercised by payload_test.go / dispatcher_test.go / audit_emit_test.go:
// the remaining purchase marshallers, loadAndMarshal dispatch + error paths,
// the Emitter, the DrainOnce/Run dispatcher loop, and AuditOutboxAdapter.
package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-common/envelope"

	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"

	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// -----------------------------------------------------------------------------
// CoursePurchase — all 5 event types
// -----------------------------------------------------------------------------

func TestMarshalCoursePurchase_AllFiveEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	cpAgg, err := coursepurchase.New(
		testPurchaseID, testTenantID, testLearnerGCID, "course_0001",
		1990, "SGD", testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("coursepurchase.New: %v", err)
	}
	_ = cpAgg.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute))
	_ = cpAgg.MarkPaymentFailed("card_declined", "declined", now.Add(2*time.Minute))
	_ = cpAgg.MarkRefunded("re_test", 1990, "customer_request", now.Add(3*time.Minute))
	_ = cpAgg.MarkExpired(now.Add(4 * time.Minute))

	for _, evType := range []dispatcher.EventType{
		dispatcher.EventCheckoutStarted,
		dispatcher.EventPaymentCaptured,
		dispatcher.EventPaymentFailed,
		dispatcher.EventRefunded,
		dispatcher.EventExpired,
	} {
		payload, topic, err := marshalCoursePurchase(testEnvelope(), cpAgg, evType)
		if err != nil {
			t.Fatalf("marshalCoursePurchase(%s): %v", evType, err)
		}
		if topic != topicForEvent(wh.AggregateCoursePurchase, evType) {
			t.Errorf("topic=%q", topic)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", evType)
		}
	}
}

func TestMarshalCoursePurchase_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	cpAgg, _ := coursepurchase.New(
		testPurchaseID, testTenantID, testLearnerGCID, "course_0001",
		1990, "SGD", testSessionID, testCheckoutURL, fixedTime(),
	)
	_, _, err := marshalCoursePurchase(testEnvelope(), cpAgg, "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// ApplicationPayment — all 5 event types
// -----------------------------------------------------------------------------

func TestMarshalApplicationPayment_AllFiveEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	appAgg, err := apppay.New(
		testPurchaseID, testTenantID, testLearnerGCID, "app_0001", "course_0001",
		1990, "SGD", testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("apppay.New: %v", err)
	}
	_ = appAgg.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute))

	for _, evType := range []dispatcher.EventType{
		dispatcher.EventCheckoutStarted,
		dispatcher.EventPaymentCaptured,
		dispatcher.EventPaymentFailed,
		dispatcher.EventRefunded,
		dispatcher.EventExpired,
	} {
		payload, topic, err := marshalApplicationPayment(testEnvelope(), appAgg, evType)
		if err != nil {
			t.Fatalf("marshalApplicationPayment(%s): %v", evType, err)
		}
		if topic != topicForEvent(wh.AggregateApplicationPayment, evType) {
			t.Errorf("topic=%q", topic)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", evType)
		}
	}
}

func TestMarshalApplicationPayment_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	appAgg, _ := apppay.New(
		testPurchaseID, testTenantID, testLearnerGCID, "app_0001", "course_0001",
		1990, "SGD", testSessionID, testCheckoutURL, fixedTime(),
	)
	_, _, err := marshalApplicationPayment(testEnvelope(), appAgg, "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// FamiliarEggPurchase — all 5 event types
// -----------------------------------------------------------------------------

func TestMarshalFamiliarEggPurchase_AllFiveEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	eggAgg, err := egg.New(
		testPurchaseID, testTenantID, testLearnerGCID, "egg.standard.v1",
		"focal_atom_0001",
		1990, "SGD", testSessionID, testCheckoutURL, nil, nil, now,
	)
	if err != nil {
		t.Fatalf("egg.New: %v", err)
	}
	_ = eggAgg.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute))
	eggAgg.RefundCreditOnly = true

	for _, evType := range []dispatcher.EventType{
		dispatcher.EventCheckoutStarted,
		dispatcher.EventPaymentCaptured,
		dispatcher.EventPaymentFailed,
		dispatcher.EventRefunded,
		dispatcher.EventExpired,
	} {
		payload, topic, err := marshalFamiliarEggPurchase(testEnvelope(), eggAgg, evType)
		if err != nil {
			t.Fatalf("marshalFamiliarEggPurchase(%s): %v", evType, err)
		}
		if topic != topicForEvent(wh.AggregateFamiliarEggPurchase, evType) {
			t.Errorf("topic=%q", topic)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", evType)
		}
	}
}

func TestMarshalFamiliarEggPurchase_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	eggAgg, _ := egg.New(
		testPurchaseID, testTenantID, testLearnerGCID, "egg.standard.v1",
		"focal_atom_0001",
		1990, "SGD", testSessionID, testCheckoutURL, nil, nil, fixedTime(),
	)
	_, _, err := marshalFamiliarEggPurchase(testEnvelope(), eggAgg, "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// TenantManaTopUp — all 5 event types
// -----------------------------------------------------------------------------

func TestMarshalTenantManaTopUp_AllFiveEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	topAgg, err := mana.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.tenant_topup.standard_v1",
		5000, 4990, "SGD", testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("mana.New: %v", err)
	}
	_ = topAgg.MarkPaymentCaptured("pi_test", "ch_test", 4990, now.Add(time.Minute))
	topAgg.ManaUnitsDebited = 5000

	for _, evType := range []dispatcher.EventType{
		dispatcher.EventCheckoutStarted,
		dispatcher.EventPaymentCaptured,
		dispatcher.EventPaymentFailed,
		dispatcher.EventRefunded,
		dispatcher.EventExpired,
	} {
		payload, topic, err := marshalTenantManaTopUp(testEnvelope(), topAgg, evType)
		if err != nil {
			t.Fatalf("marshalTenantManaTopUp(%s): %v", evType, err)
		}
		if topic != topicForEvent(wh.AggregateTenantManaTopUp, evType) {
			t.Errorf("topic=%q", topic)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", evType)
		}
	}
}

func TestMarshalTenantManaTopUp_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	topAgg, _ := mana.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.tenant_topup.standard_v1",
		5000, 4990, "SGD", testSessionID, testCheckoutURL, fixedTime(),
	)
	_, _, err := marshalTenantManaTopUp(testEnvelope(), topAgg, "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// UserSubscription — 6 event types (5 payment lifecycle + cancelled)
// -----------------------------------------------------------------------------

func TestMarshalUserSubscription_AllEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	subAgg, err := sub.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.subscription.standard_v1",
		sub.BillingMonthly,
		1990, "SGD", testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("sub.New: %v", err)
	}
	_ = subAgg.MarkPaymentCaptured("pi_test", "ch_test", 1990, now.Add(time.Minute))
	subAgg.StripeSubscriptionID = "sub_test_xyz"
	subAgg.StripeInvoiceID = "in_test_xyz"
	subAgg.CurrentPeriodStart = &now
	subAgg.CurrentPeriodEnd = &now
	subAgg.MarkSubscriptionCancelled("user requested", true, now.AddDate(0, 0, 30), now)

	for _, evType := range []dispatcher.EventType{
		dispatcher.EventCheckoutStarted,
		dispatcher.EventPaymentCaptured,
		dispatcher.EventPaymentFailed,
		dispatcher.EventRefunded,
		dispatcher.EventExpired,
		dispatcher.EventSubscriptionCancelled,
	} {
		payload, topic, err := marshalUserSubscription(testEnvelope(), subAgg, evType)
		if err != nil {
			t.Fatalf("marshalUserSubscription(%s): %v", evType, err)
		}
		if topic != topicForEvent(wh.AggregateUserSubscription, evType) {
			t.Errorf("topic=%q", topic)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", evType)
		}
	}
}

func TestMarshalUserSubscription_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	subAgg, _ := sub.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.subscription.standard_v1",
		sub.BillingMonthly,
		1990, "SGD", testSessionID, testCheckoutURL, fixedTime(),
	)
	_, _, err := marshalUserSubscription(testEnvelope(), subAgg, "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// TenantAddonPurchase — full event matrix (incl. subscription lifecycle)
// -----------------------------------------------------------------------------

func TestMarshalTenantAddonPurchase_AllEventTypes(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	addonAgg, err := tap.New(
		testPurchaseID, testTenantID, testLearnerGCID,
		"addon_plan_0001", "enterprise", "tier1",
		7990, "SGD", testSessionID, testCheckoutURL, now,
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	_ = addonAgg.MarkPaymentCaptured("pi_test", "ch_test", 7990, now.Add(time.Minute))
	addonAgg.StripeSubscriptionID = "sub_test_xyz"
	addonAgg.StripeCustomerID = "cus_test_xyz"

	for _, evType := range []dispatcher.EventType{
		dispatcher.EventCheckoutStarted,
		dispatcher.EventPaymentCaptured,
		dispatcher.EventPaymentFailed,
		dispatcher.EventRefunded,
		dispatcher.EventExpired,
		dispatcher.EventSubscriptionCancelled,
		dispatcher.EventSubscriptionPaymentFailed,
		dispatcher.EventPaymentRecovered,
		dispatcher.EventSubscriptionScheduleReleased,
	} {
		payload, topic, err := marshalTenantAddonPurchase(testEnvelope(), addonAgg, evType)
		if err != nil {
			t.Fatalf("marshalTenantAddonPurchase(%s): %v", evType, err)
		}
		if topic != topicForEvent(wh.AggregateTenantAddonPurchase, evType) {
			t.Errorf("topic=%q", topic)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", evType)
		}
	}
}

func TestMarshalTenantAddonPurchase_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	addonAgg, _ := tap.New(
		testPurchaseID, testTenantID, testLearnerGCID,
		"addon_plan_0001", "enterprise", "tier1",
		7990, "SGD", testSessionID, testCheckoutURL, fixedTime(),
	)
	_, _, err := marshalTenantAddonPurchase(testEnvelope(), addonAgg, "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

// -----------------------------------------------------------------------------
// Dispute — 4 event types + topicForDisputeEvent
// -----------------------------------------------------------------------------

func disputeAggregate(t *testing.T) *dispute.Dispute {
	t.Helper()
	now := fixedTime()
	d, err := dispute.New(
		"01970000-0000-7000-b000-000000000099", testTenantID,
		wh.AggregateCoursePurchase, testPurchaseID,
		"dp_test_0001", "ch_test_0001",
		1990, "SGD", dispute.ReasonFraudulent,
		now.Add(7*24*time.Hour), now,
	)
	if err != nil {
		t.Fatalf("dispute.New: %v", err)
	}
	return d
}

func TestMarshalDispute_AllFourEventTypes(t *testing.T) {
	t.Parallel()
	d := disputeAggregate(t)
	now := fixedTime()
	_ = d.RecordFundsWithdrawn(now.Add(time.Hour))
	_ = d.RecordFundsReinstated(now.Add(2 * time.Hour))
	_ = d.MarkClosed(dispute.OutcomeWon, now.Add(3*time.Hour))

	cases := []struct {
		evType dispatcher.EventType
		decode proto.Message
	}{
		{dispatcher.EventDisputeRaised, &paymentsv1.DisputeRaised{}},
		{dispatcher.EventDisputeClosed, &paymentsv1.DisputeClosed{}},
		{dispatcher.EventDisputeFundsWithdrawn, &paymentsv1.DisputeFundsWithdrawn{}},
		{dispatcher.EventDisputeFundsReinstated, &paymentsv1.DisputeFundsReinstated{}},
	}
	for _, tc := range cases {
		payload, topic, err := marshalDispute(testEnvelope(), d, tc.evType)
		if err != nil {
			t.Fatalf("marshalDispute(%s): %v", tc.evType, err)
		}
		if topic != topicForDisputeEvent(tc.evType) {
			t.Errorf("topic=%q, want chora.payments.dispute.%s.v1", topic, tc.evType)
		}
		if err := proto.Unmarshal(payload, tc.decode); err != nil {
			t.Errorf("unmarshal %s: %v", tc.evType, err)
		}
	}
}

func TestMarshalDispute_UnknownEventTypeErrors(t *testing.T) {
	t.Parallel()
	_, _, err := marshalDispute(testEnvelope(), disputeAggregate(t), "bogus")
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err=%v, want 'not supported'", err)
	}
}

func TestLoadAndMarshalDispute_NoRepoWired(t *testing.T) {
	t.Parallel()
	deps := EmitterDeps{} // Dispute nil
	_, _, err := loadAndMarshalDispute(context.Background(), deps, testEnvelope(), "dispute_1", dispatcher.EventDisputeRaised)
	if err == nil || !strings.Contains(err.Error(), "dispute repo not wired") {
		t.Errorf("err=%v, want 'dispute repo not wired'", err)
	}
}

func TestLoadAndMarshalDispute_LookupError(t *testing.T) {
	t.Parallel()
	deps := EmitterDeps{Dispute: inmem.NewDisputeRepo()} // empty repo
	_, _, err := loadAndMarshalDispute(context.Background(), deps, testEnvelope(), "missing", dispatcher.EventDisputeRaised)
	if err == nil || !strings.Contains(err.Error(), "dispute lookup") {
		t.Errorf("err=%v, want 'dispute lookup'", err)
	}
}

func TestLoadAndMarshalDispute_Happy(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDisputeRepo()
	_ = repo.Save(context.Background(), disputeAggregate(t))
	deps := EmitterDeps{Dispute: repo}
	payload, topic, err := loadAndMarshalDispute(context.Background(), deps, testEnvelope(), "01970000-0000-7000-b000-000000000099", dispatcher.EventDisputeRaised)
	if err != nil {
		t.Fatalf("loadAndMarshalDispute: %v", err)
	}
	if topic != "chora.payments.dispute.raised.v1" {
		t.Errorf("topic=%q", topic)
	}
	if len(payload) == 0 {
		t.Errorf("payload empty")
	}
}

// -----------------------------------------------------------------------------
// loadAndMarshal dispatch — remaining aggregates + error paths
// -----------------------------------------------------------------------------

func TestLoadAndMarshal_DispatchesRemainingAggregates(t *testing.T) {
	t.Parallel()
	now := fixedTime()

	courseRepo := inmem.NewCoursePurchaseRepo()
	appRepo := inmem.NewApplicationPaymentRepo()
	eggRepo := inmem.NewFamiliarEggPurchaseRepo()
	manaRepo := inmem.NewTenantManaTopUpRepo()
	subRepo := inmem.NewUserSubscriptionRepo()
	addonRepo := inmem.NewTenantAddonPurchaseRepo()

	cpAgg, _ := coursepurchase.New(id1(), testTenantID, testLearnerGCID, "course_0001", 1990, "SGD", "cs_cp", testCheckoutURL, now)
	_ = courseRepo.Save(context.Background(), cpAgg)

	appAgg, _ := apppay.New(id2(), testTenantID, testLearnerGCID, "app_0001", "course_0001", 1990, "SGD", "cs_app", testCheckoutURL, now)
	_ = appRepo.Save(context.Background(), appAgg)

	eggAgg, _ := egg.New(id3(), testTenantID, testLearnerGCID, "egg.standard.v1", "focal", 1990, "SGD", "cs_egg", testCheckoutURL, nil, nil, now)
	_ = eggRepo.Save(context.Background(), eggAgg)

	manaAgg, _ := mana.New(id4(), testTenantID, testLearnerGCID, "mana.tenant_topup.standard_v1", 5000, 4990, "SGD", "cs_top", testCheckoutURL, now)
	_ = manaRepo.Save(context.Background(), manaAgg)

	subAgg, _ := sub.New(id5(), testTenantID, testLearnerGCID, "mana.subscription.standard_v1", sub.BillingMonthly, 1990, "SGD", "cs_sub", testCheckoutURL, now)
	_ = subRepo.Save(context.Background(), subAgg)

	addonAgg, _ := tap.New(id6(), testTenantID, testLearnerGCID, "addon_plan_0001", "enterprise", "tier1", 7990, "SGD", "cs_addon", testCheckoutURL, now)
	_ = addonRepo.Save(context.Background(), addonAgg)

	deps := EmitterDeps{
		Course: courseRepo, Application: appRepo, FamiliarEgg: eggRepo,
		ManaTopUp: manaRepo, Subscription: subRepo,
		AddonPurchase: addonRepo,
	}

	cases := []struct {
		agg    wh.AggregateType
		id     string
		evType dispatcher.EventType
		want   string
	}{
		{wh.AggregateCoursePurchase, id1(), dispatcher.EventCheckoutStarted, "chora.payments.course_purchase.checkout_started.v1"},
		{wh.AggregateApplicationPayment, id2(), dispatcher.EventCheckoutStarted, "chora.payments.application_payment.checkout_started.v1"},
		{wh.AggregateFamiliarEggPurchase, id3(), dispatcher.EventCheckoutStarted, "chora.payments.familiar_egg_purchase.checkout_started.v1"},
		{wh.AggregateTenantManaTopUp, id4(), dispatcher.EventCheckoutStarted, "chora.payments.tenant_mana_topup.checkout_started.v1"},
		{wh.AggregateUserSubscription, id5(), dispatcher.EventCheckoutStarted, "chora.payments.user_subscription.checkout_started.v1"},
		{wh.AggregateTenantAddonPurchase, id6(), dispatcher.EventCheckoutStarted, "chora.payments.tenant_addon_purchase.checkout_started.v1"},
	}
	for _, tc := range cases {
		payload, topic, err := loadAndMarshal(context.Background(), deps, testEnvelope(), tc.agg, tc.id, tc.evType)
		if err != nil {
			t.Fatalf("loadAndMarshal(%s): %v", tc.agg, err)
		}
		if topic != tc.want {
			t.Errorf("topic=%q, want %q", topic, tc.want)
		}
		if len(payload) == 0 {
			t.Errorf("payload empty for %s", tc.agg)
		}
	}
}

func TestLoadAndMarshal_LookupError(t *testing.T) {
	t.Parallel()
	deps := EmitterDeps{Course: inmem.NewCoursePurchaseRepo()} // empty
	_, _, err := loadAndMarshal(context.Background(), deps, testEnvelope(), wh.AggregateCoursePurchase, "missing", dispatcher.EventCheckoutStarted)
	if err == nil || !strings.Contains(err.Error(), "course_purchase lookup") {
		t.Errorf("err=%v, want 'course_purchase lookup'", err)
	}
}

func TestLoadAndMarshal_UnknownAggregate(t *testing.T) {
	t.Parallel()
	deps := EmitterDeps{}
	_, _, err := loadAndMarshal(context.Background(), deps, testEnvelope(), "mystery", "purchase_1", dispatcher.EventCheckoutStarted)
	if err == nil || !strings.Contains(err.Error(), "unknown aggregate") {
		t.Errorf("err=%v, want 'unknown aggregate'", err)
	}
}

func id1() string { return "01970000-0000-7000-b000-000000000101" }
func id2() string { return "01970000-0000-7000-b000-000000000102" }
func id3() string { return "01970000-0000-7000-b000-000000000103" }
func id4() string { return "01970000-0000-7000-b000-000000000104" }
func id5() string { return "01970000-0000-7000-b000-000000000105" }
func id6() string { return "01970000-0000-7000-b000-000000000106" }

// -----------------------------------------------------------------------------
// Emitter — NewEmitter + Emit + EmitDispute
// -----------------------------------------------------------------------------

func TestNewEmitter_PanicsWithoutOutbox(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewEmitter with nil Outbox should panic")
		}
	}()
	NewEmitter(EmitterDeps{})
}

func TestNewEmitter_DefaultsNow(t *testing.T) {
	t.Parallel()
	e := NewEmitter(EmitterDeps{Outbox: newFakeOutboxRepo().OutboxRepo, Course: inmem.NewCoursePurchaseRepo()})
	if e.deps.Now == nil {
		t.Fatal("Now not defaulted")
	}
}

func TestEmitter_Emit_Happy(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	repo := inmem.NewCoursePurchaseRepo()
	cpAgg, _ := coursepurchase.New(
		testPurchaseID, testTenantID, testLearnerGCID, "course_0001",
		1990, "SGD", testSessionID, testCheckoutURL, now,
	)
	_ = repo.Save(context.Background(), cpAgg)

	emitter := NewEmitter(EmitterDeps{
		Course: repo,
		Outbox: newFakeOutboxRepo().OutboxRepo,
		Now:    func() time.Time { return now },
	})
	err := emitter.Emit(context.Background(), dispatcher.EmitInput{
		TenantID:       testTenantID,
		Aggregate:      wh.AggregateCoursePurchase,
		PurchaseID:     testPurchaseID,
		EventType:      dispatcher.EventCheckoutStarted,
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
}

func TestEmitter_Emit_LookupError(t *testing.T) {
	t.Parallel()
	emitter := NewEmitter(EmitterDeps{
		Course: inmem.NewCoursePurchaseRepo(), // empty → lookup fails
		Outbox: newFakeOutboxRepo().OutboxRepo,
		Now:    fixedTime,
	})
	err := emitter.Emit(context.Background(), dispatcher.EmitInput{
		TenantID:       testTenantID,
		Aggregate:      wh.AggregateCoursePurchase,
		PurchaseID:     testPurchaseID,
		EventType:      dispatcher.EventCheckoutStarted,
		IdempotencyKey: "idem-1",
	})
	if err == nil || !strings.Contains(err.Error(), "course_purchase lookup") {
		t.Errorf("err=%v, want lookup error", err)
	}
}

func TestEmitter_EmitDispute_Happy(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	repo := inmem.NewDisputeRepo()
	_ = repo.Save(context.Background(), disputeAggregate(t))

	emitter := NewEmitter(EmitterDeps{
		Dispute: repo,
		Outbox:  newFakeOutboxRepo().OutboxRepo,
		Now:     func() time.Time { return now },
	})
	err := emitter.EmitDispute(context.Background(), dispatcher.EmitDisputeInput{
		TenantID:       testTenantID,
		DisputeID:      "01970000-0000-7000-b000-000000000099",
		EventType:      dispatcher.EventDisputeRaised,
		IdempotencyKey: "idem-dispute-1",
	})
	if err != nil {
		t.Fatalf("EmitDispute: %v", err)
	}
}

func TestNewEventID_IsUUID(t *testing.T) {
	t.Parallel()
	id := newEventID()
	if len(id) != 36 {
		t.Errorf("newEventID()=%q, want 36-char UUID", id)
	}
}

// -----------------------------------------------------------------------------
// AuditOutboxAdapter
// -----------------------------------------------------------------------------

func TestAuditOutboxAdapter_InsertAudit_Happy(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	adapter := &AuditOutboxAdapter{Outbox: newFakeOutboxRepo().OutboxRepo, Now: func() time.Time { return now }}
	err := adapter.InsertAudit(context.Background(), "chora.governance.audit.refund_issued.v1", testTenantID, "audit", "purchase_1", []byte("{}"))
	if err != nil {
		t.Fatalf("InsertAudit: %v", err)
	}
}

func TestAuditOutboxAdapter_InsertAudit_NilAdapterOrOutbox(t *testing.T) {
	t.Parallel()
	var nilAdapter *AuditOutboxAdapter
	if err := nilAdapter.InsertAudit(context.Background(), "t", "tenant", "audit", "id", []byte("{}")); err == nil {
		t.Error("nil adapter should error")
	}
	adapter := &AuditOutboxAdapter{} // Outbox nil
	if err := adapter.InsertAudit(context.Background(), "t", "tenant", "audit", "id", []byte("{}")); err == nil {
		t.Error("adapter without Outbox should error")
	}
}

func TestPurchaseFilter_ToProto(t *testing.T) {
	t.Parallel()
	from := fixedTime()
	to := fixedTime().Add(time.Hour)
	f := PurchaseFilter{AggregateType: "course_purchase", State: "refunded", From: &from, To: &to}
	pf := f.toProto()
	if pf.GetAggregateType() != "course_purchase" || pf.GetState() != "refunded" {
		t.Errorf("pf agg/state lost: %v", pf)
	}
	if pf.From == nil || pf.To == nil {
		t.Errorf("From/To not set")
	}
}

// -----------------------------------------------------------------------------
// Outbox Dispatcher — NewDispatcher panics + DrainOnce/Run flows
// -----------------------------------------------------------------------------

type fakePublisher struct {
	msgID  string
	err    error
	called int
}

func (f *fakePublisher) Publish(_ context.Context, _ string, _ envelope.Envelope, _ []byte) error {
	f.called++
	return f.err
}

func TestNewDispatcher_PanicsWithoutOutbox(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewDispatcher with nil Outbox should panic")
		}
	}()
	NewDispatcher(DispatcherConfig{Publisher: &fakePublisher{}})
}

func TestNewDispatcher_PanicsWithoutPublisher(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewDispatcher with nil Publisher should panic")
		}
	}()
	NewDispatcher(DispatcherConfig{Outbox: newFakeOutboxRepo().OutboxRepo})
}

func TestNewDispatcher_Defaults(t *testing.T) {
	t.Parallel()
	d := NewDispatcher(DispatcherConfig{Outbox: newFakeOutboxRepo().OutboxRepo, Publisher: &fakePublisher{}})
	if d.cfg.BatchSize != 50 || d.cfg.PollInterval != 250*time.Millisecond || d.cfg.MaxAttempts != 5 {
		t.Errorf("defaults not applied: %+v", d.cfg)
	}
	if d.cfg.BackoffBase != 5*time.Second || d.cfg.BackoffCap != 5*time.Minute {
		t.Errorf("backoff defaults not applied")
	}
	if d.cfg.Now == nil {
		t.Errorf("Now not defaulted")
	}
}

func TestDrainOnce_CancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := NewDispatcher(DispatcherConfig{Outbox: newFakeOutboxRepo().OutboxRepo, Publisher: &fakePublisher{}})
	if _, err := d.DrainOnce(ctx); err == nil {
		t.Fatal("DrainOnce on cancelled ctx should return error")
	}
}

func TestDrainOnce_ClaimError(t *testing.T) {
	t.Parallel()
	repo := newFakeOutboxRepo()
	repo.tx.queryErr = errors.New("claim failed")
	d := NewDispatcher(DispatcherConfig{Outbox: repo.OutboxRepo, Publisher: &fakePublisher{}, Now: fixedTime})
	if _, err := d.DrainOnce(context.Background()); err == nil || err.Error() != "claim failed" {
		t.Errorf("err=%v, want claim failed", err)
	}
}

func TestDrainOnce_PublishesClaimedRows(t *testing.T) {
	t.Parallel()
	repo := newFakeOutboxRepo()
	now := fixedTime()
	repo.tx.claimRows = []func(dest ...any) error{
		scanOutboxRowValues("evt-1", "course_purchase", "purchase-1", testTenantID, "chora.payments.course_purchase.payment_captured.v1", now),
		scanOutboxRowValues("evt-2", "dispute", "dispute-1", testTenantID, "chora.payments.dispute.raised.v1", now),
	}
	pub := &fakePublisher{msgID: "msg-1"}
	d := NewDispatcher(DispatcherConfig{Outbox: repo.OutboxRepo, Publisher: pub, Now: func() time.Time { return now }})
	n, err := d.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 2 {
		t.Errorf("dispatched=%d, want 2", n)
	}
	if pub.called != 2 {
		t.Errorf("publish calls=%d, want 2", pub.called)
	}
	if len(repo.tx.q.markedDispatched) != 2 {
		t.Errorf("MarkDispatched rows=%d, want 2", len(repo.tx.q.markedDispatched))
	}
}

func TestDrainOnce_PublishErrorMarksRetry(t *testing.T) {
	t.Parallel()
	repo := newFakeOutboxRepo()
	now := fixedTime()
	repo.tx.claimRows = []func(dest ...any) error{
		scanOutboxRowValues("evt-1", "course_purchase", "purchase-1", testTenantID, "chora.payments.course_purchase.payment_captured.v1", now),
	}
	pub := &fakePublisher{err: errors.New("publish failed")}
	d := NewDispatcher(DispatcherConfig{Outbox: repo.OutboxRepo, Publisher: pub, Now: func() time.Time { return now }})
	// DrainOnce applies row-level retry semantics: publish failures are
	// recorded via MarkRetry, not propagated.
	n, err := d.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("dispatched=%d, want 0", n)
	}
	if len(repo.tx.q.markedRetry) != 1 {
		t.Errorf("MarkRetry rows=%d, want 1", len(repo.tx.q.markedRetry))
	}
}

func TestDrainOnce_ExhaustsAttempts(t *testing.T) {
	t.Parallel()
	repo := newFakeOutboxRepo()
	now := fixedTime()
	repo.tx.claimRows = []func(dest ...any) error{
		func(dest ...any) error {
			return scanOutboxRowValuesFinal(5)(dest...) // DispatchAttempts=5 == max
		},
	}
	pub := &fakePublisher{err: errors.New("publish failed")}
	d := NewDispatcher(DispatcherConfig{Outbox: repo.OutboxRepo, Publisher: pub, MaxAttempts: 5, Now: func() time.Time { return now }})
	if _, err := d.DrainOnce(context.Background()); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(repo.tx.q.markedRetry) != 1 {
		t.Errorf("MarkRetry rows=%d, want 1", len(repo.tx.q.markedRetry))
	}
}

func TestRun_ReturnsOnCancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := NewDispatcher(DispatcherConfig{Outbox: newFakeOutboxRepo().OutboxRepo, Publisher: &fakePublisher{}})
	if err := d.Run(ctx); err == nil {
		t.Fatal("Run on cancelled ctx should return error")
	}
}

func TestEmitAuditHelpers_NilOutbox(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	ctx := context.Background()
	filter := PurchaseFilter{AggregateType: "course_purchase"}
	if err := EmitTenantAdminViewedPayments(ctx, nil, testTenantID, "gcid-1", "TENANT_ADMIN", filter, now); err == nil {
		t.Error("EmitTenantAdminViewedPayments with nil out should error")
	}
	if err := EmitCrossTenantPaymentsViewed(ctx, nil, "gcid-1", "PLATFORM_OPERATOR", filter, []string{testTenantID}, now); err == nil {
		t.Error("EmitCrossTenantPaymentsViewed with nil out should error")
	}
	if err := EmitRefundIssued(ctx, nil, testTenantID, "gcid-1", "TENANT_ADMIN", testPurchaseID, "course_purchase", 100, "SGD", "reason", "re_1", now); err == nil {
		t.Error("EmitRefundIssued with nil out should error")
	}
}

func TestPurchaseFilter_ToProtoNilBounds(t *testing.T) {
	t.Parallel()
	pf := (PurchaseFilter{}).toProto()
	if pf.From != nil || pf.To != nil {
		t.Errorf("nil From/To should stay nil, got %v/%v", pf.From, pf.To)
	}
}

func TestRun_LoopsUntilCancelled(t *testing.T) {
	t.Parallel()
	repo := newFakeOutboxRepo()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	d := NewDispatcher(DispatcherConfig{
		Outbox:       repo.OutboxRepo,
		Publisher:    &fakePublisher{},
		PollInterval: time.Millisecond,
		Now:          fixedTime,
	})
	err := d.Run(ctx)
	if err == nil {
		t.Fatal("Run should return context error on cancellation")
	}
}

func TestTimestamppbOrNilFrom(t *testing.T) {
	t.Parallel()
	if timestamppbOrNilFrom(nil) != nil {
		t.Error("nil input should yield nil timestamp")
	}
	now := fixedTime()
	if ts := timestamppbOrNilFrom(&now); ts == nil || !ts.AsTime().Equal(now) {
		t.Errorf("ts=%v, want %v", ts, now)
	}
}
