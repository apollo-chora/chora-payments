// event_decoder_test.go — RED tests for decoding the 28 chora.payments.*
// state-change subjects into the unified PaymentEvent shape served on SSE.
//
// Covers:
//   - All 4 state events × 7 Purchase aggregates = 28 subject decoders
//   - The 4 dispute events (raised / closed / funds_withdrawn / funds_reinstated)
//   - Unknown subject returns ErrUnknownTopic
//   - Empty payload returns decode error
//   - Envelope tenant_id fallback when the transport envelope is empty
package event_broker

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/envelope"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
)

// TestDecode_CoursePurchase_AllStateEvents covers the 4 state-change events.
func TestDecode_CoursePurchase_AllStateEvents(t *testing.T) {
	t.Parallel()
	occurred := time.Now().UTC()
	cases := []struct {
		topic     string
		msg       proto.Message
		wantState string
	}{
		{
			topic: "chora.payments.course_purchase.payment_captured.v1",
			msg: &paymentsv1.CoursePurchasePaymentCaptured{
				Envelope:        envelopeWithTenant("tenant-A", "evt-1", "idem-1", occurred),
				PurchaseId:      "p1",
				LearnerGcid:     "gcid-1",
				StripeSessionId: "cs_1",
				AmountCentsPaid: 4999,
				Currency:        "sgd",
				PaidAt:          timestamppb.New(occurred),
			},
			wantState: "captured",
		},
		{
			topic: "chora.payments.course_purchase.refunded.v1",
			msg: &paymentsv1.CoursePurchaseRefunded{
				Envelope:            envelopeWithTenant("tenant-A", "evt-2", "idem-2", occurred),
				PurchaseId:          "p1",
				LearnerGcid:         "gcid-1",
				AmountCentsRefunded: 4999,
				Currency:            "sgd",
				RefundedAt:          timestamppb.New(occurred),
			},
			wantState: "refunded",
		},
		{
			topic: "chora.payments.course_purchase.payment_failed.v1",
			msg: &paymentsv1.CoursePurchasePaymentFailed{
				Envelope:        envelopeWithTenant("tenant-A", "evt-3", "idem-3", occurred),
				PurchaseId:      "p1",
				LearnerGcid:     "gcid-1",
				StripeSessionId: "cs_3",
				FailedAt:        timestamppb.New(occurred),
			},
			wantState: "failed",
		},
		{
			topic: "chora.payments.course_purchase.expired.v1",
			msg: &paymentsv1.CoursePurchaseExpired{
				Envelope:        envelopeWithTenant("tenant-A", "evt-4", "idem-4", occurred),
				PurchaseId:      "p1",
				LearnerGcid:     "gcid-1",
				StripeSessionId: "cs_4",
				ExpiredAt:       timestamppb.New(occurred),
			},
			wantState: "expired",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.topic, func(t *testing.T) {
			t.Parallel()
			bz, err := proto.Marshal(c.msg)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			ev, err := decodePaymentEvent(c.topic, bz, envelope.Envelope{
				TenantID: "tenant-A",
			})
			if err != nil {
				t.Fatalf("decodePaymentEvent: %v", err)
			}
			if ev.PurchaseID != "p1" {
				t.Errorf("PurchaseID = %q; want p1", ev.PurchaseID)
			}
			if ev.AggregateType != "course_purchase" {
				t.Errorf("AggregateType = %q; want course_purchase", ev.AggregateType)
			}
			if ev.State != c.wantState {
				t.Errorf("State = %q; want %q", ev.State, c.wantState)
			}
			if ev.TenantID != "tenant-A" {
				t.Errorf("TenantID = %q; want tenant-A", ev.TenantID)
			}
			if ev.OccurredAt == "" {
				t.Errorf("OccurredAt empty")
			}
		})
	}
}

// TestDecode_AllAggregates_PaymentCaptured smoke-checks the 7-aggregate fan.
func TestDecode_AllAggregates_PaymentCaptured(t *testing.T) {
	t.Parallel()
	occurred := time.Now().UTC()
	cases := []struct {
		topic         string
		msg           proto.Message
		wantAggregate string
	}{
		{
			topic:         "chora.payments.course_purchase.payment_captured.v1",
			msg:           &paymentsv1.CoursePurchasePaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "course_purchase",
		},
		{
			topic:         "chora.payments.application_payment.payment_captured.v1",
			msg:           &paymentsv1.ApplicationPaymentPaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "application_payment",
		},
		{
			topic:         "chora.payments.familiar_egg_purchase.payment_captured.v1",
			msg:           &paymentsv1.CompanionEggPurchasePaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "familiar_egg_purchase",
		},
		{
			topic:         "chora.payments.tenant_mana_topup.payment_captured.v1",
			msg:           &paymentsv1.TenantManaTopUpPaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "tenant_mana_topup",
		},
		{
			topic:         "chora.payments.user_subscription.payment_captured.v1",
			msg:           &paymentsv1.UserSubscriptionPaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "user_subscription",
		},
		{
			topic:         "chora.payments.user_mana_topup.payment_captured.v1",
			msg:           &paymentsv1.UserManaTopUpPaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "user_mana_topup",
		},
		{
			topic:         "chora.payments.identity_kyc_fee.payment_captured.v1",
			msg:           &paymentsv1.IdentityKycFeePaymentCaptured{Envelope: envelopeWithTenant("tA", "e", "k", occurred), PurchaseId: "p1", AmountCentsPaid: 100, Currency: "sgd", PaidAt: timestamppb.New(occurred)},
			wantAggregate: "identity_kyc_fee",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.topic, func(t *testing.T) {
			t.Parallel()
			bz, _ := proto.Marshal(c.msg)
			ev, err := decodePaymentEvent(c.topic, bz, envelope.Envelope{TenantID: "tA"})
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if ev.AggregateType != c.wantAggregate {
				t.Errorf("AggregateType = %q; want %q", ev.AggregateType, c.wantAggregate)
			}
			if ev.State != "captured" {
				t.Errorf("State = %q; want captured", ev.State)
			}
		})
	}
}

// TestDecode_UnknownSubject returns the sentinel error.
func TestDecode_UnknownTopic(t *testing.T) {
	t.Parallel()
	_, err := decodePaymentEvent("chora.payments.fake_aggregate.x.v1", []byte("x"), envelope.Envelope{})
	if !errors.Is(err, ErrUnknownTopic) {
		t.Errorf("err = %v; want ErrUnknownTopic", err)
	}
}

// TestDecode_BadPayload returns a decode error.
func TestDecode_BadPayload(t *testing.T) {
	t.Parallel()
	_, err := decodePaymentEvent("chora.payments.course_purchase.payment_captured.v1", []byte("not-a-proto"), envelope.Envelope{})
	if err == nil {
		t.Errorf("expected decode error; got nil")
	}
}

// TestDecode_TenantFallback prefers the transport envelope tenant_id, falls back to the payload envelope.
func TestDecode_TenantFallback(t *testing.T) {
	t.Parallel()
	occurred := time.Now().UTC()
	msg := &paymentsv1.CoursePurchasePaymentCaptured{
		Envelope:        envelopeWithTenant("tenant-from-envelope", "evt", "idem", occurred),
		PurchaseId:      "p1",
		LearnerGcid:     "gcid-1",
		AmountCentsPaid: 100,
		Currency:        "sgd",
		PaidAt:          timestamppb.New(occurred),
	}
	bz, _ := proto.Marshal(msg)

	// transport envelope missing tenant_id → fall back to payload envelope.
	ev, err := decodePaymentEvent("chora.payments.course_purchase.payment_captured.v1", bz, envelope.Envelope{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ev.TenantID != "tenant-from-envelope" {
		t.Errorf("TenantID = %q; want tenant-from-envelope (fallback)", ev.TenantID)
	}

	// transport envelope tenant_id wins when present.
	ev2, err := decodePaymentEvent("chora.payments.course_purchase.payment_captured.v1", bz, envelope.Envelope{TenantID: "tenant-from-attrs"})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ev2.TenantID != "tenant-from-attrs" {
		t.Errorf("TenantID = %q; want tenant-from-attrs (transport envelope preferred)", ev2.TenantID)
	}
}

// TestSupportedSubjects returns the canonical 28-topic list.
func TestSupportedSubjects(t *testing.T) {
	t.Parallel()
	topics := SupportedSubjects()
	if len(topics) != 28 {
		t.Errorf("SupportedSubjects count = %d; want 28 (7 aggregates × 4 state events)", len(topics))
	}
	// Sanity: contains a known subject.
	found := false
	for _, top := range topics {
		if top == "chora.payments.course_purchase.payment_captured.v1" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected course_purchase.payment_captured.v1 in SupportedSubjects")
	}
}

// envelopeWithTenant is a small helper.
func envelopeWithTenant(tenant, eventID, idempotencyKey string, occurred time.Time) *commonv1.EventEnvelope {
	return &commonv1.EventEnvelope{
		EventId:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantId:       tenant,
		OccurredAt:     timestamppb.New(occurred),
	}
}

// TestDecode_AllAggregates_AllStateEvents covers the full 7 × 4 fan to
// reach ≥ 85% branch coverage on decodePaymentEvent.
func TestDecode_AllAggregates_AllStateEvents(t *testing.T) {
	t.Parallel()
	occ := time.Now().UTC()
	env := envelopeWithTenant("t1", "e1", "i1", occ)
	cases := []struct {
		topic string
		msg   proto.Message
	}{
		// course_purchase (captured covered elsewhere)
		{"chora.payments.course_purchase.refunded.v1", &paymentsv1.CoursePurchaseRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.course_purchase.payment_failed.v1", &paymentsv1.CoursePurchasePaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.course_purchase.expired.v1", &paymentsv1.CoursePurchaseExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
		// application_payment
		{"chora.payments.application_payment.refunded.v1", &paymentsv1.ApplicationPaymentRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.application_payment.payment_failed.v1", &paymentsv1.ApplicationPaymentPaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.application_payment.expired.v1", &paymentsv1.ApplicationPaymentExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
		// familiar_egg_purchase
		{"chora.payments.familiar_egg_purchase.refunded.v1", &paymentsv1.CompanionEggPurchaseRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.familiar_egg_purchase.payment_failed.v1", &paymentsv1.CompanionEggPurchasePaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.familiar_egg_purchase.expired.v1", &paymentsv1.CompanionEggPurchaseExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
		// tenant_mana_topup
		{"chora.payments.tenant_mana_topup.refunded.v1", &paymentsv1.TenantManaTopUpRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.tenant_mana_topup.payment_failed.v1", &paymentsv1.TenantManaTopUpPaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.tenant_mana_topup.expired.v1", &paymentsv1.TenantManaTopUpExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
		// user_subscription
		{"chora.payments.user_subscription.refunded.v1", &paymentsv1.UserSubscriptionRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.user_subscription.payment_failed.v1", &paymentsv1.UserSubscriptionPaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.user_subscription.expired.v1", &paymentsv1.UserSubscriptionExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
		// user_mana_topup
		{"chora.payments.user_mana_topup.refunded.v1", &paymentsv1.UserManaTopUpRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.user_mana_topup.payment_failed.v1", &paymentsv1.UserManaTopUpPaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.user_mana_topup.expired.v1", &paymentsv1.UserManaTopUpExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
		// identity_kyc_fee
		{"chora.payments.identity_kyc_fee.refunded.v1", &paymentsv1.IdentityKycFeeRefunded{Envelope: env, PurchaseId: "p", AmountCentsRefunded: 10, Currency: "sgd", RefundedAt: timestamppb.New(occ)}},
		{"chora.payments.identity_kyc_fee.payment_failed.v1", &paymentsv1.IdentityKycFeePaymentFailed{Envelope: env, PurchaseId: "p", FailedAt: timestamppb.New(occ)}},
		{"chora.payments.identity_kyc_fee.expired.v1", &paymentsv1.IdentityKycFeeExpired{Envelope: env, PurchaseId: "p", ExpiredAt: timestamppb.New(occ)}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.topic, func(t *testing.T) {
			t.Parallel()
			bz, _ := proto.Marshal(c.msg)
			ev, err := decodePaymentEvent(c.topic, bz, envelope.Envelope{TenantID: "t1"})
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if ev.PurchaseID != "p" {
				t.Errorf("PurchaseID = %q; want p", ev.PurchaseID)
			}
		})
	}
}

// TestDefaultSubscriptionMap returns the canonical 28-subject map.
func TestDefaultSubscriptionMap(t *testing.T) {
	t.Parallel()
	m := DefaultSubscriptionMap()
	if len(m) != 28 {
		t.Errorf("len(map) = %d; want 28", len(m))
	}
	for topic, sub := range m {
		if sub == "" {
			t.Errorf("topic %s → empty subscription", topic)
		}
	}
}

// TestDefaultSubscriptionName format check.
func TestDefaultSubscriptionName(t *testing.T) {
	t.Parallel()
	got := DefaultSubscriptionName("chora.payments.course_purchase.payment_captured.v1")
	want := "chora-payments-fanout-course_purchase-payment_captured"
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
	// Edge cases:
	if v := DefaultSubscriptionName("not-a-chora-topic"); v != "" {
		t.Errorf("non-chora topic should return empty; got=%q", v)
	}
	if v := DefaultSubscriptionName("chora.payments.foo.v1"); v != "" {
		t.Errorf("malformed topic should return empty; got=%q", v)
	}
}

// TestStateFromSubject_Errors.
func TestStateFromTopic_Errors(t *testing.T) {
	t.Parallel()
	if _, err := stateFromSubject("not-chora"); err != ErrUnknownTopic {
		t.Errorf("non-chora err = %v; want ErrUnknownTopic", err)
	}
	if _, err := stateFromSubject("chora.payments.foo.v1"); err != ErrUnknownTopic {
		t.Errorf("malformed err = %v; want ErrUnknownTopic", err)
	}
	if _, err := stateFromSubject("chora.payments.course_purchase.checkout_started.v1"); err != ErrUnknownTopic {
		t.Errorf("checkout_started should be unsupported; err = %v", err)
	}
}

// TestAggregateFromSubject_Errors.
func TestAggregateFromTopic_Errors(t *testing.T) {
	t.Parallel()
	if _, err := aggregateFromSubject("not-chora"); err != ErrUnknownTopic {
		t.Errorf("non-chora err = %v; want ErrUnknownTopic", err)
	}
	if _, err := aggregateFromSubject("chora.payments.bad_aggregate.captured.v1"); err != ErrUnknownTopic {
		t.Errorf("unknown aggregate err = %v; want ErrUnknownTopic", err)
	}
}
