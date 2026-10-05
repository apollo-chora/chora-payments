// dispatch_handlers_test.go — coverage for the 4 remaining webhook
// handlers: checkout.session.expired, payment_intent.payment_failed,
// charge.refunded, checkout.session.async_payment_failed — across all 8
// Purchase aggregates + their error/ack-only branches.
package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	inmem "github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	"github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	"github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	"github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const dispatchSessionID = "cs_dispatch_test"

// allAggregates enumerates the 8 Purchase aggregate types in dispatch order.
func allAggregates() []wh.AggregateType {
	return []wh.AggregateType{
		wh.AggregateCoursePurchase,
		wh.AggregateApplicationPayment,
		wh.AggregateFamiliarEggPurchase,
		wh.AggregateTenantManaTopUp,
		wh.AggregateUserSubscription,
		wh.AggregateUserManaTopUp,
		wh.AggregateIdentityKycFee,
		wh.AggregateTenantAddonPurchase,
	}
}

// seededDispatcher wires fresh inmem repos + recording emitter for each test.
type seededDispatcher struct {
	d    *Dispatcher
	em   *recordingEmitter
	deps Deps
}

func newSeededDispatcher() *seededDispatcher {
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	em := &recordingEmitter{}
	deps := Deps{
		Course:         inmem.NewCoursePurchaseRepo(),
		Application:    inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:    inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:      inmem.NewTenantManaTopUpRepo(),
		Subscription:   inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:  inmem.NewUserManaTopUpRepo(),
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(),
		AddonPurchase:  inmem.NewTenantAddonPurchaseRepo(),
		Dispute:        inmem.NewDisputeRepo(),
		Outbox:         em,
		Now:            func() time.Time { return now },
	}
	return &seededDispatcher{d: New(deps), em: em, deps: deps}
}

func dispatchSeedNow() time.Time {
	return time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
}

// seed saves a checkout_started aggregate of the given type into its repo.
func (s *seededDispatcher) seed(t *testing.T, agg wh.AggregateType, purchaseID, sessionID string) {
	t.Helper()
	ctx := context.Background()
	now := dispatchSeedNow()
	switch agg {
	case wh.AggregateCoursePurchase:
		a, err := coursepurchase.New(purchaseID, testTenantID, testLearnerGCID, "course_1", 1990, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("coursepurchase.New: %v", err)
		}
		if err := s.deps.Course.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateApplicationPayment:
		a, err := application_payment.New(purchaseID, testTenantID, testLearnerGCID, "app_1", "course_1", 1990, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("apppay.New: %v", err)
		}
		if err := s.deps.Application.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateFamiliarEggPurchase:
		a, err := familiar_egg_purchase.New(purchaseID, testTenantID, testLearnerGCID, "egg.standard.v1", "focal", 1990, "USD", sessionID, "https://checkout.test", nil, nil, now)
		if err != nil {
			t.Fatalf("egg.New: %v", err)
		}
		if err := s.deps.FamiliarEgg.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateTenantManaTopUp:
		a, err := tenant_mana_topup.New(purchaseID, testTenantID, testLearnerGCID, "mana.topup.v1", 5000, 1990, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("mana.New: %v", err)
		}
		if err := s.deps.ManaTopUp.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateUserSubscription:
		a, err := user_subscription.New(purchaseID, testTenantID, testLearnerGCID, "mana.sub.v1", user_subscription.BillingMonthly, 1990, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("sub.New: %v", err)
		}
		if err := s.deps.Subscription.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateUserManaTopUp:
		a, err := user_mana_topup.New(purchaseID, testTenantID, testLearnerGCID, "mana.user.v1", 2500, 1990, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("umt.New: %v", err)
		}
		if err := s.deps.UserManaTopUp.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateIdentityKycFee:
		a, err := identity_kyc_fee.New(purchaseID, testTenantID, testLearnerGCID, "manual_id_doc", 999, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("kyc.New: %v", err)
		}
		if err := s.deps.IdentityKycFee.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	case wh.AggregateTenantAddonPurchase:
		a, err := tap.New(purchaseID, testTenantID, testLearnerGCID, "addon_plan_1", "knowledge_graph", "pro", 7990, "USD", sessionID, "https://checkout.test", now)
		if err != nil {
			t.Fatalf("tap.New: %v", err)
		}
		if err := s.deps.AddonPurchase.Save(ctx, a); err != nil {
			t.Fatalf("save: %v", err)
		}
	default:
		t.Fatalf("unhandled aggregate %q", agg)
	}
}

// markCaptured transitions the seeded aggregate to payment_captured (the
// pre-condition for refund webhooks).
func (s *seededDispatcher) markCaptured(t *testing.T, agg wh.AggregateType, purchaseID string) {
	t.Helper()
	ctx := context.Background()
	now := dispatchSeedNow()
	switch agg {
	case wh.AggregateCoursePurchase:
		p, _ := s.deps.Course.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.Course.Save(ctx, p)
	case wh.AggregateApplicationPayment:
		p, _ := s.deps.Application.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.Application.Save(ctx, p)
	case wh.AggregateFamiliarEggPurchase:
		p, _ := s.deps.FamiliarEgg.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.FamiliarEgg.Save(ctx, p)
	case wh.AggregateTenantManaTopUp:
		p, _ := s.deps.ManaTopUp.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.ManaTopUp.Save(ctx, p)
	case wh.AggregateUserSubscription:
		p, _ := s.deps.Subscription.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.Subscription.Save(ctx, p)
	case wh.AggregateUserManaTopUp:
		p, _ := s.deps.UserManaTopUp.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.UserManaTopUp.Save(ctx, p)
	case wh.AggregateIdentityKycFee:
		p, _ := s.deps.IdentityKycFee.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.IdentityKycFee.Save(ctx, p)
	case wh.AggregateTenantAddonPurchase:
		p, _ := s.deps.AddonPurchase.GetByID(ctx, testTenantID, purchaseID)
		_ = p.MarkPaymentCaptured("pi_cap", "ch_cap", 1990, now)
		_ = s.deps.AddonPurchase.Save(ctx, p)
	default:
		t.Fatalf("unhandled aggregate %q", agg)
	}
}

// stateOf reads the aggregate's payment-lifecycle state back from its repo.
func (s *seededDispatcher) stateOf(t *testing.T, agg wh.AggregateType, purchaseID string) shared.State {
	t.Helper()
	ctx := context.Background()
	switch agg {
	case wh.AggregateCoursePurchase:
		p, err := s.deps.Course.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateApplicationPayment:
		p, err := s.deps.Application.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateFamiliarEggPurchase:
		p, err := s.deps.FamiliarEgg.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateTenantManaTopUp:
		p, err := s.deps.ManaTopUp.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateUserSubscription:
		p, err := s.deps.Subscription.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateUserManaTopUp:
		p, err := s.deps.UserManaTopUp.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateIdentityKycFee:
		p, err := s.deps.IdentityKycFee.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	case wh.AggregateTenantAddonPurchase:
		p, err := s.deps.AddonPurchase.GetByID(ctx, testTenantID, purchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		return p.State
	default:
		t.Fatalf("unhandled aggregate %q", agg)
		return ""
	}
}

func dispatchMetadata(agg wh.AggregateType) map[string]string {
	return map[string]string{
		"purchase_id":   testPurchaseID,
		"purchase_type": string(agg),
		"tenant_id":     testTenantID,
	}
}

// --- event builders ---------------------------------------------------------

func sessionEvent(t *testing.T, evType stripeGo.EventType, sessID string, metadata map[string]string) *stripeGo.Event {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"id": sessID, "metadata": metadata})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &stripeGo.Event{ID: "evt_session", Type: evType, Data: &stripeGo.EventData{Raw: raw}}
}

func paymentIntentEvent(t *testing.T, piID string, metadata map[string]string) *stripeGo.Event {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":       piID,
		"metadata": metadata,
		"last_payment_error": map[string]any{
			"code":    "card_declined",
			"message": "Your card was declined.",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &stripeGo.Event{ID: "evt_pi", Type: stripeGo.EventTypePaymentIntentPaymentFailed, Data: &stripeGo.EventData{Raw: raw}}
}

func chargeEvent(t *testing.T, chID string, metadata map[string]string, withRefunds bool) *stripeGo.Event {
	t.Helper()
	charge := map[string]any{
		"id":              chID,
		"metadata":        metadata,
		"amount_refunded": int64(500),
	}
	if withRefunds {
		charge["refunds"] = map[string]any{
			"data": []map[string]any{{"id": "re_dispatch", "amount": int64(500)}},
		}
	}
	raw, err := json.Marshal(charge)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &stripeGo.Event{ID: "evt_charge", Type: stripeGo.EventTypeChargeRefunded, Data: &stripeGo.EventData{Raw: raw}}
}

// --- checkout.session.expired ------------------------------------------------

func TestDispatch_CheckoutSessionExpired_AllAggregates(t *testing.T) {
	for _, agg := range allAggregates() {
		t.Run(string(agg), func(t *testing.T) {
			sd := newSeededDispatcher()
			sd.seed(t, agg, testPurchaseID, dispatchSessionID)
			ev := sessionEvent(t, stripeGo.EventTypeCheckoutSessionExpired, dispatchSessionID, dispatchMetadata(agg))

			gotAgg, pid, err := sd.d.Dispatch(context.Background(), ev)
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if gotAgg != agg || pid != testPurchaseID {
				t.Errorf("agg=%s pid=%s, want %s/%s", gotAgg, pid, agg, testPurchaseID)
			}
			if st := sd.stateOf(t, agg, testPurchaseID); st != shared.StateExpired {
				t.Errorf("state=%s, want expired", st)
			}
			if len(sd.em.emits) != 1 {
				t.Fatalf("emits=%d, want 1", len(sd.em.emits))
			}
			if sd.em.emits[0].EventType != EventExpired {
				t.Errorf("emit event=%s, want expired", sd.em.emits[0].EventType)
			}
			if sd.em.emits[0].TenantID != testTenantID {
				t.Errorf("emit tenant=%s", sd.em.emits[0].TenantID)
			}
		})
	}
}

func TestDispatch_CheckoutSessionExpired_MissingTenantAcks(t *testing.T) {
	sd := newSeededDispatcher()
	md := dispatchMetadata(wh.AggregateCoursePurchase)
	delete(md, "tenant_id")
	ev := sessionEvent(t, stripeGo.EventTypeCheckoutSessionExpired, dispatchSessionID, md)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_CheckoutSessionExpired_NoMetadataErrors(t *testing.T) {
	sd := newSeededDispatcher()
	ev := sessionEvent(t, stripeGo.EventTypeCheckoutSessionExpired, dispatchSessionID, nil)
	if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
		t.Fatal("expected resolve error (no metadata propagates)")
	}
}

func TestDispatch_CheckoutSessionExpired_ParseError(t *testing.T) {
	sd := newSeededDispatcher()
	ev := &stripeGo.Event{ID: "evt_bad", Type: stripeGo.EventTypeCheckoutSessionExpired, Data: &stripeGo.EventData{Raw: []byte("not json")}}
	if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
		t.Fatal("expected parse error")
	}
}

// --- payment_intent.payment_failed ------------------------------------------

func TestDispatch_PaymentIntentFailed_AllAggregates(t *testing.T) {
	for _, agg := range allAggregates() {
		t.Run(string(agg), func(t *testing.T) {
			sd := newSeededDispatcher()
			sd.seed(t, agg, testPurchaseID, dispatchSessionID)
			ev := paymentIntentEvent(t, "pi_dispatch", dispatchMetadata(agg))

			gotAgg, pid, err := sd.d.Dispatch(context.Background(), ev)
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if gotAgg != agg || pid != testPurchaseID {
				t.Errorf("agg=%s pid=%s, want %s/%s", gotAgg, pid, agg, testPurchaseID)
			}
			if st := sd.stateOf(t, agg, testPurchaseID); st != shared.StatePaymentFailed {
				t.Errorf("state=%s, want payment_failed", st)
			}
			if len(sd.em.emits) != 1 || sd.em.emits[0].EventType != EventPaymentFailed {
				t.Errorf("emits=%+v", sd.em.emits)
			}
		})
	}
}

func TestDispatch_PaymentIntentFailed_NoMetadataAcks(t *testing.T) {
	sd := newSeededDispatcher()
	ev := paymentIntentEvent(t, "pi_dispatch", nil)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_PaymentIntentFailed_MissingTenantAcks(t *testing.T) {
	sd := newSeededDispatcher()
	md := dispatchMetadata(wh.AggregateCoursePurchase)
	delete(md, "tenant_id")
	ev := paymentIntentEvent(t, "pi_dispatch", md)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_PaymentIntentFailed_ParseError(t *testing.T) {
	sd := newSeededDispatcher()
	ev := &stripeGo.Event{ID: "evt_bad", Type: stripeGo.EventTypePaymentIntentPaymentFailed, Data: &stripeGo.EventData{Raw: []byte("not json")}}
	if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
		t.Fatal("expected parse error")
	}
}

// --- charge.refunded ---------------------------------------------------------

func TestDispatch_ChargeRefunded_AllAggregates(t *testing.T) {
	for _, agg := range allAggregates() {
		t.Run(string(agg), func(t *testing.T) {
			sd := newSeededDispatcher()
			sd.seed(t, agg, testPurchaseID, dispatchSessionID)
			sd.markCaptured(t, agg, testPurchaseID)
			// With refunds + without refunds (fallback to amount_refunded).
			for _, withRefunds := range []bool{true, false} {
				ev := chargeEvent(t, "ch_dispatch", dispatchMetadata(agg), withRefunds)
				gotAgg, pid, err := sd.d.Dispatch(context.Background(), ev)
				if err != nil {
					t.Fatalf("Dispatch: %v", err)
				}
				if gotAgg != agg || pid != testPurchaseID {
					t.Errorf("agg=%s pid=%s, want %s/%s", gotAgg, pid, agg, testPurchaseID)
				}
				if st := sd.stateOf(t, agg, testPurchaseID); st != shared.StateRefunded {
					t.Errorf("state=%s, want refunded", st)
				}
				if len(sd.em.emits) != 1 || sd.em.emits[0].EventType != EventRefunded {
					t.Errorf("emits=%+v", sd.em.emits)
				}
				sd.em.emits = nil
			}
		})
	}
}

func TestDispatch_ChargeRefunded_NoMetadataAcks(t *testing.T) {
	sd := newSeededDispatcher()
	ev := chargeEvent(t, "ch_dispatch", nil, true)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_ChargeRefunded_MissingTenantAcks(t *testing.T) {
	sd := newSeededDispatcher()
	md := dispatchMetadata(wh.AggregateCoursePurchase)
	delete(md, "tenant_id")
	ev := chargeEvent(t, "ch_dispatch", md, true)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_ChargeRefunded_ParseError(t *testing.T) {
	sd := newSeededDispatcher()
	ev := &stripeGo.Event{ID: "evt_bad", Type: stripeGo.EventTypeChargeRefunded, Data: &stripeGo.EventData{Raw: []byte("not json")}}
	if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
		t.Fatal("expected parse error")
	}
}

// --- checkout.session.async_payment_failed ----------------------------------

func TestDispatch_AsyncPaymentFailed_AllAggregates(t *testing.T) {
	for _, agg := range allAggregates() {
		t.Run(string(agg), func(t *testing.T) {
			sd := newSeededDispatcher()
			sd.seed(t, agg, testPurchaseID, dispatchSessionID)
			ev := sessionEvent(t, stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed, dispatchSessionID, dispatchMetadata(agg))

			gotAgg, pid, err := sd.d.Dispatch(context.Background(), ev)
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if gotAgg != agg || pid != testPurchaseID {
				t.Errorf("agg=%s pid=%s, want %s/%s", gotAgg, pid, agg, testPurchaseID)
			}
			if st := sd.stateOf(t, agg, testPurchaseID); st != shared.StatePaymentFailed {
				t.Errorf("state=%s, want payment_failed", st)
			}
			if len(sd.em.emits) != 1 || sd.em.emits[0].EventType != EventPaymentFailed {
				t.Errorf("emits=%+v", sd.em.emits)
			}
		})
	}
}

func TestDispatch_AsyncPaymentFailed_NoMetadataAcks(t *testing.T) {
	sd := newSeededDispatcher()
	ev := sessionEvent(t, stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed, dispatchSessionID, nil)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_AsyncPaymentFailed_MissingTenantAcks(t *testing.T) {
	sd := newSeededDispatcher()
	md := dispatchMetadata(wh.AggregateCoursePurchase)
	delete(md, "tenant_id")
	ev := sessionEvent(t, stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed, dispatchSessionID, md)
	agg, _, err := sd.d.Dispatch(context.Background(), ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Errorf("agg=%s, want unknown (ack only)", agg)
	}
}

func TestDispatch_AsyncPaymentFailed_ParseError(t *testing.T) {
	sd := newSeededDispatcher()
	ev := &stripeGo.Event{ID: "evt_bad", Type: stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed, Data: &stripeGo.EventData{Raw: []byte("not json")}}
	if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
		t.Fatal("expected parse error")
	}
}

// --- New defaults + NoOpEmitter ----------------------------------------------

func TestNew_DefaultsOutboxAndNow(t *testing.T) {
	d := New(Deps{})
	if d.deps.Outbox == nil {
		t.Fatal("Outbox not defaulted")
	}
	if d.deps.Now == nil {
		t.Fatal("Now not defaulted")
	}
	if _, ok := d.deps.Outbox.(NoOpEmitter); !ok {
		t.Errorf("default Outbox=%T, want NoOpEmitter", d.deps.Outbox)
	}
}

func TestNoOpEmitter_Logs(t *testing.T) {
	em := NoOpEmitter{}
	if err := em.Emit(context.Background(), EmitInput{TenantID: "t", Aggregate: wh.AggregateCoursePurchase, PurchaseID: "p", EventType: EventPaymentCaptured, IdempotencyKey: "k"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := em.EmitDispute(context.Background(), EmitDisputeInput{TenantID: "t", DisputeID: "d", EventType: EventDisputeRaised, IdempotencyKey: "k"}); err != nil {
		t.Fatalf("EmitDispute: %v", err)
	}
}

func TestNew_RespectsInjectedOutbox(t *testing.T) {
	em := &recordingEmitter{}
	d := New(Deps{Outbox: em, Now: dispatchSeedNow})
	if d.deps.Outbox != em {
		t.Error("injected Outbox not preserved")
	}
	if err := d.emit(context.Background(), testTenantID, wh.AggregateCoursePurchase, testPurchaseID, EventExpired, "evt_emit"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if len(em.emits) != 1 || em.emits[0].EventType != EventExpired {
		t.Errorf("emits=%+v", em.emits)
	}
	if em.emits[0].IdempotencyKey != outboxIdempotencyKey(wh.AggregateCoursePurchase, EventExpired, testPurchaseID, "evt_emit") {
		t.Errorf("idempotency key=%s", em.emits[0].IdempotencyKey)
	}
}

func TestEmit_PropagatesEmitterError(t *testing.T) {
	em := &failingEmitter{err: errors.New("outbox insert failed")}
	d := New(Deps{Outbox: em})
	err := d.emit(context.Background(), testTenantID, wh.AggregateCoursePurchase, testPurchaseID, EventExpired, "evt_emit")
	if err == nil {
		t.Fatal("expected emit error to propagate")
	}
}

func TestResolveTenantFromParent_ReturnsEmpty(t *testing.T) {
	sd := newSeededDispatcher()
	if got := sd.d.resolveTenantFromParent(context.Background(), wh.AggregateCoursePurchase, testPurchaseID); got != "" {
		t.Errorf("resolveTenantFromParent=%q, want empty", got)
	}
	if got := sd.d.resolveTenantFromParent(context.Background(), wh.AggregateTenantManaTopUp, testPurchaseID); got != "" {
		t.Errorf("resolveTenantFromParent(mana)=%q, want empty", got)
	}
}

func TestLatestRefund_FallbackToAmountRefunded(t *testing.T) {
	ch := &stripeGo.Charge{ID: "ch_1", AmountRefunded: 700}
	id, amount := latestRefund(ch)
	if id != "" || amount != 700 {
		t.Errorf("no-refunds charge: id=%q amount=%d, want /700", id, amount)
	}
	ch.Refunds = &stripeGo.RefundList{Data: []*stripeGo.Refund{{ID: "re_2", Amount: 200}}}
	id, amount = latestRefund(ch)
	if id != "re_2" || amount != 200 {
		t.Errorf("with-refunds charge: id=%q amount=%d, want re_2/200", id, amount)
	}
}

func TestStripeRawIDValue(t *testing.T) {
	if got := stripeRawIDValue(nil); got != "" {
		t.Errorf("nil raw=%q", got)
	}
	if got := stripeRawIDValue([]byte("null")); got != "" {
		t.Errorf("null raw=%q", got)
	}
	if got := stripeRawIDValue([]byte(`"cus_123"`)); got != "cus_123" {
		t.Errorf("string raw=%q", got)
	}
	if got := stripeRawIDValue([]byte(`{"id":"cus_expanded"}`)); got != "cus_expanded" {
		t.Errorf("object raw=%q", got)
	}
	if got := stripeRawIDValue([]byte(`42`)); got != "" {
		t.Errorf("unparseable raw=%q", got)
	}
}
