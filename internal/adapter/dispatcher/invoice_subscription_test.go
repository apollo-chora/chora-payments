// invoice_subscription_test.go — dunning-path webhook handlers:
// invoice.payment_failed / invoice.payment_succeeded / the CHO-1772
// subscription_schedule.released release path.
package dispatcher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func invoiceEvent(t *testing.T, evType stripeGo.EventType, invoiceID, subscriptionID string) *stripeGo.Event {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":           invoiceID,
		"subscription": subscriptionID,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &stripeGo.Event{ID: "evt_invoice_" + invoiceID, Type: evType, Data: &stripeGo.EventData{Raw: raw}}
}

func seedAddonWithSub(t *testing.T, sd *seededDispatcher, customerID, subID string, status tap.Status) {
	t.Helper()
	now := dispatchSeedNow()
	a, err := tap.New(
		testPurchaseID, testTenantID, testLearnerGCID,
		"addon_plan_1", "knowledge_graph", "pro",
		7990, "USD", dispatchSessionID, "https://checkout.test", now,
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	if err := a.ApplyStripeSubscriptionState(customerID, subID, status, nil, nil); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	if err := sd.deps.AddonPurchase.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func TestHandleInvoicePaymentFailed_Paths(t *testing.T) {
	t.Run("nil_addon_repo_skips", func(t *testing.T) {
		d := New(Deps{}) // AddonPurchase nil
		agg, _, err := d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentFailed, "in_1", "sub_1"))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("parse_error", func(t *testing.T) {
		sd := newSeededDispatcher()
		ev := &stripeGo.Event{ID: "e", Type: stripeGo.EventTypeInvoicePaymentFailed, Data: &stripeGo.EventData{Raw: []byte("not json")}}
		if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
			t.Fatal("expected parse error")
		}
	})
	t.Run("missing_subscription_skips", func(t *testing.T) {
		sd := newSeededDispatcher()
		agg, _, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentFailed, "in_1", ""))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("unknown_sub_skips", func(t *testing.T) {
		sd := newSeededDispatcher()
		agg, _, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentFailed, "in_1", "sub_missing"))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("past_due_transition_emits", func(t *testing.T) {
		sd := newSeededDispatcher()
		seedAddonWithSub(t, sd, "cus_x", "sub_pastdue", tap.StatusActive)
		agg, pid, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentFailed, "in_1", "sub_pastdue"))
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if agg != wh.AggregateTenantAddonPurchase || pid != testPurchaseID {
			t.Errorf("agg=%s pid=%s", agg, pid)
		}
		if len(sd.em.emits) != 1 || sd.em.emits[0].EventType != EventSubscriptionPaymentFailed {
			t.Errorf("emits=%+v", sd.em.emits)
		}
	})
	t.Run("redelivery_past_due_skips_emit", func(t *testing.T) {
		sd := newSeededDispatcher()
		seedAddonWithSub(t, sd, "cus_x", "sub_pastdue2", tap.StatusPastDue)
		agg, _, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentFailed, "in_2", "sub_pastdue2"))
		if err != nil || agg != wh.AggregateTenantAddonPurchase {
			t.Errorf("agg=%s err=%v", agg, err)
		}
		if len(sd.em.emits) != 0 {
			t.Errorf("emits=%d, want 0 (redelivery)", len(sd.em.emits))
		}
	})
}

func TestHandleInvoicePaymentSucceeded_Paths(t *testing.T) {
	t.Run("nil_addon_repo_skips", func(t *testing.T) {
		d := New(Deps{})
		agg, _, err := d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentSucceeded, "in_1", "sub_1"))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("missing_subscription_skips", func(t *testing.T) {
		sd := newSeededDispatcher()
		agg, _, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentSucceeded, "in_1", ""))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("unknown_sub_skips", func(t *testing.T) {
		sd := newSeededDispatcher()
		agg, _, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentSucceeded, "in_1", "sub_missing"))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("normal_renewal_noop", func(t *testing.T) {
		sd := newSeededDispatcher()
		seedAddonWithSub(t, sd, "cus_x", "sub_active", tap.StatusActive)
		agg, _, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentSucceeded, "in_1", "sub_active"))
		if err != nil || agg != wh.AggregateTenantAddonPurchase {
			t.Errorf("agg=%s err=%v", agg, err)
		}
		if len(sd.em.emits) != 0 {
			t.Errorf("emits=%d, want 0 (no state change)", len(sd.em.emits))
		}
	})
	t.Run("past_due_recovered_emits", func(t *testing.T) {
		sd := newSeededDispatcher()
		seedAddonWithSub(t, sd, "cus_x", "sub_recover", tap.StatusPastDue)
		agg, pid, err := sd.d.Dispatch(context.Background(), invoiceEvent(t, stripeGo.EventTypeInvoicePaymentSucceeded, "in_1", "sub_recover"))
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if agg != wh.AggregateTenantAddonPurchase || pid != testPurchaseID {
			t.Errorf("agg=%s pid=%s", agg, pid)
		}
		if len(sd.em.emits) != 1 || sd.em.emits[0].EventType != EventPaymentRecovered {
			t.Errorf("emits=%+v", sd.em.emits)
		}
	})
}

func TestHandleSubscriptionScheduleReleased_Paths(t *testing.T) {
	scheduleEvent := func(id string) *stripeGo.Event {
		raw, _ := json.Marshal(map[string]string{"id": id})
		return &stripeGo.Event{ID: "evt_sched", Type: stripeGo.EventTypeSubscriptionScheduleReleased, Data: &stripeGo.EventData{Raw: raw}}
	}

	t.Run("nil_addon_repo_skips", func(t *testing.T) {
		d := New(Deps{})
		agg, _, err := d.Dispatch(context.Background(), scheduleEvent("sched_1"))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("parse_error", func(t *testing.T) {
		sd := newSeededDispatcher()
		ev := &stripeGo.Event{ID: "e", Type: stripeGo.EventTypeSubscriptionScheduleReleased, Data: &stripeGo.EventData{Raw: []byte("not json")}}
		if _, _, err := sd.d.Dispatch(context.Background(), ev); err == nil {
			t.Fatal("expected parse error")
		}
	})
	t.Run("missing_id_skips", func(t *testing.T) {
		sd := newSeededDispatcher()
		agg, _, err := sd.d.Dispatch(context.Background(), scheduleEvent(""))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("unknown_schedule_skips", func(t *testing.T) {
		sd := newSeededDispatcher()
		agg, _, err := sd.d.Dispatch(context.Background(), scheduleEvent("sched_missing"))
		if err != nil || agg != wh.AggregateUnknown {
			t.Errorf("agg=%s err=%v", agg, err)
		}
	})
	t.Run("release_promotes_tier_and_emits", func(t *testing.T) {
		sd := newSeededDispatcher()
		now := dispatchSeedNow()
		a, err := tap.New(
			testPurchaseID, testTenantID, testLearnerGCID,
			"addon_plan_1", "knowledge_graph", "pro",
			7990, "USD", dispatchSessionID, "https://checkout.test", now,
		)
		if err != nil {
			t.Fatalf("tap.New: %v", err)
		}
		if err := a.ApplySchedule("sched_release_1", "enterprise", now.Add(30*24*time.Hour)); err != nil {
			t.Fatalf("ApplySchedule: %v", err)
		}
		if err := sd.deps.AddonPurchase.Save(context.Background(), a); err != nil {
			t.Fatalf("Save: %v", err)
		}
		agg, pid, err := sd.d.Dispatch(context.Background(), scheduleEvent("sched_release_1"))
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if agg != wh.AggregateTenantAddonPurchase || pid != testPurchaseID {
			t.Errorf("agg=%s pid=%s", agg, pid)
		}
		got, err := sd.deps.AddonPurchase.GetByID(context.Background(), testTenantID, testPurchaseID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if got.TierCode != "enterprise" || got.HasPendingSchedule() {
			t.Errorf("release not applied: tier=%s scheduled=%s", got.TierCode, got.StripeSubscriptionScheduleID)
		}
		if len(sd.em.emits) != 1 || sd.em.emits[0].EventType != EventSubscriptionScheduleReleased {
			t.Errorf("emits=%+v", sd.em.emits)
		}
	})
}

// compile-time silence for shared import.
var _ = shared.StateCheckoutStarted
