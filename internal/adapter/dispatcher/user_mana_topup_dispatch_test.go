package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	"github.com/apollo-chora/chora-common/tracing"
	inmem "github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// tenantCapturingUMTRepo wraps the inmem UserManaTopUp repo and records the
// tenant_id present on ctx at GetByStripeSessionID time. The WS-2 mana-credit
// bug was: the dispatcher did not stamp tenant_id onto ctx before the
// session-id lookup (for the expired/async paths), so the pg repo's
// rls.ApplySession read the pooled connection's empty-string GUC reset-value
// and 22P02-failed. This fake proves the dispatcher now stamps the tenant for
// the completed AND expired paths.
type tenantCapturingUMTRepo struct {
	*inmem.UserManaTopUpRepo
	seenTenant        []string // ctx tenant at GetByStripeSessionID
	seenGetByIDTenant []string // ctx tenant at GetByID (payment_failed/refunded path)
}

func (r *tenantCapturingUMTRepo) GetByStripeSessionID(ctx context.Context, sessID string) (*umt.UserManaTopUp, error) {
	r.seenTenant = append(r.seenTenant, tracing.TenantIDFromContext(ctx))
	return r.UserManaTopUpRepo.GetByStripeSessionID(ctx, sessID)
}

func (r *tenantCapturingUMTRepo) GetByID(ctx context.Context, tenantID, purchaseID string) (*umt.UserManaTopUp, error) {
	r.seenGetByIDTenant = append(r.seenGetByIDTenant, tracing.TenantIDFromContext(ctx))
	return r.UserManaTopUpRepo.GetByID(ctx, tenantID, purchaseID)
}

var _ umt.Repo = (*tenantCapturingUMTRepo)(nil)

// failingEmitter always returns an error from Emit — to prove the dispatcher
// propagates outbox-emit failures (so the webhook 500s and Stripe retries
// rather than silently marking the event processed with no event published).
type failingEmitter struct{ err error }

func (f failingEmitter) Emit(_ context.Context, _ EmitInput) error               { return f.err }
func (f failingEmitter) EmitDispute(_ context.Context, _ EmitDisputeInput) error { return f.err }

const umtSessionID = "cs_test_umt_session"

func newDispatcherForUMT(t *testing.T) (*Dispatcher, *recordingEmitter, *tenantCapturingUMTRepo) {
	t.Helper()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	umtRepo := &tenantCapturingUMTRepo{UserManaTopUpRepo: inmem.NewUserManaTopUpRepo()}
	// Seed a checkout_started top-up keyed by the session id the webhook
	// references.
	u, err := umt.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.user_topup.standard_v1",
		1000,
		199, "USD",
		umtSessionID, "https://stripe.test/checkout",
		now,
	)
	if err != nil {
		t.Fatalf("umt.New: %v", err)
	}
	if err := umtRepo.Save(context.Background(), u); err != nil {
		t.Fatalf("seed umt: %v", err)
	}
	emitter := &recordingEmitter{}
	d := New(Deps{
		Course:         inmem.NewCoursePurchaseRepo(),
		Application:    inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:    inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:      inmem.NewTenantManaTopUpRepo(),
		Subscription:   inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:  umtRepo,
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(),
		Dispute:        inmem.NewDisputeRepo(),
		Outbox:         emitter,
		Now:            func() time.Time { return now },
	})
	return d, emitter, umtRepo
}

func umtSessionEvent(t *testing.T, eventID string, eventType stripeGo.EventType, tenantID string) *stripeGo.Event {
	t.Helper()
	meta := map[string]string{
		"purchase_type": string(wh.AggregateUserManaTopUp),
		"purchase_id":   testPurchaseID,
	}
	if tenantID != "" {
		meta["tenant_id"] = tenantID
	}
	sessObj := map[string]any{
		"id":           umtSessionID,
		"amount_total": int64(199),
		"metadata":     meta,
		"payment_intent": map[string]any{
			"id":            "pi_umt",
			"latest_charge": map[string]any{"id": "ch_umt"},
		},
	}
	raw, _ := json.Marshal(sessObj)
	return &stripeGo.Event{ID: eventID, Type: eventType, Data: &stripeGo.EventData{Raw: raw}}
}

// TestDispatch_UserManaTopUp_Completed_StampsTenantAndCaptures is the
// dispatcher-level proof of the WS-2 mana-credit fix: completed → tenant
// stamped onto ctx → lookup → payment_captured → outbox emit.
func TestDispatch_UserManaTopUp_Completed_StampsTenantAndCaptures(t *testing.T) {
	d, emitter, umtRepo := newDispatcherForUMT(t)
	event := umtSessionEvent(t, "evt_umt_completed", stripeGo.EventTypeCheckoutSessionCompleted, testTenantID)

	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateUserManaTopUp || pid != testPurchaseID {
		t.Fatalf("routing: agg=%s pid=%s, want user_mana_topup/%s", agg, pid, testPurchaseID)
	}
	if len(umtRepo.seenTenant) != 1 || umtRepo.seenTenant[0] != testTenantID {
		t.Fatalf("tenant not stamped onto ctx before lookup: seen=%v want [%s]", umtRepo.seenTenant, testTenantID)
	}
	if len(emitter.emits) != 1 || emitter.emits[0].EventType != EventPaymentCaptured {
		t.Fatalf("expected one payment_captured emit, got %#v", emitter.emits)
	}
	stored, _ := umtRepo.GetByID(context.Background(), testTenantID, testPurchaseID)
	if stored == nil || stored.State != "payment_captured" {
		t.Fatalf("topup not captured: %+v", stored)
	}
}

// TestDispatch_UserManaTopUp_Expired_StampsTenant covers the expired-path
// stamp added alongside the completed-path one (previously absent → the
// session lookup would 22P02 once the repos apply RLS).
func TestDispatch_UserManaTopUp_Expired_StampsTenant(t *testing.T) {
	d, _, umtRepo := newDispatcherForUMT(t)
	// reset the captured tenants (seed Save did not call GetByStripeSessionID).
	umtRepo.seenTenant = nil
	event := umtSessionEvent(t, "evt_umt_expired", stripeGo.EventTypeCheckoutSessionExpired, testTenantID)

	agg, _, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("Dispatch expired: %v", err)
	}
	if agg != wh.AggregateUserManaTopUp {
		t.Fatalf("expected user_mana_topup routing, got %s", agg)
	}
	if len(umtRepo.seenTenant) != 1 || umtRepo.seenTenant[0] != testTenantID {
		t.Fatalf("expired path did not stamp tenant: seen=%v want [%s]", umtRepo.seenTenant, testTenantID)
	}
}

// TestDispatch_UserManaTopUp_Expired_NoTenant_Skips proves the synthetic-event
// guard: a session without tenant_id metadata is skipped (no lookup, no error)
// rather than spinning a meaningless RLS error.
func TestDispatch_UserManaTopUp_Expired_NoTenant_Skips(t *testing.T) {
	d, _, umtRepo := newDispatcherForUMT(t)
	umtRepo.seenTenant = nil
	event := umtSessionEvent(t, "evt_umt_expired_notenant", stripeGo.EventTypeCheckoutSessionExpired, "")

	agg, _, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("ack-only skip should not error: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Fatalf("expected AggregateUnknown skip, got %s", agg)
	}
	if len(umtRepo.seenTenant) != 0 {
		t.Fatalf("expected no lookup on tenant-less event, saw %v", umtRepo.seenTenant)
	}
}

func umtPaymentIntentFailedEvent(t *testing.T, eventID, tenantID string) *stripeGo.Event {
	t.Helper()
	meta := map[string]string{
		"purchase_type": string(wh.AggregateUserManaTopUp),
		"purchase_id":   testPurchaseID,
	}
	if tenantID != "" {
		meta["tenant_id"] = tenantID
	}
	piObj := map[string]any{
		"id":       "pi_umt_fail",
		"metadata": meta,
		"last_payment_error": map[string]any{
			"code":    "card_declined",
			"message": "your card was declined",
		},
	}
	raw, _ := json.Marshal(piObj)
	return &stripeGo.Event{ID: eventID, Type: stripeGo.EventTypePaymentIntentPaymentFailed, Data: &stripeGo.EventData{Raw: raw}}
}

// TestDispatch_UserManaTopUp_PaymentIntentFailed_StampsTenant covers Issue A
// from the adversarial review: handlePaymentIntentFailed calls GetByID (which
// applies RLS from ctx) but previously never stamped the tenant onto ctx →
// ErrNoTenantContext for every aggregate including user_mana_topup.
func TestDispatch_UserManaTopUp_PaymentIntentFailed_StampsTenant(t *testing.T) {
	d, _, umtRepo := newDispatcherForUMT(t)
	umtRepo.seenGetByIDTenant = nil
	event := umtPaymentIntentFailedEvent(t, "evt_umt_pifail", testTenantID)

	agg, _, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("Dispatch payment_failed: %v", err)
	}
	if agg != wh.AggregateUserManaTopUp {
		t.Fatalf("expected user_mana_topup routing, got %s", agg)
	}
	if len(umtRepo.seenGetByIDTenant) != 1 || umtRepo.seenGetByIDTenant[0] != testTenantID {
		t.Fatalf("payment_failed path did not stamp tenant before GetByID: seen=%v want [%s]", umtRepo.seenGetByIDTenant, testTenantID)
	}
}

// TestDispatch_UserManaTopUp_EmitFailure_PropagatesError covers Issue B: an
// outbox-emit failure MUST propagate out of Dispatch so the webhook returns
// 500 and Stripe retries — otherwise the event is marked processed, no event
// is published, and the wallet is permanently stuck uncredited.
func TestDispatch_UserManaTopUp_EmitFailure_PropagatesError(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	umtRepo := &tenantCapturingUMTRepo{UserManaTopUpRepo: inmem.NewUserManaTopUpRepo()}
	u, err := umt.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.user_topup.standard_v1",
		1000, 199, "USD", umtSessionID, "https://stripe.test/checkout", now,
	)
	if err != nil {
		t.Fatalf("umt.New: %v", err)
	}
	if err := umtRepo.Save(context.Background(), u); err != nil {
		t.Fatalf("seed: %v", err)
	}
	d := New(Deps{
		Course: inmem.NewCoursePurchaseRepo(), Application: inmem.NewApplicationPaymentRepo(),
		FamiliarEgg: inmem.NewFamiliarEggPurchaseRepo(), ManaTopUp: inmem.NewTenantManaTopUpRepo(),
		Subscription: inmem.NewUserSubscriptionRepo(), UserManaTopUp: umtRepo,
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(), Dispute: inmem.NewDisputeRepo(),
		Outbox: failingEmitter{err: fmt.Errorf("outbox InsertStandalone: connection reset")},
		Now:    func() time.Time { return now },
	})
	event := umtSessionEvent(t, "evt_umt_emitfail", stripeGo.EventTypeCheckoutSessionCompleted, testTenantID)

	_, _, derr := d.Dispatch(context.Background(), event)
	if derr == nil {
		t.Fatalf("expected Dispatch to propagate the outbox-emit failure (webhook must 500 so Stripe retries); got nil")
	}
}
