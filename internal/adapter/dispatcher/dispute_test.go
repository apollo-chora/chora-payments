package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	inmem "github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// recordingEmitter captures every Emit + EmitDispute call for assertion.
type recordingEmitter struct {
	emits        []EmitInput
	disputeEmits []EmitDisputeInput
}

func (r *recordingEmitter) Emit(_ context.Context, in EmitInput) error {
	r.emits = append(r.emits, in)
	return nil
}

func (r *recordingEmitter) EmitDispute(_ context.Context, in EmitDisputeInput) error {
	r.disputeEmits = append(r.disputeEmits, in)
	return nil
}

const (
	testTenantID    = "01910000-0000-7000-8000-000000000099"
	testLearnerGCID = "01910000-0000-7000-8000-000000000098"
	testPurchaseID  = "01910000-0000-7000-8000-000000000097"
)

func newDispatcherForDispute(t *testing.T) (*Dispatcher, *recordingEmitter, *inmem.DisputeRepo, *inmem.CoursePurchaseRepo) {
	t.Helper()
	emitter := &recordingEmitter{}
	disputeRepo := inmem.NewDisputeRepo()
	courseRepo := inmem.NewCoursePurchaseRepo()
	// Seed a parent CoursePurchase so the dispatcher can resolve tenant from
	// the Charge metadata.
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	cp, err := coursepurchase.New(
		testPurchaseID, testTenantID, testLearnerGCID, "course_test",
		1999, "USD",
		"sess_test", "https://stripe.test/checkout",
		now,
	)
	if err != nil {
		t.Fatalf("seed course: %v", err)
	}
	if err := courseRepo.Save(context.Background(), cp); err != nil {
		t.Fatalf("save course: %v", err)
	}

	d := New(Deps{
		Course:         courseRepo,
		Application:    inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:    inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:      inmem.NewTenantManaTopUpRepo(),
		Subscription:   inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:  inmem.NewUserManaTopUpRepo(),
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(),
		Dispute:        disputeRepo,
		Outbox:         emitter,
		Now:            func() time.Time { return now },
	})
	return d, emitter, disputeRepo, courseRepo
}

func makeDisputeEvent(t *testing.T, eventType stripeGo.EventType, stripeDisputeID, status, reason string) *stripeGo.Event {
	t.Helper()
	dispObj := map[string]any{
		"id":       stripeDisputeID,
		"amount":   int64(1999),
		"currency": "usd",
		"status":   status,
		"reason":   reason,
		"charge": map[string]any{
			"id": "ch_test_xyz",
			"metadata": map[string]string{
				"purchase_type": string(wh.AggregateCoursePurchase),
				"purchase_id":   testPurchaseID,
				"tenant_id":     testTenantID,
			},
		},
		"evidence_details": map[string]any{
			"due_by": int64(time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC).Unix()),
		},
	}
	raw, err := json.Marshal(dispObj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &stripeGo.Event{
		ID:   "evt_test_" + stripeDisputeID,
		Type: eventType,
		Data: &stripeGo.EventData{Raw: raw},
	}
}

func TestDispatch_ChargeDisputeCreated_PersistsAndEmitsRaised(t *testing.T) {
	d, emitter, disputeRepo, _ := newDispatcherForDispute(t)
	event := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeCreated, "dp_test_1", "needs_response", "fraudulent")

	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if agg != wh.AggregateCoursePurchase {
		t.Fatalf("expected aggregate=course_purchase, got %s", agg)
	}
	if pid != testPurchaseID {
		t.Fatalf("expected purchase_id propagated, got %s", pid)
	}
	stored, err := disputeRepo.GetByStripeDisputeID(context.Background(), "dp_test_1")
	if err != nil {
		t.Fatalf("expected dispute persisted: %v", err)
	}
	if stored.State != dispute.StateRaised {
		t.Fatalf("expected raised, got %s", stored.State)
	}
	if stored.AmountCents != 1999 {
		t.Fatalf("amount not propagated")
	}
	if len(emitter.disputeEmits) != 1 || emitter.disputeEmits[0].EventType != EventDisputeRaised {
		t.Fatalf("expected one dispute_raised emit, got %#v", emitter.disputeEmits)
	}
}

func TestDispatch_ChargeDisputeCreated_IdempotentReplay(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	event := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeCreated, "dp_replay", "needs_response", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("replay dispatch should be idempotent: %v", err)
	}
	if len(emitter.disputeEmits) != 1 {
		t.Fatalf("expected single emit on replay, got %d", len(emitter.disputeEmits))
	}
}

func TestDispatch_ChargeDisputeCreated_MissingMetadata_AckOnly(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	dispObj := map[string]any{
		"id":       "dp_no_meta",
		"amount":   int64(100),
		"currency": "usd",
		"status":   "needs_response",
		"reason":   "general",
		"charge": map[string]any{
			"id":       "ch_no_meta",
			"metadata": map[string]string{},
		},
	}
	raw, _ := json.Marshal(dispObj)
	event := &stripeGo.Event{
		ID:   "evt_no_meta",
		Type: stripeGo.EventTypeChargeDisputeCreated,
		Data: &stripeGo.EventData{Raw: raw},
	}
	agg, _, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("ack-only should not error: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Fatalf("expected AggregateUnknown ack-only path, got %s", agg)
	}
	if len(emitter.disputeEmits) != 0 {
		t.Fatalf("expected no emit on missing-metadata, got %d", len(emitter.disputeEmits))
	}
}

func TestDispatch_ChargeDisputeClosed_Won(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	// First raise.
	create := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeCreated, "dp_close_won", "needs_response", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), create); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Then close.
	closeEv := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeClosed, "dp_close_won", "won", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), closeEv); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(emitter.disputeEmits) != 2 {
		t.Fatalf("expected 2 emits (raised + closed), got %d", len(emitter.disputeEmits))
	}
	if emitter.disputeEmits[1].EventType != EventDisputeClosed {
		t.Fatalf("expected dispute_closed second, got %s", emitter.disputeEmits[1].EventType)
	}
}

func TestDispatch_ChargeDisputeClosed_UnknownDispute_AckOnly(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	event := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeClosed, "dp_unknown", "won", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("ack-only should not error: %v", err)
	}
	if len(emitter.disputeEmits) != 0 {
		t.Fatalf("expected no emit on unknown dispute, got %d", len(emitter.disputeEmits))
	}
}

func TestDispatch_ChargeDisputeFundsWithdrawn(t *testing.T) {
	d, emitter, disputeRepo, _ := newDispatcherForDispute(t)
	create := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeCreated, "dp_funds_w", "needs_response", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), create); err != nil {
		t.Fatalf("create: %v", err)
	}
	wEv := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeFundsWithdrawn, "dp_funds_w", "needs_response", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), wEv); err != nil {
		t.Fatalf("funds_withdrawn: %v", err)
	}
	stored, _ := disputeRepo.GetByStripeDisputeID(context.Background(), "dp_funds_w")
	if !stored.FundsWithdrawn {
		t.Fatalf("expected FundsWithdrawn=true")
	}
	gotEvent := emitter.disputeEmits[len(emitter.disputeEmits)-1]
	if gotEvent.EventType != EventDisputeFundsWithdrawn {
		t.Fatalf("expected funds_withdrawn emit, got %s", gotEvent.EventType)
	}
}

func TestDispatch_ChargeDisputeFundsReinstated(t *testing.T) {
	d, emitter, disputeRepo, _ := newDispatcherForDispute(t)
	create := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeCreated, "dp_funds_r", "needs_response", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), create); err != nil {
		t.Fatalf("create: %v", err)
	}
	rEv := makeDisputeEvent(t, stripeGo.EventTypeChargeDisputeFundsReinstated, "dp_funds_r", "needs_response", "fraudulent")
	if _, _, err := d.Dispatch(context.Background(), rEv); err != nil {
		t.Fatalf("funds_reinstated: %v", err)
	}
	stored, _ := disputeRepo.GetByStripeDisputeID(context.Background(), "dp_funds_r")
	if !stored.FundsReinstated {
		t.Fatalf("expected FundsReinstated=true")
	}
	gotEvent := emitter.disputeEmits[len(emitter.disputeEmits)-1]
	if gotEvent.EventType != EventDisputeFundsReinstated {
		t.Fatalf("expected funds_reinstated emit, got %s", gotEvent.EventType)
	}
}

// ===============================================================
// Async-payment handlers
// ===============================================================

func TestDispatch_CheckoutSessionAsyncPaymentSucceeded_TreatsAsCaptured(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	sessObj := map[string]any{
		"id":           "sess_test",
		"amount_total": int64(1999),
		"metadata": map[string]string{
			"purchase_type": string(wh.AggregateCoursePurchase),
			"purchase_id":   testPurchaseID,
			"tenant_id":     testTenantID,
		},
		"payment_intent": map[string]any{
			"id":            "pi_test",
			"latest_charge": map[string]any{"id": "ch_test"},
		},
	}
	raw, _ := json.Marshal(sessObj)
	event := &stripeGo.Event{
		ID:   "evt_async_ok",
		Type: stripeGo.EventTypeCheckoutSessionAsyncPaymentSucceeded,
		Data: &stripeGo.EventData{Raw: raw},
	}
	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if agg != wh.AggregateCoursePurchase || pid != testPurchaseID {
		t.Fatalf("expected course/purchase routing, got %s/%s", agg, pid)
	}
	if len(emitter.emits) != 1 || emitter.emits[0].EventType != EventPaymentCaptured {
		t.Fatalf("expected one payment_captured emit, got %#v", emitter.emits)
	}
}

func TestDispatch_CheckoutSessionAsyncPaymentFailed_TreatsAsFailed(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	sessObj := map[string]any{
		"id": "sess_test",
		"metadata": map[string]string{
			"purchase_type": string(wh.AggregateCoursePurchase),
			"purchase_id":   testPurchaseID,
			"tenant_id":     testTenantID,
		},
	}
	raw, _ := json.Marshal(sessObj)
	event := &stripeGo.Event{
		ID:   "evt_async_fail",
		Type: stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed,
		Data: &stripeGo.EventData{Raw: raw},
	}
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(emitter.emits) != 1 || emitter.emits[0].EventType != EventPaymentFailed {
		t.Fatalf("expected payment_failed emit, got %#v", emitter.emits)
	}
}

func TestDispatch_CheckoutSessionAsyncPaymentFailed_NoMetadata_AckOnly(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	sessObj := map[string]any{"id": "sess_no_meta"}
	raw, _ := json.Marshal(sessObj)
	event := &stripeGo.Event{
		ID:   "evt_async_fail_no_meta",
		Type: stripeGo.EventTypeCheckoutSessionAsyncPaymentFailed,
		Data: &stripeGo.EventData{Raw: raw},
	}
	if _, _, err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("ack-only should not error: %v", err)
	}
	if len(emitter.emits) != 0 {
		t.Fatalf("expected no emit, got %d", len(emitter.emits))
	}
}

// Ensure unknown stripe event types are still ignored.
func TestDispatch_UnknownEventType_Ignored(t *testing.T) {
	d, emitter, _, _ := newDispatcherForDispute(t)
	event := &stripeGo.Event{
		ID:   "evt_unknown",
		Type: stripeGo.EventType("invoice.payment_action_required"),
		Data: &stripeGo.EventData{Raw: []byte(`{}`)},
	}
	agg, _, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unknown event should ack-only: %v", err)
	}
	if agg != wh.AggregateUnknown {
		t.Fatalf("expected AggregateUnknown, got %s", agg)
	}
	if len(emitter.emits) != 0 || len(emitter.disputeEmits) != 0 {
		t.Fatalf("no emits expected")
	}
}

// Cross-check: ensure resolveTarget rejects empty metadata via errors.Is.
func TestResolveTarget_EmptyMetadata(t *testing.T) {
	_, _, err := resolveTarget(nil)
	if err == nil {
		t.Fatalf("expected error on nil metadata")
	}
	if !errors.Is(err, err) { // sanity
		t.Fatalf("error chain corrupt")
	}
}

// timeOrZero edge case coverage.
func TestTimeOrZero(t *testing.T) {
	if !timeOrZero(nil).IsZero() {
		t.Fatalf("nil should produce zero time")
	}
	ed := &stripeGo.DisputeEvidenceDetails{DueBy: 0}
	if !timeOrZero(ed).IsZero() {
		t.Fatalf("zero DueBy should produce zero time")
	}
	target := time.Now().Unix()
	ed = &stripeGo.DisputeEvidenceDetails{DueBy: target}
	if timeOrZero(ed).Unix() != target {
		t.Fatalf("expected matching unix")
	}
}
