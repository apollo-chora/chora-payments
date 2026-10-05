// reconstruct_on_miss_test.go — ADR-188 / CHO-1831.
//
// Pins the webhook-authoritative-fulfilment contract: when a paid
// checkout.session.completed arrives but the create-side Purchase row is
// absent (the orphaned-paid-session bug that drove the Stripe "webhook
// failing/recovered" emails), the dispatcher reconstructs the aggregate
// from the signed session metadata + Stripe's authoritative amount_total,
// persists it, and fulfils — instead of 500-ing with "purchase not found".
//
// Covers the two tenant-scoped checkout flows that exhibited the defect:
// tenant_mana_topup (mode=payment) and tenant_addon_purchase (mode=
// subscription / payment). Amounts are taken from the session, never from
// client-echoed metadata (ADR-188 D2).
package dispatcher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	stripeGo "github.com/stripe/stripe-go/v78"

	inmem "github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const reconSessionID = "cs_test_orphan_reconstruct"

// newDispatcherEmptyRepos builds a dispatcher whose Purchase repos are all
// empty — i.e. no create-side row was ever persisted, exactly the orphaned-
// paid-session state proven live on 2026-06-22.
func newDispatcherEmptyRepos(t *testing.T) (*Dispatcher, *recordingEmitter, *inmem.TenantManaTopUpRepo, *inmem.TenantAddonPurchaseRepo) {
	t.Helper()
	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	manaRepo := inmem.NewTenantManaTopUpRepo()
	addonRepo := inmem.NewTenantAddonPurchaseRepo()
	emitter := &recordingEmitter{}
	d := New(Deps{
		Course:         inmem.NewCoursePurchaseRepo(),
		Application:    inmem.NewApplicationPaymentRepo(),
		FamiliarEgg:    inmem.NewFamiliarEggPurchaseRepo(),
		ManaTopUp:      manaRepo,
		Subscription:   inmem.NewUserSubscriptionRepo(),
		UserManaTopUp:  inmem.NewUserManaTopUpRepo(),
		IdentityKycFee: inmem.NewIdentityKycFeeRepo(),
		AddonPurchase:  addonRepo,
		Dispute:        inmem.NewDisputeRepo(),
		Outbox:         emitter,
		Now:            func() time.Time { return now },
	})
	return d, emitter, manaRepo, addonRepo
}

// reconSessionEvent crafts a checkout.session.completed carrying the full
// reconstruction field-set each Create*Session handler stamps (ADR-188 D2).
func reconSessionEvent(t *testing.T, eventID string, extraMeta map[string]string) *stripeGo.Event {
	t.Helper()
	meta := map[string]string{
		"purchase_id":  testPurchaseID,
		"tenant_id":    testTenantID,
		"learner_gcid": testLearnerGCID,
	}
	for k, v := range extraMeta {
		meta[k] = v
	}
	sessObj := map[string]any{
		"id":           reconSessionID,
		"amount_total": int64(9900),
		"currency":     "sgd",
		"metadata":     meta,
		"payment_intent": map[string]any{
			"id":            "pi_recon",
			"latest_charge": map[string]any{"id": "ch_recon"},
		},
	}
	raw, _ := json.Marshal(sessObj)
	return &stripeGo.Event{ID: eventID, Type: stripeGo.EventTypeCheckoutSessionCompleted, Data: &stripeGo.EventData{Raw: raw}}
}

// TestDispatch_TenantManaTopUp_ReconstructsWhenRowMissing is the core
// ADR-188 proof for tenant_mana_topup: a paid completed event with no
// pre-created row fulfils (reconstruct → payment_captured → emit) rather
// than erroring with "purchase not found".
func TestDispatch_TenantManaTopUp_ReconstructsWhenRowMissing(t *testing.T) {
	d, emitter, manaRepo, _ := newDispatcherEmptyRepos(t)
	event := reconSessionEvent(t, "evt_mana_orphan", map[string]string{
		"purchase_type": string(wh.AggregateTenantManaTopUp),
		"sku":           "tenant-mana-5000",
		"mana_units":    "5000",
	})

	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("expected reconstruct-and-fulfil (no error), got: %v", err)
	}
	if agg != wh.AggregateTenantManaTopUp || pid != testPurchaseID {
		t.Fatalf("routing: agg=%s pid=%s, want tenant_mana_topup/%s", agg, pid, testPurchaseID)
	}
	stored, gerr := manaRepo.GetByStripeSessionID(context.Background(), reconSessionID)
	if gerr != nil || stored == nil {
		t.Fatalf("reconstructed row not persisted: err=%v stored=%v", gerr, stored)
	}
	if stored.State != "payment_captured" {
		t.Fatalf("reconstructed row not captured: state=%s", stored.State)
	}
	if stored.ManaUnits != 5000 || stored.SKU != "tenant-mana-5000" {
		t.Fatalf("reconstructed fields wrong: units=%d sku=%s", stored.ManaUnits, stored.SKU)
	}
	// Amount comes from Stripe's authoritative amount_total, not metadata.
	if stored.AmountCents != 9900 {
		t.Fatalf("amount must come from session.amount_total=9900, got %d", stored.AmountCents)
	}
	if len(emitter.emits) != 1 || emitter.emits[0].EventType != EventPaymentCaptured {
		t.Fatalf("expected one payment_captured emit, got %#v", emitter.emits)
	}
}

// TestDispatch_TenantAddonPurchase_ReconstructsWhenRowMissing — same proof
// for the H+ Marketplace add-on subscribe flow.
func TestDispatch_TenantAddonPurchase_ReconstructsWhenRowMissing(t *testing.T) {
	d, emitter, _, addonRepo := newDispatcherEmptyRepos(t)
	event := reconSessionEvent(t, "evt_addon_orphan", map[string]string{
		"purchase_type": string(wh.AggregateTenantAddonPurchase),
		"addon_plan_id": "0190dddd-0000-7000-8000-000000000002",
		"addon_code":    "tms",
		"tier_code":     "pro",
	})

	agg, pid, err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("expected reconstruct-and-fulfil (no error), got: %v", err)
	}
	if agg != wh.AggregateTenantAddonPurchase || pid != testPurchaseID {
		t.Fatalf("routing: agg=%s pid=%s, want tenant_addon_purchase/%s", agg, pid, testPurchaseID)
	}
	stored, gerr := addonRepo.GetByStripeSessionID(context.Background(), reconSessionID)
	if gerr != nil || stored == nil {
		t.Fatalf("reconstructed row not persisted: err=%v stored=%v", gerr, stored)
	}
	if stored.State != "payment_captured" {
		t.Fatalf("reconstructed row not captured: state=%s", stored.State)
	}
	if stored.AddonCode != "tms" || stored.TierCode != "pro" {
		t.Fatalf("reconstructed fields wrong: addon=%s tier=%s", stored.AddonCode, stored.TierCode)
	}
	if len(emitter.emits) != 1 || emitter.emits[0].EventType != EventPaymentCaptured {
		t.Fatalf("expected one payment_captured emit, got %#v", emitter.emits)
	}
}

// TestDispatch_TenantManaTopUp_ReconstructIdempotent proves Stripe's
// at-least-once redelivery does not create a duplicate: the second
// completed dispatch finds the reconstructed row (no second reconstruction)
// and the captured-state transition is a no-op.
func TestDispatch_TenantManaTopUp_ReconstructIdempotent(t *testing.T) {
	d, _, manaRepo, _ := newDispatcherEmptyRepos(t)
	mk := func(id string) *stripeGo.Event {
		return reconSessionEvent(t, id, map[string]string{
			"purchase_type": string(wh.AggregateTenantManaTopUp),
			"sku":           "tenant-mana-5000",
			"mana_units":    "5000",
		})
	}
	if _, _, err := d.Dispatch(context.Background(), mk("evt_mana_orphan_1")); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if _, _, err := d.Dispatch(context.Background(), mk("evt_mana_orphan_2")); err != nil {
		t.Fatalf("redelivery dispatch must be idempotent (no error), got: %v", err)
	}
	// Same session_id + purchase_id → the inmem maps (keyed by both) hold a
	// single row; redelivery resolves it (no second reconstruction) and the
	// captured-state transition is a no-op.
	stored, err := manaRepo.GetByStripeSessionID(context.Background(), reconSessionID)
	if err != nil || stored == nil || stored.PurchaseID != testPurchaseID {
		t.Fatalf("redelivery lost/duplicated the row: err=%v stored=%v", err, stored)
	}
	if stored.State != "payment_captured" {
		t.Fatalf("row not captured after redelivery: state=%s", stored.State)
	}
}

// allEmptyRepos exposes every Purchase repo so the table test can verify the
// reconstructed row per type.
type allEmptyRepos struct {
	course   *inmem.CoursePurchaseRepo
	app      *inmem.ApplicationPaymentRepo
	egg      *inmem.FamiliarEggPurchaseRepo
	sub      *inmem.UserSubscriptionRepo
	userMana *inmem.UserManaTopUpRepo
	kyc      *inmem.IdentityKycFeeRepo
}

func newDispatcherAllEmpty(t *testing.T) (*Dispatcher, *recordingEmitter, allEmptyRepos) {
	t.Helper()
	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	r := allEmptyRepos{
		course:   inmem.NewCoursePurchaseRepo(),
		app:      inmem.NewApplicationPaymentRepo(),
		egg:      inmem.NewFamiliarEggPurchaseRepo(),
		sub:      inmem.NewUserSubscriptionRepo(),
		userMana: inmem.NewUserManaTopUpRepo(),
		kyc:      inmem.NewIdentityKycFeeRepo(),
	}
	emitter := &recordingEmitter{}
	d := New(Deps{
		Course:         r.course,
		Application:    r.app,
		FamiliarEgg:    r.egg,
		ManaTopUp:      inmem.NewTenantManaTopUpRepo(),
		Subscription:   r.sub,
		UserManaTopUp:  r.userMana,
		IdentityKycFee: r.kyc,
		AddonPurchase:  inmem.NewTenantAddonPurchaseRepo(),
		Dispute:        inmem.NewDisputeRepo(),
		Outbox:         emitter,
		Now:            func() time.Time { return now },
	})
	return d, emitter, r
}

// TestDispatch_AllCheckoutTypes_ReconstructWhenRowMissing proves ADR-188 D1
// reconstruct-on-miss for the remaining six checkout.session.completed
// purchase types (extended per owner direction so the webhook is
// authoritative for every checkout flow, not just the two that exhibited
// the original defect).
func TestDispatch_AllCheckoutTypes_ReconstructWhenRowMissing(t *testing.T) {
	cases := []struct {
		name  string
		agg   wh.AggregateType
		meta  map[string]string
		state func(r allEmptyRepos) (string, error)
	}{
		{
			"course_purchase", wh.AggregateCoursePurchase,
			map[string]string{"purchase_type": string(wh.AggregateCoursePurchase), "course_id": "0190cccc-0000-7000-8000-000000000001"},
			func(r allEmptyRepos) (string, error) {
				x, err := r.course.GetByStripeSessionID(context.Background(), reconSessionID)
				if err != nil {
					return "", err
				}
				return string(x.State), nil
			},
		},
		{
			"application_payment", wh.AggregateApplicationPayment,
			map[string]string{"purchase_type": string(wh.AggregateApplicationPayment), "application_id": "0190aaaa-0000-7000-8000-000000000001", "course_id": "0190cccc-0000-7000-8000-000000000001"},
			func(r allEmptyRepos) (string, error) {
				x, err := r.app.GetByStripeSessionID(context.Background(), reconSessionID)
				if err != nil {
					return "", err
				}
				return string(x.State), nil
			},
		},
		{
			"familiar_egg_purchase", wh.AggregateFamiliarEggPurchase,
			map[string]string{"purchase_type": string(wh.AggregateFamiliarEggPurchase), "egg_sku": "egg.standard_v1", "suggested_focal_atom_id": "0190eeee-0000-7000-8000-000000000001"},
			func(r allEmptyRepos) (string, error) {
				x, err := r.egg.GetByStripeSessionID(context.Background(), reconSessionID)
				if err != nil {
					return "", err
				}
				return string(x.State), nil
			},
		},
		{
			"user_subscription", wh.AggregateUserSubscription,
			map[string]string{"purchase_type": string(wh.AggregateUserSubscription), "plan_sku": "familiar.pro_v1", "billing_period": "monthly"},
			func(r allEmptyRepos) (string, error) {
				x, err := r.sub.GetByStripeSessionID(context.Background(), reconSessionID)
				if err != nil {
					return "", err
				}
				return string(x.State), nil
			},
		},
		{
			"user_mana_topup", wh.AggregateUserManaTopUp,
			map[string]string{"purchase_type": string(wh.AggregateUserManaTopUp), "sku": "mana.user_topup.standard_v1", "mana_units": "1000"},
			func(r allEmptyRepos) (string, error) {
				x, err := r.userMana.GetByStripeSessionID(context.Background(), reconSessionID)
				if err != nil {
					return "", err
				}
				return string(x.State), nil
			},
		},
		{
			"identity_kyc_fee", wh.AggregateIdentityKycFee,
			map[string]string{"purchase_type": string(wh.AggregateIdentityKycFee), "kyc_doc_type": "passport"},
			func(r allEmptyRepos) (string, error) {
				x, err := r.kyc.GetByStripeSessionID(context.Background(), reconSessionID)
				if err != nil {
					return "", err
				}
				return string(x.State), nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, emitter, repos := newDispatcherAllEmpty(t)
			event := reconSessionEvent(t, "evt_"+tc.name+"_orphan", tc.meta)
			agg, pid, err := d.Dispatch(context.Background(), event)
			if err != nil {
				t.Fatalf("%s: expected reconstruct-and-fulfil, got: %v", tc.name, err)
			}
			if agg != tc.agg || pid != testPurchaseID {
				t.Fatalf("%s routing: agg=%s pid=%s", tc.name, agg, pid)
			}
			state, gerr := tc.state(repos)
			if gerr != nil {
				t.Fatalf("%s: reconstructed row not persisted: %v", tc.name, gerr)
			}
			if state != "payment_captured" {
				t.Fatalf("%s: row not captured: state=%s", tc.name, state)
			}
			if len(emitter.emits) != 1 || emitter.emits[0].EventType != EventPaymentCaptured {
				t.Fatalf("%s: expected one payment_captured emit, got %#v", tc.name, emitter.emits)
			}
		})
	}
}

// TestDispatch_TenantManaTopUp_ReconstructNeedsManaUnits guards the D2
// metadata contract: a malformed/absent mana_units fails loud rather than
// fabricating a zero-unit purchase.
func TestDispatch_TenantManaTopUp_ReconstructNeedsManaUnits(t *testing.T) {
	d, _, _, _ := newDispatcherEmptyRepos(t)
	event := reconSessionEvent(t, "evt_mana_badunits", map[string]string{
		"purchase_type": string(wh.AggregateTenantManaTopUp),
		"sku":           "tenant-mana-5000",
		// mana_units intentionally omitted
	})
	if _, _, err := d.Dispatch(context.Background(), event); err == nil {
		t.Fatalf("expected fail-loud on missing mana_units, got nil")
	}
}
