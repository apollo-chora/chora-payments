// repos_coverage_test.go — coverage for the remaining inmem repos not
// exercised by dispute_test.go / tenant_addon_purchase_test.go.
package inmem

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	"github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	"github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	"github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	"github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	"github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	"github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const (
	repoTenantID    = "01970000-0000-7000-8000-000000000001"
	repoLearnerGCID = "01970000-0000-7000-a000-000000000002"
	repoPurchaseID  = "01970000-0000-7000-b000-000000000003"
	repoSessionID   = "cs_test_inmem"
	repoCheckoutURL = "https://checkout.stripe.com/c/pay/cs_test_inmem"
)

func repoNow() time.Time {
	return time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
}

func TestCoursePurchaseRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewCoursePurchaseRepo()
	agg, err := coursepurchase.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "course_0001",
		1990, "SGD", repoSessionID, repoCheckoutURL, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v, want ErrNotFound", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.CourseID != "course_0001" {
		t.Errorf("CourseID=%s", got.CourseID)
	}

	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("GetByID wrong tenant err=%v", err)
	}
	if _, err := repo.GetByID(ctx, repoTenantID, "missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("GetByID missing err=%v", err)
	}

	bySess, err := repo.GetByStripeSessionID(ctx, repoSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if bySess.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSessionID PurchaseID=%s", bySess.PurchaseID)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("GetByStripeSessionID blank err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("GetByStripeSessionID missing err=%v", err)
	}
}

func TestApplicationPaymentRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewApplicationPaymentRepo()
	agg, err := application_payment.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "app_0001", "course_0001",
		1990, "SGD", repoSessionID, repoCheckoutURL, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ApplicationID != "app_0001" {
		t.Errorf("ApplicationID=%s", got.ApplicationID)
	}
	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("GetByID wrong tenant err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
	bySess, err := repo.GetByStripeSessionID(ctx, repoSessionID)
	if err != nil || bySess.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSessionID err=%v", err)
	}
}

func TestFamiliarEggPurchaseRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewFamiliarEggPurchaseRepo()
	agg, err := familiar_egg_purchase.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "egg.standard.v1",
		"focal",
		1990, "SGD", repoSessionID, repoCheckoutURL, nil, nil, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil || got.EggSKU != "egg.standard.v1" {
		t.Errorf("GetByID err=%v", err)
	}
	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("wrong tenant err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
	bySess, err := repo.GetByStripeSessionID(ctx, repoSessionID)
	if err != nil || bySess.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSessionID err=%v", err)
	}
}

func TestTenantManaTopUpRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewTenantManaTopUpRepo()
	agg, err := tenant_mana_topup.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "mana.tenant_topup.standard_v1",
		5000, 4990, "SGD", repoSessionID, repoCheckoutURL, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil || got.ManaUnits != 5000 {
		t.Errorf("GetByID err=%v", err)
	}
	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("wrong tenant err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
	bySess, err := repo.GetByStripeSessionID(ctx, repoSessionID)
	if err != nil || bySess.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSessionID err=%v", err)
	}
}

func TestUserSubscriptionRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewUserSubscriptionRepo()
	agg, err := user_subscription.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "mana.subscription.standard_v1",
		user_subscription.BillingMonthly,
		1990, "SGD", repoSessionID, repoCheckoutURL, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	agg.StripeSubscriptionID = "sub_test_xyz"

	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil || got.PlanSKU != "mana.subscription.standard_v1" {
		t.Errorf("GetByID err=%v", err)
	}
	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("wrong tenant err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
	if _, err := repo.GetByStripeSubscriptionID(ctx, "  "); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("blank sub id err=%v", err)
	}
	if _, err := repo.GetByStripeSubscriptionID(ctx, "sub_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing sub id err=%v", err)
	}
	bySub, err := repo.GetByStripeSubscriptionID(ctx, "sub_test_xyz")
	if err != nil || bySub.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSubscriptionID err=%v", err)
	}
}

func TestWebhookEventRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewWebhookEventRepo()
	w, err := webhook_event.New("evt_test_0001", "checkout.session.completed", repoNow())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := repo.Insert(ctx, nil); !errors.Is(err, webhook_event.ErrEventIDRequired) {
		t.Errorf("Insert(nil) err=%v", err)
	}
	if err := repo.Insert(ctx, w); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := repo.Insert(ctx, w); !errors.Is(err, webhook_event.ErrAlreadyProcessed) {
		t.Errorf("duplicate insert err=%v", err)
	}
	if err := repo.MarkProcessed(ctx, "evt_missing", webhook_event.AggregateCoursePurchase, "p", repoNow()); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("MarkProcessed missing err=%v", err)
	}
	if err := repo.MarkProcessed(ctx, "evt_test_0001", webhook_event.AggregateCoursePurchase, repoPurchaseID, repoNow()); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := repo.MarkFailed(ctx, "evt_test_0001", "boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if err := repo.MarkFailed(ctx, "evt_missing", "boom"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("MarkFailed missing err=%v", err)
	}
	got, err := repo.GetByEventID(ctx, "evt_test_0001")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if got.TargetPurchaseID != repoPurchaseID || got.ProcessingError != "boom" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if _, err := repo.GetByEventID(ctx, "evt_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("GetByEventID missing err=%v", err)
	}
	if !got.IsProcessed() {
		t.Errorf("IsProcessed()=false after MarkProcessed")
	}
}

func TestStripeCustomerRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewStripeCustomerRepo()
	sc, err := stripe_customer.New(repoTenantID, repoLearnerGCID, "cus_test_0001", "learner@example.com", repoNow())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := repo.Insert(ctx, nil); !errors.Is(err, stripe_customer.ErrStripeCustomerEmpty) {
		t.Errorf("Insert(nil) err=%v", err)
	}
	if err := repo.Insert(ctx, sc); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Idempotent re-insert keeps the existing row.
	sc2, _ := stripe_customer.New(repoTenantID, repoLearnerGCID, "cus_test_0002", "", repoNow())
	if err := repo.Insert(ctx, sc2); err != nil {
		t.Fatalf("re-Insert: %v", err)
	}
	got, err := repo.GetByGCID(ctx, repoTenantID, repoLearnerGCID)
	if err != nil {
		t.Fatalf("GetByGCID: %v", err)
	}
	if got.StripeCustomerID != "cus_test_0001" {
		t.Errorf("StripeCustomerID=%s, want cus_test_0001 (existing row wins)", got.StripeCustomerID)
	}
	if _, err := repo.GetByGCID(ctx, repoTenantID, "gcid_missing"); !errors.Is(err, stripe_customer.ErrNotFound) {
		t.Errorf("GetByGCID missing err=%v", err)
	}

	byGCIDs, err := repo.GetByGCIDs(ctx, repoTenantID, []string{repoLearnerGCID, "gcid_missing", ""})
	if err != nil {
		t.Fatalf("GetByGCIDs: %v", err)
	}
	if len(byGCIDs) != 1 {
		t.Errorf("GetByGCIDs len=%d, want 1 (missing + empty omitted)", len(byGCIDs))
	}
	if byGCIDs[repoLearnerGCID] == nil {
		t.Errorf("GetByGCIDs missing learner key")
	}
}

func TestUserManaTopUpRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewUserManaTopUpRepo()
	agg, err := user_mana_topup.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "mana.user_topup.standard_v1",
		2500, 1990, "SGD", repoSessionID, repoCheckoutURL, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil || got.ManaUnits != 2500 {
		t.Errorf("GetByID err=%v", err)
	}
	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("wrong tenant err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
	bySess, err := repo.GetByStripeSessionID(ctx, repoSessionID)
	if err != nil || bySess.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSessionID err=%v", err)
	}
}

// -----------------------------------------------------------------------------
// TenantAddonPurchaseRepo — remaining lookup paths
// -----------------------------------------------------------------------------

func TestTenantAddonPurchaseRepo_GetByStripeSessionID(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()
	a := tapFixture(t, "")
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByStripeSessionID(context.Background(), tapTestStripeSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if got.PurchaseID != a.PurchaseID {
		t.Errorf("wrong purchase: %q", got.PurchaseID)
	}
	if _, err := repo.GetByStripeSessionID(context.Background(), "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(context.Background(), "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
}

func TestTenantAddonPurchaseRepo_GetActiveByTenantAndAddonCode(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()

	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), "", tapTestAddonCode); !errors.Is(err, tap.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), tapTestTenantID, ""); !errors.Is(err, tap.ErrAddonCodeRequired) {
		t.Errorf("blank addon err=%v", err)
	}
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), tapTestTenantID, tapTestAddonCode); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("no rows err=%v, want ErrNotFound", err)
	}

	// checkout_started row must be skipped (only payment_captured counts).
	started := tapFixture(t, "a")
	if err := repo.Save(context.Background(), started); err != nil {
		t.Fatalf("Save started: %v", err)
	}
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), tapTestTenantID, tapTestAddonCode); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("no captured row err=%v, want ErrNotFound", err)
	}

	// Two captured rows; the most-recently-updated one wins.
	older := tapFixture(t, "b")
	_ = older.MarkPaymentCaptured("pi_older", "ch_older", 4900, repoNow().Add(-2*time.Hour))
	if err := repo.Save(context.Background(), older); err != nil {
		t.Fatalf("Save older: %v", err)
	}
	newer := tapFixture(t, "c")
	_ = newer.MarkPaymentCaptured("pi_newer", "ch_newer", 4900, repoNow().Add(-time.Hour))
	if err := repo.Save(context.Background(), newer); err != nil {
		t.Fatalf("Save newer: %v", err)
	}
	got, err := repo.GetActiveByTenantAndAddonCode(context.Background(), tapTestTenantID, tapTestAddonCode)
	if err != nil {
		t.Fatalf("GetActiveByTenantAndAddonCode: %v", err)
	}
	if got.PurchaseID != newer.PurchaseID {
		t.Errorf("got %q, want most-recent %q", got.PurchaseID, newer.PurchaseID)
	}

	// Different addon code must not match.
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), tapTestTenantID, "other_addon"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("other addon err=%v, want ErrNotFound", err)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeScheduleID(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo()

	if _, err := repo.GetByStripeScheduleID(context.Background(), ""); !errors.Is(err, tap.ErrStripeScheduleRequired) {
		t.Errorf("blank schedule err=%v", err)
	}
	if _, err := repo.GetByStripeScheduleID(context.Background(), "sched_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing schedule err=%v", err)
	}

	a := tapFixture(t, "")
	if err := a.ApplySchedule("sched_test_0001", "tier2", repoNow().Add(30*24*time.Hour)); err != nil {
		t.Fatalf("ApplySchedule: %v", err)
	}
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByStripeScheduleID(context.Background(), "sched_test_0001")
	if err != nil {
		t.Fatalf("GetByStripeScheduleID: %v", err)
	}
	if got.PurchaseID != a.PurchaseID {
		t.Errorf("wrong purchase: %q", got.PurchaseID)
	}

	// Eviction: ReleaseSchedule clears the schedule id; a re-Save must drop
	// the stale bySchedID mapping so a duplicate webhook cannot re-hit it.
	a.ReleaseSchedule()
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("Save after release: %v", err)
	}
	if _, err := repo.GetByStripeScheduleID(context.Background(), "sched_test_0001"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("stale schedule still resolvable: %v", err)
	}
}

// -----------------------------------------------------------------------------
// DisputeRepo — remaining branches
// -----------------------------------------------------------------------------

func TestDisputeRepo_InsertDuplicateStripeDisputeID(t *testing.T) {
	t.Parallel()
	repo := NewDisputeRepo()
	d1 := newDispute(t, "dispute-dup-1", "dp_dup_0001", webhook_event.AggregateCoursePurchase, repoPurchaseID)
	if err := repo.Insert(context.Background(), d1); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	d2 := newDispute(t, "dispute-dup-2", "dp_dup_0001", webhook_event.AggregateCoursePurchase, repoPurchaseID)
	if err := repo.Insert(context.Background(), d2); !errors.Is(err, dispute.ErrAlreadyExists) {
		t.Errorf("duplicate stripe dispute id err=%v, want ErrAlreadyExists", err)
	}
}

func TestDisputeRepo_InsertWithoutStripeDisputeID(t *testing.T) {
	t.Parallel()
	repo := NewDisputeRepo()
	d := newDispute(t, "dispute-nostripe", "dp_placeholder", webhook_event.AggregateCoursePurchase, repoPurchaseID)
	// dispute.New validates stripe_dispute_id non-empty; force-empty here to
	// exercise the Insert path where the Stripe handle is absent.
	d.StripeDisputeID = ""
	if err := repo.Insert(context.Background(), d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, err := repo.GetByID(context.Background(), d.TenantID, "dispute-nostripe")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.DisputeID != "dispute-nostripe" {
		t.Errorf("DisputeID=%s", got.DisputeID)
	}
	if _, err := repo.GetByID(context.Background(), "other-tenant", "dispute-nostripe"); !errors.Is(err, dispute.ErrNotFound) {
		t.Errorf("wrong tenant err=%v", err)
	}
}

func TestDisputeRepo_SaveIsUpsert(t *testing.T) {
	t.Parallel()
	repo := NewDisputeRepo()
	d := newDispute(t, "dispute-save-1", "dp_save_0001", webhook_event.AggregateCoursePurchase, repoPurchaseID)
	if err := repo.Save(context.Background(), d); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Re-Save with same stripe dispute id — index must not duplicate.
	if err := repo.Save(context.Background(), d); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	list, err := repo.ListByPurchase(context.Background(), d.TenantID, webhook_event.AggregateCoursePurchase, repoPurchaseID)
	if err != nil {
		t.Fatalf("ListByPurchase: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("ListByPurchase len=%d, want 1 (no duplicate index entries)", len(list))
	}
	if _, err := repo.GetByStripeDisputeID(context.Background(), ""); !errors.Is(err, dispute.ErrStripeDisputeRequired) {
		t.Errorf("blank stripe dispute id err=%v", err)
	}
	if _, err := repo.GetByStripeDisputeID(context.Background(), "dp_missing"); !errors.Is(err, dispute.ErrNotFound) {
		t.Errorf("missing stripe dispute id err=%v", err)
	}
	byStripe, err := repo.GetByStripeDisputeID(context.Background(), "dp_save_0001")
	if err != nil || byStripe.DisputeID != "dispute-save-1" {
		t.Errorf("GetByStripeDisputeID err=%v", err)
	}
	// ListByPurchase filters by tenant: other tenant sees nothing.
	if list, _ := repo.ListByPurchase(context.Background(), "other-tenant", webhook_event.AggregateCoursePurchase, repoPurchaseID); len(list) != 0 {
		t.Errorf("other-tenant list len=%d, want 0", len(list))
	}
}

func TestIdentityKycFeeRepo_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := NewIdentityKycFeeRepo()
	agg, err := identity_kyc_fee.New(
		repoPurchaseID, repoTenantID, repoLearnerGCID, "manual_id_doc",
		999, "USD", repoSessionID, repoCheckoutURL, repoNow(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Save(ctx, nil); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("Save(nil) err=%v", err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.GetByID(ctx, repoTenantID, repoPurchaseID)
	if err != nil || got.KYCDocType != "manual_id_doc" {
		t.Errorf("GetByID err=%v", err)
	}
	if _, err := repo.GetByID(ctx, "other-tenant", repoPurchaseID); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("wrong tenant err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "  "); !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("blank session err=%v", err)
	}
	if _, err := repo.GetByStripeSessionID(ctx, "cs_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("missing session err=%v", err)
	}
	bySess, err := repo.GetByStripeSessionID(ctx, repoSessionID)
	if err != nil || bySess.PurchaseID != repoPurchaseID {
		t.Errorf("GetByStripeSessionID err=%v", err)
	}
}
