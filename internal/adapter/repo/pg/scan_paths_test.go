// scan_paths_test.go — scan-path coverage for the 4 non-course Purchase
// repos + webhook_event repo. Mirrors course_purchase_test.go's scanRow
// helper for each aggregate's column shape.
package pg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// -----------------------------------------------------------------------------
// ApplicationPaymentRepo — GetByID scan path
// -----------------------------------------------------------------------------

func scanApplicationPaymentRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "01970000-0000-7000-d000-000000000005"
	*(dest[4].(*string)) = testCourseID
	*(dest[5].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[6].(*int64)) = 99900
	*(dest[7].(*int64)) = 0
	*(dest[8].(*int64)) = 0
	*(dest[9].(*string)) = "SGD"
	*(dest[10].(*string)) = testSessionID
	*(dest[18].(*time.Time)) = now
	*(dest[23].(*time.Time)) = now
	*(dest[24].(*time.Time)) = now
	return nil
}

func TestApplicationPaymentRepo_GetByID_ScansCorrectly(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanApplicationPaymentRow
	repo := NewApplicationPaymentRepo(newStubTxRunner(q))
	ap, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if ap.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", ap.PurchaseID, testPurchaseID)
	}
	if ap.ApplicationID != "01970000-0000-7000-d000-000000000005" {
		t.Errorf("ApplicationID mismatch: %s", ap.ApplicationID)
	}
	if ap.CourseID != testCourseID {
		t.Errorf("CourseID=%s, want %s", ap.CourseID, testCourseID)
	}
}

func TestApplicationPaymentRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanApplicationPaymentRow
	repo := NewApplicationPaymentRepo(newStubTxRunner(q))
	ap, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if ap.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", ap.PurchaseID, testPurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
}

func TestApplicationPaymentRepo_GetByID_NoRows_ReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo := NewApplicationPaymentRepo(newStubTxRunner(q))
	_, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("err=%v, want shared.ErrNotFound", err)
	}
}

func TestApplicationPaymentRepo_GetByStripeSessionID_EmptyReturnsErrRequired(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewApplicationPaymentRepo(newStubTxRunner(q))
	_, err := repo.GetByStripeSessionID(context.Background(), "")
	if !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("err=%v, want shared.ErrStripeSessionRequired", err)
	}
}

// -----------------------------------------------------------------------------
// FamiliarEggPurchaseRepo — GetByID scan path
// -----------------------------------------------------------------------------

func scanFamiliarEggPurchaseRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "egg.standard.v1"
	// dest[4] suggested_focal_atom_id is *string; leave nil.
	*(dest[5].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[6].(*int64)) = 1900
	*(dest[7].(*int64)) = 0
	*(dest[8].(*int64)) = 0
	*(dest[9].(*string)) = "SGD"
	*(dest[10].(*string)) = testSessionID
	*(dest[18].(*bool)) = false
	*(dest[21].(*time.Time)) = now
	*(dest[27].(*time.Time)) = now
	*(dest[28].(*time.Time)) = now
	return nil
}

func TestFamiliarEggPurchaseRepo_GetByID_ScansCorrectly(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanFamiliarEggPurchaseRow
	repo := NewFamiliarEggPurchaseRepo(newStubTxRunner(q))
	ep, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if ep.EggSKU != "egg.standard.v1" {
		t.Errorf("EggSKU=%s, want egg.standard.v1", ep.EggSKU)
	}
}

func TestFamiliarEggPurchaseRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanFamiliarEggPurchaseRow
	repo := NewFamiliarEggPurchaseRepo(newStubTxRunner(q))
	ep, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if ep.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", ep.PurchaseID, testPurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
}

// -----------------------------------------------------------------------------
// TenantManaTopUpRepo — GetByID scan path
// -----------------------------------------------------------------------------

func scanTenantManaTopUpRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "mana.topup.5000.v1"
	*(dest[4].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[5].(*int64)) = 2900
	*(dest[6].(*int64)) = 0
	*(dest[7].(*int64)) = 0
	*(dest[8].(*string)) = "SGD"
	*(dest[9].(*int64)) = 5000
	*(dest[10].(*int64)) = 0
	*(dest[11].(*string)) = testSessionID
	*(dest[19].(*time.Time)) = now
	*(dest[24].(*time.Time)) = now
	*(dest[25].(*time.Time)) = now
	return nil
}

func TestTenantManaTopUpRepo_GetByID_ScansCorrectly(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTenantManaTopUpRow
	repo := NewTenantManaTopUpRepo(newStubTxRunner(q))
	m, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if m.SKU != "mana.topup.5000.v1" {
		t.Errorf("SKU=%s, want mana.topup.5000.v1", m.SKU)
	}
	if m.ManaUnits != 5000 {
		t.Errorf("ManaUnits=%d, want 5000", m.ManaUnits)
	}
	if m.AdminGCID() != testLearnerGCID {
		t.Errorf("AdminGCID=%s, want %s", m.AdminGCID(), testLearnerGCID)
	}
}

func TestTenantManaTopUpRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTenantManaTopUpRow
	repo := NewTenantManaTopUpRepo(newStubTxRunner(q))
	m, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if m.SKU != "mana.topup.5000.v1" {
		t.Errorf("SKU=%s, want mana.topup.5000.v1", m.SKU)
	}
	assertRLSApplied(t, q, testTenantID)
}

// -----------------------------------------------------------------------------
// UserSubscriptionRepo — GetByID scan path
// -----------------------------------------------------------------------------

func scanUserSubscriptionRow(state string) func(dest ...any) error {
	return func(dest ...any) error {
		now := fixedTime()
		*(dest[0].(*string)) = testPurchaseID
		*(dest[1].(*string)) = testTenantID
		*(dest[2].(*string)) = testLearnerGCID
		*(dest[3].(*string)) = "familiar.standard.monthly.v1"
		*(dest[4].(*string)) = "monthly"
		*(dest[5].(*string)) = state
		*(dest[6].(*int64)) = 999
		*(dest[7].(*int64)) = 0
		*(dest[8].(*int64)) = 0
		*(dest[9].(*string)) = "SGD"
		// 10..19 nullable *string; leave nil.
		// 20..21 nullable *time.Time; leave nil.
		// 22..24 nullable *string; leave nil.
		*(dest[25].(*bool)) = false
		// 26 *time.Time; leave nil.
		*(dest[27].(*time.Time)) = now
		// 28..31 nullable *time.Time; leave nil.
		// 32..36 nullable *time.Time; leave nil.
		*(dest[37].(*time.Time)) = now
		*(dest[38].(*time.Time)) = now
		return nil
	}
}

func TestUserSubscriptionRepo_GetByID_PaymentState(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserSubscriptionRow(string(shared.StateCheckoutStarted))
	repo := NewUserSubscriptionRepo(newStubTxRunner(q))
	u, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if u.State != shared.StateCheckoutStarted {
		t.Errorf("State=%s, want %s", u.State, shared.StateCheckoutStarted)
	}
	if u.LifecycleState != "" {
		t.Errorf("LifecycleState=%s, want empty (payment-state row)", u.LifecycleState)
	}
}

func TestUserSubscriptionRepo_GetByID_LifecycleState(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserSubscriptionRow("active")
	repo := NewUserSubscriptionRepo(newStubTxRunner(q))
	u, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if u.LifecycleState != sub.SubStateActive {
		t.Errorf("LifecycleState=%s, want active", u.LifecycleState)
	}
}

func TestUserSubscriptionRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserSubscriptionRow(string(shared.StateCheckoutStarted))
	repo := NewUserSubscriptionRepo(newStubTxRunner(q))
	u, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if u.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", u.PurchaseID, testPurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
}

func TestUserSubscriptionRepo_GetByStripeSubscriptionID_HappyPath(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserSubscriptionRow(string(shared.StateCheckoutStarted))
	repo := NewUserSubscriptionRepo(newStubTxRunner(q))
	u, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_test_xxx")
	if err != nil {
		t.Fatalf("GetByStripeSubscriptionID: %v", err)
	}
	if u.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", u.PurchaseID, testPurchaseID)
	}
}

// -----------------------------------------------------------------------------
// WebhookEventRepo — Get scan path
// -----------------------------------------------------------------------------

func scanWebhookEventRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = "evt_test_aaa"
	*(dest[1].(*string)) = "checkout.session.completed"
	*(dest[2].(*time.Time)) = now
	// dest[3] processed_at *time.Time; leave nil.
	// dest[4] processing_error *string; leave nil.
	// dest[5] target_aggregate_type *string; leave nil.
	// dest[6] target_purchase_id *string; leave nil.
	return nil
}

func TestWebhookEventRepo_GetByEventID_HappyPath(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanWebhookEventRow
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	w, err := repo.GetByEventID(context.Background(), "evt_test_aaa")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if w.EventID != "evt_test_aaa" {
		t.Errorf("EventID=%s, want evt_test_aaa", w.EventID)
	}
}

func TestWebhookEventRepo_GetByEventID_EmptyReturnsErr(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	_, err := repo.GetByEventID(context.Background(), "")
	if !errors.Is(err, wh.ErrEventIDRequired) {
		t.Errorf("err=%v, want wh.ErrEventIDRequired", err)
	}
}

func TestWebhookEventRepo_GetByEventID_NoRows(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	_, err := repo.GetByEventID(context.Background(), "evt_x")
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("err=%v, want shared.ErrNotFound", err)
	}
}

func TestWebhookEventRepo_MarkFailed(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	if err := repo.MarkFailed(context.Background(), "evt_x", "stripe verify failed"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if len(q.execs) != 1 {
		t.Fatalf("expected 1 Exec, got %d", len(q.execs))
	}
	if !strings.Contains(q.execs[0].sql, "UPDATE stripe_webhook_events") {
		t.Errorf("update sql does not target stripe_webhook_events: %q", q.execs[0].sql)
	}
}

// -----------------------------------------------------------------------------
// UserManaTopUpRepo (6th aggregate — Stage A.5) — GetByID scan path
// -----------------------------------------------------------------------------

func scanUserManaTopUpRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "mana.user_topup.standard_v1"
	*(dest[4].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[5].(*int64)) = 1990
	*(dest[6].(*int64)) = 0
	*(dest[7].(*int64)) = 0
	*(dest[8].(*string)) = "SGD"
	*(dest[9].(*int64)) = 2500
	*(dest[10].(*int64)) = 0
	*(dest[11].(*string)) = testSessionID
	*(dest[19].(*time.Time)) = now
	*(dest[24].(*time.Time)) = now
	*(dest[25].(*time.Time)) = now
	return nil
}

func TestUserManaTopUpRepo_GetByID_ScansCorrectly(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserManaTopUpRow
	repo := NewUserManaTopUpRepo(newStubTxRunner(q))
	u, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if u.SKU != "mana.user_topup.standard_v1" {
		t.Errorf("SKU=%s, want mana.user_topup.standard_v1", u.SKU)
	}
	if u.ManaUnits != 2500 {
		t.Errorf("ManaUnits=%d, want 2500", u.ManaUnits)
	}
	if u.LearnerGCID != testLearnerGCID {
		t.Errorf("LearnerGCID=%s, want %s", u.LearnerGCID, testLearnerGCID)
	}
}

func TestUserManaTopUpRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserManaTopUpRow
	repo := NewUserManaTopUpRepo(newStubTxRunner(q))
	// THE WS-2 e2e blocker: the per-user mana top-up credit. The webhook
	// dispatch stamps the tenant from session metadata onto ctx; this
	// lookup must apply RLS with it (mirror course_purchase) or the pooled
	// connection's '' GUC reset-value casts to ''::uuid → 22P02 → the
	// payment.captured event 500s and the wallet is never credited.
	u, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if u.SKU != "mana.user_topup.standard_v1" {
		t.Errorf("SKU mismatch: %s", u.SKU)
	}
	assertRLSApplied(t, q, testTenantID)
}

func TestUserManaTopUpRepo_NoTx_Errors(t *testing.T) {
	t.Parallel()
	repo := NewUserManaTopUpRepo(nil)
	if _, err := repo.GetByID(context.Background(), testTenantID, testPurchaseID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("err=%v, want ErrNotImplemented", err)
	}
}

// -----------------------------------------------------------------------------
// IdentityKycFeeRepo (7th aggregate — Stage A.5) — GetByID scan path
// -----------------------------------------------------------------------------

func scanIdentityKycFeeRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "manual_id_doc"
	*(dest[4].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[5].(*int64)) = 999
	*(dest[6].(*int64)) = 0
	*(dest[7].(*int64)) = 0
	*(dest[8].(*string)) = "USD"
	*(dest[9].(*string)) = testSessionID
	*(dest[17].(*time.Time)) = now
	*(dest[22].(*time.Time)) = now
	*(dest[23].(*time.Time)) = now
	return nil
}

func TestIdentityKycFeeRepo_GetByID_ScansCorrectly(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanIdentityKycFeeRow
	repo := NewIdentityKycFeeRepo(newStubTxRunner(q))
	k, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if k.KYCDocType != "manual_id_doc" {
		t.Errorf("KYCDocType=%s, want manual_id_doc", k.KYCDocType)
	}
	if k.AmountCents != 999 {
		t.Errorf("AmountCents=%d, want 999", k.AmountCents)
	}
}

func TestIdentityKycFeeRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanIdentityKycFeeRow
	repo := NewIdentityKycFeeRepo(newStubTxRunner(q))
	k, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if k.KYCDocType != "manual_id_doc" {
		t.Errorf("KYCDocType=%s, want manual_id_doc", k.KYCDocType)
	}
	assertRLSApplied(t, q, testTenantID)
}

func TestIdentityKycFeeRepo_NoTx_Errors(t *testing.T) {
	t.Parallel()
	repo := NewIdentityKycFeeRepo(nil)
	if _, err := repo.GetByID(context.Background(), testTenantID, testPurchaseID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("err=%v, want ErrNotImplemented", err)
	}
}
