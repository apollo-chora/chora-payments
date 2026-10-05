package pg

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// CHO-1738 — Postgres adapter tests for tenant_addon_purchase (8th
// aggregate). Mirrors course_purchase_test.go shape: stubQuerier +
// stubTxRunner, hermetic (no live DB). 26-arg UPSERT (vs course's 24)
// because addon_plan_id + addon_code + tier_code displace course_id
// in the column list AND the aggregate carries 3 fields vs 1.
const (
	testAdminGCID    = "01970000-0000-7000-d000-aaaaaaaaaaaa"
	testAddonPlanID  = "01970000-0000-7000-e000-bbbbbbbbbbbb"
	testAddonCode    = "knowledge_graph"
	testTierCode     = "pro"
	testTapSessionID = "cs_test_tap_session"
)

func mkTenantAddonPurchase(t *testing.T) *tap.TenantAddonPurchase {
	t.Helper()
	a, err := tap.New(
		testPurchaseID, testTenantID, testAdminGCID,
		testAddonPlanID, testAddonCode, testTierCode,
		4900, "SGD",
		testTapSessionID, "https://checkout.stripe.com/c/pay/cs_test_tap",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	return a
}

func TestTenantAddonPurchaseRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo(nil)
	a := mkTenantAddonPurchase(t)
	if err := repo.Save(context.Background(), a); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Save err=%v, want ErrNotImplemented", err)
	}
	if _, err := repo.GetByID(context.Background(), testTenantID, testPurchaseID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("GetByID err=%v, want ErrNotImplemented", err)
	}
	if _, err := repo.GetByStripeSessionID(context.Background(), testTapSessionID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("GetByStripeSessionID err=%v, want ErrNotImplemented", err)
	}
}

func TestTenantAddonPurchaseRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	a := mkTenantAddonPurchase(t)

	ctx := withTenant(context.Background(), testTenantID)
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("Save: %v", err)
	}

	assertRLSApplied(t, q, testTenantID)

	if len(q.execs) < 2 {
		t.Fatalf("expected >=2 Exec calls (rls + upsert), got %d", len(q.execs))
	}
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO tenant_addon_purchases") {
		t.Errorf("upsert sql does not target tenant_addon_purchases: %q", upsert.sql)
	}
	if !strings.Contains(upsert.sql, "ON CONFLICT (purchase_id) DO UPDATE") {
		t.Errorf("upsert sql missing ON CONFLICT clause: %q", upsert.sql)
	}
	// CHO-1761 extended the column list with 5 Subscription correlation
	// fields → 31 args; CHO-1772 added 3 SubscriptionSchedule fields
	// (stripe_subscription_schedule_id, scheduled_tier_code,
	// scheduled_effective_at) → 34 total args.
	if len(upsert.args) != 34 {
		t.Errorf("upsert args=%d, want 34", len(upsert.args))
	}
	if upsert.args[0] != testPurchaseID {
		t.Errorf("upsert arg[0] purchase_id = %v, want %s", upsert.args[0], testPurchaseID)
	}
	if upsert.args[2] != testAdminGCID {
		t.Errorf("upsert arg[2] admin_gcid = %v, want %s", upsert.args[2], testAdminGCID)
	}
	if upsert.args[3] != testAddonPlanID {
		t.Errorf("upsert arg[3] addon_plan_id = %v, want %s", upsert.args[3], testAddonPlanID)
	}
	if upsert.args[4] != testAddonCode {
		t.Errorf("upsert arg[4] addon_code = %v, want %s", upsert.args[4], testAddonCode)
	}
	if upsert.args[5] != testTierCode {
		t.Errorf("upsert arg[5] tier_code = %v, want %s", upsert.args[5], testTierCode)
	}
	if upsert.args[6] != string(shared.StateCheckoutStarted) {
		t.Errorf("upsert arg[6] state = %v, want %s", upsert.args[6], shared.StateCheckoutStarted)
	}
}

func TestTenantAddonPurchaseRepo_Save_NilAggregate_ReturnsErrInvalidAggregate(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	err := repo.Save(withTenant(context.Background(), testTenantID), nil)
	if !errors.Is(err, ErrInvalidAggregate) {
		t.Errorf("Save(nil) err=%v, want ErrInvalidAggregate", err)
	}
}

// scanTenantAddonPurchaseRow seeds a 34-column row scan with a minimal
// TenantAddonPurchase shape. Column order mirrors tenantAddonPurchaseCols
// (CHO-1761 added 5 Subscription correlation columns; CHO-1772 added 3
// SubscriptionSchedule correlation columns at the tail).
func scanTenantAddonPurchaseRow(dest ...any) error {
	if len(dest) != 34 {
		return fmt.Errorf("scan: dest len=%d, want 34", len(dest))
	}
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testAdminGCID
	*(dest[3].(*string)) = testAddonPlanID
	*(dest[4].(*string)) = testAddonCode
	*(dest[5].(*string)) = testTierCode
	*(dest[6].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[7].(*int64)) = 4900
	*(dest[8].(*int64)) = 0
	*(dest[9].(*int64)) = 0
	*(dest[10].(*string)) = "SGD"
	*(dest[11].(*string)) = testTapSessionID
	// 12..16 are nullable *string pointers; leave nil.
	// 18 is nullable *string refund_reason; leave nil.
	*(dest[19].(*time.Time)) = now
	// 20..23 are nullable *time.Time pointers; leave nil.
	*(dest[24].(*time.Time)) = now
	*(dest[25].(*time.Time)) = now
	// 26..28 are nullable *string Subscription correlation pointers; leave nil.
	// 29..30 are nullable *time.Time period boundaries; leave nil.
	// 31..32 are nullable *string Schedule correlation pointers; leave nil.
	// 33 is nullable *time.Time scheduled_effective_at; leave nil.
	return nil
}

func TestTenantAddonPurchaseRepo_GetByID_AppliesRLSAndSelectSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTenantAddonPurchaseRow
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	a, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)

	if len(q.queries) != 1 {
		t.Fatalf("expected 1 QueryRow call, got %d", len(q.queries))
	}
	if !strings.Contains(q.queries[0].sql, "FROM tenant_addon_purchases") {
		t.Errorf("query sql does not target tenant_addon_purchases: %q", q.queries[0].sql)
	}
	if a.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", a.PurchaseID, testPurchaseID)
	}
	if a.AddonPlanID != testAddonPlanID {
		t.Errorf("AddonPlanID=%s, want %s", a.AddonPlanID, testAddonPlanID)
	}
	if a.AddonCode != testAddonCode {
		t.Errorf("AddonCode=%s, want %s", a.AddonCode, testAddonCode)
	}
	if a.TierCode != testTierCode {
		t.Errorf("TierCode=%s, want %s", a.TierCode, testTierCode)
	}
	if a.AdminGCID() != testAdminGCID {
		t.Errorf("AdminGCID=%s, want %s", a.AdminGCID(), testAdminGCID)
	}
}

func TestTenantAddonPurchaseRepo_GetByID_NoRows_ReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error {
		return errors.New("sql: no rows in result set")
	}
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	_, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("err=%v, want shared.ErrNotFound", err)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSessionID_Empty_ReturnsErrStripeSessionRequired(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	_, err := repo.GetByStripeSessionID(context.Background(), "")
	if !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("err=%v, want shared.ErrStripeSessionRequired", err)
	}
}

// -----------------------------------------------------------------------------
// CHO-1761 — Subscription correlation columns in upsert + GetByStripeSubscriptionID
// -----------------------------------------------------------------------------

func TestTenantAddonPurchaseRepo_Save_PopulatesSubscriptionColumns(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	a := mkTenantAddonPurchase(t)
	periodStart := fixedTime()
	periodEnd := periodStart.AddDate(0, 1, 0)
	if err := a.ApplyStripeSubscriptionState(
		"cus_test_pg", "sub_test_pg", tap.StatusActive,
		&periodStart, &periodEnd,
	); err != nil {
		t.Fatalf("ApplyStripeSubscriptionState: %v", err)
	}
	ctx := withTenant(context.Background(), testTenantID)
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	upsert := q.execs[len(q.execs)-1]
	if len(upsert.args) != 34 {
		t.Fatalf("upsert args=%d, want 34", len(upsert.args))
	}
	if upsert.args[26] != "cus_test_pg" {
		t.Errorf("upsert arg[26] stripe_customer_id = %v, want cus_test_pg", upsert.args[26])
	}
	if upsert.args[27] != "sub_test_pg" {
		t.Errorf("upsert arg[27] stripe_subscription_id = %v, want sub_test_pg", upsert.args[27])
	}
	if upsert.args[28] != "active" {
		t.Errorf("upsert arg[28] sub_status = %v, want active", upsert.args[28])
	}
	if !strings.Contains(upsert.sql, "stripe_subscription_id") {
		t.Errorf("upsert sql missing stripe_subscription_id column: %q", upsert.sql)
	}
	// The ON CONFLICT SET clause's alignment whitespace shifts when
	// new (longer) column names are added — match the column→EXCLUDED
	// pair via a whitespace-tolerant regex instead of an exact substring.
	if !regexp.MustCompile(`stripe_subscription_id\s*=\s*EXCLUDED\.stripe_subscription_id`).MatchString(upsert.sql) {
		t.Errorf("upsert sql missing ON CONFLICT update for stripe_subscription_id: %q", upsert.sql)
	}
}

// Legacy (pre-CHO-1762) rows write all 5 new columns as NULL.
func TestTenantAddonPurchaseRepo_Save_LegacyRow_WritesNullSubscriptionColumns(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	a := mkTenantAddonPurchase(t) // ApplyStripeSubscriptionState NOT called
	ctx := withTenant(context.Background(), testTenantID)
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("Save: %v", err)
	}
	upsert := q.execs[len(q.execs)-1]
	// nullStr("") returns nil for the wire — args[26..28] should be nil.
	for i := 26; i <= 30; i++ {
		if upsert.args[i] != nil {
			t.Errorf("upsert arg[%d] = %v, want nil (legacy row)", i, upsert.args[i])
		}
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_Empty_Rejects(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	_, err := repo.GetByStripeSubscriptionID(context.Background(), "")
	if !errors.Is(err, tap.ErrStripeSubscriptionRequired) {
		t.Errorf("err=%v, want tap.ErrStripeSubscriptionRequired", err)
	}
	if len(q.queries) != 0 {
		t.Errorf("expected 0 QueryRow calls on empty subscriptionID; got %d", len(q.queries))
	}
}

// CHO-1788 — webhooks dispatching `customer.subscription.updated` were
// 500ing with SQLSTATE 22P02 because the tenant_isolation policy on
// tenant_addon_purchases evaluates `current_setting('chora.tenant_id',
// true)::uuid` for every row touched even when the WHERE filters by a
// globally-unique Stripe ID. The fix is to SET LOCAL chora.tenant_id
// via rls.ApplySession before the SELECT — the dispatcher already
// resolves tenant from stripe_customer ahead of this lookup. So the
// repo contract now requires tenant on context (renamed from
// "_SkipsRLS_" to make that explicit).
func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_EnforcesRLS_HitsCorrectSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTenantAddonPurchaseRow
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	ctx := withTenant(context.Background(), testTenantID)
	a, err := repo.GetByStripeSubscriptionID(ctx, "sub_test_pg")
	if err != nil {
		t.Fatalf("GetByStripeSubscriptionID: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	if len(q.queries) != 1 {
		t.Fatalf("expected 1 QueryRow call, got %d", len(q.queries))
	}
	if !strings.Contains(q.queries[0].sql, "WHERE stripe_subscription_id = $1") {
		t.Errorf("query sql wrong WHERE clause: %q", q.queries[0].sql)
	}
	if a.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", a.PurchaseID, testPurchaseID)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_NoRows_ReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error {
		return errors.New("sql: no rows in result set")
	}
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	ctx := withTenant(context.Background(), testTenantID)
	_, err := repo.GetByStripeSubscriptionID(ctx, "sub_unknown")
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("err=%v, want shared.ErrNotFound", err)
	}
}

// CHO-1788 — guard the new contract: without tenant on context the
// lookup must surface ErrNoTenantContext rather than running the SELECT
// and hitting the 22P02 cast error mid-policy.
func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_NoTenantCtx_ReturnsRLSErr(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTenantAddonPurchaseRow
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	_, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_test_pg")
	if err == nil {
		t.Fatalf("expected ErrNoTenantContext-wrapped error, got nil")
	}
	if !strings.Contains(err.Error(), "tenant_id missing on context") {
		t.Errorf("err=%v, want ErrNoTenantContext", err)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeSubscriptionID_NilTxRunner(t *testing.T) {
	t.Parallel()
	repo := NewTenantAddonPurchaseRepo(nil)
	if _, err := repo.GetByStripeSubscriptionID(context.Background(), "sub_x"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("err=%v, want ErrNotImplemented", err)
	}
}

// CHO-1788 — session-id lookup now applies the tenant_isolation policy
// pre-emptively (same root-cause as the subscription-id lookup; the
// table-level policy fires regardless of WHERE clause uniqueness).
func TestTenantAddonPurchaseRepo_GetByStripeSessionID_EnforcesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTenantAddonPurchaseRow
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	ctx := withTenant(context.Background(), testTenantID)
	a, err := repo.GetByStripeSessionID(ctx, testTapSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if a.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", a.PurchaseID, testPurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
	if len(q.queries) != 1 {
		t.Errorf("expected 1 QueryRow call, got %d", len(q.queries))
	}
}
