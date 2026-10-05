package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
)

const (
	testTenantID    = "01970000-0000-7000-8000-000000000001"
	testLearnerGCID = "01970000-0000-7000-a000-000000000002"
	testPurchaseID  = "01970000-0000-7000-b000-000000000003"
	testCourseID    = "01970000-0000-7000-c000-000000000004"
	testSessionID   = "cs_test_abc123"
)

func mkCoursePurchase(t *testing.T) *coursepurchase.CoursePurchase {
	t.Helper()
	cp, err := coursepurchase.New(
		testPurchaseID, testTenantID, testLearnerGCID, testCourseID,
		99900, "SGD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test_abc123",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("coursepurchase.New: %v", err)
	}
	return cp
}

func TestCoursePurchaseRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	repo := NewCoursePurchaseRepo(nil)
	cp := mkCoursePurchase(t)
	if err := repo.Save(context.Background(), cp); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Save err=%v, want ErrNotImplemented", err)
	}
	if _, err := repo.GetByID(context.Background(), testTenantID, testPurchaseID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("GetByID err=%v, want ErrNotImplemented", err)
	}
	if _, err := repo.GetByStripeSessionID(context.Background(), testSessionID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("GetByStripeSessionID err=%v, want ErrNotImplemented", err)
	}
}

func TestCoursePurchaseRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	cp := mkCoursePurchase(t)

	ctx := withTenant(context.Background(), testTenantID)
	if err := repo.Save(ctx, cp); err != nil {
		t.Fatalf("Save: %v", err)
	}

	assertRLSApplied(t, q, testTenantID)

	if len(q.execs) < 2 {
		t.Fatalf("expected >=2 Exec calls (rls + upsert), got %d", len(q.execs))
	}
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO course_purchases") {
		t.Errorf("upsert sql does not target course_purchases: %q", upsert.sql)
	}
	if !strings.Contains(upsert.sql, "ON CONFLICT (purchase_id) DO UPDATE") {
		t.Errorf("upsert sql missing ON CONFLICT clause: %q", upsert.sql)
	}
	if len(upsert.args) != 24 {
		t.Errorf("upsert args=%d, want 24", len(upsert.args))
	}
	if upsert.args[0] != testPurchaseID {
		t.Errorf("upsert arg[0] purchase_id = %v, want %s", upsert.args[0], testPurchaseID)
	}
	if upsert.args[3] != testCourseID {
		t.Errorf("upsert arg[3] course_id = %v, want %s", upsert.args[3], testCourseID)
	}
	if upsert.args[4] != string(shared.StateCheckoutStarted) {
		t.Errorf("upsert arg[4] state = %v, want %s", upsert.args[4], shared.StateCheckoutStarted)
	}
}

func TestCoursePurchaseRepo_Save_NilAggregate_ReturnsErrInvalidAggregate(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	err := repo.Save(withTenant(context.Background(), testTenantID), nil)
	if !errors.Is(err, ErrInvalidAggregate) {
		t.Errorf("Save(nil) err=%v, want ErrInvalidAggregate", err)
	}
}

// scanCoursePurchaseRow is a test helper that seeds a row scan with a
// minimal CoursePurchase shape. 24 column order mirrors coursePurchaseCols.
func scanCoursePurchaseRow(dest ...any) error {
	if len(dest) != 24 {
		return fmt.Errorf("scan: dest len=%d, want 24", len(dest))
	}
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = testCourseID
	*(dest[4].(*string)) = string(shared.StateCheckoutStarted)
	*(dest[5].(*int64)) = 99900
	*(dest[6].(*int64)) = 0
	*(dest[7].(*int64)) = 0
	*(dest[8].(*string)) = "SGD"
	*(dest[9].(*string)) = testSessionID
	// 10..16 are nullable *string pointers; leave nil.
	// 17 is nullable *string refund_reason; leave nil.
	*(dest[17].(*time.Time)) = now
	// 18..21 are nullable *time.Time pointers; leave nil.
	*(dest[22].(*time.Time)) = now
	*(dest[23].(*time.Time)) = now
	return nil
}

func TestCoursePurchaseRepo_GetByID_AppliesRLSAndSelectSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanCoursePurchaseRow
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	cp, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)

	if len(q.queries) != 1 {
		t.Fatalf("expected 1 QueryRow call, got %d", len(q.queries))
	}
	if !strings.Contains(q.queries[0].sql, "FROM course_purchases") {
		t.Errorf("query sql does not target course_purchases: %q", q.queries[0].sql)
	}
	if cp.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", cp.PurchaseID, testPurchaseID)
	}
	if cp.CourseID != testCourseID {
		t.Errorf("CourseID=%s, want %s", cp.CourseID, testCourseID)
	}
}

func TestCoursePurchaseRepo_GetByID_NoRows_ReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error {
		return errors.New("sql: no rows in result set")
	}
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	_, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("err=%v, want shared.ErrNotFound", err)
	}
}

func TestCoursePurchaseRepo_GetByStripeSessionID_Empty_ReturnsErrStripeSessionRequired(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	_, err := repo.GetByStripeSessionID(context.Background(), "")
	if !errors.Is(err, shared.ErrStripeSessionRequired) {
		t.Errorf("err=%v, want shared.ErrStripeSessionRequired", err)
	}
}

func TestCoursePurchaseRepo_GetByStripeSessionID_AppliesRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanCoursePurchaseRow
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	// The webhook dispatch path stamps the tenant from the Stripe session
	// metadata onto ctx BEFORE this lookup (dispatcher.go). The session-id
	// lookup MUST emit SET LOCAL chora.tenant_id first: the user_mana_topups
	// (and every Purchase table's) tenant_isolation RLS policy evaluates
	// current_setting('chora.tenant_id', true)::uuid for every row touched,
	// and on a pooled pgx connection that previously ran a SET LOCAL the GUC
	// reset-value is '' (NOT NULL) — so a lookup that skips ApplySession
	// hits ''::uuid → 22P02. Applying RLS with the real tenant is the fix.
	cp, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID)
	if err != nil {
		t.Fatalf("GetByStripeSessionID: %v", err)
	}
	if cp.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s, want %s", cp.PurchaseID, testPurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
	if len(q.queries) != 1 {
		t.Errorf("expected 1 QueryRow call, got %d", len(q.queries))
	}
}
