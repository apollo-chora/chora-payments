// repos_extra_test.go — coverage for the pg repos not yet exercised by
// the per-repo RED→GREEN tests: DisputeRepo (all methods), the
// StripeCustomerRepo read/insert paths, and the three remaining
// TenantAddonPurchaseRepo lookups (CHO-1764 / CHO-1772 / H+ billing).
package pg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	stripecustomer "github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// -----------------------------------------------------------------------------
// DisputeRepo
// -----------------------------------------------------------------------------

func mkDispute(t *testing.T) *dispute.Dispute {
	t.Helper()
	now := fixedTime()
	d, err := dispute.New(
		"01970000-0000-7000-b000-000000000099", testTenantID,
		wh.AggregateCoursePurchase, testPurchaseID,
		"dp_test_0001", "ch_test_0001",
		19900, "SGD", dispute.ReasonFraudulent,
		now.Add(7*24*time.Hour), now,
	)
	if err != nil {
		t.Fatalf("dispute.New: %v", err)
	}
	return d
}

func TestDisputeRepo_NilTxRunner(t *testing.T) {
	t.Parallel()
	repo := NewDisputeRepo(nil)
	d := mkDispute(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, d); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Insert err=%v", err)
	}
	if err := repo.Save(ctx, d); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Save err=%v", err)
	}
	if _, err := repo.GetByStripeDisputeID(ctx, "dp_1"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("GetByStripeDisputeID err=%v", err)
	}
	if _, err := repo.GetByID(ctx, testTenantID, "d_1"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("GetByID err=%v", err)
	}
	if _, err := repo.ListByPurchase(ctx, testTenantID, wh.AggregateCoursePurchase, testPurchaseID); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("ListByPurchase err=%v", err)
	}
}

func TestDisputeRepo_Insert_Happy(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewDisputeRepo(newStubTxRunner(q))
	d := mkDispute(t)
	ctx := withTenant(context.Background(), testTenantID)
	if err := repo.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	last := q.execs[len(q.execs)-1]
	if !strings.Contains(last.sql, "INSERT INTO disputes") {
		t.Errorf("sql=%q", last.sql)
	}
	if len(last.args) != 20 {
		t.Errorf("args=%d, want 20", len(last.args))
	}
	if last.args[0] != d.DisputeID {
		t.Errorf("arg[0]=%v", last.args[0])
	}
}

func TestDisputeRepo_Insert_UniqueViolation(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.execErr = errors.New(`pq: duplicate key value violates unique constraint "stripe_dispute_id_key"`)
	q.execErrOnIdx = 1 // after the rls exec
	repo := NewDisputeRepo(newStubTxRunner(q))
	err := repo.Insert(withTenant(context.Background(), testTenantID), mkDispute(t))
	if !errors.Is(err, dispute.ErrAlreadyExists) {
		t.Errorf("err=%v, want ErrAlreadyExists", err)
	}
}

func TestDisputeRepo_Insert_GenericError(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.execErr = errors.New("connection refused")
	q.execErrOnIdx = 1
	repo := NewDisputeRepo(newStubTxRunner(q))
	err := repo.Insert(withTenant(context.Background(), testTenantID), mkDispute(t))
	if err == nil || !strings.Contains(err.Error(), "insert dispute") {
		t.Errorf("err=%v, want wrapped insert error", err)
	}
}

func TestDisputeRepo_Insert_NilAggregate(t *testing.T) {
	t.Parallel()
	repo := NewDisputeRepo(newStubTxRunner(newStubQuerier()))
	if err := repo.Insert(context.Background(), nil); !errors.Is(err, ErrInvalidAggregate) {
		t.Errorf("err=%v, want ErrInvalidAggregate", err)
	}
}

func TestDisputeRepo_Save_HappyAndError(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewDisputeRepo(newStubTxRunner(q))
	d := mkDispute(t)
	_ = d.RecordFundsWithdrawn(fixedTime())
	if err := repo.Save(withTenant(context.Background(), testTenantID), d); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	if !strings.Contains(q.execs[len(q.execs)-1].sql, "ON CONFLICT (dispute_id) DO UPDATE") {
		t.Errorf("Save sql missing ON CONFLICT")
	}
	if err := repo.Save(context.Background(), nil); !errors.Is(err, ErrInvalidAggregate) {
		t.Errorf("Save(nil) err=%v", err)
	}

	q2 := newStubQuerier()
	q2.execErr = errors.New("db down")
	q2.execErrOnIdx = 1
	repo2 := NewDisputeRepo(newStubTxRunner(q2))
	if err := repo2.Save(withTenant(context.Background(), testTenantID), mkDispute(t)); err == nil || !strings.Contains(err.Error(), "upsert dispute") {
		t.Errorf("Save error path: %v", err)
	}
}

func scanDisputeRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = "dispute-1"
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = string(wh.AggregateCoursePurchase)
	*(dest[3].(*string)) = testPurchaseID
	*(dest[4].(*string)) = "dp_test_0001"
	*(dest[5].(*string)) = "ch_test_0001"
	*(dest[6].(*string)) = string(dispute.StateRaised)
	// dest[7] is **string (outcome) — leave nil for "no outcome".
	*(dest[8].(*int64)) = 19900
	*(dest[9].(*string)) = "SGD"
	*(dest[10].(*string)) = string(dispute.ReasonFraudulent)
	*(dest[11].(*time.Time)) = now.Add(7 * 24 * time.Hour)
	*(dest[12].(*bool)) = true
	withdrawnAt := now
	*(dest[13].(**time.Time)) = &withdrawnAt
	*(dest[14].(*bool)) = false
	// dest[15] is **time.Time (funds_reinstated_at) — leave nil.
	*(dest[16].(*time.Time)) = now
	closedAt := now.Add(time.Hour)
	*(dest[17].(**time.Time)) = &closedAt
	*(dest[18].(*time.Time)) = now
	*(dest[19].(*time.Time)) = now
	return nil
}

func TestDisputeRepo_GetByStripeDisputeID_Happy(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanDisputeRow
	repo := NewDisputeRepo(newStubTxRunner(q))
	d, err := repo.GetByStripeDisputeID(context.Background(), "dp_test_0001")
	if err != nil {
		t.Fatalf("GetByStripeDisputeID: %v", err)
	}
	if d.DisputeID != "dispute-1" || d.StripeDisputeID != "dp_test_0001" {
		t.Errorf("d=%+v", d)
	}
	if !d.FundsWithdrawn || d.FundsWithdrawnAt == nil {
		t.Errorf("funds fields not mapped: %+v", d)
	}
	if d.Outcome != "" {
		t.Errorf("Outcome should stay empty, got %q", d.Outcome)
	}
	if _, err := repo.GetByStripeDisputeID(context.Background(), ""); !errors.Is(err, dispute.ErrStripeDisputeRequired) {
		t.Errorf("blank id err=%v", err)
	}
	// no-rows → dispute.ErrNotFound
	q2 := newStubQuerier()
	q2.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo2 := NewDisputeRepo(newStubTxRunner(q2))
	if _, err := repo2.GetByStripeDisputeID(context.Background(), "dp_missing"); !errors.Is(err, dispute.ErrNotFound) {
		t.Errorf("no-rows err=%v, want ErrNotFound", err)
	}
}

func TestDisputeRepo_GetByID_HappyAndNoRows(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanDisputeRow
	repo := NewDisputeRepo(newStubTxRunner(q))
	d, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, "dispute-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if d.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s", d.PurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
	if _, err := repo.GetByID(context.Background(), "", "dispute-1"); !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
}

func TestDisputeRepo_ListByPurchase(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowsScan = func(idx int, dest ...any) error { return scanDisputeRow(dest...) }
	q.nextN = 2
	repo := NewDisputeRepo(newStubTxRunner(q))
	out, err := repo.ListByPurchase(withTenant(context.Background(), testTenantID), testTenantID, wh.AggregateCoursePurchase, testPurchaseID)
	if err != nil {
		t.Fatalf("ListByPurchase: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len=%d, want 2", len(out))
	}
	assertRLSApplied(t, q, testTenantID)
	if _, err := repo.ListByPurchase(context.Background(), "", wh.AggregateCoursePurchase, testPurchaseID); !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
	if _, err := repo.ListByPurchase(context.Background(), testTenantID, "mystery", testPurchaseID); !errors.Is(err, dispute.ErrAggregateTypeRequired) {
		t.Errorf("invalid agg err=%v", err)
	}
	// No rows → empty slice, nil error.
	q2 := newStubQuerier()
	repo2 := NewDisputeRepo(newStubTxRunner(q2))
	out2, err := repo2.ListByPurchase(withTenant(context.Background(), testTenantID), testTenantID, wh.AggregateCoursePurchase, testPurchaseID)
	if err != nil || len(out2) != 0 {
		t.Errorf("no-rows: out=%v err=%v", out2, err)
	}
	// Scan error mid-iteration propagates.
	q3 := newStubQuerier()
	q3.rowsScan = func(idx int, dest ...any) error { return errors.New("bad column") }
	q3.nextN = 1
	repo3 := NewDisputeRepo(newStubTxRunner(q3))
	if _, err := repo3.ListByPurchase(withTenant(context.Background(), testTenantID), testTenantID, wh.AggregateCoursePurchase, testPurchaseID); err == nil {
		t.Error("scan error should propagate")
	}
}

// -----------------------------------------------------------------------------
// StripeCustomerRepo
// -----------------------------------------------------------------------------

func mkStripeCustomer() *stripecustomer.StripeCustomer {
	sc, _ := stripecustomer.New(testTenantID, testLearnerGCID, "cus_test_0001", "learner@example.com", fixedTime())
	return sc
}

func scanStripeCustomerRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testTenantID
	*(dest[1].(*string)) = testLearnerGCID
	*(dest[2].(*string)) = "cus_test_0001"
	email := "learner@example.com"
	*(dest[3].(**string)) = &email
	*(dest[4].(*time.Time)) = now
	*(dest[5].(*time.Time)) = now
	return nil
}

func TestStripeCustomerRepo_GetByGCID(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanStripeCustomerRow
	repo := NewStripeCustomerRepo(newStubTxRunner(q))
	sc, err := repo.GetByGCID(withTenant(context.Background(), testTenantID), testTenantID, testLearnerGCID)
	if err != nil {
		t.Fatalf("GetByGCID: %v", err)
	}
	if sc.StripeCustomerID != "cus_test_0001" || sc.Email != "learner@example.com" {
		t.Errorf("sc=%+v", sc)
	}
	assertRLSApplied(t, q, testTenantID)
	if _, err := repo.GetByGCID(context.Background(), "", testLearnerGCID); !errors.Is(err, stripecustomer.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
	if _, err := repo.GetByGCID(context.Background(), testTenantID, ""); !errors.Is(err, stripecustomer.ErrLearnerRequired) {
		t.Errorf("blank gcid err=%v", err)
	}
	// no rows
	q2 := newStubQuerier()
	q2.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo2 := NewStripeCustomerRepo(newStubTxRunner(q2))
	if _, err := repo2.GetByGCID(withTenant(context.Background(), testTenantID), testTenantID, testLearnerGCID); !errors.Is(err, stripecustomer.ErrNotFound) {
		t.Errorf("no-rows err=%v, want ErrNotFound", err)
	}
	// scan error not no-rows
	q3 := newStubQuerier()
	q3.rowScan = func(_ ...any) error { return errors.New("bad column") }
	repo3 := NewStripeCustomerRepo(newStubTxRunner(q3))
	if _, err := repo3.GetByGCID(withTenant(context.Background(), testTenantID), testTenantID, testLearnerGCID); err == nil {
		t.Error("scan error should propagate")
	}
}

func TestStripeCustomerRepo_Insert(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewStripeCustomerRepo(newStubTxRunner(q))
	if err := repo.Insert(withTenant(context.Background(), testTenantID), mkStripeCustomer()); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	if !strings.Contains(q.execs[len(q.execs)-1].sql, "INSERT INTO stripe_customers") {
		t.Errorf("sql=%q", q.execs[len(q.execs)-1].sql)
	}
	if err := repo.Insert(context.Background(), nil); !errors.Is(err, stripecustomer.ErrStripeCustomerEmpty) {
		t.Errorf("Insert(nil) err=%v", err)
	}
	q2 := newStubQuerier()
	q2.execErr = errors.New("db down")
	q2.execErrOnIdx = 1
	repo2 := NewStripeCustomerRepo(newStubTxRunner(q2))
	if err := repo2.Insert(withTenant(context.Background(), testTenantID), mkStripeCustomer()); err == nil || !strings.Contains(err.Error(), "insert stripe_customer") {
		t.Errorf("Insert error path: %v", err)
	}
}

func TestStripeCustomerRepo_GetByGCIDs(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowsScan = func(idx int, dest ...any) error {
		*(dest[0].(*string)) = testLearnerGCID
		email := "a@b.c"
		*(dest[1].(**string)) = &email
		return nil
	}
	q.nextN = 1
	repo := NewStripeCustomerRepo(newStubTxRunner(q))
	out, err := repo.GetByGCIDs(withTenant(context.Background(), testTenantID), testTenantID, []string{testLearnerGCID, testLearnerGCID, "  "})
	if err != nil {
		t.Fatalf("GetByGCIDs: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("out len=%d, want 1 (deduped)", len(out))
	}
	sc := out[testLearnerGCID]
	if sc == nil || sc.Email != "a@b.c" {
		t.Errorf("sc=%+v", sc)
	}
	assertRLSApplied(t, q, testTenantID)
	// Empty cleaned list → no DB call.
	q2 := newStubQuerier()
	repo2 := NewStripeCustomerRepo(newStubTxRunner(q2))
	out2, err := repo2.GetByGCIDs(context.Background(), testTenantID, []string{"", " "})
	if err != nil || len(out2) != 0 {
		t.Errorf("empty cleaned: out=%v err=%v", out2, err)
	}
	if len(q2.queries) != 0 {
		t.Errorf("expected no DB call for empty input, got %d", len(q2.queries))
	}
	if _, err := repo2.GetByGCIDs(context.Background(), "", []string{testLearnerGCID}); !errors.Is(err, stripecustomer.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
}

func TestDedupeGCIDs(t *testing.T) {
	t.Parallel()
	got := dedupeGCIDs([]string{"a", " a ", "", "b", "a", "c"})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len=%d, want 3: %v", len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d]=%q, want %q", i, got[i], want[i])
		}
	}
}

// -----------------------------------------------------------------------------
// TenantAddonPurchaseRepo — CHO-1764 / CHO-1772 / H+ billing lookups
// -----------------------------------------------------------------------------

func scanTapRow(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "addon_plan_1"
	*(dest[4].(*string)) = "knowledge_graph"
	*(dest[5].(*string)) = "pro"
	*(dest[6].(*string)) = string(shared.StatePaymentCaptured)
	*(dest[7].(*int64)) = 7990
	*(dest[8].(*int64)) = 7990
	*(dest[9].(*int64)) = 0
	*(dest[10].(*string)) = "SGD"
	*(dest[11].(*string)) = "cs_addon_1"
	// Pointer slots (12-18, 20-23, 26-33) stay nil — their **string /
	// **time.Time shapes are exercised by tenant_addon_purchase_test.go.
	*(dest[19].(*time.Time)) = now
	*(dest[24].(*time.Time)) = now
	*(dest[25].(*time.Time)) = now
	return nil
}

func TestTenantAddonPurchaseRepo_GetActiveByTenantAndAddonCode(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTapRow
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	a, err := repo.GetActiveByTenantAndAddonCode(withTenant(context.Background(), testTenantID), testTenantID, "knowledge_graph")
	if err != nil {
		t.Fatalf("GetActiveByTenantAndAddonCode: %v", err)
	}
	if a.PurchaseID != testPurchaseID || a.AddonCode != "knowledge_graph" {
		t.Errorf("a=%+v", a)
	}
	assertRLSApplied(t, q, testTenantID)
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), "", "knowledge_graph"); !errors.Is(err, tap.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
	if _, err := repo.GetActiveByTenantAndAddonCode(context.Background(), testTenantID, ""); !errors.Is(err, tap.ErrAddonCodeRequired) {
		t.Errorf("blank addon err=%v", err)
	}
	q2 := newStubQuerier()
	q2.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo2 := NewTenantAddonPurchaseRepo(newStubTxRunner(q2))
	if _, err := repo2.GetActiveByTenantAndAddonCode(withTenant(context.Background(), testTenantID), testTenantID, "knowledge_graph"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("no-rows err=%v", err)
	}
}

func TestTenantAddonPurchaseRepo_GetByStripeScheduleID(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanTapRow
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	a, err := repo.GetByStripeScheduleID(withTenant(context.Background(), testTenantID), "sub_sched_1")
	if err != nil {
		t.Fatalf("GetByStripeScheduleID: %v", err)
	}
	if a.PurchaseID != testPurchaseID {
		t.Errorf("PurchaseID=%s", a.PurchaseID)
	}
	assertRLSApplied(t, q, testTenantID)
	if _, err := repo.GetByStripeScheduleID(context.Background(), ""); !errors.Is(err, tap.ErrStripeScheduleRequired) {
		t.Errorf("blank schedule err=%v", err)
	}
	q2 := newStubQuerier()
	q2.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo2 := NewTenantAddonPurchaseRepo(newStubTxRunner(q2))
	if _, err := repo2.GetByStripeScheduleID(withTenant(context.Background(), testTenantID), "sub_sched_missing"); !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("no-rows err=%v", err)
	}
}

func TestTenantAddonPurchaseRepo_GetStripeCustomerIDByTenant(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(dest ...any) error {
		*(dest[0].(*string)) = "cus_billing_1"
		return nil
	}
	repo := NewTenantAddonPurchaseRepo(newStubTxRunner(q))
	id, err := repo.GetStripeCustomerIDByTenant(withTenant(context.Background(), testTenantID), testTenantID)
	if err != nil {
		t.Fatalf("GetStripeCustomerIDByTenant: %v", err)
	}
	if id != "cus_billing_1" {
		t.Errorf("id=%q", id)
	}
	assertRLSApplied(t, q, testTenantID)
	if _, err := repo.GetStripeCustomerIDByTenant(context.Background(), ""); !errors.Is(err, tap.ErrTenantRequired) {
		t.Errorf("blank tenant err=%v", err)
	}
	// No rows → empty string + nil error (billing renders empty list).
	q2 := newStubQuerier()
	q2.rowScan = func(_ ...any) error { return errors.New("sql: no rows in result set") }
	repo2 := NewTenantAddonPurchaseRepo(newStubTxRunner(q2))
	id2, err := repo2.GetStripeCustomerIDByTenant(withTenant(context.Background(), testTenantID), testTenantID)
	if err != nil || id2 != "" {
		t.Errorf("no-rows: id=%q err=%v, want empty/nil", id2, err)
	}
	// Non-no-rows scan error propagates.
	q3 := newStubQuerier()
	q3.rowScan = func(_ ...any) error { return errors.New("bad column") }
	repo3 := NewTenantAddonPurchaseRepo(newStubTxRunner(q3))
	if _, err := repo3.GetStripeCustomerIDByTenant(withTenant(context.Background(), testTenantID), testTenantID); err == nil {
		t.Error("scan error should propagate")
	}
}
