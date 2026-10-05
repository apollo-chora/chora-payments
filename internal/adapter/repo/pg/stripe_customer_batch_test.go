// stripe_customer_batch_test.go — RED→GREEN tests for the
// StripeCustomerRepo.GetByGCIDs batch hydration path (used by
// purchase_history.go to attach learner_email to the UNION ALL projection
// without an N+1 GetByGCID per row).
package pg

import (
	"context"
	"strings"
	"testing"
)

func TestStripeCustomerRepo_GetByGCIDs_ReturnsEmptyMapForEmptyInput(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewStripeCustomerRepo(newStubTxRunner(q))
	out, err := repo.GetByGCIDs(withTenant(context.Background(), testTenantID), testTenantID, nil)
	if err != nil {
		t.Fatalf("GetByGCIDs(nil): %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty map, got %d entries", len(out))
	}
	if len(q.queries) != 0 || len(q.execs) != 0 {
		t.Errorf("expected zero DB calls for empty input, got queries=%d execs=%d",
			len(q.queries), len(q.execs))
	}
}

func TestStripeCustomerRepo_GetByGCIDs_EmitsRLSAndSelectInClause(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	// Stub one row.
	q.nextN = 1
	q.rowsScan = func(_ int, dest ...any) error {
		// scan: (learner_gcid, email)
		if len(dest) != 2 {
			return fmtError("scan: dest len=%d, want 2", len(dest))
		}
		*(dest[0].(*string)) = testLearnerGCID
		emailPtr := dest[1].(**string)
		s := "alice@example.com"
		*emailPtr = &s
		return nil
	}
	repo := NewStripeCustomerRepo(newStubTxRunner(q))

	out, err := repo.GetByGCIDs(
		withTenant(context.Background(), testTenantID),
		testTenantID,
		[]string{testLearnerGCID},
	)
	if err != nil {
		t.Fatalf("GetByGCIDs: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(out))
	}
	if out[testLearnerGCID] == nil {
		t.Fatalf("expected entry for gcid %q", testLearnerGCID)
	}
	if out[testLearnerGCID].Email != "alice@example.com" {
		t.Errorf("Email=%q want alice@example.com", out[testLearnerGCID].Email)
	}

	assertRLSApplied(t, q, testTenantID)

	if len(q.queries) != 1 {
		t.Fatalf("expected 1 Query call, got %d", len(q.queries))
	}
	if !strings.Contains(q.queries[0].sql, "FROM stripe_customers") {
		t.Errorf("query sql does not target stripe_customers: %q", q.queries[0].sql)
	}
	if !strings.Contains(q.queries[0].sql, "ANY(") && !strings.Contains(q.queries[0].sql, "= ANY") {
		t.Errorf("query sql does not use ANY(...) for batch: %q", q.queries[0].sql)
	}
}

func TestStripeCustomerRepo_GetByGCIDs_DedupesAndOmitsEmpties(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.nextN = 0 // no rows seeded; we only assert the input shape
	q.rowsScan = func(_ int, _ ...any) error { return nil }
	repo := NewStripeCustomerRepo(newStubTxRunner(q))

	_, err := repo.GetByGCIDs(
		withTenant(context.Background(), testTenantID),
		testTenantID,
		[]string{testLearnerGCID, testLearnerGCID, "", "  ", testLearnerGCID},
	)
	if err != nil {
		t.Fatalf("GetByGCIDs: %v", err)
	}
	if len(q.queries) != 1 {
		t.Fatalf("expected 1 Query call, got %d", len(q.queries))
	}
	// args[1] should be the cleaned []string with one unique entry.
	if len(q.queries[0].args) < 2 {
		t.Fatalf("expected >=2 args, got %d", len(q.queries[0].args))
	}
	got, ok := q.queries[0].args[1].([]string)
	if !ok {
		t.Fatalf("arg[1] type %T, want []string", q.queries[0].args[1])
	}
	if len(got) != 1 || got[0] != testLearnerGCID {
		t.Errorf("deduped gcid arg = %v; want [%s]", got, testLearnerGCID)
	}
}
