// repos_test.go — RED→GREEN tests for the 4 non-course Purchase repos +
// webhook_event repo + outbox repo. Mirrors the canonical pattern from
// course_purchase_test.go.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// -----------------------------------------------------------------------------
// ApplicationPaymentRepo
// -----------------------------------------------------------------------------

func TestApplicationPaymentRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewApplicationPaymentRepo(newStubTxRunner(q))
	ap, err := apppay.New(
		testPurchaseID, testTenantID, testLearnerGCID,
		"01970000-0000-7000-d000-000000000005",
		testCourseID,
		99900, "SGD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test_abc123",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("apppay.New: %v", err)
	}
	if err := repo.Save(withTenant(context.Background(), testTenantID), ap); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO application_payments") {
		t.Errorf("upsert sql does not target application_payments: %q", upsert.sql)
	}
	if len(upsert.args) != 25 {
		t.Errorf("upsert args=%d, want 25", len(upsert.args))
	}
}

func TestApplicationPaymentRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	repo := NewApplicationPaymentRepo(nil)
	if err := repo.Save(context.Background(), &apppay.ApplicationPayment{}); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Save err=%v, want ErrNotImplemented", err)
	}
}

// -----------------------------------------------------------------------------
// FamiliarEggPurchaseRepo
// -----------------------------------------------------------------------------

func TestFamiliarEggPurchaseRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewFamiliarEggPurchaseRepo(newStubTxRunner(q))
	ep, err := egg.New(
		testPurchaseID, testTenantID, testLearnerGCID, "egg.standard.v1",
		"",
		1900, "SGD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test",
		nil, nil,
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("egg.New: %v", err)
	}
	if err := repo.Save(withTenant(context.Background(), testTenantID), ep); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO familiar_egg_purchases") {
		t.Errorf("upsert sql does not target familiar_egg_purchases: %q", upsert.sql)
	}
	if len(upsert.args) != 29 {
		t.Errorf("upsert args=%d, want 29", len(upsert.args))
	}
}

// -----------------------------------------------------------------------------
// TenantManaTopUpRepo
// -----------------------------------------------------------------------------

func TestTenantManaTopUpRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewTenantManaTopUpRepo(newStubTxRunner(q))
	m, err := mana.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.topup.5000.v1",
		5000,
		2900, "SGD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("mana.New: %v", err)
	}
	if err := repo.Save(withTenant(context.Background(), testTenantID), m); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO tenant_mana_topups") {
		t.Errorf("upsert sql does not target tenant_mana_topups: %q", upsert.sql)
	}
	if len(upsert.args) != 26 {
		t.Errorf("upsert args=%d, want 26", len(upsert.args))
	}
}

// -----------------------------------------------------------------------------
// UserSubscriptionRepo
// -----------------------------------------------------------------------------

func TestUserSubscriptionRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewUserSubscriptionRepo(newStubTxRunner(q))
	u, err := sub.New(
		testPurchaseID, testTenantID, testLearnerGCID, "familiar.standard.monthly.v1",
		sub.BillingMonthly,
		999, "SGD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("sub.New: %v", err)
	}
	if err := repo.Save(withTenant(context.Background(), testTenantID), u); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO user_subscriptions") {
		t.Errorf("upsert sql does not target user_subscriptions: %q", upsert.sql)
	}
	if len(upsert.args) != 39 {
		t.Errorf("upsert args=%d, want 39", len(upsert.args))
	}
}

func TestUserSubscriptionRepo_GetByStripeSubscriptionID_EmptyReturnsNotFound(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewUserSubscriptionRepo(newStubTxRunner(q))
	_, err := repo.GetByStripeSubscriptionID(context.Background(), "")
	if !errors.Is(err, shared.ErrNotFound) {
		t.Errorf("err=%v, want shared.ErrNotFound", err)
	}
}

// -----------------------------------------------------------------------------
// UserManaTopUpRepo — Save (Stage A.5)
// -----------------------------------------------------------------------------

func TestUserManaTopUpRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewUserManaTopUpRepo(newStubTxRunner(q))
	u, err := umt.New(
		testPurchaseID, testTenantID, testLearnerGCID, "mana.user_topup.standard_v1",
		2500,
		1990, "SGD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("umt.New: %v", err)
	}
	if err := repo.Save(withTenant(context.Background(), testTenantID), u); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO user_mana_topups") {
		t.Errorf("upsert sql does not target user_mana_topups: %q", upsert.sql)
	}
	if len(upsert.args) != 26 {
		t.Errorf("upsert args=%d, want 26", len(upsert.args))
	}
}

func TestUserManaTopUpRepo_NilAggregateReturnsErrInvalidAggregate(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewUserManaTopUpRepo(newStubTxRunner(q))
	if err := repo.Save(withTenant(context.Background(), testTenantID), nil); !errors.Is(err, ErrInvalidAggregate) {
		t.Errorf("err=%v, want ErrInvalidAggregate", err)
	}
}

// -----------------------------------------------------------------------------
// IdentityKycFeeRepo — Save (Stage A.5)
// -----------------------------------------------------------------------------

func TestIdentityKycFeeRepo_Save_AppliesRLSAndUpsertSQL(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewIdentityKycFeeRepo(newStubTxRunner(q))
	k, err := kyc.New(
		testPurchaseID, testTenantID, testLearnerGCID, "manual_id_doc",
		999, "USD",
		testSessionID, "https://checkout.stripe.com/c/pay/cs_test",
		fixedTime(),
	)
	if err != nil {
		t.Fatalf("kyc.New: %v", err)
	}
	if err := repo.Save(withTenant(context.Background(), testTenantID), k); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertRLSApplied(t, q, testTenantID)
	upsert := q.execs[len(q.execs)-1]
	if !strings.Contains(upsert.sql, "INSERT INTO identity_kyc_fees") {
		t.Errorf("upsert sql does not target identity_kyc_fees: %q", upsert.sql)
	}
	if len(upsert.args) != 24 {
		t.Errorf("upsert args=%d, want 24", len(upsert.args))
	}
}

func TestIdentityKycFeeRepo_NilAggregateReturnsErrInvalidAggregate(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewIdentityKycFeeRepo(newStubTxRunner(q))
	if err := repo.Save(withTenant(context.Background(), testTenantID), nil); !errors.Is(err, ErrInvalidAggregate) {
		t.Errorf("err=%v, want ErrInvalidAggregate", err)
	}
}

// -----------------------------------------------------------------------------
// WebhookEventRepo
// -----------------------------------------------------------------------------

func TestWebhookEventRepo_Insert_NoRLS(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	w, err := wh.New("evt_test_aaa", "checkout.session.completed", fixedTime())
	if err != nil {
		t.Fatalf("wh.New: %v", err)
	}
	if err := repo.Insert(context.Background(), w); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(q.execs) != 1 {
		t.Fatalf("expected 1 Exec call (insert), got %d", len(q.execs))
	}
	insert := q.execs[0]
	if !strings.HasPrefix(insert.sql, "\nINSERT INTO stripe_webhook_events") {
		t.Errorf("insert sql does not target stripe_webhook_events: %q", insert.sql)
	}
	if len(insert.args) != 3 {
		t.Errorf("insert args=%d, want 3", len(insert.args))
	}
}

func TestWebhookEventRepo_Insert_DuplicateReturnsAlreadyProcessed(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.execErr = fmt.Errorf("ERROR: duplicate key value violates unique constraint \"stripe_webhook_events_pkey\" (SQLSTATE 23505)")
	q.execErrOnIdx = 0
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	w, _ := wh.New("evt_dup", "checkout.session.completed", fixedTime())
	err := repo.Insert(context.Background(), w)
	if !errors.Is(err, wh.ErrAlreadyProcessed) {
		t.Errorf("err=%v, want wh.ErrAlreadyProcessed", err)
	}
}

func TestWebhookEventRepo_MarkProcessed(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	if err := repo.MarkProcessed(
		context.Background(),
		"evt_test_aaa",
		wh.AggregateCoursePurchase,
		testPurchaseID,
		fixedTime(),
	); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if len(q.execs) != 1 {
		t.Fatalf("expected 1 Exec call, got %d", len(q.execs))
	}
	update := q.execs[0]
	if !strings.Contains(update.sql, "UPDATE stripe_webhook_events") {
		t.Errorf("update sql does not target stripe_webhook_events: %q", update.sql)
	}
	if len(update.args) != 4 {
		t.Errorf("update args=%d, want 4", len(update.args))
	}
}

// -----------------------------------------------------------------------------
// OutboxRepo
// -----------------------------------------------------------------------------

func TestOutboxRepo_Insert_ValidatesRequiredFields(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewOutboxRepo(newStubTxRunner(q))

	// EventID required.
	err := repo.InsertStandalone(context.Background(), OutboxRow{IdempotencyKey: "key"})
	if err == nil || !strings.Contains(err.Error(), "event_id required") {
		t.Errorf("err=%v, want event_id required", err)
	}

	// IdempotencyKey required.
	err = repo.InsertStandalone(context.Background(), OutboxRow{EventID: "id"})
	if err == nil || !strings.Contains(err.Error(), "idempotency_key required") {
		t.Errorf("err=%v, want idempotency_key required", err)
	}
}

func TestOutboxRepo_Insert_DuplicateReturnsErrDuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.execErr = fmt.Errorf("ERROR: duplicate key value violates unique constraint (SQLSTATE 23505)")
	q.execErrOnIdx = 0
	repo := NewOutboxRepo(newStubTxRunner(q))
	err := repo.InsertStandalone(context.Background(), OutboxRow{
		EventID:        "01970000-0000-7000-e000-000000000001",
		AggregateType:  "course_purchase",
		AggregateID:    testPurchaseID,
		TenantID:       testTenantID,
		Topic:          "chora.payments.course_purchase.payment_captured.v1",
		IdempotencyKey: "dup-key",
		Payload:        []byte("payload"),
		CreatedAt:      fixedTime(),
	})
	if !errors.Is(err, ErrDuplicateIdempotencyKey) {
		t.Errorf("err=%v, want ErrDuplicateIdempotencyKey", err)
	}
}

func TestOutboxRepo_Insert_HappyPath(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewOutboxRepo(newStubTxRunner(q))
	now := fixedTime()
	err := repo.InsertStandalone(context.Background(), OutboxRow{
		EventID:        "01970000-0000-7000-e000-000000000002",
		AggregateType:  "course_purchase",
		AggregateID:    testPurchaseID,
		TenantID:       testTenantID,
		Topic:          "chora.payments.course_purchase.payment_captured.v1",
		Payload:        []byte("proto-binary"),
		IdempotencyKey: "01970000-0000-7000-e000-000000000003",
		SchemaVersion:  1,
		CreatedAt:      now,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(q.execs) != 1 {
		t.Fatalf("expected 1 Exec call, got %d", len(q.execs))
	}
	insert := q.execs[0]
	if !strings.Contains(insert.sql, "INSERT INTO outbox_events") {
		t.Errorf("insert sql does not target outbox_events: %q", insert.sql)
	}
	if len(insert.args) != 11 {
		t.Errorf("insert args=%d, want 11", len(insert.args))
	}
}

func TestOutboxRepo_MarkDispatched(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewOutboxRepo(newStubTxRunner(q))
	if err := repo.MarkDispatched(context.Background(), "evt_id", fixedTime()); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	if len(q.execs) != 1 {
		t.Fatalf("expected 1 Exec call, got %d", len(q.execs))
	}
	if !strings.Contains(q.execs[0].sql, "UPDATE outbox_events") {
		t.Errorf("update sql does not target outbox_events: %q", q.execs[0].sql)
	}
}

func TestOutboxRepo_MarkRetry_TruncatesError(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	repo := NewOutboxRepo(newStubTxRunner(q))
	huge := strings.Repeat("x", 1500)
	if err := repo.MarkRetry(context.Background(), "evt_id", huge, fixedTime().Add(time.Minute)); err != nil {
		t.Fatalf("MarkRetry: %v", err)
	}
	update := q.execs[0]
	got, ok := update.args[1].(string)
	if !ok {
		t.Fatalf("update args[1] not a string: %T", update.args[1])
	}
	if len(got) != 1000 {
		t.Errorf("MarkRetry did not truncate error: got len=%d, want 1000", len(got))
	}
}

func TestOutboxRepo_SafeStripError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
		want string
	}{
		{"empty", nil, ""},
		{"no secret", errors.New("connection refused"), "connection refused"},
		{"strips password", errors.New("dial failed user=foo password=secret host=db sslmode=disable"), "dial failed <redacted>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SafeStripError(tc.in); got != tc.want {
				t.Errorf("SafeStripError(%v) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}
