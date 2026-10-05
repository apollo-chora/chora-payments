//go:build integration

// tenant_addon_purchase_integration_test.go — real-Postgres round-trip test
// for the TenantAddonPurchaseRepo. The most complex repository in the
// service (34 columns: shared Purchase + addon + Stripe Subscription +
// Stripe SubscriptionSchedule correlation), so it is the one most exposed to
// the silently-dropped-INSERT-column defect class: a stub-querier unit
// suite cannot catch a column the INSERT omits, because the stub records
// SQL text, not values. This test writes a row with EVERY field set to a
// distinct value, reads it back through the production SELECT + scan
// path, and compares every field.
//
// Skip contract: CHORA_TEST_DSN unset → t.Skip (hermetic `go test ./...`
// stays green without a database).
package pg

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
)

// integrationTenantID is a fixed UUID for the RLS session GUC. The
// tenant_addon_purchases RLS policy casts chora.tenant_id to uuid, so the
// value must be a valid UUID literal.
const integrationTenantID = "11111111-1111-4111-8111-111111111111"

// integrationTxRunner is the pgxpool-backed TxRunner, mirroring
// cmd/server/pgx_txrunner.go (which lives in package main and is not
// importable from this package's tests).
type integrationTxRunner struct {
	pool *pgxpool.Pool
}

func (t *integrationTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q Querier) error) (err error) {
	tx, err := t.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
			return
		}
		err = tx.Commit(ctx)
	}()
	return fn(ctx, &integrationQuerier{tx: tx})
}

type integrationQuerier struct{ tx pgx.Tx }

func (q *integrationQuerier) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
	tag, err := q.tx.Exec(ctx, sql, args...)
	if err != nil {
		return rls.CommandTag{}, err
	}
	return rls.CommandTag{RowsAffected: tag.RowsAffected()}, nil
}

func (q *integrationQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return &integrationRow{r: q.tx.QueryRow(ctx, sql, args...)}
}

func (q *integrationQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rs, err := q.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &integrationRows{r: rs}, nil
}

type integrationRow struct{ r pgx.Row }

func (r *integrationRow) Scan(dest ...any) error { return r.r.Scan(dest...) }

type integrationRows struct{ r pgx.Rows }

func (r *integrationRows) Next() bool             { return r.r.Next() }
func (r *integrationRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *integrationRows) Close() error           { r.r.Close(); return nil }
func (r *integrationRows) Err() error             { return r.r.Err() }

// TestTenantAddonPurchaseRepo_RealPostgresRoundTrip is the real-Postgres
// gate: write a record with every field distinct, read it back, compare
// EVERY field.
func TestTenantAddonPurchaseRepo_RealPostgresRoundTrip(t *testing.T) {
	dsn := os.Getenv("CHORA_TEST_DSN")
	if dsn == "" {
		t.Skip("CHORA_TEST_DSN unset — skipping real-Postgres integration test")
	}

	ctx := tracing.WithTenantID(context.Background(), integrationTenantID)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	repo := NewTenantAddonPurchaseRepo(&integrationTxRunner{pool: pool})

	// Every field distinct — no two values alike, so a dropped INSERT
	// column or a swapped scan destination cannot pass by coincidence.
	now := time.Date(2026, 10, 5, 12, 30, 45, 0, time.UTC)
	paidAt := now.Add(2 * time.Minute)
	failedAt := now.Add(3 * time.Minute)
	refundedAt := now.Add(4 * time.Minute)
	expiredAt := now.Add(5 * time.Minute)
	periodStart := now.Add(24 * time.Hour)
	periodEnd := now.Add(30 * 24 * time.Hour)
	scheduledEffectiveAt := now.Add(48 * time.Hour)

	a, err := tap.New(
		"11111111-1111-4111-8111-000000000001", // purchase_id
		integrationTenantID,                    // tenant_id
		"22222222-2222-4222-8222-000000000002", // admin_gcid
		"33333333-3333-4333-8333-000000000003", // addon_plan_id
		"knowledge_graph",                      // addon_code
		"pro",                                  // tier_code
		12345,                                  // amount_cents
		"SGD",                                  // currency
		"cs_test_integration_0001",             // stripe_session_id
		"https://checkout.stripe.com/pay/cs_test_integration_0001", // checkout_url
		now,
	)
	if err != nil {
		t.Fatalf("tap.New: %v", err)
	}
	a.State = shared.StatePaymentCaptured
	a.AmountCentsPaid = 11111
	a.AmountCentsRefunded = 22222
	a.StripePaymentIntentID = "pi_integration_0001"
	a.StripeChargeID = "ch_integration_0001"
	a.StripeRefundID = "re_integration_0001"
	a.StripeFailureCode = "card_declined"
	a.StripeFailureMessage = "Your card was declined."
	a.RefundReason = "requested_by_customer"
	a.CheckoutStartedAt = now
	a.PaidAt = &paidAt
	a.FailedAt = &failedAt
	a.RefundedAt = &refundedAt
	a.ExpiredAt = &expiredAt
	a.CreatedAt = now
	a.UpdatedAt = now.Add(time.Minute)
	// CHO-1761 Stripe Subscription correlation.
	a.StripeCustomerID = "cus_integration_0001"
	a.StripeSubscriptionID = "sub_integration_0001"
	a.Status = tap.StatusActive
	a.CurrentPeriodStart = &periodStart
	a.CurrentPeriodEnd = &periodEnd
	// CHO-1772 Stripe SubscriptionSchedule correlation.
	a.StripeSubscriptionScheduleID = "sub_sched_integration_0001"
	a.ScheduledTierCode = "enterprise"
	a.ScheduledEffectiveAt = &scheduledEffectiveAt

	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := repo.GetByID(ctx, integrationTenantID, a.PurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	// Compare EVERY field.
	if got.PurchaseID != a.PurchaseID {
		t.Errorf("PurchaseID = %q; want %q", got.PurchaseID, a.PurchaseID)
	}
	if got.TenantID != a.TenantID {
		t.Errorf("TenantID = %q; want %q", got.TenantID, a.TenantID)
	}
	if got.LearnerGCID != a.LearnerGCID {
		t.Errorf("LearnerGCID = %q; want %q", got.LearnerGCID, a.LearnerGCID)
	}
	if got.State != a.State {
		t.Errorf("State = %q; want %q", got.State, a.State)
	}
	if got.AmountCents != a.AmountCents {
		t.Errorf("AmountCents = %d; want %d", got.AmountCents, a.AmountCents)
	}
	if got.AmountCentsPaid != a.AmountCentsPaid {
		t.Errorf("AmountCentsPaid = %d; want %d", got.AmountCentsPaid, a.AmountCentsPaid)
	}
	if got.AmountCentsRefunded != a.AmountCentsRefunded {
		t.Errorf("AmountCentsRefunded = %d; want %d", got.AmountCentsRefunded, a.AmountCentsRefunded)
	}
	if got.Currency != a.Currency {
		t.Errorf("Currency = %q; want %q", got.Currency, a.Currency)
	}
	if got.StripeSessionID != a.StripeSessionID {
		t.Errorf("StripeSessionID = %q; want %q", got.StripeSessionID, a.StripeSessionID)
	}
	if got.StripePaymentIntentID != a.StripePaymentIntentID {
		t.Errorf("StripePaymentIntentID = %q; want %q", got.StripePaymentIntentID, a.StripePaymentIntentID)
	}
	if got.StripeChargeID != a.StripeChargeID {
		t.Errorf("StripeChargeID = %q; want %q", got.StripeChargeID, a.StripeChargeID)
	}
	if got.StripeRefundID != a.StripeRefundID {
		t.Errorf("StripeRefundID = %q; want %q", got.StripeRefundID, a.StripeRefundID)
	}
	if got.StripeCheckoutURL != a.StripeCheckoutURL {
		t.Errorf("StripeCheckoutURL = %q; want %q", got.StripeCheckoutURL, a.StripeCheckoutURL)
	}
	if got.StripeFailureCode != a.StripeFailureCode {
		t.Errorf("StripeFailureCode = %q; want %q", got.StripeFailureCode, a.StripeFailureCode)
	}
	if got.StripeFailureMessage != a.StripeFailureMessage {
		t.Errorf("StripeFailureMessage = %q; want %q", got.StripeFailureMessage, a.StripeFailureMessage)
	}
	if got.RefundReason != a.RefundReason {
		t.Errorf("RefundReason = %q; want %q", got.RefundReason, a.RefundReason)
	}
	if !got.CheckoutStartedAt.Equal(a.CheckoutStartedAt) {
		t.Errorf("CheckoutStartedAt = %v; want %v", got.CheckoutStartedAt, a.CheckoutStartedAt)
	}
	if !got.PaidAt.Equal(*a.PaidAt) {
		t.Errorf("PaidAt = %v; want %v", got.PaidAt, *a.PaidAt)
	}
	if !got.FailedAt.Equal(*a.FailedAt) {
		t.Errorf("FailedAt = %v; want %v", got.FailedAt, *a.FailedAt)
	}
	if !got.RefundedAt.Equal(*a.RefundedAt) {
		t.Errorf("RefundedAt = %v; want %v", got.RefundedAt, *a.RefundedAt)
	}
	if !got.ExpiredAt.Equal(*a.ExpiredAt) {
		t.Errorf("ExpiredAt = %v; want %v", got.ExpiredAt, *a.ExpiredAt)
	}
	if !got.CreatedAt.Equal(a.CreatedAt) {
		t.Errorf("CreatedAt = %v; want %v", got.CreatedAt, a.CreatedAt)
	}
	if !got.UpdatedAt.Equal(a.UpdatedAt) {
		t.Errorf("UpdatedAt = %v; want %v", got.UpdatedAt, a.UpdatedAt)
	}
	if got.AddonPlanID != a.AddonPlanID {
		t.Errorf("AddonPlanID = %q; want %q", got.AddonPlanID, a.AddonPlanID)
	}
	if got.AddonCode != a.AddonCode {
		t.Errorf("AddonCode = %q; want %q", got.AddonCode, a.AddonCode)
	}
	if got.TierCode != a.TierCode {
		t.Errorf("TierCode = %q; want %q", got.TierCode, a.TierCode)
	}
	if got.StripeCustomerID != a.StripeCustomerID {
		t.Errorf("StripeCustomerID = %q; want %q", got.StripeCustomerID, a.StripeCustomerID)
	}
	if got.StripeSubscriptionID != a.StripeSubscriptionID {
		t.Errorf("StripeSubscriptionID = %q; want %q", got.StripeSubscriptionID, a.StripeSubscriptionID)
	}
	if got.Status != a.Status {
		t.Errorf("Status = %q; want %q", got.Status, a.Status)
	}
	if got.CurrentPeriodStart == nil || !got.CurrentPeriodStart.Equal(*a.CurrentPeriodStart) {
		t.Errorf("CurrentPeriodStart = %v; want %v", got.CurrentPeriodStart, *a.CurrentPeriodStart)
	}
	if got.CurrentPeriodEnd == nil || !got.CurrentPeriodEnd.Equal(*a.CurrentPeriodEnd) {
		t.Errorf("CurrentPeriodEnd = %v; want %v", got.CurrentPeriodEnd, *a.CurrentPeriodEnd)
	}
	if got.StripeSubscriptionScheduleID != a.StripeSubscriptionScheduleID {
		t.Errorf("StripeSubscriptionScheduleID = %q; want %q", got.StripeSubscriptionScheduleID, a.StripeSubscriptionScheduleID)
	}
	if got.ScheduledTierCode != a.ScheduledTierCode {
		t.Errorf("ScheduledTierCode = %q; want %q", got.ScheduledTierCode, a.ScheduledTierCode)
	}
	if got.ScheduledEffectiveAt == nil || !got.ScheduledEffectiveAt.Equal(*a.ScheduledEffectiveAt) {
		t.Errorf("ScheduledEffectiveAt = %v; want %v", got.ScheduledEffectiveAt, *a.ScheduledEffectiveAt)
	}
}

// TestTenantAddonPurchaseRepo_RealPostgresNotFound verifies the not-found
// path against a real database (RLS-scoped miss, not a driver error).
func TestTenantAddonPurchaseRepo_RealPostgresNotFound(t *testing.T) {
	dsn := os.Getenv("CHORA_TEST_DSN")
	if dsn == "" {
		t.Skip("CHORA_TEST_DSN unset — skipping real-Postgres integration test")
	}

	ctx := tracing.WithTenantID(context.Background(), integrationTenantID)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	repo := NewTenantAddonPurchaseRepo(&integrationTxRunner{pool: pool})

	_, err = repo.GetByID(ctx, integrationTenantID, "11111111-1111-4111-8111-000000000999")
	if err == nil {
		t.Fatal("expected not-found error for unknown purchase_id")
	}
}
