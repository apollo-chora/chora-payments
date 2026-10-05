// scans_full_test.go — full-column scan coverage for the remaining
// scanners: every nullable pointer branch (payment intent, charge, refund,
// failure info, refund reason, paid/failed/refunded/expired timestamps)
// plus the webhook_event scanner.
package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	"github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func strPtr(s string) *string { return &s }

// scanCoursePurchaseRowFull populates every column of the course scan,
// including all nullable pointers.
func scanCoursePurchaseRowFull(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = testCourseID
	*(dest[4].(*string)) = string(shared.StateRefunded)
	*(dest[5].(*int64)) = 99900
	*(dest[6].(*int64)) = 99900
	*(dest[7].(*int64)) = 35000
	*(dest[8].(*string)) = "SGD"
	*(dest[9].(*string)) = testSessionID
	*(dest[10].(**string)) = strPtr("pi_test_full")
	*(dest[11].(**string)) = strPtr("ch_test_full")
	*(dest[12].(**string)) = strPtr("re_test_full")
	*(dest[13].(**string)) = strPtr("https://checkout.stripe.com/c/pay/full")
	*(dest[14].(**string)) = strPtr("card_declined")
	*(dest[15].(**string)) = strPtr("declined")
	*(dest[16].(**string)) = strPtr("customer_request")
	*(dest[17].(*time.Time)) = now
	paid := now.Add(time.Minute)
	*(dest[18].(**time.Time)) = &paid
	failed := now.Add(2 * time.Minute)
	*(dest[19].(**time.Time)) = &failed
	refunded := now.Add(3 * time.Minute)
	*(dest[20].(**time.Time)) = &refunded
	expired := now.Add(4 * time.Minute)
	*(dest[21].(**time.Time)) = &expired
	*(dest[22].(*time.Time)) = now
	*(dest[23].(*time.Time)) = now
	return nil
}

func TestCoursePurchaseRepo_GetByID_FullScanMapsPointers(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanCoursePurchaseRowFull
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	cp, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if cp.StripePaymentIntentID != "pi_test_full" {
		t.Errorf("PaymentIntentID=%q", cp.StripePaymentIntentID)
	}
	if cp.StripeChargeID != "ch_test_full" {
		t.Errorf("ChargeID=%q", cp.StripeChargeID)
	}
	if cp.StripeRefundID != "re_test_full" {
		t.Errorf("RefundID=%q", cp.StripeRefundID)
	}
	if cp.StripeCheckoutURL != "https://checkout.stripe.com/c/pay/full" {
		t.Errorf("CheckoutURL=%q", cp.StripeCheckoutURL)
	}
	if cp.StripeFailureCode != "card_declined" || cp.StripeFailureMessage != "declined" {
		t.Errorf("failure=%q/%q", cp.StripeFailureCode, cp.StripeFailureMessage)
	}
	if cp.RefundReason != shared.RefundReasonCustomerRequest {
		t.Errorf("RefundReason=%q", cp.RefundReason)
	}
	if cp.PaidAt == nil || cp.FailedAt == nil || cp.RefundedAt == nil || cp.ExpiredAt == nil {
		t.Errorf("timestamps not mapped: %+v", cp.Purchase)
	}
	if cp.AmountCentsRefunded != 35000 {
		t.Errorf("AmountCentsRefunded=%d", cp.AmountCentsRefunded)
	}
}

func scanUserManaTopUpRowFull(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = testPurchaseID
	*(dest[1].(*string)) = testTenantID
	*(dest[2].(*string)) = testLearnerGCID
	*(dest[3].(*string)) = "mana.user_topup.standard_v1"
	*(dest[4].(*string)) = string(shared.StatePaymentCaptured)
	*(dest[5].(*int64)) = 1990
	*(dest[6].(*int64)) = 1990
	*(dest[7].(*int64)) = 0
	*(dest[8].(*string)) = "SGD"
	*(dest[9].(*int64)) = 2500
	*(dest[10].(*int64)) = 0
	*(dest[11].(*string)) = testSessionID
	*(dest[12].(**string)) = strPtr("pi_full")
	*(dest[13].(**string)) = strPtr("ch_full")
	*(dest[14].(**string)) = strPtr("re_full")
	*(dest[15].(**string)) = strPtr("https://u")
	*(dest[16].(**string)) = strPtr("bank_declined")
	*(dest[17].(**string)) = strPtr("bank declined")
	*(dest[18].(**string)) = strPtr("customer_request")
	*(dest[19].(*time.Time)) = now
	paid := now.Add(time.Minute)
	*(dest[20].(**time.Time)) = &paid
	*(dest[21].(**time.Time)) = &paid
	*(dest[22].(**time.Time)) = &paid
	*(dest[23].(**time.Time)) = &paid
	*(dest[24].(*time.Time)) = now
	*(dest[25].(*time.Time)) = now
	return nil
}

func TestUserManaTopUpRepo_GetByID_FullScanMapsPointers(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanUserManaTopUpRowFull
	repo := NewUserManaTopUpRepo(newStubTxRunner(q))
	u, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if u.SKU != "mana.user_topup.standard_v1" || u.ManaUnits != 2500 {
		t.Errorf("u=%+v", u)
	}
	if u.StripePaymentIntentID != "pi_full" || u.StripeFailureCode != "bank_declined" {
		t.Errorf("pointers: %+v", u.Purchase)
	}
	if u.RefundReason != shared.RefundReasonCustomerRequest {
		t.Errorf("RefundReason=%q", u.RefundReason)
	}
	if u.PaidAt == nil {
		t.Errorf("PaidAt not mapped")
	}
}

func scanWebhookEventRowFull(dest ...any) error {
	now := fixedTime()
	*(dest[0].(*string)) = "evt_full"
	*(dest[1].(*string)) = "checkout.session.completed"
	*(dest[2].(*time.Time)) = now
	processed := now.Add(time.Minute)
	*(dest[3].(**time.Time)) = &processed
	*(dest[4].(**string)) = strPtr("boom")
	*(dest[5].(**string)) = strPtr("course_purchase")
	*(dest[6].(**string)) = strPtr(testPurchaseID)
	return nil
}

func TestWebhookEventRepo_GetByEventID_FullScanMapsPointers(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = scanWebhookEventRowFull
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	w, err := repo.GetByEventID(context.Background(), "evt_full")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if w.ProcessingError != "boom" {
		t.Errorf("ProcessingError=%q", w.ProcessingError)
	}
	if w.TargetAggregateType != wh.AggregateCoursePurchase {
		t.Errorf("TargetAggregateType=%q", w.TargetAggregateType)
	}
	if w.TargetPurchaseID != testPurchaseID {
		t.Errorf("TargetPurchaseID=%q", w.TargetPurchaseID)
	}
	if w.ProcessedAt == nil || !w.ProcessedAt.Equal(fixedTime().Add(time.Minute)) {
		t.Errorf("ProcessedAt=%v", w.ProcessedAt)
	}
}

func TestWebhookEventRepo_GetByEventID_ScanErrorPath(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error { return errors.New("scan failed") }
	repo := NewWebhookEventRepo(newStubTxRunner(q))
	if _, err := repo.GetByEventID(context.Background(), "evt_x"); err == nil {
		t.Error("scan error should propagate")
	}
}

func TestUserManaTopUpRepo_GetByStripeSessionID_ScanErrors(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error { return errors.New("scan failed") }
	repo := NewUserManaTopUpRepo(newStubTxRunner(q))
	if _, err := repo.GetByStripeSessionID(withTenant(context.Background(), testTenantID), testSessionID); err == nil {
		t.Error("scan error should propagate")
	}
}

func TestCoursePurchaseRepo_GetByID_ScanErrorPath(t *testing.T) {
	t.Parallel()
	q := newStubQuerier()
	q.rowScan = func(_ ...any) error { return errors.New("scan failed") }
	repo := NewCoursePurchaseRepo(newStubTxRunner(q))
	if _, err := repo.GetByID(withTenant(context.Background(), testTenantID), testTenantID, testPurchaseID); err == nil {
		t.Error("scan error should propagate")
	}
}

var _ = coursepurchase.ErrCourseRequired
var _ = user_mana_topup.ErrSKURequired
