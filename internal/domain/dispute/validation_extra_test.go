// validation_extra_test.go — residual validation + idempotency branches of
// the Dispute aggregate not covered by dispute_test.go.
package dispute

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

func TestNew_RequiresDisputeID(t *testing.T) {
	t.Parallel()
	_, err := New("", "tenant-1", wh.AggregateCoursePurchase, "p-1", "dp_1", "ch_1", 100, "USD", ReasonFraudulent, time.Now(), time.Now())
	if !errors.Is(err, ErrDisputeIDRequired) {
		t.Errorf("err=%v, want ErrDisputeIDRequired", err)
	}
}

func TestNew_RequiresTenantID(t *testing.T) {
	t.Parallel()
	_, err := New("d-1", "", wh.AggregateCoursePurchase, "p-1", "dp_1", "ch_1", 100, "USD", ReasonFraudulent, time.Now(), time.Now())
	if !errors.Is(err, shared.ErrTenantRequired) {
		t.Errorf("err=%v, want shared.ErrTenantRequired", err)
	}
}

func TestNew_RequiresPurchaseID(t *testing.T) {
	t.Parallel()
	_, err := New("d-1", "t-1", wh.AggregateCoursePurchase, "", "dp_1", "ch_1", 100, "USD", ReasonFraudulent, time.Now(), time.Now())
	if !errors.Is(err, ErrPurchaseIDRequired) {
		t.Errorf("err=%v, want ErrPurchaseIDRequired", err)
	}
}

func TestNew_RequiresStripeChargeID(t *testing.T) {
	t.Parallel()
	_, err := New("d-1", "t-1", wh.AggregateCoursePurchase, "p-1", "dp_1", "", 100, "USD", ReasonFraudulent, time.Now(), time.Now())
	if !errors.Is(err, ErrStripeChargeRequired) {
		t.Errorf("err=%v, want ErrStripeChargeRequired", err)
	}
}

func TestRecordFundsReinstated_Idempotent(t *testing.T) {
	t.Parallel()
	d := mustNew(t)
	now := time.Now().UTC()
	if err := d.RecordFundsReinstated(now); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := d.RecordFundsReinstated(now.Add(time.Hour)); err != nil {
		t.Fatalf("idempotent repeat: %v", err)
	}
	if d.FundsReinstatedAt == nil || !d.FundsReinstatedAt.Equal(now) {
		t.Errorf("FundsReinstatedAt=%v, want first timestamp preserved", d.FundsReinstatedAt)
	}
}
