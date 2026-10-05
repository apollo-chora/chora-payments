// coverage_extra_test.go — residual event_broker branches: handleMessage
// without a dedup key, Inbox failure nack, newClientID, tsString.
package event_broker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
)

const extraSubject = "chora.payments.course_purchase.payment_captured.v1"

// TestHandleMessage_NoDedupKeyFansOutAndAcks — a payload with neither an
// idempotency_key nor an event_id must still fan out (defensive branch).
func TestHandleMessage_NoDedupKeyFansOutAndAcks(t *testing.T) {
	stub := newStubBus()
	b, err := NewBroker(BrokerConfig{
		Bus:                    stub,
		SubscriptionsBySubject: map[string]string{extraSubject: "extra-sub-1"},
	})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, cleanup, err := b.Subscribe(ctx, "tenant-A", "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cleanup()

	payload := &paymentsv1.CoursePurchasePaymentCaptured{
		Envelope:        &commonv1.EventEnvelope{TenantId: "tenant-A"},
		PurchaseId:      "p-nodedupe",
		LearnerGcid:     "g-1",
		AmountCentsPaid: 4999,
		Currency:        "sgd",
		PaidAt:          timestamppb.New(time.Now().UTC()),
	}
	bz, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := eventbus.Message{Subject: extraSubject, Payload: bz}

	if err := b.handleMessage(ctx, extraSubject, msg); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	select {
	case ev := <-ch:
		if ev.PurchaseID != "p-nodedupe" {
			t.Errorf("ev=%+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("no fan-out received")
	}
}

// failingStore forces the Inbox.Process error path (return error → nack).
type failingStore struct{}

func (failingStore) Process(_ context.Context, _ string, _ time.Duration, _ func() error) error {
	return errors.New("inbox store unavailable")
}

func (failingStore) Seen(_ context.Context, _ string) (bool, error) { return false, nil }

func (failingStore) Mark(_ context.Context, _ string, _ time.Duration) error { return nil }

func (failingStore) CleanupExpired(_ context.Context) (int, error) { return 0, nil }

func TestHandleMessage_InboxFailureNacks(t *testing.T) {
	b, err := NewBroker(BrokerConfig{
		Bus:                    newStubBus(),
		SubscriptionsBySubject: map[string]string{extraSubject: "extra-sub-2"},
		Inbox:                  failingStore{},
	})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := buildBusMessageCoursePaymentCaptured(t, "tenant-A", "p-inboxfail", "idem-fail")

	if err := b.handleMessage(ctx, extraSubject, msg); err == nil {
		t.Fatal("expected inbox error to propagate")
	}
}

func TestNewClientID_IsPrefixedUUID(t *testing.T) {
	id := newClientID()
	if !strings.HasPrefix(id, "brkclient-") || len(id) != len("brkclient-")+36 {
		t.Errorf("client id=%q", id)
	}
}

func TestTsString(t *testing.T) {
	if got := tsString(nil); got != "" {
		t.Errorf("nil=%q", got)
	}
	if got := tsString(timestamppb.New(time.Time{})); got != "" {
		t.Errorf("zero=%q", got)
	}
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	if got := tsString(timestamppb.New(now)); got != "2026-05-24T12:00:00Z" {
		t.Errorf("got=%q", got)
	}
}

var _ idempotent.Store = failingStore{}
