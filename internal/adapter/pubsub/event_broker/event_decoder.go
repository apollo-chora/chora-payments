// event_decoder.go — decodes one of the 28 chora.payments.{aggregate}.
// {state_event}.v1 Protobuf payloads into the unified PaymentEvent shape
// served on SSE.
//
// 7 Purchase aggregates × 4 state-change event types = 28 supported
// subjects:
//
//	chora.payments.course_purchase.{payment_captured|refunded|payment_failed|expired}.v1
//	chora.payments.application_payment.{...}.v1
//	chora.payments.familiar_egg_purchase.{...}.v1
//	chora.payments.tenant_mana_topup.{...}.v1
//	chora.payments.user_subscription.{...}.v1
//	chora.payments.user_mana_topup.{...}.v1
//	chora.payments.identity_kyc_fee.{...}.v1
//
// Per the OpenAPI contract (chora-contracts/openapi/payments-admin.yaml::
// streamAdminPaymentEvents), the FE's PaymentEventEnvelope.state surface
// is a closed enum of 4 values: captured | refunded | failed | expired.
// The 5-state shared.State FSM (pending / captured / failed / refunded /
// expired) maps onto this trivially:
//   - payment_captured  → "captured"
//   - refunded          → "refunded"
//   - payment_failed    → "failed"
//   - expired           → "expired"
//
// `checkout_started` events are NOT in the SSE feed — they happen pre-
// payment and the FE renders rows only for committed state transitions.
// Same for the 4 dispute events — those are admin-side chargeback signals
// rendered separately (out of scope for this broker).
//
// Hexagonal: leaf decoder; depends only on chora-contracts proto +
// chora-common envelope.
package event_broker

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/envelope"

	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"
)

// -----------------------------------------------------------------------------
// PaymentEvent — wire shape served to SSE clients. Mirrors the OpenAPI
// PaymentEventEnvelope shape so the broker output drops straight into the
// admin_handler.go SSE writer.
//
// MUST stay in lockstep with httpadapter.PaymentEvent in admin_handler.go
// (the http layer alias-converts to its own struct because Go forbids
// cross-package type assertions). Field names match the JSON keys served
// on the wire.
// -----------------------------------------------------------------------------

// PaymentEvent is the unified, FE-rendered event shape.
type PaymentEvent struct {
	PurchaseID      string
	AggregateType   string
	State           string // captured | refunded | failed | expired
	OccurredAt      string // RFC3339Nano
	TenantID        string
	LearnerGCID     string
	AmountCents     int64
	Currency        string
	StripeSessionID string

	// IdempotencyKey is the dedup handle — broker uses it (fallback EventID)
	// to skip replayed messages within the inbox TTL window.
	IdempotencyKey string
	EventID        string

	// Topic is the canonical event subject the event arrived on.
	// Kept so the SSE writer can label the wire event if needed.
	Topic string
}

// ErrUnknownTopic is returned when the subject is not in SupportedSubjects.
var ErrUnknownTopic = errors.New("event_decoder: unknown subject")

// -----------------------------------------------------------------------------
// Subject taxonomy — generated from the 7 aggregates × 4 state events fan.
// -----------------------------------------------------------------------------

// aggregates is the canonical 7-element ordering used by the subject map.
var aggregates = []string{
	"course_purchase",
	"application_payment",
	"familiar_egg_purchase",
	"tenant_mana_topup",
	"user_subscription",
	"user_mana_topup",
	"identity_kyc_fee",
}

// stateEvents are the 4 chora.payments.*.{event_type}.v1 suffixes carried
// on the SSE feed. (checkout_started is pre-state and skipped.)
var stateEvents = []string{
	"payment_captured", // → "captured"
	"refunded",         // → "refunded"
	"payment_failed",   // → "failed"
	"expired",          // → "expired"
}

// SupportedSubjects returns the 28 canonical subject names the broker
// subscribes to. Used by NewBroker validation + main.go env-var defaults.
func SupportedSubjects() []string {
	out := make([]string, 0, len(aggregates)*len(stateEvents))
	for _, agg := range aggregates {
		for _, ev := range stateEvents {
			out = append(out, fmt.Sprintf("chora.payments.%s.%s.v1", agg, ev))
		}
	}
	return out
}

// DefaultSubscriptionName returns the canonical durable-consumer name for
// a chora.payments.* subject. Format mirrors the historical pull-subscription
// convention:
//
//	chora-payments-fanout-{aggregate}-{event_type}
//
// (chora-payments OWNS these consumers even though chora-payments is
// also the publisher — the broker subscribes back to its own outbox stream
// for the SSE fan-out. This is the only Chora service that legitimately
// loops back through the bus.)
func DefaultSubscriptionName(subject string) string {
	const prefix = "chora.payments."
	if !strings.HasPrefix(subject, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(subject, prefix)
	parts := strings.Split(rest, ".")
	if len(parts) < 3 {
		return ""
	}
	aggregate := parts[0]
	// Drop the trailing version segment.
	eventType := strings.Join(parts[1:len(parts)-1], "_")
	return fmt.Sprintf("chora-payments-fanout-%s-%s", aggregate, eventType)
}

// -----------------------------------------------------------------------------
// decodePaymentEvent — the single entry point invoked by the broker.
// -----------------------------------------------------------------------------

// stateFromSubject maps the subject's event-type segment to the SSE state enum.
func stateFromSubject(subject string) (string, error) {
	const prefix = "chora.payments."
	if !strings.HasPrefix(subject, prefix) {
		return "", ErrUnknownTopic
	}
	rest := strings.TrimPrefix(subject, prefix)
	parts := strings.Split(rest, ".")
	if len(parts) < 3 {
		return "", ErrUnknownTopic
	}
	switch strings.Join(parts[1:len(parts)-1], "_") {
	case "payment_captured":
		return "captured", nil
	case "refunded":
		return "refunded", nil
	case "payment_failed":
		return "failed", nil
	case "expired":
		return "expired", nil
	}
	return "", ErrUnknownTopic
}

// aggregateFromSubject extracts the aggregate segment.
func aggregateFromSubject(subject string) (string, error) {
	const prefix = "chora.payments."
	if !strings.HasPrefix(subject, prefix) {
		return "", ErrUnknownTopic
	}
	rest := strings.TrimPrefix(subject, prefix)
	parts := strings.Split(rest, ".")
	if len(parts) < 3 {
		return "", ErrUnknownTopic
	}
	agg := parts[0]
	for _, valid := range aggregates {
		if agg == valid {
			return agg, nil
		}
	}
	return "", ErrUnknownTopic
}

// decodePaymentEvent is the worker called by the broker per-message handler.
// Returns ErrUnknownSubject for subjects outside SupportedSubjects.
//
// env is the transport envelope the eventbus reconstructed from the
// publisher's headers; tenant_id from env is preferred, fallback to the
// proto envelope's tenant_id (publisher invariant).
func decodePaymentEvent(subject string, body []byte, env envelope.Envelope) (PaymentEvent, error) {
	state, err := stateFromSubject(subject)
	if err != nil {
		return PaymentEvent{}, err
	}
	agg, err := aggregateFromSubject(subject)
	if err != nil {
		return PaymentEvent{}, err
	}

	// Dispatch to the per-(aggregate, state) decoder. The 28 combinations
	// produce 28 distinct generated message types; each carries a different
	// (envelope, purchase_id, currency, ...) shape. Common fields are
	// extracted into the PaymentEvent below.
	out := PaymentEvent{
		AggregateType:  agg,
		State:          state,
		Topic:          subject,
		EventID:        env.EventID,
		IdempotencyKey: env.IdempotencyKey,
	}
	if env.TenantID != "" {
		out.TenantID = env.TenantID
	}

	switch {
	case agg == "course_purchase" && state == "captured":
		m := &paymentsv1.CoursePurchasePaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "course_purchase" && state == "refunded":
		m := &paymentsv1.CoursePurchaseRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		// course_purchase.refunded doesn't carry stripe_session_id; use empty.
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "course_purchase" && state == "failed":
		m := &paymentsv1.CoursePurchasePaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "course_purchase" && state == "expired":
		m := &paymentsv1.CoursePurchaseExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetExpiredAt())

	case agg == "application_payment" && state == "captured":
		m := &paymentsv1.ApplicationPaymentPaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "application_payment" && state == "refunded":
		m := &paymentsv1.ApplicationPaymentRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "application_payment" && state == "failed":
		m := &paymentsv1.ApplicationPaymentPaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "application_payment" && state == "expired":
		m := &paymentsv1.ApplicationPaymentExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetExpiredAt())

	case agg == "familiar_egg_purchase" && state == "captured":
		m := &paymentsv1.CompanionEggPurchasePaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "familiar_egg_purchase" && state == "refunded":
		m := &paymentsv1.CompanionEggPurchaseRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "familiar_egg_purchase" && state == "failed":
		m := &paymentsv1.CompanionEggPurchasePaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "familiar_egg_purchase" && state == "expired":
		m := &paymentsv1.CompanionEggPurchaseExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetExpiredAt())

	case agg == "tenant_mana_topup" && state == "captured":
		m := &paymentsv1.TenantManaTopUpPaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "tenant_mana_topup" && state == "refunded":
		m := &paymentsv1.TenantManaTopUpRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "tenant_mana_topup" && state == "failed":
		m := &paymentsv1.TenantManaTopUpPaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "tenant_mana_topup" && state == "expired":
		m := &paymentsv1.TenantManaTopUpExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetExpiredAt())

	case agg == "user_subscription" && state == "captured":
		m := &paymentsv1.UserSubscriptionPaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "user_subscription" && state == "refunded":
		m := &paymentsv1.UserSubscriptionRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "user_subscription" && state == "failed":
		m := &paymentsv1.UserSubscriptionPaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "user_subscription" && state == "expired":
		m := &paymentsv1.UserSubscriptionExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.OccurredAt = tsString(m.GetExpiredAt())

	case agg == "user_mana_topup" && state == "captured":
		m := &paymentsv1.UserManaTopUpPaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "user_mana_topup" && state == "refunded":
		m := &paymentsv1.UserManaTopUpRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "user_mana_topup" && state == "failed":
		m := &paymentsv1.UserManaTopUpPaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "user_mana_topup" && state == "expired":
		m := &paymentsv1.UserManaTopUpExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetExpiredAt())

	case agg == "identity_kyc_fee" && state == "captured":
		m := &paymentsv1.IdentityKycFeePaymentCaptured{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsPaid()
		out.Currency = m.GetCurrency()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetPaidAt())

	case agg == "identity_kyc_fee" && state == "refunded":
		m := &paymentsv1.IdentityKycFeeRefunded{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.AmountCents = m.GetAmountCentsRefunded()
		out.Currency = m.GetCurrency()
		out.OccurredAt = tsString(m.GetRefundedAt())

	case agg == "identity_kyc_fee" && state == "failed":
		m := &paymentsv1.IdentityKycFeePaymentFailed{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetFailedAt())

	case agg == "identity_kyc_fee" && state == "expired":
		m := &paymentsv1.IdentityKycFeeExpired{}
		if err := proto.Unmarshal(body, m); err != nil {
			return out, fmt.Errorf("event_decoder: unmarshal %s: %w", subject, err)
		}
		fillFromEnvelope(&out, m.GetEnvelope())
		out.PurchaseID = m.GetPurchaseId()
		out.LearnerGCID = m.GetLearnerGcid()
		out.StripeSessionID = m.GetStripeSessionId()
		out.OccurredAt = tsString(m.GetExpiredAt())

	default:
		return out, ErrUnknownTopic
	}

	// If OccurredAt wasn't filled by the per-event timestamp (defensive —
	// some published rows omit the per-state timestamp), fall back to the
	// envelope's occurred_at.
	if out.OccurredAt == "" {
		out.OccurredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return out, nil
}

// fillFromEnvelope copies tenant_id + idempotency_key + event_id from the
// proto envelope when the corresponding PaymentEvent fields are still empty
// (i.e. attrs didn't provide them).
func fillFromEnvelope(out *PaymentEvent, env interface {
	GetTenantId() string
	GetIdempotencyKey() string
	GetEventId() string
	GetGcid() string
	GetOccurredAt() *timestamppb.Timestamp
}) {
	if env == nil {
		return
	}
	if out.TenantID == "" {
		out.TenantID = env.GetTenantId()
	}
	if out.IdempotencyKey == "" {
		out.IdempotencyKey = env.GetIdempotencyKey()
	}
	if out.EventID == "" {
		out.EventID = env.GetEventId()
	}
	if out.LearnerGCID == "" {
		out.LearnerGCID = env.GetGcid()
	}
	if out.OccurredAt == "" {
		out.OccurredAt = tsString(env.GetOccurredAt())
	}
}

// tsString returns "" for nil/zero-value timestamps, otherwise RFC3339Nano.
func tsString(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	t := ts.AsTime()
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
