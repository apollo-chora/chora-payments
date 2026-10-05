// Package outbox is chora-payments' D6.2 producer-side transactional
// outbox adapter for the 29-topic chora.payments.*.v1 taxonomy.
//
// payload.go — per-(aggregate, event_type) proto marshalling. Each
// (aggregate, event_type) maps to a distinct event message in
// chora-contracts/proto/events/payments/*.proto.
package outbox

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	paymentsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/payments/v1"

	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"

	"github.com/apollo-chora/chora-common/env"
)

// topicForEvent returns the canonical event subject for a given
// (aggregate, event_type). Subject taxonomy: chora.payments.{aggregate}.
// {event_type}.v1 — the canonical event names are valid NATS subjects, so
// the taxonomy carries over unchanged.
// sourceProject is the project label this service runs in, stamped into the
// event envelope's mandatory source_project field.
//
// Was a hardcoded literal until 2026-09-02 (CHO-2419). No manifest could reach
// it, so a second org stamped every payments event with chora-489812 and any
// consumer filtering on source_project would have been filtering on a lie. The
// literal stays as the fallback so this estate is provably unchanged.
var sourceProject = env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812")

func topicForEvent(agg wh.AggregateType, evType dispatcher.EventType) string {
	return "chora.payments." + string(agg) + "." + string(evType) + ".v1"
}

// topicForDisputeEvent returns the cross-aggregate dispute topic name
// per ADR-164 §229.5. Topics: chora.payments.dispute.{event_type}.v1.
func topicForDisputeEvent(evType dispatcher.EventType) string {
	return "chora.payments.dispute." + string(evType) + ".v1"
}

// buildEnvelope returns a chora.common.v1.EventEnvelope for the outbox row.
//
// traceparent is the W3C trace-context the event is published under. Payments
// is the trace ROOT for Stripe webhook-originated events (Stripe sends no
// traceparent), so the caller mints one when the emit context carries none —
// see emitTraceparent. The embedded envelope MUST carry it (contract honesty
// per the "traceparent mandatory in the event envelope" invariant); the same
// value is also stamped on the outbox row so the dispatcher's reconstructed
// envelope carries it for subscribers that validate the envelope.
func buildEnvelope(tenantID, eventID, idempotencyKey, traceparent string, occurredAt time.Time) *commonv1.EventEnvelope {
	return &commonv1.EventEnvelope{
		EventId:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantId:       tenantID,
		OccurredAt:     timestamppb.New(occurredAt),
		PublishedAt:    timestamppb.New(occurredAt),
		Traceparent:    traceparent,
		SourceProject:  sourceProject,
		SourceService:  "chora-payments",
		SchemaVersion:  1,
	}
}

// loadAndMarshal fetches the aggregate matching (env.TenantId, agg, purchaseID)
// from the repos and marshals the per-event-type proto.
func loadAndMarshal(ctx context.Context, deps EmitterDeps, env *commonv1.EventEnvelope, agg wh.AggregateType, purchaseID string, evType dispatcher.EventType) ([]byte, string, error) {
	switch agg {
	case wh.AggregateCoursePurchase:
		cp, err := deps.Course.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("course_purchase lookup: %w", err)
		}
		return marshalCoursePurchase(env, cp, evType)
	case wh.AggregateApplicationPayment:
		ap, err := deps.Application.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("application_payment lookup: %w", err)
		}
		return marshalApplicationPayment(env, ap, evType)
	case wh.AggregateFamiliarEggPurchase:
		ep, err := deps.FamiliarEgg.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("familiar_egg_purchase lookup: %w", err)
		}
		return marshalFamiliarEggPurchase(env, ep, evType)
	case wh.AggregateTenantManaTopUp:
		m, err := deps.ManaTopUp.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("tenant_mana_topup lookup: %w", err)
		}
		return marshalTenantManaTopUp(env, m, evType)
	case wh.AggregateUserSubscription:
		u, err := deps.Subscription.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("user_subscription lookup: %w", err)
		}
		return marshalUserSubscription(env, u, evType)
	case wh.AggregateUserManaTopUp:
		um, err := deps.UserManaTopUp.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("user_mana_topup lookup: %w", err)
		}
		return marshalUserManaTopUp(env, um, evType)
	case wh.AggregateIdentityKycFee:
		k, err := deps.IdentityKycFee.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("identity_kyc_fee lookup: %w", err)
		}
		return marshalIdentityKycFee(env, k, evType)
	case wh.AggregateTenantAddonPurchase:
		a, err := deps.AddonPurchase.GetByID(ctx, env.TenantId, purchaseID)
		if err != nil {
			return nil, "", fmt.Errorf("tenant_addon_purchase lookup: %w", err)
		}
		return marshalTenantAddonPurchase(env, a, evType)
	}
	return nil, "", fmt.Errorf("outbox: unknown aggregate %q", agg)
}

func marshalCoursePurchase(env *commonv1.EventEnvelope, cp *coursepurchase.CoursePurchase, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.CoursePurchaseCheckoutStarted{
			Envelope:          env,
			PurchaseId:        cp.PurchaseID,
			LearnerGcid:       cp.LearnerGCID,
			CourseId:          cp.CourseID,
			StripeSessionId:   cp.StripeSessionID,
			StripeCheckoutUrl: cp.StripeCheckoutURL,
			AmountCents:       cp.AmountCents,
			Currency:          cp.Currency,
			CheckoutStartedAt: timestamppb.New(cp.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateCoursePurchase, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.CoursePurchasePaymentCaptured{
			Envelope:              env,
			PurchaseId:            cp.PurchaseID,
			LearnerGcid:           cp.LearnerGCID,
			CourseId:              cp.CourseID,
			StripeSessionId:       cp.StripeSessionID,
			StripePaymentIntentId: cp.StripePaymentIntentID,
			StripeChargeId:        cp.StripeChargeID,
			AmountCentsPaid:       cp.AmountCentsPaid,
			AmountCentsRefunded:   cp.AmountCentsRefunded,
			Currency:              cp.Currency,
			PaidAt:                timestamppbOrNil(cp.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateCoursePurchase, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.CoursePurchasePaymentFailed{
			Envelope:             env,
			PurchaseId:           cp.PurchaseID,
			LearnerGcid:          cp.LearnerGCID,
			CourseId:             cp.CourseID,
			StripeSessionId:      cp.StripeSessionID,
			StripeFailureCode:    cp.StripeFailureCode,
			StripeFailureMessage: cp.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(cp.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateCoursePurchase, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.CoursePurchaseRefunded{
			Envelope:            env,
			PurchaseId:          cp.PurchaseID,
			LearnerGcid:         cp.LearnerGCID,
			CourseId:            cp.CourseID,
			StripeChargeId:      cp.StripeChargeID,
			StripeRefundId:      cp.StripeRefundID,
			AmountCentsRefunded: cp.AmountCentsRefunded,
			Currency:            cp.Currency,
			Reason:              string(cp.RefundReason),
			RefundedAt:          timestamppbOrNil(cp.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateCoursePurchase, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.CoursePurchaseExpired{
			Envelope:        env,
			PurchaseId:      cp.PurchaseID,
			LearnerGcid:     cp.LearnerGCID,
			CourseId:        cp.CourseID,
			StripeSessionId: cp.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(cp.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateCoursePurchase, evType)
	}
	return nil, "", fmt.Errorf("outbox: course_purchase event_type %q not supported", evType)
}

func marshalApplicationPayment(env *commonv1.EventEnvelope, ap *apppay.ApplicationPayment, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.ApplicationPaymentCheckoutStarted{
			Envelope:          env,
			PurchaseId:        ap.PurchaseID,
			LearnerGcid:       ap.LearnerGCID,
			ApplicationId:     ap.ApplicationID,
			CourseId:          ap.CourseID,
			StripeSessionId:   ap.StripeSessionID,
			StripeCheckoutUrl: ap.StripeCheckoutURL,
			AmountCents:       ap.AmountCents,
			Currency:          ap.Currency,
			CheckoutStartedAt: timestamppb.New(ap.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateApplicationPayment, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.ApplicationPaymentPaymentCaptured{
			Envelope:              env,
			PurchaseId:            ap.PurchaseID,
			LearnerGcid:           ap.LearnerGCID,
			ApplicationId:         ap.ApplicationID,
			CourseId:              ap.CourseID,
			StripeSessionId:       ap.StripeSessionID,
			StripePaymentIntentId: ap.StripePaymentIntentID,
			StripeChargeId:        ap.StripeChargeID,
			AmountCentsPaid:       ap.AmountCentsPaid,
			AmountCentsRefunded:   ap.AmountCentsRefunded,
			Currency:              ap.Currency,
			PaidAt:                timestamppbOrNil(ap.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateApplicationPayment, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.ApplicationPaymentPaymentFailed{
			Envelope:             env,
			PurchaseId:           ap.PurchaseID,
			LearnerGcid:          ap.LearnerGCID,
			ApplicationId:        ap.ApplicationID,
			StripeSessionId:      ap.StripeSessionID,
			StripeFailureCode:    ap.StripeFailureCode,
			StripeFailureMessage: ap.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(ap.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateApplicationPayment, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.ApplicationPaymentRefunded{
			Envelope:            env,
			PurchaseId:          ap.PurchaseID,
			LearnerGcid:         ap.LearnerGCID,
			ApplicationId:       ap.ApplicationID,
			StripeChargeId:      ap.StripeChargeID,
			StripeRefundId:      ap.StripeRefundID,
			AmountCentsRefunded: ap.AmountCentsRefunded,
			Currency:            ap.Currency,
			Reason:              string(ap.RefundReason),
			RefundedAt:          timestamppbOrNil(ap.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateApplicationPayment, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.ApplicationPaymentExpired{
			Envelope:        env,
			PurchaseId:      ap.PurchaseID,
			LearnerGcid:     ap.LearnerGCID,
			ApplicationId:   ap.ApplicationID,
			StripeSessionId: ap.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(ap.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateApplicationPayment, evType)
	}
	return nil, "", fmt.Errorf("outbox: application_payment event_type %q not supported", evType)
}

func marshalFamiliarEggPurchase(env *commonv1.EventEnvelope, ep *egg.FamiliarEggPurchase, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.CompanionEggPurchaseCheckoutStarted{
			Envelope:             env,
			PurchaseId:           ep.PurchaseID,
			LearnerGcid:          ep.LearnerGCID,
			TargetTenantId:       ep.TenantID,
			EggSku:               ep.EggSKU,
			StripeSessionId:      ep.StripeSessionID,
			StripeCheckoutUrl:    ep.StripeCheckoutURL,
			AmountCents:          ep.AmountCents,
			Currency:             ep.Currency,
			SuggestedFocalAtomId: ep.SuggestedFocalAtomID,
			CheckoutStartedAt:    timestamppb.New(ep.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateFamiliarEggPurchase, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.CompanionEggPurchasePaymentCaptured{
			Envelope:              env,
			PurchaseId:            ep.PurchaseID,
			LearnerGcid:           ep.LearnerGCID,
			TargetTenantId:        ep.TenantID,
			EggSku:                ep.EggSKU,
			StripeSessionId:       ep.StripeSessionID,
			StripePaymentIntentId: ep.StripePaymentIntentID,
			StripeChargeId:        ep.StripeChargeID,
			AmountCentsPaid:       ep.AmountCentsPaid,
			AmountCentsRefunded:   ep.AmountCentsRefunded,
			Currency:              ep.Currency,
			SuggestedFocalAtomId:  ep.SuggestedFocalAtomID,
			PaidAt:                timestamppbOrNil(ep.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateFamiliarEggPurchase, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.CompanionEggPurchasePaymentFailed{
			Envelope:             env,
			PurchaseId:           ep.PurchaseID,
			LearnerGcid:          ep.LearnerGCID,
			EggSku:               ep.EggSKU,
			StripeSessionId:      ep.StripeSessionID,
			StripeFailureCode:    ep.StripeFailureCode,
			StripeFailureMessage: ep.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(ep.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateFamiliarEggPurchase, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.CompanionEggPurchaseRefunded{
			Envelope:            env,
			PurchaseId:          ep.PurchaseID,
			LearnerGcid:         ep.LearnerGCID,
			EggSku:              ep.EggSKU,
			StripeChargeId:      ep.StripeChargeID,
			StripeRefundId:      ep.StripeRefundID,
			AmountCentsRefunded: ep.AmountCentsRefunded,
			Currency:            ep.Currency,
			Reason:              string(ep.RefundReason),
			CreditOnly:          ep.RefundCreditOnly,
			RefundedAt:          timestamppbOrNil(ep.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateFamiliarEggPurchase, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.CompanionEggPurchaseExpired{
			Envelope:        env,
			PurchaseId:      ep.PurchaseID,
			LearnerGcid:     ep.LearnerGCID,
			EggSku:          ep.EggSKU,
			StripeSessionId: ep.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(ep.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateFamiliarEggPurchase, evType)
	}
	return nil, "", fmt.Errorf("outbox: familiar_egg_purchase event_type %q not supported", evType)
}

func marshalTenantManaTopUp(env *commonv1.EventEnvelope, m *mana.TenantManaTopUp, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.TenantManaTopUpCheckoutStarted{
			Envelope:          env,
			PurchaseId:        m.PurchaseID,
			AdminGcid:         m.AdminGCID(),
			Sku:               m.SKU,
			ManaUnits:         m.ManaUnits,
			StripeSessionId:   m.StripeSessionID,
			StripeCheckoutUrl: m.StripeCheckoutURL,
			AmountCents:       m.AmountCents,
			Currency:          m.Currency,
			CheckoutStartedAt: timestamppb.New(m.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantManaTopUp, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.TenantManaTopUpPaymentCaptured{
			Envelope:              env,
			PurchaseId:            m.PurchaseID,
			AdminGcid:             m.AdminGCID(),
			Sku:                   m.SKU,
			ManaUnits:             m.ManaUnits,
			StripeSessionId:       m.StripeSessionID,
			StripePaymentIntentId: m.StripePaymentIntentID,
			StripeChargeId:        m.StripeChargeID,
			AmountCentsPaid:       m.AmountCentsPaid,
			AmountCentsRefunded:   m.AmountCentsRefunded,
			Currency:              m.Currency,
			PaidAt:                timestamppbOrNil(m.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantManaTopUp, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.TenantManaTopUpPaymentFailed{
			Envelope:             env,
			PurchaseId:           m.PurchaseID,
			AdminGcid:            m.AdminGCID(),
			Sku:                  m.SKU,
			StripeSessionId:      m.StripeSessionID,
			StripeFailureCode:    m.StripeFailureCode,
			StripeFailureMessage: m.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(m.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantManaTopUp, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.TenantManaTopUpRefunded{
			Envelope:            env,
			PurchaseId:          m.PurchaseID,
			AdminGcid:           m.AdminGCID(),
			Sku:                 m.SKU,
			StripeChargeId:      m.StripeChargeID,
			StripeRefundId:      m.StripeRefundID,
			AmountCentsRefunded: m.AmountCentsRefunded,
			Currency:            m.Currency,
			ManaUnitsToDebit:    m.ManaUnitsDebited,
			Reason:              string(m.RefundReason),
			RefundedAt:          timestamppbOrNil(m.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantManaTopUp, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.TenantManaTopUpExpired{
			Envelope:        env,
			PurchaseId:      m.PurchaseID,
			AdminGcid:       m.AdminGCID(),
			Sku:             m.SKU,
			StripeSessionId: m.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(m.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantManaTopUp, evType)
	}
	return nil, "", fmt.Errorf("outbox: tenant_mana_topup event_type %q not supported", evType)
}

func marshalUserSubscription(env *commonv1.EventEnvelope, u *sub.UserSubscription, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.UserSubscriptionCheckoutStarted{
			Envelope:          env,
			PurchaseId:        u.PurchaseID,
			LearnerGcid:       u.LearnerGCID,
			PlanSku:           u.PlanSKU,
			BillingPeriod:     string(u.BillingPeriod),
			StripeSessionId:   u.StripeSessionID,
			StripeCheckoutUrl: u.StripeCheckoutURL,
			AmountCents:       u.AmountCents,
			Currency:          u.Currency,
			CheckoutStartedAt: timestamppb.New(u.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserSubscription, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.UserSubscriptionPaymentCaptured{
			Envelope:              env,
			PurchaseId:            u.PurchaseID,
			LearnerGcid:           u.LearnerGCID,
			PlanSku:               u.PlanSKU,
			BillingPeriod:         string(u.BillingPeriod),
			StripeSubscriptionId:  u.StripeSubscriptionID,
			StripeInvoiceId:       u.StripeInvoiceID,
			StripePaymentIntentId: u.StripePaymentIntentID,
			StripeChargeId:        u.StripeChargeID,
			AmountCentsPaid:       u.AmountCentsPaid,
			Currency:              u.Currency,
			PeriodStart:           timestamppbOrNilFrom(u.CurrentPeriodStart),
			PeriodEnd:             timestamppbOrNilFrom(u.CurrentPeriodEnd),
			PaidAt:                timestamppbOrNil(u.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserSubscription, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.UserSubscriptionPaymentFailed{
			Envelope:             env,
			PurchaseId:           u.PurchaseID,
			LearnerGcid:          u.LearnerGCID,
			PlanSku:              u.PlanSKU,
			StripeSubscriptionId: u.StripeSubscriptionID,
			StripeInvoiceId:      u.StripeInvoiceID,
			StripeFailureCode:    u.StripeFailureCode,
			StripeFailureMessage: u.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(u.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserSubscription, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.UserSubscriptionRefunded{
			Envelope:             env,
			PurchaseId:           u.PurchaseID,
			LearnerGcid:          u.LearnerGCID,
			StripeSubscriptionId: u.StripeSubscriptionID,
			StripeInvoiceId:      u.StripeInvoiceID,
			StripeChargeId:       u.StripeChargeID,
			StripeRefundId:       u.StripeRefundID,
			AmountCentsRefunded:  u.AmountCentsRefunded,
			Currency:             u.Currency,
			Reason:               string(u.RefundReason),
			RefundedAt:           timestamppbOrNil(u.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserSubscription, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.UserSubscriptionExpired{
			Envelope:        env,
			PurchaseId:      u.PurchaseID,
			LearnerGcid:     u.LearnerGCID,
			PlanSku:         u.PlanSKU,
			StripeSessionId: u.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(u.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserSubscription, evType)
	case dispatcher.EventSubscriptionCancelled:
		msg := &paymentsv1.UserSubscriptionCancelled{
			Envelope:             env,
			PurchaseId:           u.PurchaseID,
			LearnerGcid:          u.LearnerGCID,
			StripeSubscriptionId: u.StripeSubscriptionID,
			Reason:               u.CancellationReason,
			AtPeriodEnd:          u.CancelAtPeriodEnd,
			EffectiveAt:          timestamppbOrNilFrom(u.EffectiveAt),
			CancelledAt:          timestamppbOrNilFrom(u.CancelledAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserSubscription, evType)
	}
	return nil, "", fmt.Errorf("outbox: user_subscription event_type %q not supported", evType)
}

func marshalUserManaTopUp(env *commonv1.EventEnvelope, u *umt.UserManaTopUp, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.UserManaTopUpCheckoutStarted{
			Envelope:          env,
			PurchaseId:        u.PurchaseID,
			LearnerGcid:       u.LearnerGCID,
			Sku:               u.SKU,
			ManaUnits:         u.ManaUnits,
			StripeSessionId:   u.StripeSessionID,
			StripeCheckoutUrl: u.StripeCheckoutURL,
			AmountCents:       u.AmountCents,
			Currency:          u.Currency,
			CheckoutStartedAt: timestamppb.New(u.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserManaTopUp, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.UserManaTopUpPaymentCaptured{
			Envelope:              env,
			PurchaseId:            u.PurchaseID,
			LearnerGcid:           u.LearnerGCID,
			Sku:                   u.SKU,
			ManaUnits:             u.ManaUnits,
			StripeSessionId:       u.StripeSessionID,
			StripePaymentIntentId: u.StripePaymentIntentID,
			StripeChargeId:        u.StripeChargeID,
			AmountCentsPaid:       u.AmountCentsPaid,
			AmountCentsRefunded:   u.AmountCentsRefunded,
			Currency:              u.Currency,
			PaidAt:                timestamppbOrNil(u.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserManaTopUp, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.UserManaTopUpPaymentFailed{
			Envelope:             env,
			PurchaseId:           u.PurchaseID,
			LearnerGcid:          u.LearnerGCID,
			Sku:                  u.SKU,
			StripeSessionId:      u.StripeSessionID,
			StripeFailureCode:    u.StripeFailureCode,
			StripeFailureMessage: u.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(u.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserManaTopUp, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.UserManaTopUpRefunded{
			Envelope:            env,
			PurchaseId:          u.PurchaseID,
			LearnerGcid:         u.LearnerGCID,
			Sku:                 u.SKU,
			StripeChargeId:      u.StripeChargeID,
			StripeRefundId:      u.StripeRefundID,
			AmountCentsRefunded: u.AmountCentsRefunded,
			Currency:            u.Currency,
			ManaUnitsToDebit:    u.ManaUnitsDebited,
			Reason:              string(u.RefundReason),
			RefundedAt:          timestamppbOrNil(u.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserManaTopUp, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.UserManaTopUpExpired{
			Envelope:        env,
			PurchaseId:      u.PurchaseID,
			LearnerGcid:     u.LearnerGCID,
			Sku:             u.SKU,
			StripeSessionId: u.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(u.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateUserManaTopUp, evType)
	}
	return nil, "", fmt.Errorf("outbox: user_mana_topup event_type %q not supported", evType)
}

func marshalIdentityKycFee(env *commonv1.EventEnvelope, k *kyc.IdentityKycFee, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.IdentityKycFeeCheckoutStarted{
			Envelope:          env,
			PurchaseId:        k.PurchaseID,
			LearnerGcid:       k.LearnerGCID,
			KycDocType:        k.KYCDocType,
			StripeSessionId:   k.StripeSessionID,
			StripeCheckoutUrl: k.StripeCheckoutURL,
			AmountCents:       k.AmountCents,
			Currency:          k.Currency,
			CheckoutStartedAt: timestamppb.New(k.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateIdentityKycFee, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.IdentityKycFeePaymentCaptured{
			Envelope:              env,
			PurchaseId:            k.PurchaseID,
			LearnerGcid:           k.LearnerGCID,
			KycDocType:            k.KYCDocType,
			StripeSessionId:       k.StripeSessionID,
			StripePaymentIntentId: k.StripePaymentIntentID,
			StripeChargeId:        k.StripeChargeID,
			AmountCentsPaid:       k.AmountCentsPaid,
			AmountCentsRefunded:   k.AmountCentsRefunded,
			Currency:              k.Currency,
			PaidAt:                timestamppbOrNil(k.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateIdentityKycFee, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.IdentityKycFeePaymentFailed{
			Envelope:             env,
			PurchaseId:           k.PurchaseID,
			LearnerGcid:          k.LearnerGCID,
			KycDocType:           k.KYCDocType,
			StripeSessionId:      k.StripeSessionID,
			StripeFailureCode:    k.StripeFailureCode,
			StripeFailureMessage: k.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(k.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateIdentityKycFee, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.IdentityKycFeeRefunded{
			Envelope:            env,
			PurchaseId:          k.PurchaseID,
			LearnerGcid:         k.LearnerGCID,
			KycDocType:          k.KYCDocType,
			StripeChargeId:      k.StripeChargeID,
			StripeRefundId:      k.StripeRefundID,
			AmountCentsRefunded: k.AmountCentsRefunded,
			Currency:            k.Currency,
			Reason:              string(k.RefundReason),
			RefundedAt:          timestamppbOrNil(k.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateIdentityKycFee, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.IdentityKycFeeExpired{
			Envelope:        env,
			PurchaseId:      k.PurchaseID,
			LearnerGcid:     k.LearnerGCID,
			KycDocType:      k.KYCDocType,
			StripeSessionId: k.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(k.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateIdentityKycFee, evType)
	}
	return nil, "", fmt.Errorf("outbox: identity_kyc_fee event_type %q not supported", evType)
}

func marshalAndTopic(msg proto.Message, agg wh.AggregateType, evType dispatcher.EventType) ([]byte, string, error) {
	bz, err := proto.Marshal(msg)
	if err != nil {
		return nil, "", fmt.Errorf("outbox: marshal %s.%s: %w", agg, evType, err)
	}
	return bz, topicForEvent(agg, evType), nil
}

func timestamppbOrNil(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func timestamppbOrNilFrom(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

// -----------------------------------------------------------------------------
// ADR-164 §229.5 — Dispute event marshalling (cross-aggregate family).
// -----------------------------------------------------------------------------

// loadAndMarshalDispute fetches the Dispute aggregate matching disputeID
// (RLS-scoped via env.TenantId) and marshals the per-event-type proto.
func loadAndMarshalDispute(ctx context.Context, deps EmitterDeps, env *commonv1.EventEnvelope, disputeID string, evType dispatcher.EventType) ([]byte, string, error) {
	if deps.Dispute == nil {
		return nil, "", fmt.Errorf("outbox: dispute repo not wired")
	}
	d, err := deps.Dispute.GetByID(ctx, env.TenantId, disputeID)
	if err != nil {
		return nil, "", fmt.Errorf("dispute lookup: %w", err)
	}
	return marshalDispute(env, d, evType)
}

func marshalDispute(env *commonv1.EventEnvelope, d *dispute.Dispute, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventDisputeRaised:
		msg := &paymentsv1.DisputeRaised{
			Envelope:        env,
			DisputeId:       d.DisputeID,
			AggregateType:   string(d.AggregateType),
			PurchaseId:      d.PurchaseID,
			StripeDisputeId: d.StripeDisputeID,
			StripeChargeId:  d.StripeChargeID,
			AmountCents:     d.AmountCents,
			Currency:        d.Currency,
			Reason:          string(d.Reason),
			EvidenceDueBy:   timestamppb.New(d.EvidenceDueBy),
			RaisedAt:        timestamppb.New(d.RaisedAt),
		}
		return marshalDisputeAndTopic(msg, evType)
	case dispatcher.EventDisputeClosed:
		msg := &paymentsv1.DisputeClosed{
			Envelope:        env,
			DisputeId:       d.DisputeID,
			AggregateType:   string(d.AggregateType),
			PurchaseId:      d.PurchaseID,
			StripeDisputeId: d.StripeDisputeID,
			StripeChargeId:  d.StripeChargeID,
			Outcome:         string(d.Outcome),
			AmountCents:     d.AmountCents,
			Currency:        d.Currency,
			ClosedAt:        timestamppbOrNil(d.ClosedAt),
		}
		return marshalDisputeAndTopic(msg, evType)
	case dispatcher.EventDisputeFundsWithdrawn:
		msg := &paymentsv1.DisputeFundsWithdrawn{
			Envelope:        env,
			DisputeId:       d.DisputeID,
			AggregateType:   string(d.AggregateType),
			PurchaseId:      d.PurchaseID,
			StripeDisputeId: d.StripeDisputeID,
			StripeChargeId:  d.StripeChargeID,
			AmountCents:     d.AmountCents,
			Currency:        d.Currency,
			WithdrawnAt:     timestamppbOrNil(d.FundsWithdrawnAt),
		}
		return marshalDisputeAndTopic(msg, evType)
	case dispatcher.EventDisputeFundsReinstated:
		msg := &paymentsv1.DisputeFundsReinstated{
			Envelope:        env,
			DisputeId:       d.DisputeID,
			AggregateType:   string(d.AggregateType),
			PurchaseId:      d.PurchaseID,
			StripeDisputeId: d.StripeDisputeID,
			StripeChargeId:  d.StripeChargeID,
			AmountCents:     d.AmountCents,
			Currency:        d.Currency,
			ReinstatedAt:    timestamppbOrNil(d.FundsReinstatedAt),
		}
		return marshalDisputeAndTopic(msg, evType)
	}
	return nil, "", fmt.Errorf("outbox: dispute event_type %q not supported", evType)
}

func marshalDisputeAndTopic(msg proto.Message, evType dispatcher.EventType) ([]byte, string, error) {
	bz, err := proto.Marshal(msg)
	if err != nil {
		return nil, "", fmt.Errorf("outbox: marshal dispute.%s: %w", evType, err)
	}
	return bz, topicForDisputeEvent(evType), nil
}

// -----------------------------------------------------------------------------
// CHO-1738 — tenant_addon_purchase (8th Purchase aggregate).
// -----------------------------------------------------------------------------

func marshalTenantAddonPurchase(env *commonv1.EventEnvelope, a *tap.TenantAddonPurchase, evType dispatcher.EventType) ([]byte, string, error) {
	switch evType {
	case dispatcher.EventCheckoutStarted:
		msg := &paymentsv1.TenantAddonPurchaseCheckoutStarted{
			Envelope:          env,
			PurchaseId:        a.PurchaseID,
			AdminGcid:         a.AdminGCID(),
			AddonPlanId:       a.AddonPlanID,
			AddonCode:         a.AddonCode,
			TierCode:          a.TierCode,
			StripeSessionId:   a.StripeSessionID,
			StripeCheckoutUrl: a.StripeCheckoutURL,
			AmountCents:       a.AmountCents,
			Currency:          a.Currency,
			CheckoutStartedAt: timestamppb.New(a.CheckoutStartedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	case dispatcher.EventPaymentCaptured:
		msg := &paymentsv1.TenantAddonPurchasePaymentCaptured{
			Envelope:              env,
			PurchaseId:            a.PurchaseID,
			AdminGcid:             a.AdminGCID(),
			AddonPlanId:           a.AddonPlanID,
			AddonCode:             a.AddonCode,
			TierCode:              a.TierCode,
			StripeSessionId:       a.StripeSessionID,
			StripePaymentIntentId: a.StripePaymentIntentID,
			StripeChargeId:        a.StripeChargeID,
			AmountCentsPaid:       a.AmountCentsPaid,
			AmountCentsRefunded:   a.AmountCentsRefunded,
			Currency:              a.Currency,
			PaidAt:                timestamppbOrNil(a.PaidAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	case dispatcher.EventPaymentFailed:
		msg := &paymentsv1.TenantAddonPurchasePaymentFailed{
			Envelope:             env,
			PurchaseId:           a.PurchaseID,
			AdminGcid:            a.AdminGCID(),
			AddonPlanId:          a.AddonPlanID,
			StripeSessionId:      a.StripeSessionID,
			StripeFailureCode:    a.StripeFailureCode,
			StripeFailureMessage: a.StripeFailureMessage,
			FailedAt:             timestamppbOrNil(a.FailedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	case dispatcher.EventRefunded:
		msg := &paymentsv1.TenantAddonPurchaseRefunded{
			Envelope:            env,
			PurchaseId:          a.PurchaseID,
			AdminGcid:           a.AdminGCID(),
			AddonPlanId:         a.AddonPlanID,
			StripeChargeId:      a.StripeChargeID,
			StripeRefundId:      a.StripeRefundID,
			AmountCentsRefunded: a.AmountCentsRefunded,
			Currency:            a.Currency,
			Reason:              string(a.RefundReason),
			RefundedAt:          timestamppbOrNil(a.RefundedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	case dispatcher.EventExpired:
		msg := &paymentsv1.TenantAddonPurchaseExpired{
			Envelope:        env,
			PurchaseId:      a.PurchaseID,
			AdminGcid:       a.AdminGCID(),
			AddonPlanId:     a.AddonPlanID,
			StripeSessionId: a.StripeSessionID,
			ExpiredAt:       timestamppbOrNil(a.ExpiredAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	// CHO-1775 — Stripe Subscription lifecycle Phase 2 events.
	case dispatcher.EventSubscriptionCancelled:
		msg := &paymentsv1.TenantAddonPurchaseSubscriptionCancelled{
			Envelope:             env,
			PurchaseId:           a.PurchaseID,
			AdminGcid:            a.AdminGCID(),
			AddonPlanId:          a.AddonPlanID,
			AddonCode:            a.AddonCode,
			TierCode:             a.TierCode,
			StripeSubscriptionId: a.StripeSubscriptionID,
			StripeCustomerId:     a.StripeCustomerID,
			CancelledAt:          timestamppb.New(a.UpdatedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	case dispatcher.EventSubscriptionPaymentFailed:
		msg := &paymentsv1.TenantAddonPurchaseSubscriptionPaymentFailed{
			Envelope:             env,
			PurchaseId:           a.PurchaseID,
			AdminGcid:            a.AdminGCID(),
			AddonPlanId:          a.AddonPlanID,
			AddonCode:            a.AddonCode,
			TierCode:             a.TierCode,
			StripeSubscriptionId: a.StripeSubscriptionID,
			StripeCustomerId:     a.StripeCustomerID,
			Currency:             a.Currency,
			FailedAt:             timestamppb.New(a.UpdatedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	case dispatcher.EventPaymentRecovered:
		msg := &paymentsv1.TenantAddonPurchasePaymentRecovered{
			Envelope:             env,
			PurchaseId:           a.PurchaseID,
			AdminGcid:            a.AdminGCID(),
			AddonPlanId:          a.AddonPlanID,
			AddonCode:            a.AddonCode,
			TierCode:             a.TierCode,
			StripeSubscriptionId: a.StripeSubscriptionID,
			StripeCustomerId:     a.StripeCustomerID,
			Currency:             a.Currency,
			RecoveredAt:          timestamppb.New(a.UpdatedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	// CHO-1779 — SubscriptionSchedule release. By the time the marshaller
	// runs, the dispatcher has already called ReleaseSchedule so:
	//   a.TierCode = the now-active tier (was a.ScheduledTierCode)
	//   a.StripeSubscriptionScheduleID / a.ScheduledTierCode /
	//     a.ScheduledEffectiveAt are ALL cleared (zero-valued)
	// Hence the v1 payload omits from_tier + schedule_id — the consumer
	// derives from_tier from its own registry's CurrentTier.
	case dispatcher.EventSubscriptionScheduleReleased:
		msg := &paymentsv1.TenantAddonPurchaseSubscriptionScheduleReleased{
			Envelope:             env,
			PurchaseId:           a.PurchaseID,
			AdminGcid:            a.AdminGCID(),
			AddonPlanId:          a.AddonPlanID,
			AddonCode:            a.AddonCode,
			ToTier:               a.TierCode,
			StripeSubscriptionId: a.StripeSubscriptionID,
			ReleasedAt:           timestamppb.New(a.UpdatedAt),
		}
		return marshalAndTopic(msg, wh.AggregateTenantAddonPurchase, evType)
	}
	return nil, "", fmt.Errorf("outbox: tenant_addon_purchase event_type %q not supported", evType)
}
