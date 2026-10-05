-- =============================================================================
-- chora-payments : 0010_tenant_addon_purchase_schedule_fields.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Stage         : CHO-1759 epic follow-up / CHO-1772 SubscriptionSchedule.
--
-- Adds the 3 Stripe SubscriptionSchedule correlation fields to
-- tenant_addon_purchases. CHO-1772 wires the end-of-cycle tier change
-- path: the FE picker's `effective_at=end_of_cycle` radio creates a
-- Stripe SubscriptionSchedule that holds the current tier until the
-- billing-cycle anchor, then activates the new tier.
--
-- Backwards-compatible:
--   - All 3 columns are nullable. Rows with no pending schedule keep
--     them NULL.
--   - The pre-CHO-1772 immediate-tier-change path is unaffected: when
--     effective_at='' or 'immediate' the schedule columns stay NULL +
--     ChangeTierHandler keeps calling UpdateSubscriptionPrice.
--   - Migration is additive only; rollback drops all 3 columns + the
--     unique partial index.
--
-- Schema choices:
--   - stripe_subscription_schedule_id is the Stripe `sub_sched_...` ID;
--     populated when the admin defers tier change to cycle end + nulled
--     by the subscription_schedule.released webhook handler when the
--     schedule consumes itself (or by ClearSchedule when the admin
--     backs out before release).
--   - scheduled_tier_code is the DEFERRED-TARGET tier_code (NOT the
--     current paid-for tier). The current tier stays in tier_code; the
--     FE renders "Pro starting YYYY-MM-DD" using scheduled_tier_code +
--     scheduled_effective_at when both are non-null.
--   - scheduled_effective_at is when Stripe will activate the new phase.
--     Typically equals current_period_end at schedule-creation time.
--   - UNIQUE partial index on stripe_subscription_schedule_id WHERE NOT
--     NULL keeps the webhook lookup O(1). Stripe's schedule IDs are
--     globally unique so a tenant-scoped index isn't needed.
-- =============================================================================

BEGIN;

ALTER TABLE tenant_addon_purchases
    ADD COLUMN stripe_subscription_schedule_id TEXT,
    ADD COLUMN scheduled_tier_code             TEXT,
    ADD COLUMN scheduled_effective_at          TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS tenant_addon_purchases_stripe_subscription_schedule_id_idx
    ON tenant_addon_purchases (stripe_subscription_schedule_id)
    WHERE stripe_subscription_schedule_id IS NOT NULL;

COMMENT ON COLUMN tenant_addon_purchases.stripe_subscription_schedule_id IS 'Stripe SubscriptionSchedule `sub_sched_...` ID for a pending end-of-cycle tier change. NULL when no schedule pending. CHO-1772; cleared by subscription_schedule.released webhook.';
COMMENT ON COLUMN tenant_addon_purchases.scheduled_tier_code             IS 'Deferred-target tier_code while a schedule is pending. The CURRENT paid-for tier remains in tier_code until release. CHO-1772.';
COMMENT ON COLUMN tenant_addon_purchases.scheduled_effective_at          IS 'When Stripe activates the schedule''s second phase (typically current_period_end at schedule creation). UTC. CHO-1772.';

COMMIT;
