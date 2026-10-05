-- =============================================================================
-- chora-payments : 0009_tenant_addon_purchase_subscription_fields.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Stage         : CHO-1759 epic / CHO-1761 (Phase 0 foundation step 2).
--
-- Adds the 5 Stripe Subscription correlation fields to
-- tenant_addon_purchases. CHO-1762 will switch Checkout to
-- mode=subscription and start populating stripe_customer_id +
-- stripe_subscription_id via the CHO-1763 webhook handler.
--
-- Backwards-compatible:
--   - All 5 columns are nullable. Legacy rows (created via mode=payment
--     before CHO-1762) keep all 5 NULL forever — that's the signal
--     CHO-1764 uses to refuse change-tier with `no_stripe_subscription`.
--   - Existing reads keep working (no NOT NULL constraint added; existing
--     SELECT * + scanTenantAddonPurchase continue to compile).
--   - Migration is additive only; rollback drops all 5 columns + the
--     unique partial index.
--
-- Schema choices:
--   - status enum constrained to ('pending','active','past_due','cancelled')
--     mirrors the Stripe wire shape (lowercase). chora-payments handles
--     exactly these 4; other Stripe statuses (trialing, unpaid, incomplete)
--     are out of scope for the H+ Marketplace v1.
--   - current_period_{start,end} are TIMESTAMPTZ nullable — Stripe emits
--     them on active subscriptions but cancellation events may omit.
--   - UNIQUE partial index on stripe_subscription_id WHERE NOT NULL
--     keeps the webhook lookup O(1) without polluting the existing rows.
-- =============================================================================

BEGIN;

ALTER TABLE tenant_addon_purchases
    ADD COLUMN stripe_customer_id     TEXT,
    ADD COLUMN stripe_subscription_id TEXT,
    ADD COLUMN sub_status             TEXT,
    ADD COLUMN current_period_start   TIMESTAMPTZ,
    ADD COLUMN current_period_end     TIMESTAMPTZ;

ALTER TABLE tenant_addon_purchases
    ADD CONSTRAINT tenant_addon_purchases_sub_status_check
        CHECK (sub_status IS NULL OR sub_status IN ('pending','active','past_due','cancelled'));

CREATE UNIQUE INDEX IF NOT EXISTS tenant_addon_purchases_stripe_subscription_id_idx
    ON tenant_addon_purchases (stripe_subscription_id)
    WHERE stripe_subscription_id IS NOT NULL;

COMMENT ON COLUMN tenant_addon_purchases.stripe_customer_id     IS 'Stripe Customer ID holding the recurring subscription''s saved payment method. CHO-1761; populated by CHO-1763 webhook.';
COMMENT ON COLUMN tenant_addon_purchases.stripe_subscription_id IS 'Stripe Subscription `sub_...` ID. NULL for legacy mode=payment rows (pre-CHO-1762). CHO-1764 refuses change-tier when NULL.';
COMMENT ON COLUMN tenant_addon_purchases.sub_status             IS 'Lowercase Stripe Subscription lifecycle status. NULL until webhook fires; transitions pending → active → past_due → active or cancelled.';
COMMENT ON COLUMN tenant_addon_purchases.current_period_start   IS 'Stripe Subscription current_period_start. UTC. Nullable on cancelled / past_due.';
COMMENT ON COLUMN tenant_addon_purchases.current_period_end     IS 'Stripe Subscription current_period_end. UTC. Used by FE deferred-tier-change "effective on YYYY-MM-DD" copy.';

COMMIT;
