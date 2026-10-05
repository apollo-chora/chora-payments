-- =============================================================================
-- chora-payments : 0005_user_mana_topups.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Date          : 2026-05-24
-- Stage         : ADR-164 Stage A.5 — debt close-out.
--
-- Adds the 6th Purchase aggregate: user_mana_topups (per-learner FE-initiated
-- mana top-up). Mirrors tenant_mana_topups but per-user instead of per-tenant
-- (admin_gcid → learner_gcid; no tenant subsidy field).
--
-- Closes Stage E debt: `/api/v1/me/mana/topup` (per-user FE-initiated mana
-- purchase) was retired from chora-identity without a payments-side
-- counterpart. Stage A.5 introduces the 6th aggregate.
--
-- Resilience-priority directive (feedback_resilience_priority):
--   - Idempotent (IF NOT EXISTS).
--   - RLS-enabled (tenant_isolation policy).
--   - State CHECK constraint matches shared.State enum.
--
-- HARD RULE per ddd-enforcement.md: cross-database queries forbidden. Touches
-- only chora_payments. chora-identity (PRIMARY consumer of payment_captured)
-- updates its own per-user mana balance in chora_identity via Pub/Sub event.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS user_mana_topups (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,
    learner_gcid             UUID         NOT NULL,
    sku                      TEXT         NOT NULL,

    state                    TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN ('checkout_started','payment_captured',
                         'payment_failed','refunded','expired')),

    amount_cents             BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid        BIGINT       NOT NULL DEFAULT 0,
    amount_cents_refunded    BIGINT       NOT NULL DEFAULT 0,
    currency                 TEXT         NOT NULL CHECK (length(currency) = 3),

    mana_units               BIGINT       NOT NULL CHECK (mana_units >= 0),
    mana_units_debited       BIGINT       NOT NULL DEFAULT 0,

    stripe_session_id        TEXT         NOT NULL UNIQUE,
    stripe_payment_intent_id TEXT,
    stripe_charge_id         TEXT,
    stripe_refund_id         TEXT,
    stripe_checkout_url      TEXT,
    stripe_failure_code      TEXT,
    stripe_failure_message   TEXT,

    refund_reason            TEXT
        CHECK (refund_reason IS NULL OR refund_reason IN (
            'support_initiated','customer_request','duplicate_charge',
            'fraud','expired_unhatched'
        )),

    checkout_started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at                  TIMESTAMPTZ,
    failed_at                TIMESTAMPTZ,
    refunded_at              TIMESTAMPTZ,
    expired_at               TIMESTAMPTZ,

    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_mana_topups_learner
    ON user_mana_topups (tenant_id, learner_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_mana_topups_session
    ON user_mana_topups (stripe_session_id);

CREATE TRIGGER trg_user_mana_topups_updated_at
    BEFORE UPDATE ON user_mana_topups
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE user_mana_topups ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_mana_topups FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON user_mana_topups
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Grants (re-runnable safe).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON user_mana_topups TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT SELECT ON user_mana_topups TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;
