-- =============================================================================
-- chora-payments : 0001_initial.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments (13th DB, per ADR-164)
-- Author        : Chora Platform Team (Team 3)
-- Date          : 2026-05-24
-- Architecture  : ADR-164 — chora-payments service + 13th DB + Pub/Sub topic
--                 taxonomy (PROPOSED 2026-05-24).
--
-- Initial schema for the 5 Purchase aggregates + the global Stripe webhook
-- de-dup table. Mirrors `services/chora-tenancy/migrations/0007_familiar_egg_
-- purchases.sql` (canonical pattern) for FSM + Stripe handles + RLS.
--
-- Aggregates:
--
--   course_purchases        — CJ#2 paid course Stripe Checkout
--   application_payments    — Course application Singpass+SkillsFuture
--   familiar_egg_purchases  — ADR-149 FamiliarEgg buy
--   tenant_mana_topups      — Tenant mana pool top-up
--   user_subscriptions      — Per-learner Familiar mana subscription (recurring)
--
-- Plus:
--
--   stripe_webhook_events   — Global Stripe webhook de-dup (RLS-DISABLED;
--                             event.id from Stripe is not tenant-scoped at
--                             receive time).
--
-- Resilience-priority directive (feedback_resilience_priority):
--   - Idempotent (IF NOT EXISTS, UNIQUE constraints on Stripe handles).
--   - RLS-enabled on all 5 Purchase tables (tenant_isolation policy).
--   - State CHECK constraints + sanity timestamp pairs.
--   - Soft-delete via deleted_at where applicable; hard-delete via crypto-shred
--     only (per closure saga ADR).
--
-- HARD RULE per ddd-enforcement.md: cross-database queries forbidden. This
-- migration only touches chora_payments. Originating-service-side aggregates
-- (chora_delivery.enrollments, chora_tenancy.familiar_egg_purchases, etc.)
-- remain in their own DBs and receive payment outcomes via Pub/Sub events.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- Trigger function — updated_at maintenance (used by every table below).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION payments_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- course_purchases — CJ#2 paid course Stripe Checkout
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS course_purchases (
    purchase_id              UUID         PRIMARY KEY,          -- UUIDv7 (app layer)
    tenant_id                UUID         NOT NULL,
    learner_gcid             UUID         NOT NULL,
    course_id                UUID         NOT NULL,             -- soft FK to chora_delivery.courses

    state                    TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN ('checkout_started','payment_captured',
                         'payment_failed','refunded','expired')),

    amount_cents             BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid        BIGINT       NOT NULL DEFAULT 0 CHECK (amount_cents_paid >= 0),
    amount_cents_refunded    BIGINT       NOT NULL DEFAULT 0 CHECK (amount_cents_refunded >= 0),
    currency                 TEXT         NOT NULL CHECK (length(currency) = 3),

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
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CHECK (state != 'payment_captured' OR paid_at     IS NOT NULL),
    CHECK (state != 'payment_failed'   OR failed_at   IS NOT NULL),
    CHECK (state != 'refunded'         OR refunded_at IS NOT NULL),
    CHECK (state != 'expired'          OR expired_at  IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_course_purchases_learner
    ON course_purchases (tenant_id, learner_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_course_purchases_course
    ON course_purchases (tenant_id, course_id, state);
CREATE INDEX IF NOT EXISTS idx_course_purchases_session
    ON course_purchases (stripe_session_id);
CREATE INDEX IF NOT EXISTS idx_course_purchases_in_flight
    ON course_purchases (state, checkout_started_at)
    WHERE state IN ('checkout_started','payment_captured');

CREATE TRIGGER trg_course_purchases_updated_at
    BEFORE UPDATE ON course_purchases
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE course_purchases ENABLE ROW LEVEL SECURITY;
ALTER TABLE course_purchases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON course_purchases
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- application_payments — Course Application Singpass+SkillsFuture flow
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS application_payments (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,
    learner_gcid             UUID         NOT NULL,
    application_id           UUID         NOT NULL,             -- soft FK to chora_delivery.applications
    course_id                UUID         NOT NULL,

    state                    TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN ('checkout_started','payment_captured',
                         'payment_failed','refunded','expired')),

    amount_cents             BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid        BIGINT       NOT NULL DEFAULT 0,
    amount_cents_refunded    BIGINT       NOT NULL DEFAULT 0,
    currency                 TEXT         NOT NULL CHECK (length(currency) = 3),

    stripe_session_id        TEXT         NOT NULL UNIQUE,
    stripe_payment_intent_id TEXT,
    stripe_charge_id         TEXT,
    stripe_refund_id         TEXT,
    stripe_checkout_url      TEXT,
    stripe_failure_code      TEXT,
    stripe_failure_message   TEXT,

    refund_reason            TEXT,

    checkout_started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at                  TIMESTAMPTZ,
    failed_at                TIMESTAMPTZ,
    refunded_at              TIMESTAMPTZ,
    expired_at               TIMESTAMPTZ,

    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CHECK (state != 'payment_captured' OR paid_at     IS NOT NULL),
    CHECK (state != 'payment_failed'   OR failed_at   IS NOT NULL),
    CHECK (state != 'refunded'         OR refunded_at IS NOT NULL),
    CHECK (state != 'expired'          OR expired_at  IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_application_payments_learner
    ON application_payments (tenant_id, learner_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_application_payments_application
    ON application_payments (tenant_id, application_id);
CREATE INDEX IF NOT EXISTS idx_application_payments_session
    ON application_payments (stripe_session_id);

CREATE TRIGGER trg_application_payments_updated_at
    BEFORE UPDATE ON application_payments
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE application_payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE application_payments FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON application_payments
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- familiar_egg_purchases — ADR-149 FamiliarEgg buy (migrated from
-- chora_tenancy.familiar_egg_purchases at Wave 1 Stage D)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS familiar_egg_purchases (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,             -- target_tenant (envelope.tenant_id)
    learner_gcid             UUID         NOT NULL,             -- purchaser_gcid
    egg_sku                  TEXT         NOT NULL,

    suggested_focal_atom_id  UUID,

    state                    TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN ('checkout_started','payment_captured',
                         'payment_failed','refunded','expired',
                         'provisioned')),

    amount_cents             BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid        BIGINT       NOT NULL DEFAULT 0,
    amount_cents_refunded    BIGINT       NOT NULL DEFAULT 0,
    currency                 TEXT         NOT NULL CHECK (length(currency) = 3),

    stripe_session_id        TEXT         NOT NULL UNIQUE,
    stripe_payment_intent_id TEXT,
    stripe_charge_id         TEXT,
    stripe_refund_id         TEXT,
    stripe_checkout_url      TEXT,
    stripe_failure_code      TEXT,
    stripe_failure_message   TEXT,

    refund_reason            TEXT
        CHECK (refund_reason IS NULL OR refund_reason IN (
            'expired_unhatched','support_initiated','customer_request',
            'duplicate_charge','fraud'
        )),
    refund_credit_only       BOOLEAN      NOT NULL DEFAULT FALSE,

    soft_expiry_at           TIMESTAMPTZ,
    hard_expiry_at           TIMESTAMPTZ,

    checkout_started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at                  TIMESTAMPTZ,
    failed_at                TIMESTAMPTZ,
    refunded_at              TIMESTAMPTZ,
    expired_at               TIMESTAMPTZ,
    provisioned_at           TIMESTAMPTZ,

    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_learner
    ON familiar_egg_purchases (tenant_id, learner_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_state
    ON familiar_egg_purchases (state, hard_expiry_at)
    WHERE state IN ('payment_captured','provisioned');
CREATE INDEX IF NOT EXISTS idx_familiar_egg_purchases_session
    ON familiar_egg_purchases (stripe_session_id);

CREATE TRIGGER trg_familiar_egg_purchases_updated_at
    BEFORE UPDATE ON familiar_egg_purchases
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE familiar_egg_purchases ENABLE ROW LEVEL SECURITY;
ALTER TABLE familiar_egg_purchases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON familiar_egg_purchases
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- tenant_mana_topups — Tenant mana pool top-ups (migrated from
-- chora_tenancy.tenant_mana_pool_topups at Wave 1 Stage D)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_mana_topups (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,
    admin_gcid               UUID         NOT NULL,
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

    refund_reason            TEXT,

    checkout_started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at                  TIMESTAMPTZ,
    failed_at                TIMESTAMPTZ,
    refunded_at              TIMESTAMPTZ,
    expired_at               TIMESTAMPTZ,

    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_mana_topups_admin
    ON tenant_mana_topups (tenant_id, admin_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tenant_mana_topups_session
    ON tenant_mana_topups (stripe_session_id);

CREATE TRIGGER trg_tenant_mana_topups_updated_at
    BEFORE UPDATE ON tenant_mana_topups
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE tenant_mana_topups ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_mana_topups FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_mana_topups
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- user_subscriptions — Per-learner Familiar mana subscription (recurring;
-- migrated from chora_identity.user_subscriptions at Wave 1 Stage E —
-- chora_identity retains the business-logic UserSubscription aggregate;
-- this table holds the payments-side Stripe correlation only)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_subscriptions (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,
    learner_gcid             UUID         NOT NULL,
    plan_sku                 TEXT         NOT NULL,
    billing_period           TEXT         NOT NULL
        CHECK (billing_period IN ('monthly','annually')),

    state                    TEXT         NOT NULL DEFAULT 'checkout_started'
        CHECK (state IN ('checkout_started','payment_captured',
                         'payment_failed','refunded','expired',
                         'created','active','grace','paused','cancelled')),

    amount_cents             BIGINT       NOT NULL CHECK (amount_cents >= 0),
    amount_cents_paid_total  BIGINT       NOT NULL DEFAULT 0,
    amount_cents_refunded    BIGINT       NOT NULL DEFAULT 0,
    currency                 TEXT         NOT NULL CHECK (length(currency) = 3),

    stripe_session_id        TEXT         UNIQUE,                -- nullable for renewals (no Session, only Subscription)
    stripe_subscription_id   TEXT         UNIQUE,                -- canonical handle
    stripe_customer_id       TEXT,
    stripe_payment_intent_id TEXT,
    stripe_charge_id         TEXT,
    stripe_invoice_id        TEXT,
    stripe_refund_id         TEXT,
    stripe_checkout_url      TEXT,
    stripe_failure_code      TEXT,
    stripe_failure_message   TEXT,

    current_period_start     TIMESTAMPTZ,
    current_period_end       TIMESTAMPTZ,

    refund_reason            TEXT,
    pause_reason             TEXT,
    cancellation_reason      TEXT,
    cancel_at_period_end     BOOLEAN      NOT NULL DEFAULT FALSE,
    effective_at             TIMESTAMPTZ,

    checkout_started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    paid_at                  TIMESTAMPTZ,
    failed_at                TIMESTAMPTZ,
    refunded_at              TIMESTAMPTZ,
    expired_at               TIMESTAMPTZ,
    created_at_stripe        TIMESTAMPTZ,
    renewed_at               TIMESTAMPTZ,
    paused_at                TIMESTAMPTZ,
    resumes_at               TIMESTAMPTZ,
    cancelled_at             TIMESTAMPTZ,

    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_subscriptions_learner
    ON user_subscriptions (tenant_id, learner_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_stripe_sub
    ON user_subscriptions (stripe_subscription_id);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_active
    ON user_subscriptions (state, current_period_end)
    WHERE state IN ('active','grace');

CREATE TRIGGER trg_user_subscriptions_updated_at
    BEFORE UPDATE ON user_subscriptions
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE user_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_subscriptions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON user_subscriptions
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- stripe_webhook_events — Global Stripe webhook de-dup (RLS-DISABLED)
--
-- Stripe retries failed-delivery webhooks with the same event.ID; this
-- table is the durable de-dup record so a redelivery cannot double-process
-- (re-transition an aggregate, re-publish outbox events, re-issue refund
-- credits).
--
-- RLS-DISABLED rationale: Stripe events arrive without tenant scope;
-- tenant_id is in the Session metadata, resolved by the dispatch path
-- AFTER the dedup gate. Per-aggregate access control happens at the
-- per-Purchase RLS layer above.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stripe_webhook_events (
    event_id                 TEXT         PRIMARY KEY,          -- Stripe's evt_xxx
    event_type               TEXT         NOT NULL,             -- e.g. checkout.session.completed

    received_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at             TIMESTAMPTZ,                       -- NULL == received but not yet processed
    processing_error         TEXT,                              -- NULL == no failure recorded
    target_aggregate_type    TEXT,                              -- e.g., 'course_purchase' (resolved post-dedup)
    target_purchase_id       UUID                               -- resolved from session.metadata
);

CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_received
    ON stripe_webhook_events (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_unprocessed
    ON stripe_webhook_events (received_at)
    WHERE processed_at IS NULL;

COMMENT ON TABLE stripe_webhook_events IS
    'ADR-164 Stripe webhook event de-dup. Global operational table (RLS-disabled). Stripe retries with same event.ID must not double-process.';

-- -----------------------------------------------------------------------------
-- Grants (re-runnable safe).
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON course_purchases       TO chora_payments_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON application_payments   TO chora_payments_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON familiar_egg_purchases TO chora_payments_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_mana_topups     TO chora_payments_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON user_subscriptions     TO chora_payments_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON stripe_webhook_events  TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT SELECT ON course_purchases       TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON application_payments   TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON familiar_egg_purchases TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON tenant_mana_topups     TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON user_subscriptions     TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON stripe_webhook_events  TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after apply):
--
--   SELECT count(*) FROM information_schema.tables
--    WHERE table_schema = 'public'
--      AND table_name IN ('course_purchases','application_payments',
--                         'familiar_egg_purchases','tenant_mana_topups',
--                         'user_subscriptions','stripe_webhook_events');
--   -- Expected: 6
--
--   SELECT relname, relrowsecurity, relforcerowsecurity
--     FROM pg_class
--    WHERE relname IN ('course_purchases','application_payments',
--                      'familiar_egg_purchases','tenant_mana_topups',
--                      'user_subscriptions');
--   -- Expected: 5 rows, all relrowsecurity=t + relforcerowsecurity=t
--
--   SELECT relname, relrowsecurity
--     FROM pg_class
--    WHERE relname = 'stripe_webhook_events';
--   -- Expected: relrowsecurity=f
-- =============================================================================
