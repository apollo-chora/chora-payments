-- =============================================================================
-- chora-payments : 0007_disputes.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Date          : 2026-05-24
-- Stage         : ADR-164 §229.5 — chargeback dispute coverage.
--
-- Adds the `disputes` aggregate (separate from the 7 Purchase aggregates per
-- DDD decision 2026-05-24: dispute has its own FSM independent of the
-- Purchase payment FSM).
--
-- Single cross-aggregate table — disputes join their parent Purchase by
-- (aggregate_type, purchase_id), NO FK constraint (Purchase aggregates
-- forbid cross-aggregate FKs per ddd-enforcement aggregate-invariant #3).
-- Dispatcher validates the parent Purchase exists before inserting.
--
-- Lifecycle FSM (state column):
--
--   raised → closed (with outcome ∈ {won, lost, warning_closed})
--
-- Funds-movement tracking (funds_withdrawn / funds_reinstated booleans +
-- timestamps) is orthogonal to lifecycle — Stripe may fire funds_withdrawn
-- during the raised window and funds_reinstated either before close
-- (warning_closed) or after close (won).
--
-- Cross-aggregate topic family: chora.payments.dispute.{raised,closed,
-- funds_withdrawn,funds_reinstated}.v1 — see proto/events/payments/
-- dispute.proto for the rationale.
--
-- Resilience-priority directive (feedback_resilience_priority):
--   - Idempotent (IF NOT EXISTS).
--   - RLS-enabled (tenant_isolation policy) — disputes inherit tenant
--     scope from their parent Purchase, replicated into this table at
--     insert time by the dispatcher.
--   - State + Outcome CHECK constraints match dispute.State / dispute.Outcome
--     enums in domain/dispute/dispute.go.
--   - UNIQUE on stripe_dispute_id — Stripe webhook redelivery for create
--     produces UNIQUE violation that the dispatcher absorbs via the
--     existing stripe_webhook_events dedup row.
--
-- HARD RULE per ddd-enforcement.md: cross-database queries forbidden. This
-- migration only touches chora_payments. Cross-domain consumers
-- (chora-tenancy refund-vault PRIMARY, chora-delivery / chora-identity for
-- per-aggregate audit) receive dispute outcomes via Pub/Sub events.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS disputes (
    dispute_id               UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,

    -- Cross-aggregate routing — joins the parent Purchase by
    -- (aggregate_type, purchase_id). No FK per ddd-enforcement.
    aggregate_type           TEXT         NOT NULL
        CHECK (aggregate_type IN (
            'course_purchase','application_payment',
            'familiar_egg_purchase','tenant_mana_topup',
            'user_subscription',
            'user_mana_topup','identity_kyc_fee'
        )),
    purchase_id              UUID         NOT NULL,

    stripe_dispute_id        TEXT         NOT NULL UNIQUE,
    stripe_charge_id         TEXT         NOT NULL,

    state                    TEXT         NOT NULL DEFAULT 'raised'
        CHECK (state IN ('raised','closed')),

    -- Outcome only set when state='closed'. CHECK enforces presence on close.
    outcome                  TEXT
        CHECK (outcome IS NULL OR outcome IN ('won','lost','warning_closed')),

    amount_cents             BIGINT       NOT NULL CHECK (amount_cents >= 0),
    currency                 TEXT         NOT NULL CHECK (length(currency) = 3),

    -- Stripe-reported dispute reason (string-typed for forward-compat with
    -- new Stripe reason codes).
    reason                   TEXT         NOT NULL DEFAULT 'general',

    evidence_due_by          TIMESTAMPTZ  NOT NULL,

    -- Funds-movement tracking (independent of lifecycle).
    funds_withdrawn          BOOLEAN      NOT NULL DEFAULT FALSE,
    funds_withdrawn_at       TIMESTAMPTZ,
    funds_reinstated         BOOLEAN      NOT NULL DEFAULT FALSE,
    funds_reinstated_at      TIMESTAMPTZ,

    raised_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    closed_at                TIMESTAMPTZ,

    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Closed-state integrity: closed disputes MUST carry outcome + closed_at.
    CONSTRAINT chk_disputes_closed_has_outcome
        CHECK (state = 'raised' OR (outcome IS NOT NULL AND closed_at IS NOT NULL)),
    -- Funds-movement timestamp integrity.
    CONSTRAINT chk_disputes_funds_withdrawn_at
        CHECK (funds_withdrawn = FALSE OR funds_withdrawn_at IS NOT NULL),
    CONSTRAINT chk_disputes_funds_reinstated_at
        CHECK (funds_reinstated = FALSE OR funds_reinstated_at IS NOT NULL)
);

-- Primary read path: dispatcher looks up by stripe_dispute_id on
-- close/funds events. Already covered by the UNIQUE constraint.

-- Cross-aggregate join queries (administrative + ListByPurchase repo path).
CREATE INDEX IF NOT EXISTS idx_disputes_purchase
    ON disputes (tenant_id, aggregate_type, purchase_id, raised_at DESC);

-- Stripe charge lookup (rare but needed for charge-level audit).
CREATE INDEX IF NOT EXISTS idx_disputes_charge
    ON disputes (stripe_charge_id);

-- Tenant-scoped recent-disputes listing (O+ governance dashboard).
CREATE INDEX IF NOT EXISTS idx_disputes_tenant_recent
    ON disputes (tenant_id, raised_at DESC)
    WHERE state = 'raised';

CREATE TRIGGER trg_disputes_updated_at
    BEFORE UPDATE ON disputes
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE disputes ENABLE ROW LEVEL SECURITY;
ALTER TABLE disputes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON disputes
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Grants (re-runnable safe).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON disputes TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT SELECT ON disputes TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;
