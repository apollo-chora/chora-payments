-- =============================================================================
-- chora-payments : 0006_identity_kyc_fees.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Date          : 2026-05-24
-- Stage         : ADR-164 Stage A.5 — debt close-out.
--
-- Adds the 7th Purchase aggregate: identity_kyc_fees (one-time KYC manual-doc
-- verification charge). Per ADR-142 the canonical fee for manual-doc KYC is
-- $9.99 (Singpass-verified GCID is free).
--
-- Closes Stage E debt: chora-identity used to charge the $9.99 manual KYC
-- fee inline (direct Stripe-go import on chora-identity's HTTP handler).
-- Stage A.5 extracts the charge to chora-payments as a 7th aggregate.
--
-- One-time charge — no FSM beyond the canonical 5 payment-phase states.
--
-- Resilience-priority directive (feedback_resilience_priority):
--   - Idempotent (IF NOT EXISTS).
--   - RLS-enabled (tenant_isolation policy).
--   - State CHECK constraint matches shared.State enum.
--
-- HARD RULE per ddd-enforcement.md: cross-database queries forbidden. Touches
-- only chora_payments. chora-identity (PRIMARY consumer) updates its own
-- KYC verification state in chora_identity via Pub/Sub event.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS identity_kyc_fees (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,
    learner_gcid             UUID         NOT NULL,

    -- Canonical KYC doc type (e.g., 'manual_id_doc', 'manual_passport',
    -- 'manual_proof_of_address'). Snapshot at session creation so
    -- chora-identity can branch the verification pipeline.
    kyc_doc_type             TEXT         NOT NULL,

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

CREATE INDEX IF NOT EXISTS idx_identity_kyc_fees_learner
    ON identity_kyc_fees (tenant_id, learner_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_identity_kyc_fees_session
    ON identity_kyc_fees (stripe_session_id);

CREATE TRIGGER trg_identity_kyc_fees_updated_at
    BEFORE UPDATE ON identity_kyc_fees
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE identity_kyc_fees ENABLE ROW LEVEL SECURITY;
ALTER TABLE identity_kyc_fees FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON identity_kyc_fees
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Grants (re-runnable safe).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON identity_kyc_fees TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT SELECT ON identity_kyc_fees TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;
