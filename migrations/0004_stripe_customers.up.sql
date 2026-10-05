-- =============================================================================
-- chora-payments : 0004_stripe_customers.up.sql
--
-- Stripe Customer registry — maps (tenant_id, learner_gcid) → stripe_customer_id
-- so subsequent checkouts for the same learner re-use the same Stripe Customer
-- object, which lets Stripe persist payment methods between charges.
--
-- Per Stripe Checkout docs: when a CheckoutSession passes `customer: cus_xxx`,
-- Stripe attaches the new PaymentMethod to that Customer automatically on
-- first checkout, and offers the saved PM at the top of future Sessions for
-- the same Customer.
--
-- Per ADR-164 §D2 — added 2026-05-24 after Stage A.4 design review.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS stripe_customers (
    tenant_id          UUID         NOT NULL,
    learner_gcid       UUID         NOT NULL,
    stripe_customer_id TEXT         NOT NULL,
    email              TEXT,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, learner_gcid),
    UNIQUE (stripe_customer_id)
);

CREATE INDEX IF NOT EXISTS idx_stripe_customers_gcid
    ON stripe_customers (learner_gcid);
CREATE INDEX IF NOT EXISTS idx_stripe_customers_stripe_id
    ON stripe_customers (stripe_customer_id);

CREATE TRIGGER trg_stripe_customers_updated_at
    BEFORE UPDATE ON stripe_customers
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

ALTER TABLE stripe_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE stripe_customers FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON stripe_customers
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMENT ON TABLE stripe_customers IS
    'ADR-164 Stripe Customer registry. Maps (tenant_id, learner_gcid) -> stripe_customer_id so Stripe can persist saved payment methods between checkouts.';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON stripe_customers TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT SELECT ON stripe_customers TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;
