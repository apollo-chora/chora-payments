-- =============================================================================
-- chora-payments : 0008_tenant_addon_purchases.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Date          : 2026-06-14
-- Stage         : CHO-1736 H+ Marketplace Subscribe — Sub 2 (CHO-1738).
--
-- Adds the 8th Purchase aggregate: tenant_addon_purchases (H+ Marketplace
-- tenant-add-on subscription Checkout). Reads from the marketplace catalog
-- shipped in CHO-1735; chora-tenancy activates the AddonSubscription via the
-- Sub 4 Pub/Sub subscriber on payment_captured.
--
-- One-shot Stripe `payment` mode (chora-tenancy holds the billing-cycle clock
-- + auto-renew per ADR-142 sibling pattern). Mirrors the tenant-scoped
-- tenant_mana_topups schema (admin_gcid + tenant RLS); aggregate-specific
-- columns: addon_plan_id (UUID), addon_code (TEXT), tier_code (TEXT).
--
-- Per `feedback_gateway_three_layer_route_wiring` analog for DB: an aggregate
-- needs (a) this table, (b) its repo + adapter, (c) the dispatcher case to
-- route Stripe webhooks. Missing the 3rd silently leaves payments dangling
-- in checkout_started forever.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS tenant_addon_purchases (
    purchase_id              UUID         PRIMARY KEY,
    tenant_id                UUID         NOT NULL,
    admin_gcid               UUID         NOT NULL,

    addon_plan_id            UUID         NOT NULL,
    addon_code               TEXT         NOT NULL,
    tier_code                TEXT         NOT NULL,

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
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Lookup indexes — mirror the tenant_mana_topups pattern.
CREATE INDEX IF NOT EXISTS idx_tenant_addon_purchases_admin
    ON tenant_addon_purchases (tenant_id, admin_gcid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tenant_addon_purchases_session
    ON tenant_addon_purchases (stripe_session_id);
-- Marketplace surface — tile-level subscription rollup query path.
CREATE INDEX IF NOT EXISTS idx_tenant_addon_purchases_plan
    ON tenant_addon_purchases (tenant_id, addon_plan_id, state);

-- Reuses the shared payments_set_updated_at trigger function (created in
-- 0001_initial.up.sql).
CREATE TRIGGER trg_tenant_addon_purchases_updated_at
    BEFORE UPDATE ON tenant_addon_purchases
    FOR EACH ROW EXECUTE FUNCTION payments_set_updated_at();

-- RLS — same `chora.tenant_id` session-var policy as every Purchase table.
-- chora_payments_app_rw bypasses via the WithRLSBypass(ctx) helper per
-- ADR-165 (single permitted intra-DB exception); every bypass MUST publish
-- governance.audit.cross_tenant_payments_viewed.v1 BEFORE the SQL fires.
ALTER TABLE tenant_addon_purchases ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_addon_purchases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_addon_purchases
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
