-- =============================================================================
-- chora-payments : 0003_idempotency_keys.up.sql
--
-- Subscriber-side idempotency table — backs libs/chora-go-common/idempotent.Store
-- for the chora-payments side of Pub/Sub redelivery (defence-in-depth on top of
-- the stripe_webhook_events table from 0001 which is the canonical dedup for
-- inbound Stripe events).
--
-- chora-payments does NOT subscribe to inbound Pub/Sub today (it terminates
-- HTTPS for Stripe directly), but the table is provisioned in advance for
-- (a) the gRPC API idempotency-key pattern + (b) future Pub/Sub admin
-- subscriptions (e.g., crypto-shred from chora-identity's account-closure
-- saga eventually flowing through payments).
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key            TEXT         PRIMARY KEY,
    purpose        TEXT         NOT NULL,             -- e.g. 'grpc.CreateCourseCheckoutSession'
    tenant_id      UUID,
    learner_gcid   UUID,
    response_body  BYTEA,                             -- cached response for replay
    response_code  INTEGER,                           -- cached status (HTTP) or gRPC status code
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ  NOT NULL              -- 24-hour TTL default
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expiry
    ON idempotency_keys (expires_at);
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_tenant
    ON idempotency_keys (tenant_id, created_at DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency_keys TO chora_payments_app_rw';
    END IF;
END$$;

COMMIT;
