-- =============================================================================
-- chora-payments : 0002_outbox.up.sql
--
-- Outbox pattern table — every domain-state-change writes both the aggregate
-- row AND the outbox event row in the same transaction. The dispatcher
-- async-publishes the outbox row to Pub/Sub; on success it sets dispatched_at.
--
-- Mirrors chora-delivery's outbox + outbox_d6 schemas (canonical D6 4-pillar
-- resilience pattern: dead-pod-survival + DLQ + at-least-once + Cloud Trace
-- attribution).
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id           UUID         PRIMARY KEY,                -- UUIDv7
    aggregate_type     TEXT         NOT NULL,                   -- e.g. 'course_purchase'
    aggregate_id       UUID         NOT NULL,                   -- purchase_id
    tenant_id          UUID         NOT NULL,
    topic              TEXT         NOT NULL,                   -- e.g. 'chora.payments.course_purchase.payment_captured.v1'
    payload            BYTEA        NOT NULL,                   -- proto3 binary
    traceparent        TEXT,
    tracestate         TEXT,
    idempotency_key    TEXT         NOT NULL,                   -- envelope.idempotency_key
    schema_version     INTEGER      NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dispatched_at      TIMESTAMPTZ,                             -- NULL = not yet published
    dispatch_attempts  INTEGER      NOT NULL DEFAULT 0,
    last_error         TEXT,
    next_retry_at      TIMESTAMPTZ,                             -- exponential backoff cursor
    UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_undispatched
    ON outbox_events (next_retry_at NULLS FIRST, created_at)
    WHERE dispatched_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_events_aggregate
    ON outbox_events (aggregate_type, aggregate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_outbox_events_topic
    ON outbox_events (topic, created_at DESC);

COMMENT ON TABLE outbox_events IS
    'ADR-164 transactional outbox. Each domain-state-change writes both the aggregate row and the outbox row in the same TX; dispatcher async-publishes to Pub/Sub. RLS-disabled (operational table; tenant_id is on the row).';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON outbox_events TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT SELECT ON outbox_events TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;
