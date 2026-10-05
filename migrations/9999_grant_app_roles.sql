-- =============================================================================
-- chora-payments : 9999_grant_app_roles.sql
--
-- Re-runnable grant of standard CRUD privileges to chora_payments_app_rw +
-- chora_payments_app_ro across all current + future tables in the public
-- schema.
--
-- Mirrors services/chora-tenancy/migrations/9999_grant_app_roles.sql pattern.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_rw') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA public TO chora_payments_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO chora_payments_app_rw';
        EXECUTE 'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO chora_payments_app_rw';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO chora_payments_app_rw';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO chora_payments_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_payments_app_ro') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA public TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO chora_payments_app_ro';
        EXECUTE 'GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO chora_payments_app_ro';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO chora_payments_app_ro';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO chora_payments_app_ro';
    END IF;
END$$;

COMMIT;
