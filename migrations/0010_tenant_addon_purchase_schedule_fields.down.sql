-- =============================================================================
-- chora-payments : 0010_tenant_addon_purchase_schedule_fields.down.sql
-- Rollback for CHO-1772 SubscriptionSchedule correlation fields.
-- =============================================================================

BEGIN;

DROP INDEX IF EXISTS tenant_addon_purchases_stripe_subscription_schedule_id_idx;

ALTER TABLE tenant_addon_purchases
    DROP COLUMN IF EXISTS stripe_subscription_schedule_id,
    DROP COLUMN IF EXISTS scheduled_tier_code,
    DROP COLUMN IF EXISTS scheduled_effective_at;

COMMIT;
