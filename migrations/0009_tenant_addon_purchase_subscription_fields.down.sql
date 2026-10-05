-- 0009 rollback — drop the CHO-1761 Subscription correlation fields.
BEGIN;

DROP INDEX IF EXISTS tenant_addon_purchases_stripe_subscription_id_idx;

ALTER TABLE tenant_addon_purchases
    DROP CONSTRAINT IF EXISTS tenant_addon_purchases_sub_status_check;

ALTER TABLE tenant_addon_purchases
    DROP COLUMN IF EXISTS stripe_customer_id,
    DROP COLUMN IF EXISTS stripe_subscription_id,
    DROP COLUMN IF EXISTS sub_status,
    DROP COLUMN IF EXISTS current_period_start,
    DROP COLUMN IF EXISTS current_period_end;

COMMIT;
