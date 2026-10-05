-- =============================================================================
-- chora-payments : 0011_nullif_safe_rls_tenant_purchases.down.sql
--
-- Reverts the two tenant_isolation policies to the original raw-cast form
-- (mig 0001 / 0008). Re-introduces the 22P02-on-empty-GUC fragility, so only
-- use to roll back ADR-188 D4.
-- =============================================================================

DROP POLICY IF EXISTS tenant_isolation ON tenant_mana_topups;
CREATE POLICY tenant_isolation ON tenant_mana_topups
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON tenant_addon_purchases;
CREATE POLICY tenant_isolation ON tenant_addon_purchases
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
