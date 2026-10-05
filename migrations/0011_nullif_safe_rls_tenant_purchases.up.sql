-- =============================================================================
-- chora-payments : 0011_nullif_safe_rls_tenant_purchases.up.sql
--
-- Domain        : Payments (supporting, ADR-164)
-- Database      : chora_payments
-- Author        : Chora Platform Team (Team 3)
-- Stage         : CHO-1831 / ADR-188 D4.
--
-- Makes the tenant_isolation RLS policy on tenant_mana_topups and
-- tenant_addon_purchases NULLIF-safe. The original mig 0001/0008 form
--
--     tenant_id = current_setting('chora.tenant_id', true)::uuid
--
-- throws SQLSTATE 22P02 (invalid input syntax for type uuid: "") when the
-- session GUC carries the empty-string reset-value of a pooled connection,
-- instead of cleanly denying. CHO-1788 closed the primary path (the
-- dispatcher now always stamps tenant_id before any repo call); this is the
-- layered-defence backstop — an empty GUC now evaluates to NULL and the
-- policy denies, never aborts mid-transaction.
--
-- Behaviour-neutral on the happy path: when chora.tenant_id is a valid UUID
-- the predicate is identical. Idempotent (DROP POLICY IF EXISTS). No data
-- change. (FOR ALL with no explicit WITH CHECK reuses USING for INSERT, so
-- both read and write paths get the NULLIF guard.)
-- =============================================================================

DROP POLICY IF EXISTS tenant_isolation ON tenant_mana_topups;
CREATE POLICY tenant_isolation ON tenant_mana_topups
    FOR ALL USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);

DROP POLICY IF EXISTS tenant_isolation ON tenant_addon_purchases;
CREATE POLICY tenant_isolation ON tenant_addon_purchases
    FOR ALL USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);
