-- Phase 6 (PLAN §5.5): the per-tenant fence of RE's Postgres writes. A new owner of a tenant (a new
-- leader of its bin, or the bin it moved to) raises (tenant_epoch, leader_epoch) here BEFORE it reads
-- the tenant's callbacks; every callback write of RE checks the fence with FOR SHARE in the same
-- statement and writes nothing unless the fence is still its own. So a write an older leader still
-- had in flight either committed before the new owner read (and the new owner sees it) or never
-- lands. Expand-only (a new table): phase-3 binaries never read it. Idempotent.

CREATE TABLE IF NOT EXISTS routing_fence (
  tenant_id     text   NOT NULL PRIMARY KEY,
  tenant_epoch  bigint NOT NULL,
  leader_epoch  bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

ALTER TABLE routing_fence ENABLE ROW LEVEL SECURITY;
ALTER TABLE routing_fence FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'routing_fence' AND policyname = 'tenant_isolation') THEN
    CREATE POLICY tenant_isolation ON routing_fence USING (tenant_id = current_setting('parity.tenant_id', true))
      WITH CHECK (tenant_id = current_setting('parity.tenant_id', true));
  END IF;
END $$;

GRANT SELECT, INSERT, UPDATE ON routing_fence TO parity_cc_routing_app;
