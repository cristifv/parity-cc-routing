-- Phases 5b+5c (PLAN §3.8, §5.9): the learned forecaster's history and model table. Expand-only:
-- a new nullable column and a new table; phase-5/8 binaries never read either. Idempotent.
--
-- offered_10s: the bucket's arrivals per 10 s step (90 counts), written by RE at each bucket end
-- with the row; NULL for rows written before V3 (the trainer then has the 15-min count only).
ALTER TABLE routing_bucket_actuals ADD COLUMN IF NOT EXISTS offered_10s integer[];

-- One row per (tenant, version): a fine-tuned model of the tenant, written ONLY by the trainer's
-- role (parity_cc_routing_trainer); RE loads a row only when written_by says so, and checks the
-- sha256 of `bytes` before parsing them. `written_by`, the bytes, their sha256, the config and the
-- normalization are fixed at insert (the trigger below): an UPDATE may change the status and the
-- shadow scores only.
CREATE TABLE IF NOT EXISTS routing_forecast_model (
  tenant_id            text    NOT NULL,
  version              bigint  NOT NULL CHECK (version > 0),
  config               jsonb   NOT NULL,
  normalization        jsonb   NOT NULL,
  sha256               text    NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  bytes                bytea   NOT NULL,
  base_sha256          text,
  train_from_ms        bigint  NOT NULL,
  train_to_ms          bigint  NOT NULL,
  holdout              jsonb   NOT NULL,
  status               text    NOT NULL DEFAULT 'candidate'
                               CHECK (status IN ('candidate', 'active', 'retired', 'demoted', 'rejected')),
  shadow               jsonb   NOT NULL DEFAULT '{}'::jsonb,
  written_by           text    NOT NULL DEFAULT '',
  created_at_ms        bigint  NOT NULL,
  status_changed_at_ms bigint  NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, version)
);
-- At most one active model per tenant.
CREATE UNIQUE INDEX IF NOT EXISTS routing_forecast_model_one_active
  ON routing_forecast_model (tenant_id) WHERE status = 'active';

ALTER TABLE routing_forecast_model ENABLE ROW LEVEL SECURITY;
ALTER TABLE routing_forecast_model FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'routing_forecast_model' AND policyname = 'tenant_isolation') THEN
    CREATE POLICY tenant_isolation ON routing_forecast_model USING (tenant_id = current_setting('parity.tenant_id', true))
      WITH CHECK (tenant_id = current_setting('parity.tenant_id', true));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'parity_cc_routing_trainer') THEN
    CREATE ROLE parity_cc_routing_trainer NOLOGIN;
  END IF;
END $$;

-- written_by: 'parity_cc_routing_trainer' when the inserting role is that role or a DIRECT member of
-- it (never via superuser rights: pg_has_role would say yes for a superuser), else the role's own
-- name (which RE refuses). An UPDATE can't change it, the bytes, the sha256, the config or the
-- normalization.
CREATE OR REPLACE FUNCTION routing_forecast_model_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF current_user = 'parity_cc_routing_trainer' OR EXISTS (
         SELECT 1 FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member
         WHERE g.rolname = 'parity_cc_routing_trainer' AND u.rolname = current_user) THEN
      NEW.written_by := 'parity_cc_routing_trainer';
    ELSE
      NEW.written_by := current_user;
    END IF;
    RETURN NEW;
  END IF;
  IF NEW.written_by IS DISTINCT FROM OLD.written_by OR NEW.bytes IS DISTINCT FROM OLD.bytes
     OR NEW.sha256 IS DISTINCT FROM OLD.sha256 OR NEW.config IS DISTINCT FROM OLD.config
     OR NEW.normalization IS DISTINCT FROM OLD.normalization OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
     OR NEW.version IS DISTINCT FROM OLD.version THEN
    RAISE EXCEPTION 'routing_forecast_model: only status and shadow may change' USING ERRCODE = '42501';
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS routing_forecast_model_guard ON routing_forecast_model;
CREATE TRIGGER routing_forecast_model_guard BEFORE INSERT OR UPDATE ON routing_forecast_model
  FOR EACH ROW EXECUTE FUNCTION routing_forecast_model_guard();

-- The trainer: reads its tenant's history (RLS + WHERE tenant_id = $1), writes candidates, and
-- changes a status (promotion by RE, rollback by the trainer). Nothing else: no callbacks, tasks or
-- fences.
GRANT SELECT ON routing_bucket_actuals TO parity_cc_routing_trainer;
GRANT SELECT, INSERT ON routing_forecast_model TO parity_cc_routing_trainer;
GRANT UPDATE (status, status_changed_at_ms) ON routing_forecast_model TO parity_cc_routing_trainer;
-- RE: reads the models, writes status and shadow scores only.
GRANT SELECT ON routing_forecast_model TO parity_cc_routing_app;
GRANT UPDATE (status, shadow, status_changed_at_ms) ON routing_forecast_model TO parity_cc_routing_app;
