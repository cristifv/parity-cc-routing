-- parity-cc-routing's Postgres (PLAN §8 row 3, §5.9): callbacks and voicemail tasks (customer
-- promises, ARCH §4) and the 15-minute actuals the forecasters learn from. Applied by the schema
-- owner role (scripts/local-postgres.sh locally; a migration step in deploys); the service runs as
-- parity_cc_routing_app, which can't change the schema. Tenant isolation: row-level security on every
-- table (FORCE: the owner too), keyed by the transaction's parity.tenant_id, and every query of the
-- service also carries a tenant_id predicate. Idempotent: safe to apply again.

CREATE TABLE IF NOT EXISTS routing_callbacks (
  tenant_id               text    NOT NULL,
  callback_id             uuid    NOT NULL,
  original_interaction_id uuid    NOT NULL,
  entry_id                uuid    NOT NULL,
  queue_id                text    NOT NULL,
  number                  text    NOT NULL,  -- PII: phone (E.164); purged 30 days after the callback ends
  priority                integer NOT NULL,
  requirements            jsonb   NOT NULL DEFAULT '[]'::jsonb,
  first_queued_at_ms      bigint  NOT NULL,
  ordering_at_ms          bigint  NOT NULL,
  state                   text    NOT NULL CHECK (state IN ('scheduled', 'dispatched', 'claimed', 'done', 'flagged')),
  attempt                 integer NOT NULL DEFAULT 0,
  attempt_interaction_id  uuid    NOT NULL,
  reservation_id          uuid,
  agent_id                text,
  outcome                 text,
  due_at_ms               bigint  NOT NULL,
  created_at_ms           bigint  NOT NULL,
  updated_at_ms           bigint  NOT NULL,
  PRIMARY KEY (tenant_id, callback_id),
  UNIQUE (tenant_id, entry_id)
);
CREATE INDEX IF NOT EXISTS routing_callbacks_open ON routing_callbacks (tenant_id, state)
  WHERE state IN ('scheduled', 'dispatched', 'claimed');

CREATE TABLE IF NOT EXISTS routing_voicemail_tasks (
  tenant_id      text    NOT NULL,
  task_id        uuid    NOT NULL,
  interaction_id uuid    NOT NULL,
  queue_id       text    NOT NULL,
  recording_id   uuid    NOT NULL,
  recorded_at_ms bigint  NOT NULL,
  duration_s     integer NOT NULL,
  state          text    NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'done')),
  created_at_ms  bigint  NOT NULL,
  PRIMARY KEY (tenant_id, task_id),
  UNIQUE (tenant_id, recording_id)
);

CREATE TABLE IF NOT EXISTS routing_bucket_actuals (
  tenant_id        text             NOT NULL,
  queue_id         text             NOT NULL,
  bucket_start_ms  bigint           NOT NULL,
  offered          bigint           NOT NULL,
  answered         bigint           NOT NULL,
  answered_in_sla  bigint           NOT NULL,
  abandoned        bigint           NOT NULL,
  overflowed       bigint           NOT NULL,
  wait_answered_ms double precision NOT NULL,
  handle_ms        double precision NOT NULL,
  handled          bigint           NOT NULL,
  PRIMARY KEY (tenant_id, queue_id, bucket_start_ms)
);

ALTER TABLE routing_callbacks ENABLE ROW LEVEL SECURITY;
ALTER TABLE routing_callbacks FORCE ROW LEVEL SECURITY;
ALTER TABLE routing_voicemail_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE routing_voicemail_tasks FORCE ROW LEVEL SECURITY;
ALTER TABLE routing_bucket_actuals ENABLE ROW LEVEL SECURITY;
ALTER TABLE routing_bucket_actuals FORCE ROW LEVEL SECURITY;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['routing_callbacks', 'routing_voicemail_tasks', 'routing_bucket_actuals'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = t AND policyname = 'tenant_isolation') THEN
      EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (tenant_id = current_setting(''parity.tenant_id'', true)) '
                     'WITH CHECK (tenant_id = current_setting(''parity.tenant_id'', true))', t);
    END IF;
  END LOOP;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'parity_cc_routing_app') THEN
    CREATE ROLE parity_cc_routing_app NOLOGIN;
  END IF;
END $$;
GRANT SELECT, INSERT, UPDATE, DELETE ON routing_callbacks TO parity_cc_routing_app;
GRANT SELECT, INSERT ON routing_voicemail_tasks TO parity_cc_routing_app;
GRANT SELECT, INSERT, UPDATE ON routing_bucket_actuals TO parity_cc_routing_app;
