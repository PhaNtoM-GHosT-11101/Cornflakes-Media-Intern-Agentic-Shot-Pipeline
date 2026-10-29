-- Demo schema. Runs automatically on first Postgres start.
CREATE TABLE IF NOT EXISTS accounts (
  user_id        text PRIMARY KEY,
  balance_cents  bigint NOT NULL DEFAULT 0,
  reserved_cents bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS price_table (
  stage      text NOT NULL,
  model      text NOT NULL,
  unit_cents int  NOT NULL,
  version    int  NOT NULL DEFAULT 1,
  PRIMARY KEY (stage, model, version)
);

CREATE TABLE IF NOT EXISTS jobs (
  id             uuid PRIMARY KEY,
  user_id        text NOT NULL,
  idem_key       text NOT NULL,
  status         text NOT NULL DEFAULT 'queued',
  story          text NOT NULL,
  shots          int  NOT NULL,
  quote_cents    bigint NOT NULL,
  reserved_cents bigint NOT NULL DEFAULT 0,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, idem_key)
);

CREATE TABLE IF NOT EXISTS shots (
  job_id         uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  idx            int  NOT NULL,
  prompt         text,
  status         text NOT NULL DEFAULT 'pending',
  provider       text,
  cost_cents     bigint NOT NULL DEFAULT 0,
  cache_hit      boolean NOT NULL DEFAULT false,
  consistency    double precision,
  regen_count    int NOT NULL DEFAULT 0,
  embedding      jsonb,
  PRIMARY KEY (job_id, idx)
);

CREATE TABLE IF NOT EXISTS tickets (
  id              bigserial PRIMARY KEY,
  job_id          uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  stage           text NOT NULL,
  shot_idx        int  NOT NULL DEFAULT -1,
  deps            bigint[] NOT NULL DEFAULT '{}',
  state           text NOT NULL DEFAULT 'pending',
  attempt         int  NOT NULL DEFAULT 0,
  lease_owner     text,
  lease_expires_at timestamptz,
  run_after       timestamptz NOT NULL DEFAULT now(),
  cost_cents      bigint NOT NULL DEFAULT 0,
  provider        text,
  error           text,
  result          jsonb,
  UNIQUE (job_id, stage, shot_idx)
);

CREATE INDEX IF NOT EXISTS tickets_state_idx ON tickets (state, run_after);
CREATE INDEX IF NOT EXISTS tickets_job_idx   ON tickets (job_id);

CREATE TABLE IF NOT EXISTS events (
  id         bigserial PRIMARY KEY,
  job_id     uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  type       text NOT NULL,
  payload    jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_job_idx ON events (job_id, id);

CREATE TABLE IF NOT EXISTS ledger (
  id           bigserial PRIMARY KEY,
  job_id       uuid REFERENCES jobs(id) ON DELETE CASCADE,
  kind         text NOT NULL,
  amount_cents bigint NOT NULL,
  idem_key     text UNIQUE,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS content_cache (
  key        text PRIMARY KEY,
  payload    jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO accounts (user_id, balance_cents) VALUES ('demo', 10000)
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO price_table (stage, model, unit_cents, version) VALUES
  ('plan',    'mock-llm',     50, 1),
  ('image',   'mock-image',  100, 1),
  ('clip',    'cheapest',    200, 1),
  ('check',   'mock-check',   20, 1),
  ('compose', 'mock-compose', 80, 1)
ON CONFLICT (stage, model, version) DO NOTHING;
