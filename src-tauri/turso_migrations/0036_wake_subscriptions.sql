CREATE TABLE IF NOT EXISTS wake_subscriptions (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_ref TEXT,
  fact_kinds_json TEXT,
  state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'muted', 'unsubscribed')),
  mute_until_kind TEXT,
  mute_until_ref TEXT,
  created_by TEXT NOT NULL DEFAULT 'agent' CHECK (created_by IN ('agent', 'user', 'system')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wake_subscriptions_job
  ON wake_subscriptions(job_id, state);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wake_subscriptions_unique_scope
  ON wake_subscriptions(
    job_id,
    source_kind,
    COALESCE(source_ref, ''),
    COALESCE(fact_kinds_json, '')
  );

CREATE TABLE IF NOT EXISTS suppressed_wakes (
  id TEXT PRIMARY KEY,
  subscription_id TEXT REFERENCES wake_subscriptions(id) ON DELETE SET NULL,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_ref TEXT,
  fact_kind TEXT,
  occurrences INTEGER NOT NULL DEFAULT 1,
  latest_detail_uri TEXT,
  content TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  delivered_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_suppressed_wakes_pending
  ON suppressed_wakes(job_id, delivered_at, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_suppressed_wakes_fact_collapse
  ON suppressed_wakes(
    job_id,
    source_kind,
    COALESCE(source_ref, ''),
    COALESCE(fact_kind, '')
  )
  WHERE content IS NULL AND delivered_at IS NULL;
