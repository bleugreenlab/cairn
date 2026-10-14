-- The durable fact log: one row per minted route fact. It is an INDEX over the
-- app's own records, never a store: `record_ref`/`record_id` point at the
-- canonical record (a transcript event, a file_changes row, an issue, a GitHub
-- comment) and `fields_json` holds only registry-typed, bounded fields. The
-- worker consumes it in id order; `indexed_at` and `route_state` are its two
-- lanes' claims. Runner-private, like `route_firings`: this is the runner's
-- dispatch queue.
CREATE TABLE fact_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id TEXT,
  source TEXT NOT NULL,
  identity TEXT NOT NULL,
  record_ref TEXT NOT NULL,
  record_id TEXT,
  fields_json TEXT NOT NULL,
  summary TEXT,
  presence TEXT NOT NULL CHECK (presence IN ('active','away')),
  actor_job_id TEXT,
  origin_principal_json TEXT,
  origin_appearance_json TEXT,
  route_provenance TEXT,
  minted_at INTEGER NOT NULL,
  indexed_at INTEGER,
  route_state TEXT NOT NULL DEFAULT 'pending' CHECK (route_state IN
    ('pending','unmatched','route_generated','stale','queued','running','done','failed','interrupted')),
  route_note TEXT,
  wakes_json TEXT
);
CREATE INDEX idx_fact_log_project ON fact_log(project_id, id DESC);
CREATE INDEX idx_fact_log_identity ON fact_log(source, identity);
CREATE INDEX idx_fact_log_subject ON fact_log(source, record_ref, id DESC);
CREATE INDEX idx_fact_log_route_state ON fact_log(route_state, id);
CREATE INDEX idx_fact_log_unindexed ON fact_log(indexed_at, id);
ALTER TABLE route_firings ADD COLUMN fact_log_id INTEGER;
CREATE INDEX idx_route_firings_fact_log ON route_firings(fact_log_id);
