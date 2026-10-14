-- Coalesced thread wakes from the fact log. A burst of rows matching one
-- thread's standing triggers folds into ONE pending wake per (job, source):
-- the first row opens it, each further row within the settle window extends
-- `deliver_after` (never past `opened_at` + the max window), and the worker's
-- tick delivers it once due. Persisted so a restart mid-window still delivers
-- with the count intact. `delivered_at` NULL is pending; a claimed wake reads
-- `delivering` until its outcome lands. `project_id` is NULL for project-less
-- rows and for a wake whose rows span projects (both render as the workspace
-- view). Runner-private, like `fact_log`.
CREATE TABLE fact_log_wakes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  source TEXT NOT NULL,
  clause_ref TEXT NOT NULL,
  project_id TEXT,
  first_row_id INTEGER NOT NULL,
  last_row_id INTEGER NOT NULL,
  count INTEGER NOT NULL,
  opened_at INTEGER NOT NULL,
  deliver_after INTEGER NOT NULL,
  delivered_at INTEGER,
  delivered_action TEXT
);
CREATE INDEX idx_fact_log_wakes_open ON fact_log_wakes(job_id, source, delivered_at, id);
CREATE INDEX idx_fact_log_wakes_due ON fact_log_wakes(delivered_at, deliver_after, id);
