-- lineage: private
-- rebuild-fk-off: Conserved(fact_log_wakes)
-- cairn/6391: a coalesced fact wake belongs to a stable recipient, not to the
-- session job that happened to be current when its first row was indexed.
--
-- `recipient_key` is `thread:<threads.id>` for a thread's session and
-- `job:<jobs.id>` for any other job. Coalescing is keyed by
-- (recipient_key, source), so a thread that rotates sessions inside the
-- window keeps one wake, delivered to whichever session is current at
-- delivery. `job_id` becomes the job the wake was delivered to (NULL while
-- pending). `level` is the highest level any folded row reached the
-- recipient at, and only rises.
--
-- Pending rows are backfilled: a session job's wake becomes its thread's, any
-- other keeps its job. Each pending wake gains one receipt for its newest row
-- so the receipt-driven drain still owes it.
CREATE TABLE fact_log_wakes_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  recipient_key TEXT NOT NULL,
  job_id TEXT,
  level TEXT NOT NULL DEFAULT 'wake' CHECK (level IN ('passive', 'wake', 'interrupt')),
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
INSERT INTO fact_log_wakes_new (
  id, recipient_key, job_id, level, source, clause_ref, project_id,
  first_row_id, last_row_id, count, opened_at, deliver_after, delivered_at, delivered_action
)
SELECT w.id,
       CASE WHEN j.thread_id IS NOT NULL AND j.parent_job_id IS NULL AND j.uri_segment = 'thread'
            THEN 'thread:' || j.thread_id
            ELSE 'job:' || w.job_id END,
       CASE WHEN w.delivered_at IS NULL THEN NULL ELSE w.job_id END,
       CASE WHEN substr(w.clause_ref, 1, 11) = 'camp-watch:' THEN 'passive' ELSE 'wake' END,
       w.source, w.clause_ref, w.project_id, w.first_row_id, w.last_row_id, w.count,
       w.opened_at, w.deliver_after, w.delivered_at, w.delivered_action
  FROM fact_log_wakes w
  LEFT JOIN jobs j ON j.id = w.job_id;
DROP TABLE fact_log_wakes;
ALTER TABLE fact_log_wakes_new RENAME TO fact_log_wakes;
CREATE INDEX idx_fact_log_wakes_open ON fact_log_wakes(recipient_key, source, delivered_at, id);
CREATE INDEX idx_fact_log_wakes_due ON fact_log_wakes(delivered_at, deliver_after, id);

INSERT INTO delivery_receipts (
  id, fact_key, recipient_kind, recipient_id, project_id, fact_row_id, level,
  reasons_json, state, wake_id, created_at, updated_at
)
SELECT 'wake-backfill-' || w.id,
       'fact_log:' || w.source || ':' || w.last_row_id,
       substr(w.recipient_key, 1, instr(w.recipient_key, ':') - 1),
       substr(w.recipient_key, instr(w.recipient_key, ':') + 1),
       w.project_id,
       w.last_row_id,
       w.level,
       '[{"kind":"backfilled","clause":' || json_quote(w.clause_ref) || ',"level":' || json_quote(w.level) || '}]',
       'queued',
       w.id,
       w.opened_at,
       w.opened_at
  FROM fact_log_wakes w
 WHERE w.delivered_at IS NULL;
