-- Catch-up read cursors (CAIRN-1894, attention/wake rebuild — see
-- docs/attention-redesign.md).
--
-- Chat-window catch-up moved onto the push queue as a passive catchup:{child}
-- push, gated on the user->child message moment and resolved AT DELIVERY against
-- a single read position per (parent, child). This table holds that position:
-- one row per (recipient, source), the end of what the parent was last shown of
-- the child's chat.
--
-- A dedicated table, NOT a column on attention_pushes, because the cursor must
-- outlive the catch-up push's supersession churn: push rows are transient
-- (superseded in place, archival-pruned, and absent entirely for long stretches
-- when no catch-up is live), while the read position is durable and must survive
-- all of it.
--
-- source is the child agent JOB id — the job whose {node}/chat a catch-up push
-- renders. It must be job-scoped (not issue-scoped) so the cursor counts exactly
-- the transcript that is shown: node chat loads events for one job's runs, while
-- an issue spans sibling jobs and sub-task runs that share the issue_id. position
-- is that job's distinct-turn count at the moment of delivery.

CREATE TABLE IF NOT EXISTS attention_read_cursors (
  -- The watcher (parent) job whose read position this is. CASCADE: watcher gone
  -- -> nothing left to position.
  recipient   TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  -- The watched chat source: the child agent job id.
  source      TEXT NOT NULL,
  -- Last-seen job-scoped chat turn count (end of what was shown). Advanced
  -- monotonically (MAX) at delivery so an out-of-order redelivery never rewinds.
  position    INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (recipient, source)
);
