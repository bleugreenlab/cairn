-- Progress marker for the one-time retirement backfill (CAIRN-4182).
--
-- 0196 gives new drains somewhere to record that a referent went terminal, but
-- it says nothing about the rows already in the table -- in production, 4,213
-- undelivered pushes, the oldest 55 days old. Those are retired by a detached
-- startup worker rather than by the migration itself: classifying a push means
-- running the liveness resolver against issues, merge_requests, artifacts, runs,
-- prompts and permission_requests, which is application logic that must not be
-- duplicated as migration SQL. Two copies of a delivery predicate drift apart
-- silently, and this one decides whether an agent is ever woken again.
--
-- Private-only and runner-transient, exactly like analytics_rollup_backfill_state
-- and archival_backfill_state: it records how far THIS install's maintenance pass
-- has got. A team replica has no use for another machine's cursor.

CREATE TABLE IF NOT EXISTS attention_push_retirement_backfill (
  -- Single row. The CHECK is the whole enforcement -- there is one backfill.
  id                INTEGER PRIMARY KEY CHECK (id = 1),
  -- MAX(rowid) captured once when the worker first starts. Rows created after
  -- this point are the ordinary drain's business, so the pass has a fixed
  -- finish line and cannot chase a table that is still growing.
  upper_bound_rowid INTEGER NOT NULL,
  -- Keyset cursor: the last rowid durably processed. Resume is `rowid > last`,
  -- never OFFSET, so an interrupted pass repeats at most one batch and skips
  -- none.
  last_rowid        INTEGER NOT NULL DEFAULT 0,
  started_at        INTEGER NOT NULL,
  -- Set only after a final bounded query proves no retirement-eligible row
  -- remains at or below upper_bound_rowid.
  completed_at      INTEGER
);
