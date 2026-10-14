-- Index the hottest read path: loading a session's transcript.
--
-- The session-events loader runs, on every transcript load:
--   SELECT id FROM runs WHERE session_id = ? ORDER BY created_at ASC
-- runs already had indexes on chat_id, job_id, project_id, and issue_id, but
-- NONE on session_id, so this was a full table SCAN plus a sort
-- (EXPLAIN QUERY PLAN: "SCAN runs" + "USE SORTER FOR ORDER BY"). The composite
-- (session_id, created_at) turns it into an index seek that also satisfies the
-- ORDER BY, dropping the sort entirely
-- ("SEARCH runs USING INDEX idx_runs_session_id_created_at").
--
-- Verified against the real schema with EXPLAIN QUERY PLAN before/after; the
-- storage::migrations test migration_0079_indexes_session_runs_query asserts the
-- plan change so it cannot silently regress.
CREATE INDEX IF NOT EXISTS idx_runs_session_id_created_at
  ON runs(session_id, created_at);
