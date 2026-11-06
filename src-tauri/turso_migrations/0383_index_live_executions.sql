-- lineage: shared
-- The companion projection's active graph reads only running and paused
-- executions, and must not touch their rows: Turso assembles a whole record to
-- read any column, and an execution's row carries its snapshot (cairn/5894).
-- This carries every column those reads take, for exactly the rows they can
-- match. Leading with issue_id lets the per-issue join seek it; the predicate
-- is spelled exactly as the reads spell theirs, which is what lets Turso
-- substitute a partial index (docs/database.md).
CREATE INDEX idx_executions_live
    ON executions(issue_id, started_at, seq, status, id)
 WHERE status IN ('running','paused');
