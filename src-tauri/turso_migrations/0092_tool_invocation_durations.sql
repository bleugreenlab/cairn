-- Tool invocation duration support for analytics.
--
-- `result_ts` records the matching tool_result event created_at (epoch seconds).
-- The incremental backfill is idempotent and upserts by stable row id, so reset
-- per-run watermarks to rebuild historical rows with result timestamps populated.

ALTER TABLE tool_invocations ADD COLUMN result_ts INTEGER;

DELETE FROM tool_invocation_runs;
