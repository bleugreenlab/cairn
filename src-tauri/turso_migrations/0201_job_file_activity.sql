-- Per-job file-touch stream: which files an agent's tool calls named, when.
--
-- This is the TIMELINE view of file activity, complementary to `file_changes`,
-- which is the OUTCOME view (a job's net branch diff, recomputed from git). A
-- job that reads twenty files and edits one produces one `file_changes` row and
-- twenty-one rows here; the difference is the point, because a live workspace
-- map renders attention, not just result.
--
-- Rows are derived from a tool call's own structured input at the moment its
-- assistant event lands (`transcripts::file_activity`), inside that event's
-- insert transaction. `action` is what the caller DECLARED, so a row asserts
-- "this tool call named this path", not "this write succeeded" -- the same
-- best-effort standing `tool_invocations` rows have. No backfill: the stream is
-- only meaningful live, and a gap is a quiet period rather than a wrong answer.

CREATE TABLE IF NOT EXISTS job_file_activity (
    id          TEXT PRIMARY KEY,    -- {event_id}:{tool_use_id}:{ordinal}
    job_id      TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    action      TEXT NOT NULL CHECK(action IN ('read', 'edit', 'create', 'delete')),
    created_at  INTEGER NOT NULL     -- source event created_at (epoch seconds)
);

-- "What has this job been touching lately" -- the per-node map query.
CREATE INDEX IF NOT EXISTS idx_job_file_activity_job
    ON job_file_activity(job_id, created_at);

-- "What has the workspace been touching lately" -- the cross-job map query.
CREATE INDEX IF NOT EXISTS idx_job_file_activity_created
    ON job_file_activity(created_at);
