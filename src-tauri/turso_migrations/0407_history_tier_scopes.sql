-- lineage: private
-- history_tier becomes keyed by scope (cairn/5587). A pack covers either one
-- execution or one thread range: a run of whole closed sessions of a thread's
-- session job, with the task jobs that ran in them. A thread's execution never
-- resolves and hosts many ranges (or is null), so the execution can no longer
-- be the key, and the read seam no longer reaches a pack through
-- `jobs.execution_id`.
--
-- `scope_id` is the execution id for an execution scope, and
-- `<session job id>/<first sequence>-<last sequence>` for a thread range.
-- `owner_id` is the execution, or the thread's session job. `through_generation`
-- is the compaction generation a thread range was cut at; null for an
-- execution.
--
-- Reads resolve only through explicit mappings: `history_tier_runs` by run and
-- `history_tier_sessions` by session. A turn resolves through its run, or its
-- session when it has no run. Mapping rows are written before their
-- `history_tier` row, which stays the commit point, and a mapping with no
-- `history_tier` row is ignored by the read seam's join. Neither has a foreign
-- key, for the same reason.
--
-- Existing rows become execution scopes, and their sessions are backfilled from
-- the execution's jobs: the set the read seam reached through the execution
-- join until now.
CREATE TABLE history_tier_scoped (
    scope_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('execution', 'thread_range')),
    owner_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    format INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    hot_bytes_estimate INTEGER NOT NULL,
    row_counts TEXT NOT NULL,
    resolved_at INTEGER NOT NULL,
    packed_at INTEGER NOT NULL,
    evicted_at INTEGER,
    through_generation INTEGER
);

INSERT INTO history_tier_scoped
    (scope_id, kind, owner_id, content_hash, format, bytes, hot_bytes_estimate,
     row_counts, resolved_at, packed_at, evicted_at, through_generation)
SELECT execution_id, 'execution', execution_id, content_hash, format, bytes,
       hot_bytes_estimate, row_counts, resolved_at, packed_at, evicted_at, NULL
FROM history_tier;

DROP TABLE history_tier;
ALTER TABLE history_tier_scoped RENAME TO history_tier;

CREATE INDEX idx_history_tier_owner ON history_tier(owner_id);

DROP INDEX idx_history_tier_runs_execution;
ALTER TABLE history_tier_runs RENAME COLUMN execution_id TO scope_id;
CREATE INDEX idx_history_tier_runs_scope ON history_tier_runs(scope_id);

CREATE TABLE history_tier_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    scope_id TEXT NOT NULL
);

CREATE INDEX idx_history_tier_sessions_scope ON history_tier_sessions(scope_id);

INSERT OR IGNORE INTO history_tier_sessions(session_id, scope_id)
SELECT s.id, h.scope_id
FROM history_tier h
JOIN jobs j ON j.execution_id = h.scope_id
JOIN sessions s ON s.job_id = j.id
WHERE h.kind = 'execution';
