-- lineage: shared
-- One row per actual measurement, not per review wave sharing its result.
CREATE TABLE replay_comparisons (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    bookmark TEXT NOT NULL,
    before_commit TEXT,
    after_commit TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('unchanged', 'changed', 'unreadable')),
    reason TEXT,
    observed_ms INTEGER NOT NULL,
    CHECK ((outcome = 'unreadable' AND reason IS NOT NULL)
        OR (outcome != 'unreadable' AND reason IS NULL))
);
CREATE INDEX idx_replay_comparisons_project_branch
    ON replay_comparisons (project_id, bookmark, observed_ms DESC, id DESC);
CREATE INDEX idx_replay_comparisons_project_time
    ON replay_comparisons (project_id, observed_ms DESC, id DESC);
