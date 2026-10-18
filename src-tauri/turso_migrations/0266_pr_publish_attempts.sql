CREATE TABLE pr_publish_attempts (
    id TEXT PRIMARY KEY,
    action_run_id TEXT NOT NULL REFERENCES action_runs(id) ON DELETE CASCADE,
    cycle INTEGER NOT NULL DEFAULT 1,
    attempt INTEGER NOT NULL,
    input_revision TEXT,
    outcome TEXT CHECK (outcome IN ('published', 'failed')),
    detail TEXT,
    started_at INTEGER NOT NULL,
    settled_at INTEGER,
    dispatched_at INTEGER,
    UNIQUE(action_run_id, cycle, attempt)
);
CREATE INDEX idx_pr_publish_attempts_run ON pr_publish_attempts(action_run_id, cycle DESC, attempt DESC);
