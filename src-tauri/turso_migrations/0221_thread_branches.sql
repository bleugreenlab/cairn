-- Numbered branch entities owned by durable thread identity.
ALTER TABLE threads ADD COLUMN next_branch_number INTEGER NOT NULL DEFAULT 1;

CREATE TABLE thread_branches (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    number INTEGER NOT NULL,
    title TEXT,
    stage TEXT NOT NULL CHECK(stage IN ('accumulating', 'pr-open', 'merged', 'abandoned')),
    git_ref TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    moved_at INTEGER NOT NULL,
    UNIQUE(thread_id, number),
    UNIQUE(git_ref),
    UNIQUE(job_id)
);

CREATE INDEX idx_thread_branches_thread ON thread_branches(thread_id, number);
