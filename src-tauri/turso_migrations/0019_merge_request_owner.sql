-- merge_requests.job_id becomes a generic "owner" reference.
--
-- Originally it FK-referenced jobs(id): every PR was produced by a job (the
-- legacy create_pr action linked the row to the builder job). Under CAIRN-1220
-- the first-class `pr` node produces the PR as an *action_run*, not a job, and
-- owns the merge_requests row directly (so resolution can fire the pr node's
-- merge/close port and complete the run). An action_run is not a job, so the FK
-- no longer holds. Recreate the table without the jobs(id) FK; job_id now holds
-- either a jobs.id (create_pr) or an action_runs.id (pr node). All other
-- constraints and indexes are preserved.

ALTER TABLE merge_requests RENAME TO merge_requests_old;

CREATE TABLE merge_requests (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,   -- owner: jobs.id (create_pr) or action_runs.id (pr node)
    project_id      TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,

    -- Ownership: exactly one of these is set
    issue_id        TEXT REFERENCES issues(id),
    manager_id      TEXT REFERENCES managers(id),

    -- Authoritative state (Cairn-owned)
    title           TEXT NOT NULL,
    body            TEXT,
    source_branch   TEXT NOT NULL,
    target_branch   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',           -- open | merged | closed
    merge_method    TEXT NOT NULL DEFAULT 'squash',
    additions       INTEGER,
    deletions       INTEGER,
    changed_files   INTEGER,
    commit_count    INTEGER,
    merged_commit   TEXT,
    checks_json     TEXT,
    checks_status   TEXT,
    opened_at       INTEGER NOT NULL,
    merged_at       INTEGER,
    closed_at       INTEGER,
    updated_at      INTEGER NOT NULL,

    -- GitHub sync (all nullable — only populated when GH integration active)
    github_pr_number    INTEGER,
    github_pr_url       TEXT UNIQUE,
    github_state        TEXT,
    github_review       TEXT,
    github_mergeable    TEXT,
    github_fetched_at   INTEGER
);

INSERT INTO merge_requests SELECT * FROM merge_requests_old;

-- Drop the old table (and its now-renamed indexes) before recreating the
-- indexes on the new table, so the index names are free.
DROP TABLE merge_requests_old;

CREATE UNIQUE INDEX idx_mr_job ON merge_requests(job_id);
CREATE INDEX idx_mr_issue ON merge_requests(issue_id) WHERE issue_id IS NOT NULL;
CREATE INDEX idx_mr_manager ON merge_requests(manager_id) WHERE manager_id IS NOT NULL;
CREATE INDEX idx_mr_project_status ON merge_requests(project_id, status);
