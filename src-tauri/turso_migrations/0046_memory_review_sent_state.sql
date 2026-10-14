-- Rebuild jobs so existing databases that already applied 0044 accept the
-- simplified memory-review state machine: NULL -> sent -> done.
-- Keep memory_review_requested_turn physically present for compatibility, but
-- code must not read or write it.

CREATE TABLE jobs_new (
    id TEXT NOT NULL PRIMARY KEY,
    execution_id TEXT,
    recipe_node_id TEXT,
    parent_job_id TEXT,
    worktree_path TEXT,
    branch TEXT,
    base_commit TEXT,
    current_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    agent_config_id TEXT,
    issue_id TEXT,
    project_id TEXT NOT NULL,
    task_description TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    parent_tool_use_id TEXT,
    task_index INTEGER,
    started_at INTEGER,
    model TEXT,
    node_name TEXT,
    base_branch TEXT,
    current_turn_id TEXT,
    resume_session_id TEXT,
    uri_segment TEXT,
    memory_review_state TEXT CHECK (memory_review_state IN ('sent', 'done')),
    memory_review_requested_turn INTEGER,
    FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE SET NULL,
    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (current_turn_id) REFERENCES turns(id),
    FOREIGN KEY (resume_session_id) REFERENCES sessions(id)
);

INSERT INTO jobs_new (
    id, execution_id, recipe_node_id, parent_job_id, worktree_path, branch,
    base_commit, current_session_id, status, agent_config_id, issue_id,
    project_id, task_description, created_at, updated_at, completed_at,
    parent_tool_use_id, task_index, started_at, model, node_name, base_branch,
    current_turn_id, resume_session_id, uri_segment, memory_review_state,
    memory_review_requested_turn
)
SELECT
    id, execution_id, recipe_node_id, parent_job_id, worktree_path, branch,
    base_commit, current_session_id, status, agent_config_id, issue_id,
    project_id, task_description, created_at, updated_at, completed_at,
    parent_tool_use_id, task_index, started_at, model, node_name, base_branch,
    current_turn_id, resume_session_id, uri_segment,
    CASE memory_review_state WHEN 'pending' THEN 'sent' ELSE memory_review_state END,
    memory_review_requested_turn
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;

CREATE UNIQUE INDEX idx_jobs_issue_execution_uri_segment
    ON jobs (issue_id, execution_id, uri_segment)
    WHERE parent_job_id IS NULL
      AND issue_id IS NOT NULL
      AND execution_id IS NOT NULL
      AND uri_segment IS NOT NULL;
CREATE UNIQUE INDEX idx_jobs_parent_uri_segment
    ON jobs (parent_job_id, uri_segment)
    WHERE parent_job_id IS NOT NULL
      AND uri_segment IS NOT NULL;
