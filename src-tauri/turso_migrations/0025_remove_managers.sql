-- CAIRN-1248: remove the manager stack.
--
-- Multi-issue delivery now rides on the `parent_issue_id` edge plus the Feature
-- Manager recipe, so the parallel manager machinery is gone. This migration
-- physically drops every manager table and every `manager_id` column, reaching
-- a fully canonical schema with no vestigial columns or dangling foreign keys.
--
-- libsql has no usable `ALTER TABLE ... DROP COLUMN`, the `manager_id` columns
-- are FK children, and `issues`/`jobs`/`turns` are referenced by other tables —
-- so each affected table is rebuilt. This migration is registered as an
-- `fk_off` migration (see storage/migration.rs): the runner disables foreign
-- keys before the transaction so the parent-table rebuilds do not trip
-- enforcement on inbound references. The rebuilds use the canonical
-- create-new / copy / drop-old / rename-new procedure (NOT rename-original):
-- renaming the original table would make modern SQLite rewrite inbound FK
-- references in other tables to the `_old` name, leaving them dangling. Creating
-- a fresh table and renaming it onto the canonical name leaves by-name
-- references intact.
--
-- Schemas below are transcribed from a freshly-migrated database
-- (`SELECT sql FROM sqlite_master`), minus the manager column/FK/index. Search
-- triggers dropped with the `issues`/`messages` rebuilds are recreated.

-- 1. Drop the manager tables (child -> parent).
DROP TABLE IF EXISTS manager_mailbox;
DROP TABLE IF EXISTS manager_wake_batches;
DROP TABLE IF EXISTS manager_scopes;
DROP TABLE IF EXISTS managers;

-- 2. issues: drop manager_id + its FK. Inbound FKs from jobs, merge_requests;
--    self-FK via parent_issue_id. Recreate indexes + search triggers.
CREATE TABLE issues_new (
    id TEXT NOT NULL PRIMARY KEY,
    project_id TEXT NOT NULL,
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'backlog',
    priority INTEGER,
    completed_at INTEGER,
    dismissed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    model TEXT,
    merged_at INTEGER,
    closed_at INTEGER,
    progress TEXT NOT NULL DEFAULT 'backlog',
    attention TEXT NOT NULL DEFAULT 'none',
    parent_issue_id TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_issue_id) REFERENCES issues(id) ON DELETE SET NULL
);
INSERT INTO issues_new (id, project_id, number, title, description, status, priority, completed_at, dismissed_at, created_at, updated_at, model, merged_at, closed_at, progress, attention, parent_issue_id)
    SELECT id, project_id, number, title, description, status, priority, completed_at, dismissed_at, created_at, updated_at, model, merged_at, closed_at, progress, attention, parent_issue_id FROM issues;
DROP TABLE issues;
ALTER TABLE issues_new RENAME TO issues;
CREATE INDEX idx_issues_parent_id ON issues (parent_issue_id);
CREATE INDEX idx_issues_project_id ON issues (project_id);
CREATE UNIQUE INDEX idx_issues_project_number ON issues (project_id, number);
CREATE TRIGGER search_issues_insert AFTER INSERT ON issues BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_issues_update AFTER UPDATE ON issues
WHEN OLD.title != NEW.title OR OLD.description IS NOT NEW.description OR OLD.project_id != NEW.project_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_issues_delete AFTER DELETE ON issues BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', OLD.id, 'issue', 'delete', 'pending', unixepoch());
END;

-- 3. jobs: drop manager_id + its FK + idx_jobs_manager_id. Self-FK via
--    parent_job_id; references executions/issues/projects/turns/sessions.
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
    FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE SET NULL,
    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (current_turn_id) REFERENCES turns(id),
    FOREIGN KEY (resume_session_id) REFERENCES sessions(id)
);
INSERT INTO jobs_new (id, execution_id, recipe_node_id, parent_job_id, worktree_path, branch, base_commit, current_session_id, status, agent_config_id, issue_id, project_id, task_description, created_at, updated_at, completed_at, parent_tool_use_id, task_index, started_at, model, node_name, base_branch, current_turn_id, resume_session_id, uri_segment)
    SELECT id, execution_id, recipe_node_id, parent_job_id, worktree_path, branch, base_commit, current_session_id, status, agent_config_id, issue_id, project_id, task_description, created_at, updated_at, completed_at, parent_tool_use_id, task_index, started_at, model, node_name, base_branch, current_turn_id, resume_session_id, uri_segment FROM jobs;
DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;
CREATE UNIQUE INDEX idx_jobs_issue_execution_uri_segment ON jobs (issue_id, execution_id, uri_segment) WHERE parent_job_id IS NULL AND issue_id IS NOT NULL AND execution_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE UNIQUE INDEX idx_jobs_parent_uri_segment ON jobs (parent_job_id, uri_segment) WHERE parent_job_id IS NOT NULL AND uri_segment IS NOT NULL;

-- 4. turns: drop manager_id + its FK + idx_turns_manager_id. Inbound FKs from
--    events/prompts/permission_requests/jobs.current_turn_id; self-FK via
--    predecessor_id.
CREATE TABLE turns_new (
    id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    run_id TEXT,
    job_id TEXT,
    sequence INTEGER NOT NULL,
    predecessor_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    yield_reason TEXT,
    start_reason TEXT NOT NULL DEFAULT 'initial',
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    ended_at INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (predecessor_id) REFERENCES turns(id)
);
INSERT INTO turns_new (id, session_id, run_id, job_id, sequence, predecessor_id, state, yield_reason, start_reason, created_at, started_at, ended_at, updated_at)
    SELECT id, session_id, run_id, job_id, sequence, predecessor_id, state, yield_reason, start_reason, created_at, started_at, ended_at, updated_at FROM turns;
DROP TABLE turns;
ALTER TABLE turns_new RENAME TO turns;
CREATE INDEX idx_turns_job_id ON turns (job_id);
CREATE INDEX idx_turns_run_id ON turns (run_id);
CREATE INDEX idx_turns_session_id ON turns (session_id);
CREATE UNIQUE INDEX idx_turns_session_sequence ON turns (session_id, sequence);

-- 5. messages: drop recipient_manager_id + its FK + idx_messages_recipient_manager_id.
--    Recreate indexes + search triggers.
CREATE TABLE messages_new (
    id TEXT NOT NULL PRIMARY KEY,
    channel_type TEXT NOT NULL CHECK(channel_type IN ('project', 'issue', 'direct')),
    channel_id TEXT,
    sender_run_id TEXT,
    sender_name TEXT NOT NULL,
    recipient_run_id TEXT,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    delivered_at INTEGER DEFAULT NULL
);
INSERT INTO messages_new (id, channel_type, channel_id, sender_run_id, sender_name, recipient_run_id, content, created_at, delivered_at)
    SELECT id, channel_type, channel_id, sender_run_id, sender_name, recipient_run_id, content, created_at, delivered_at FROM messages;
DROP TABLE messages;
ALTER TABLE messages_new RENAME TO messages;
CREATE INDEX idx_messages_channel ON messages (channel_type, channel_id, created_at);
CREATE INDEX idx_messages_created_at ON messages (created_at);
CREATE INDEX idx_messages_pending_directs ON messages (recipient_run_id, delivered_at) WHERE channel_type = 'direct' AND delivered_at IS NULL;
CREATE INDEX idx_messages_recipient ON messages (recipient_run_id) WHERE recipient_run_id IS NOT NULL;
CREATE TRIGGER search_messages_insert AFTER INSERT ON messages BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_messages_update AFTER UPDATE ON messages
WHEN OLD.content != NEW.content OR OLD.channel_type != NEW.channel_type OR OLD.channel_id IS NOT NEW.channel_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_messages_delete AFTER DELETE ON messages BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', OLD.id, 'message', 'delete', 'pending', unixepoch());
END;

-- 6. merge_requests: drop manager_id + its FK + idx_mr_manager. No inbound FKs.
CREATE TABLE merge_requests_new (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    issue_id TEXT REFERENCES issues(id),
    title TEXT NOT NULL,
    body TEXT,
    source_branch TEXT NOT NULL,
    target_branch TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    merge_method TEXT NOT NULL DEFAULT 'squash',
    additions INTEGER,
    deletions INTEGER,
    changed_files INTEGER,
    commit_count INTEGER,
    merged_commit TEXT,
    checks_json TEXT,
    checks_status TEXT,
    opened_at INTEGER NOT NULL,
    merged_at INTEGER,
    closed_at INTEGER,
    updated_at INTEGER NOT NULL,
    github_pr_number INTEGER,
    github_pr_url TEXT UNIQUE,
    github_state TEXT,
    github_review TEXT,
    github_mergeable TEXT,
    github_fetched_at INTEGER
);
INSERT INTO merge_requests_new (id, job_id, project_id, issue_id, title, body, source_branch, target_branch, status, merge_method, additions, deletions, changed_files, commit_count, merged_commit, checks_json, checks_status, opened_at, merged_at, closed_at, updated_at, github_pr_number, github_pr_url, github_state, github_review, github_mergeable, github_fetched_at)
    SELECT id, job_id, project_id, issue_id, title, body, source_branch, target_branch, status, merge_method, additions, deletions, changed_files, commit_count, merged_commit, checks_json, checks_status, opened_at, merged_at, closed_at, updated_at, github_pr_number, github_pr_url, github_state, github_review, github_mergeable, github_fetched_at FROM merge_requests;
DROP TABLE merge_requests;
ALTER TABLE merge_requests_new RENAME TO merge_requests;
CREATE INDEX idx_mr_issue ON merge_requests (issue_id) WHERE issue_id IS NOT NULL;
CREATE UNIQUE INDEX idx_mr_job ON merge_requests (job_id);
CREATE INDEX idx_mr_project_status ON merge_requests (project_id, status);
