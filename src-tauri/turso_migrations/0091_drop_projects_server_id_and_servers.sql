-- CAIRN-2277: drop the dead projects.server_id column + the servers FK, then drop
-- the dead local servers table. rebuild_fk_off: many tables FK-reference
-- projects(id), and projects itself references servers, so this runs with FK
-- enforcement OFF (registered as Migration::rebuild_fk_off).
CREATE TABLE projects_new (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    key TEXT NOT NULL UNIQUE,
    repo_path TEXT NOT NULL,
    context TEXT DEFAULT '',
    docs_enabled INTEGER DEFAULT 1,
    default_branch TEXT DEFAULT 'main',
    next_issue_number INTEGER DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    ci_commands TEXT,
    setup_commands TEXT,
    terminal_commands TEXT,
    config TEXT,
    remote_url TEXT,
    hidden INTEGER NOT NULL DEFAULT 0,
    is_workspace INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
);

INSERT INTO projects_new (
    id, workspace_id, name, key, repo_path, context, docs_enabled,
    default_branch, next_issue_number, created_at, updated_at,
    ci_commands, setup_commands, terminal_commands, config,
    remote_url, hidden, is_workspace
)
SELECT
    id, workspace_id, name, key, repo_path, context, docs_enabled,
    default_branch, next_issue_number, created_at, updated_at,
    ci_commands, setup_commands, terminal_commands, config,
    remote_url, hidden, is_workspace
FROM projects;

DROP TABLE projects;
ALTER TABLE projects_new RENAME TO projects;

DROP TABLE servers;
