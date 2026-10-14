-- CAIRN-1368: drop the dead project-chat `chats` table.
--
-- Project-level chat sessions had no frontend entry point, and the backend
-- feature (Tauri commands, core queries, headless invoke route, and the
-- `cairn://p/PROJECT/chat/NAME` resource) is removed in this change. Nothing
-- reads or writes the `chats` table anymore.
--
-- `runs.chat_id` and `sessions.chat_id` are FK children of `chats(id)`. libsql
-- enforces `PRAGMA foreign_keys = ON` and rejects any insert into a child table
-- whose FK references a missing parent ("foreign key mismatch"), so the parent
-- cannot simply be dropped: `runs` and `sessions` must first be rebuilt without
-- the `chats` foreign key. This runs as an fk_off migration (see
-- storage/migration.rs) using the canonical create-new / copy / drop-old /
-- rename-onto-canonical procedure so that inbound by-name FK references from
-- other tables (e.g. job_terminals.run_id, turns.run_id, jobs.resume_session_id,
-- the sessions self-references) stay intact.
--
-- The `chat_id` columns themselves are kept as vestigial, always-NULL columns
-- (their `idx_*_chat_id` indexes and the sessions job_id/chat_id CHECK are
-- preserved). Removing the columns would mean dropping the `chat_id` field from
-- the `Run`/`Session` models and every run/session query, which is intentionally
-- out of scope here.

-- 1. runs: drop the chat_id -> chats(id) foreign key. Keep the column, every
--    other FK, and all indexes. Inbound FKs from job_terminals, turns, etc.
CREATE TABLE runs_new (
    id TEXT PRIMARY KEY,
    issue_id TEXT REFERENCES issues(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    status TEXT,
    session_id TEXT,
    error_message TEXT,
    started_at INTEGER,
    exited_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    chat_id TEXT,
    backend TEXT,
    exit_reason TEXT,
    start_mode TEXT
);
INSERT INTO runs_new (id, issue_id, project_id, job_id, status, session_id, error_message, started_at, exited_at, created_at, updated_at, chat_id, backend, exit_reason, start_mode)
    SELECT id, issue_id, project_id, job_id, status, session_id, error_message, started_at, exited_at, created_at, updated_at, chat_id, backend, exit_reason, start_mode FROM runs;
DROP TABLE runs;
ALTER TABLE runs_new RENAME TO runs;
CREATE INDEX idx_runs_issue_id ON runs(issue_id);
CREATE INDEX idx_runs_project_id ON runs(project_id);
CREATE INDEX idx_runs_job_id ON runs(job_id);
CREATE INDEX idx_runs_chat_id ON runs(chat_id);

-- 2. sessions: drop the chat_id -> chats(id) foreign key. Keep the column, the
--    job_id/chat_id CHECK, every other FK (incl. self-references), and all
--    indexes. Inbound FKs from jobs.resume_session_id, turns, etc.
CREATE TABLE sessions_new (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT,
    chat_id TEXT,
    backend TEXT NOT NULL DEFAULT 'claude',
    status TEXT NOT NULL DEFAULT 'open',
    replaced_by_id TEXT,
    terminal_reason TEXT,
    sequence INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    closed_at INTEGER,
    updated_at INTEGER NOT NULL,
    backend_id TEXT,
    parent_session_id TEXT REFERENCES sessions(id),
    current_pos BLOB,
    channel_cursor_rowid INTEGER,
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (replaced_by_id) REFERENCES sessions(id),
    CHECK ((job_id IS NOT NULL AND chat_id IS NULL) OR (job_id IS NULL AND chat_id IS NOT NULL))
);
INSERT INTO sessions_new (id, job_id, chat_id, backend, status, replaced_by_id, terminal_reason, sequence, created_at, closed_at, updated_at, backend_id, parent_session_id, current_pos, channel_cursor_rowid)
    SELECT id, job_id, chat_id, backend, status, replaced_by_id, terminal_reason, sequence, created_at, closed_at, updated_at, backend_id, parent_session_id, current_pos, channel_cursor_rowid FROM sessions;
DROP TABLE sessions;
ALTER TABLE sessions_new RENAME TO sessions;
CREATE INDEX idx_sessions_job_id ON sessions(job_id);
CREATE INDEX idx_sessions_chat_id ON sessions(chat_id);
CREATE INDEX idx_sessions_status ON sessions(status);
CREATE INDEX idx_sessions_parent_session_id ON sessions(parent_session_id);

-- 3. With no remaining inbound foreign keys, drop the dead chats table.
DROP TABLE IF EXISTS chats;
