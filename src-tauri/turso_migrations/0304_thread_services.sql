-- lineage: private
-- Thread-owned services (cairn/5524): a thread writes cairn:~/services/<name>
-- like a page, and the runner runs it. Each row is one version of one
-- service's package as its thread wrote it: the payload (`title`,
-- `description`, `files`, `entry`, `manifest`) and the source hash of the
-- package it builds. Versions are numbered per (thread, name) from 1, and the
-- newest is the one the supervisor serves. Rows go with their thread.
--
-- Not artifacts: a session's artifacts are the thread pane's tabs, and a
-- service is a process behind a page, not something the pane shows.
CREATE TABLE thread_services (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    data TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    -- The job that wrote this version: the thread's session or one of its tasks.
    written_by_job_id TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE (thread_id, name, version)
);

CREATE INDEX idx_thread_services_thread_name_version
    ON thread_services(thread_id, name, version);

-- The service log now holds rows for project services ('') and thread-owned
-- services (the owning thread's id), which may share a name.
ALTER TABLE service_log ADD COLUMN owner_thread TEXT NOT NULL DEFAULT '';
