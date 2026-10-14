-- Inline browser (Slice A): persistence mirror for native webview panes.
-- A row is the durable, readable mirror of a live native webview held in the
-- app-side BrowserRegistry. No run_id/session_id/exit_code/command — those are
-- PTY concepts. webview_label is the stable Tauri webview label used as the
-- registry key (e.g. browser:{id}).
CREATE TABLE IF NOT EXISTS "job_browsers" (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    slug TEXT NOT NULL,
    webview_label TEXT NOT NULL,
    url TEXT,
    title TEXT,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    closed_at INTEGER
);

CREATE INDEX idx_job_browsers_job_id ON job_browsers(job_id);

CREATE INDEX idx_job_browsers_project_id ON job_browsers(project_id);

CREATE UNIQUE INDEX idx_job_browsers_job_slug ON job_browsers(job_id, slug) WHERE job_id IS NOT NULL;

CREATE UNIQUE INDEX idx_job_browsers_project_slug ON job_browsers(project_id, slug) WHERE project_id IS NOT NULL AND job_id IS NULL;
