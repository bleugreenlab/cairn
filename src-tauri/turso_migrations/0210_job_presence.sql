CREATE TABLE IF NOT EXISTS job_presence (
    job_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    issue_number INTEGER,
    role TEXT,
    agent_config_id TEXT,
    verb TEXT NOT NULL,
    file_paths TEXT NOT NULL,
    ts_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_presence_project_recency
    ON job_presence(project_id, ts_ms DESC);
