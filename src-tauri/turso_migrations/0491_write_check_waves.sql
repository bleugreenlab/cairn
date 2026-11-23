-- lineage: shared
-- Wave coordination only. Per-suite execution remains in check_requests.
CREATE TABLE write_check_waves (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    tool_use_id TEXT NOT NULL,
    driven_by TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    summary TEXT,
    convergence_json TEXT,
    rekeys_json TEXT NOT NULL DEFAULT '{}',
    verdicts_json TEXT,
    completed INTEGER NOT NULL DEFAULT 0,
    delivered INTEGER NOT NULL DEFAULT 0,
    superseded INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(job_id, tool_use_id)
);
CREATE INDEX write_check_waves_rearm ON write_check_waves(driven_by, delivered, created_at_ms);
CREATE INDEX write_check_waves_project ON write_check_waves(project_id);
CREATE INDEX write_check_waves_run ON write_check_waves(run_id);
