-- lineage: private
-- service log: lifecycle transitions, stderr lines, and call records for each
-- project service, newest 5,000 rows per service (pruned on insert by the
-- writer in cairn-core project_services::log). Text is sanitized and capped
-- at 4 KiB before it is stored; call rows never store params or values.

CREATE TABLE service_log (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    service TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL CHECK (kind IN ('lifecycle', 'stderr', 'call')),
    text TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_service_log_project_service_created
    ON service_log(project_id, service, created_at);
