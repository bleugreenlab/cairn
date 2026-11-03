-- lineage: private
-- Machine services (cairn/5932): a pack-owned service runs one resident per
-- machine the fleet row selects it on, and belongs to no project. Its log rows
-- carry no project_id and are keyed by owner_thread = 'pack:<pack>@<machine>'
-- (cairn-core project_services::ServiceOwner::key). The table is rebuilt with
-- a nullable project_id; project and thread rows keep theirs and their ids.
-- Nothing references service_log, so the rebuild runs with keys as they are.

CREATE TABLE service_log_next (
    id INTEGER PRIMARY KEY,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    service TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL CHECK (kind IN ('lifecycle', 'stderr', 'call')),
    text TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    owner_thread TEXT NOT NULL DEFAULT ''
);

INSERT INTO service_log_next
    (id, project_id, service, generation, kind, text, detail_json, created_at, owner_thread)
SELECT id, project_id, service, generation, kind, text, detail_json, created_at, owner_thread
FROM service_log;

DROP TABLE service_log;
ALTER TABLE service_log_next RENAME TO service_log;

CREATE INDEX idx_service_log_project_service_created
    ON service_log(project_id, service, created_at);
