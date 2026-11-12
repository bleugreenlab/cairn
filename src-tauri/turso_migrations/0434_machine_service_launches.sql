-- lineage: private
-- machine service launches

-- Each launch of a machine service's program (cairn/6097): a resident the
-- supervisor started on a machine, or a fleet service's stdio child the
-- runner's MCP gateway spawned on its host for a session. What ran, whether
-- it was the pack's pinned artifact or an override, who chose it, and the
-- confinement it ran under, so an approval card and the operator can cite
-- what an approved path actually ran. This runner's own record, never synced.
CREATE TABLE machine_service_launches (
    id INTEGER PRIMARY KEY,
    at_ms INTEGER NOT NULL,
    machine TEXT NOT NULL,
    service_key TEXT NOT NULL,
    path TEXT NOT NULL CHECK (path IN ('resident', 'fleet_gateway')),
    source TEXT NOT NULL CHECK (source IN ('artifact', 'override')),
    program TEXT NOT NULL,
    set_by_json TEXT,
    confinement TEXT NOT NULL CHECK (confinement IN ('unconfined', 'credential_floor')),
    session_id TEXT
);

CREATE INDEX idx_machine_service_launches_service
    ON machine_service_launches(service_key, machine, at_ms);
