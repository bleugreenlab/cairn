-- Repurpose memories as an intake/evidence ledger for canonization.
-- New captures default to pending triage and carry precise turn-URI provenance.

CREATE TABLE memories_new (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT REFERENCES projects(id),
    content TEXT NOT NULL,
    when_to_use TEXT,
    confidence TEXT NOT NULL DEFAULT 'tentative',
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'handled', 'discarded')),
    provenance_uri TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    surfaced_count INTEGER NOT NULL DEFAULT 0,
    last_surfaced_at INTEGER,
    active INTEGER NOT NULL DEFAULT 1
);

INSERT INTO memories_new (
    id, project_id, content, when_to_use, confidence, status, provenance_uri,
    created_at, updated_at, surfaced_count, last_surfaced_at, active
)
SELECT
    id, project_id, content, when_to_use, confidence, 'pending', NULL,
    created_at, updated_at, surfaced_count, last_surfaced_at, active
FROM memories;

DROP TABLE memories;
ALTER TABLE memories_new RENAME TO memories;

CREATE INDEX idx_memories_active_project ON memories(active, project_id);
CREATE INDEX idx_memories_status_project_created ON memories(status, project_id, created_at);
CREATE INDEX idx_memories_pending_created ON memories(created_at) WHERE status = 'pending';

-- Intake should not surface to live sessions. Remove legacy memory corpus entries;
-- create/update paths now keep them absent.
DELETE FROM resource_embeddings
WHERE uri LIKE 'cairn://memories/%'
   OR (uri LIKE 'cairn://p/%/memories/%');
