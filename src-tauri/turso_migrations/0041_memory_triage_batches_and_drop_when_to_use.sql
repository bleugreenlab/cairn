-- Finish memory intake ledger shape: memories no longer carry legacy
-- applicability prose, and triage issues store their claimed batch explicitly.

CREATE TABLE memories_new (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT REFERENCES projects(id),
    content TEXT NOT NULL,
    confidence TEXT NOT NULL DEFAULT 'tentative',
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'handled', 'promoted', 'discarded')),
    provenance_uri TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    surfaced_count INTEGER NOT NULL DEFAULT 0,
    last_surfaced_at INTEGER,
    active INTEGER NOT NULL DEFAULT 1
);

INSERT INTO memories_new (
    id, project_id, content, confidence, status, provenance_uri,
    created_at, updated_at, surfaced_count, last_surfaced_at, active
)
SELECT
    id, project_id, content, confidence, status, provenance_uri,
    created_at, updated_at, surfaced_count, last_surfaced_at, active
FROM memories;

DROP TABLE memories;
ALTER TABLE memories_new RENAME TO memories;

CREATE INDEX idx_memories_active_project ON memories(active, project_id);
CREATE INDEX idx_memories_status_project_created ON memories(status, project_id, created_at);
CREATE INDEX idx_memories_pending_created ON memories(created_at) WHERE status = 'pending';

CREATE TABLE memory_triage_issue_memories (
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
    PRIMARY KEY (issue_id, memory_id)
);

CREATE INDEX idx_memory_triage_issue_memories_memory
ON memory_triage_issue_memories(memory_id);
