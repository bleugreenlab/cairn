-- Canon v2 memory data-model spine: explicit scope, node ids, promotion links,
-- and the expanded status lattice.

CREATE TABLE memories_new (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT REFERENCES projects(id),
    content TEXT NOT NULL,
    confidence TEXT NOT NULL DEFAULT 'tentative',
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'pending', 'claimed', 'promoted', 'discarded', 'deferred')),
    scope TEXT NOT NULL DEFAULT 'workspace' CHECK (scope IN ('project', 'role', 'workspace')),
    scope_value TEXT NOT NULL DEFAULT 'workspace',
    job_id TEXT REFERENCES jobs(id),
    node_seq INTEGER,
    promoted_commit_sha TEXT,
    reason TEXT,
    provenance_uri TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    surfaced_count INTEGER NOT NULL DEFAULT 0,
    last_surfaced_at INTEGER,
    active INTEGER NOT NULL DEFAULT 1
);

INSERT INTO memories_new (
    id, project_id, content, confidence, status, scope, scope_value,
    job_id, node_seq, promoted_commit_sha, reason, provenance_uri,
    created_at, updated_at, surfaced_count, last_surfaced_at, active
)
SELECT
    id,
    COALESCE(project_id, 'workspace'),
    content,
    confidence,
    CASE status WHEN 'handled' THEN 'claimed' ELSE status END,
    CASE WHEN project_id = 'workspace' OR project_id IS NULL THEN 'workspace' ELSE 'project' END,
    CASE WHEN project_id = 'workspace' OR project_id IS NULL THEN 'workspace' ELSE project_id END,
    NULL,
    NULL,
    NULL,
    NULL,
    provenance_uri,
    created_at,
    updated_at,
    surfaced_count,
    last_surfaced_at,
    active
FROM memories;

DROP TABLE memories;
ALTER TABLE memories_new RENAME TO memories;

CREATE INDEX idx_memories_active_project ON memories(active, project_id);
CREATE INDEX idx_memories_status_project_created ON memories(status, project_id, created_at);
CREATE INDEX idx_memories_pending_created ON memories(created_at) WHERE status = 'pending';
CREATE UNIQUE INDEX idx_memories_job_node_seq ON memories(job_id, node_seq) WHERE job_id IS NOT NULL;
