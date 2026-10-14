-- Canon v2 memory consolidation: node-scoped identity, no legacy fields.
-- Dev memory data is disposable; old rows are intentionally not copied because
-- pre-v2 rows may lack the non-null node identity required by the canonical
-- surface. This migration is registered with rebuild_fk_off because
-- memory_triage_issue_memories.memory_id references memories(id).

DELETE FROM memory_triage_issue_memories;

CREATE TABLE memories_new (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    project_id TEXT REFERENCES projects(id),
    content TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft','pending','claimed','promoted','discarded','deferred')),
    scope TEXT NOT NULL DEFAULT 'workspace'
        CHECK (scope IN ('project','role','workspace')),
    scope_value TEXT NOT NULL DEFAULT 'workspace',
    job_id TEXT NOT NULL REFERENCES jobs(id),
    node_seq INTEGER NOT NULL,
    promoted_commit_sha TEXT,
    reason TEXT,
    triage_decision TEXT CHECK (triage_decision IN ('promote','discard','defer')),
    deferred_scope TEXT,
    deferred_scope_value TEXT,
    provenance_uri TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

DROP TABLE memories;
ALTER TABLE memories_new RENAME TO memories;

CREATE UNIQUE INDEX idx_memories_job_node_seq ON memories(job_id, node_seq);
CREATE INDEX idx_memories_status_project_created ON memories(status, project_id, created_at);
CREATE INDEX idx_memories_pending_created ON memories(created_at) WHERE status = 'pending';
