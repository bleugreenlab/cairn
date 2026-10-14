-- CAIRN-1140 memory redux: replace trigger/keyword surfacing with URI + change
-- CRUD and semantic recall. No backfill — existing memories lack when_to_use and
-- can't surface; drop and recreate cleanly (sidesteps libsql DROP COLUMN).

DROP TABLE IF EXISTS memory_triggers;
DROP TABLE IF EXISTS memories;

CREATE TABLE memories (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT REFERENCES projects(id),
    content TEXT NOT NULL,
    when_to_use TEXT,                 -- embedded text (mirrors skill description)
    confidence TEXT NOT NULL DEFAULT 'tentative',
    source_issue TEXT,
    source_run_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    surfaced_count INTEGER NOT NULL DEFAULT 0,
    last_surfaced_at INTEGER,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_memories_active_project ON memories(active, project_id);
