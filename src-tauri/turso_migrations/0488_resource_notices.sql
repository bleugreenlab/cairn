-- lineage: private
CREATE TABLE resource_notices (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    resource_uri TEXT NOT NULL,
    author TEXT NOT NULL,
    text TEXT NOT NULL,
    refs TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL,
    expires TEXT,
    shown_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(project_id, resource_uri, author)
);
