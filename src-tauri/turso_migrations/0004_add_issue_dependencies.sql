CREATE TABLE IF NOT EXISTS issue_dependencies (
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    depends_on_uri TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, depends_on_uri)
);

CREATE INDEX IF NOT EXISTS idx_issue_dependencies_depends_on_uri
    ON issue_dependencies(depends_on_uri);

CREATE TABLE IF NOT EXISTS issue_embeddings (
    issue_id TEXT PRIMARY KEY NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    embedding BLOB NOT NULL,
    model_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
