CREATE TABLE IF NOT EXISTS labels (
    id           TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    color        TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_labels_workspace_name ON labels(workspace_id, name COLLATE NOCASE);

CREATE TABLE IF NOT EXISTS issue_labels (
    issue_id   TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    label_id   TEXT NOT NULL REFERENCES labels(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, label_id)
);

CREATE INDEX idx_issue_labels_label_id ON issue_labels(label_id);
