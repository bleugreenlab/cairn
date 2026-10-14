-- Cross-advance suppression for bookmarks whose reconciliation failure is
-- permanent until the bookmark state changes.
CREATE TABLE jj_reconcile_quarantines (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    bookmark TEXT NOT NULL,
    failure_kind TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    last_diagnostic TEXT,
    strike_count INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, store_path, bookmark)
);
