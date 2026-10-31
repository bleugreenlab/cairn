-- lineage: private
-- cairn/5518: a page's bridged service call is logged in page_calls like every
-- other bridge call. Rebuild the table so its verb CHECK admits 'service';
-- every column, row, and the page/time index carry over unchanged.

CREATE TABLE page_calls_new (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    page_artifact_id TEXT NOT NULL,
    page_uri TEXT NOT NULL,
    page_version INTEGER NOT NULL,
    actor_json TEXT NOT NULL,
    verb TEXT NOT NULL CHECK (verb IN ('read', 'subscribe', 'diag', 'navigation', 'write', 'service')),
    targets_json TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'refused', 'error')),
    detail TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

INSERT INTO page_calls_new (
    id, project_id, page_artifact_id, page_uri, page_version, actor_json,
    verb, targets_json, outcome, detail, created_at
)
SELECT
    id, project_id, page_artifact_id, page_uri, page_version, actor_json,
    verb, targets_json, outcome, detail, created_at
FROM page_calls;

DROP TABLE page_calls;
ALTER TABLE page_calls_new RENAME TO page_calls;

CREATE INDEX idx_page_calls_page_uri_created_at ON page_calls (page_uri, created_at);
