-- lineage: private
-- cairn/5450: the call log of Cairn Pages. One row per bridged call a page's
-- open frame made on this install (a read, a subscription, a diagnostic, a
-- blocked navigation), with the page version, the viewer who had it open, the
-- targets, and the outcome. Private: it records what this install's viewers
-- opened and read. Pruned on insert to the newest 2,000 rows per page.
CREATE TABLE page_calls (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    page_artifact_id TEXT NOT NULL,
    page_uri TEXT NOT NULL,
    page_version INTEGER NOT NULL,
    actor_json TEXT NOT NULL,
    verb TEXT NOT NULL CHECK (verb IN ('read', 'subscribe', 'diag', 'navigation', 'write')),
    targets_json TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'refused', 'error')),
    detail TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_page_calls_page_uri_created_at ON page_calls (page_uri, created_at);
