-- Durable, coalesced downstream jj reconciliation. The intent pins one
-- destination; item rows make bookmark progress and stable failure suppression
-- resumable across runner restarts.
CREATE TABLE jj_reconcile_intents (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    target_branch TEXT NOT NULL,
    destination_commit TEXT NOT NULL,
    trigger_sources_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'superseded')),
    lease_owner TEXT,
    lease_expires_at INTEGER,
    last_diagnostic TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(project_id, store_path, target_branch, destination_commit)
);

CREATE INDEX idx_jj_reconcile_intents_claim
    ON jj_reconcile_intents(status, updated_at);

CREATE TABLE jj_reconcile_items (
    intent_id TEXT NOT NULL REFERENCES jj_reconcile_intents(id) ON DELETE CASCADE,
    bookmark TEXT NOT NULL,
    workspace_path TEXT NOT NULL,
    observed_tip TEXT,
    workspace_lineage TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'graph_moved', 'completed', 'suppressed')),
    failure_kind TEXT,
    outcome_kind TEXT,
    suppression_fingerprint TEXT,
    last_diagnostic TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    notification_sent INTEGER NOT NULL DEFAULT 0 CHECK (notification_sent IN (0, 1)),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(intent_id, bookmark)
);

CREATE INDEX idx_jj_reconcile_items_resume
    ON jj_reconcile_items(intent_id, status, updated_at);
