-- Agent jobs are durable branch/logical-head coordinates, never physical
-- checkouts. Remove the two legacy checkout ownership columns in place so Turso
-- sync triggers remain intact, then rebuild reconcile items without path-shaped
-- follower state.
ALTER TABLE jobs DROP COLUMN worktree_path;
ALTER TABLE jobs DROP COLUMN owns_ephemeral_worktree;
ALTER TABLE jj_reconcile_items RENAME TO jj_reconcile_items_legacy;

CREATE TABLE jj_reconcile_items (
    intent_id TEXT NOT NULL REFERENCES jj_reconcile_intents(id) ON DELETE CASCADE,
    bookmark TEXT NOT NULL,
    observed_tip TEXT,
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

INSERT INTO jj_reconcile_items (
    intent_id, bookmark, observed_tip, status, failure_kind, outcome_kind,
    suppression_fingerprint, last_diagnostic, attempt_count,
    notification_sent, updated_at
)
SELECT
    intent_id, bookmark, observed_tip, status, failure_kind, outcome_kind,
    suppression_fingerprint, last_diagnostic, attempt_count,
    notification_sent, updated_at
FROM jj_reconcile_items_legacy;

DROP TABLE jj_reconcile_items_legacy;

CREATE INDEX idx_jj_reconcile_items_resume
    ON jj_reconcile_items(intent_id, status, updated_at);
