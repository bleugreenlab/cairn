-- Migration: pr resolution attribution

CREATE TABLE pr_resolution_attributions (
    id TEXT PRIMARY KEY NOT NULL,
    merge_request_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('merge', 'close')),
    actor_kind TEXT NOT NULL CHECK (actor_kind IN ('operator-ui', 'operator-cli', 'agent', 'internal', 'unjournaled')),
    actor_identity TEXT,
    surface TEXT NOT NULL,
    lane_snapshot TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (merge_request_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE INDEX idx_pr_resolution_attributions_mr_created
    ON pr_resolution_attributions(merge_request_id, created_at DESC);
