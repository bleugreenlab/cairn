-- lineage: private
-- Rebuild history belongs to this install, never a project or team replica.
DROP TABLE self_rebuild_outcomes;

CREATE TABLE rebuilds (
    id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    record_json TEXT NOT NULL,
    request_id TEXT,
    attempt_id TEXT,
    authorization_event_id TEXT,
    old_boot_id TEXT,
    new_boot_id TEXT,
    old_commit TEXT,
    new_commit TEXT
);
CREATE INDEX idx_rebuilds_recent ON rebuilds(created_at DESC, id DESC);
