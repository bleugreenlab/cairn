CREATE TABLE runtime_admission_requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    workspace_key TEXT NOT NULL,
    resource_lane TEXT NOT NULL,
    launch_key TEXT NOT NULL,
    claim_json TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority IN (0, 1)),
    enqueue_seq INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','leased','starting','active','released','cancelled','failed')),
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0,
    lease_expires_at INTEGER,
    failure_detail TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (workspace_key, launch_key),
    UNIQUE (workspace_key, resource_lane, enqueue_seq)
);

CREATE INDEX idx_runtime_admission_queue
ON runtime_admission_requests(workspace_key, resource_lane, state, priority DESC, enqueue_seq);

CREATE INDEX idx_runtime_admission_lease_expiry
ON runtime_admission_requests(state, lease_expires_at)
WHERE state IN ('leased','starting','active');

CREATE TABLE runtime_admission_cursors (
    workspace_key TEXT NOT NULL,
    resource_lane TEXT NOT NULL,
    urgent_streak INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (workspace_key, resource_lane)
);
