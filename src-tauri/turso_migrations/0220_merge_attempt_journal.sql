CREATE TABLE merge_attempt_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    merge_request_id TEXT,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'not_applicable', 'policy_disabled', 'policy_unavailable', 'pending', 'denied', 'expired',
        'stale', 'consumed', 'invalid_signature', 'wrong_audience',
        'authorized_not_executed', 'executing', 'succeeded', 'failed',
        'indeterminate', 'violation'
    )),
    target_ref TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    intent_digest_sha256 TEXT,
    challenge_id TEXT,
    approval_id TEXT,
    policy_json TEXT NOT NULL,
    initiating_principal_json TEXT,
    initiating_appearance_json TEXT,
    authority_decision_json TEXT,
    human_principal_json TEXT,
    human_appearance_json TEXT,
    verification_json TEXT,
    detail TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE (attempt_id, sequence),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (merge_request_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE INDEX idx_merge_attempt_events_attempt
ON merge_attempt_events(attempt_id, sequence);

CREATE INDEX idx_merge_attempt_events_project_created
ON merge_attempt_events(project_id, created_at DESC);

CREATE TRIGGER merge_attempt_events_immutable
BEFORE UPDATE ON merge_attempt_events
BEGIN
    SELECT RAISE(ABORT, 'merge attempt events are immutable');
END;