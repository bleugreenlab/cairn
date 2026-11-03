-- lineage: shared
-- cairn/5954: a pending base-merge approval has no time-based expiry
-- (tenet #1385). A newer request for the same merge supersedes an open one, and
-- the runner journals that as `superseded` (ApprovalOutcome::Superseded), so
-- merge_attempt_events admits it. A CHECK cannot be widened in place, so the
-- table is rebuilt exactly as 0313 rebuilt it; every column, row, index, and the
-- immutability trigger carry over unchanged. Nothing references this table, so
-- the rebuild needs no foreign-key toggle.
--
-- `expired` stays admitted only because the journal is append-only history:
-- attempts the old five-minute clock ended were journaled with it, and those
-- rows must survive the copy. No runner writes it any more.

CREATE TABLE merge_attempt_events_new (
    event_id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    merge_request_id TEXT,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'not_applicable', 'policy_disabled', 'policy_unavailable', 'approval_required',
        'pending', 'denied', 'superseded', 'expired',
        'stale', 'consumed', 'invalid_signature', 'wrong_audience', 'authorization_revoked',
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

INSERT INTO merge_attempt_events_new (
    event_id, attempt_id, sequence, project_id, merge_request_id, job_id, state,
    target_ref, source_ref, intent_digest_sha256, challenge_id, approval_id, policy_json,
    initiating_principal_json, initiating_appearance_json, authority_decision_json,
    human_principal_json, human_appearance_json, verification_json, detail, created_at
)
SELECT
    event_id, attempt_id, sequence, project_id, merge_request_id, job_id, state,
    target_ref, source_ref, intent_digest_sha256, challenge_id, approval_id, policy_json,
    initiating_principal_json, initiating_appearance_json, authority_decision_json,
    human_principal_json, human_appearance_json, verification_json, detail, created_at
FROM merge_attempt_events;

DROP TABLE merge_attempt_events;
ALTER TABLE merge_attempt_events_new RENAME TO merge_attempt_events;

CREATE INDEX idx_merge_attempt_events_attempt
ON merge_attempt_events(attempt_id, sequence);

CREATE INDEX idx_merge_attempt_events_project_created
ON merge_attempt_events(project_id, created_at DESC);

CREATE TRIGGER merge_attempt_events_immutable
BEFORE UPDATE ON merge_attempt_events
BEGIN
    SELECT RAISE(ABORT, 'merge attempt events are immutable');
END;
