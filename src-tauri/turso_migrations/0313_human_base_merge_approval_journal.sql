-- lineage: shared
-- cairn/5399: the runner leg of the phone approval ceremony.
--
-- 1. merge_attempt_events admits `authorization_revoked`, the terminal state an
--    unclaimed phone authorization reaches when its approval key is retired
--    (ApprovalOutcome::AuthorizationRevoked), and `approval_required`, the gate
--    decision that an enabled policy sends this base merge to the phone (the
--    ceremony's own states follow it). A CHECK cannot be widened in
--    place, so the table is rebuilt; every column, row, index, and the
--    immutability trigger carry over unchanged. Nothing references this table,
--    so the rebuild needs no foreign-key toggle.
--
-- 2. human_base_merge_policy_events journals each operator act that enables,
--    disables, or re-points a project's provably-human base-merge policy. It
--    lives beside `projects` so the policy row and its journal row commit in one
--    transaction. Append-only: the actor and appearance snapshot are the
--    operator who acted, and `installation_id` snapshots the selected phone's
--    installation at the moment of selection (null when disabling).

CREATE TABLE merge_attempt_events_new (
    event_id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    merge_request_id TEXT,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'not_applicable', 'policy_disabled', 'policy_unavailable', 'approval_required',
        'pending', 'denied', 'expired',
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

CREATE TABLE human_base_merge_policy_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    prior_policy_json TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    installation_id TEXT,
    actor_principal_json TEXT NOT NULL,
    appearance_snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE INDEX idx_human_base_merge_policy_events_project
ON human_base_merge_policy_events(project_id, created_at DESC);

CREATE TRIGGER human_base_merge_policy_events_immutable
BEFORE UPDATE ON human_base_merge_policy_events
BEGIN
    SELECT RAISE(ABORT, 'human base-merge policy events are immutable');
END;
