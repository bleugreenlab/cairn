-- lineage: shared
-- cairn/7322: one durable row per check request, written when the request is
-- accepted and before anything is resolved, prepared or placed, so a runner
-- restart re-drives or adopts it instead of losing it. Replaces
-- manual_check_handles, whose receipt existed only for the settled answer.
CREATE TABLE check_requests (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    suite TEXT NOT NULL,
    -- What the requester named (`--branch`), or NULL for the subject job's own head.
    revision_spec TEXT,
    -- The job whose head and project the request resolves against.
    subject_job_id TEXT,
    selector_json TEXT,
    retry INTEGER NOT NULL DEFAULT 0,
    consumer TEXT NOT NULL CHECK (consumer IN ('requested','write','turn_end','main_cadence')),
    -- The principal that asked: a node, a thread, or `operator`.
    requester_ref TEXT NOT NULL,
    requester_job_id TEXT,
    priority TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('accepted','queued','granted','running','recorded','failed','cancelled','lost')),
    -- The resolved coordinate. Set once, at acceptance for the subject job's
    -- own head and an exact revision, and when preparation runs for a named
    -- managed branch, and never moved after.
    commit_sha TEXT,
    -- The request's identity while it is open (requester, suite, revision,
    -- placement, retry and the coordinate it is about), cleared when it
    -- settles. Unique, so two identical filings racing each other get one row.
    open_key TEXT UNIQUE,
    -- The runner that drives it (CAIRN_RUNNER_ID): only that runner re-arms
    -- it after a restart, since a team replica is shared with other runners.
    driven_by TEXT,
    tree_hash TEXT,
    input_hash TEXT,
    disposition TEXT,
    context_json TEXT,
    publication_id TEXT,
    -- The physical fleet request, once submitted; joins check_lane_adoptions.
    request_id TEXT,
    attempt_id TEXT,
    executor TEXT,
    diagnostic TEXT,
    -- The latest refusal, in the declining machine's own words.
    held_by TEXT,
    held_at_ms INTEGER,
    held_count INTEGER NOT NULL DEFAULT 0,
    -- The machine the selector pins, so a machine's page finds what waits for it.
    pinned_executor TEXT,
    -- What a re-armed running row is waiting to learn before it can settle.
    waiting_to_learn TEXT,
    observation_id TEXT REFERENCES check_result_observations(id) ON DELETE CASCADE,
    outcome_json TEXT,
    evidence TEXT,
    cancel_requested_by TEXT,
    accepted_at_ms INTEGER NOT NULL,
    queued_at_ms INTEGER,
    state_since_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE INDEX idx_check_requests_project_state ON check_requests(project_id, state);
CREATE INDEX idx_check_requests_requester ON check_requests(requester_job_id, state);
CREATE INDEX idx_check_requests_subject ON check_requests(subject_job_id, state);
CREATE INDEX idx_check_requests_fleet_request ON check_requests(request_id);
CREATE INDEX idx_check_requests_pinned ON check_requests(pinned_executor, state);
CREATE INDEX idx_check_requests_observation ON check_requests(observation_id);

-- A handle carried no suite, revision or requester, so none can be re-driven.
-- A settled one keeps its answer; one that never settled ended with the runner
-- that held it in memory, which is what its old reply already said.
INSERT INTO check_requests (
    id, project_id, suite, consumer, requester_ref, priority, state,
    commit_sha, tree_hash, input_hash, disposition, context_json, publication_id,
    observation_id, outcome_json, evidence,
    accepted_at_ms, state_since_ms, updated_ms
)
SELECT handle, project_id, '', 'requested', 'unknown', 'review_check',
       CASE
           WHEN response_json IS NOT NULL AND json_valid(response_json)
                AND json_extract(response_json, '$.state') = 'complete' THEN 'recorded'
           WHEN response_json IS NULL
                AND (observation_id IS NOT NULL OR publication_id IS NOT NULL) THEN 'recorded'
           ELSE 'failed'
       END,
       commit_sha, tree_hash, input_hash, disposition, context_json, publication_id,
       observation_id,
       CASE
           WHEN response_json IS NOT NULL THEN response_json
           WHEN observation_id IS NULL AND publication_id IS NULL
               THEN '{"state":"failed","error":"manual check was interrupted before its terminal answer was persisted; no recorded answer is available for this handle"}'
       END,
       CASE
           WHEN response_json IS NULL AND observation_id IS NULL AND publication_id IS NULL
               THEN 'interrupted before check requests were durable'
       END,
       created_at, created_at, created_at
  FROM manual_check_handles;

DROP INDEX idx_manual_check_handles_project;
DROP INDEX idx_manual_check_handles_observation;
DROP TABLE manual_check_handles;
