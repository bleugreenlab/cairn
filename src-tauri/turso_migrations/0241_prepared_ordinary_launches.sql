CREATE TABLE prepared_ordinary_launches (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES runtime_admission_requests(request_id) ON DELETE CASCADE,
    -- These are opaque reconstruction identities. The authoritative rows may
    -- live in a team replica while this queue is runner-local, so cross-table
    -- foreign keys here would reject every team launch.
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    launch_event_id TEXT NOT NULL,
    prompt TEXT NOT NULL,
    reason TEXT NOT NULL,
    session_start_json TEXT NOT NULL,
    model TEXT NOT NULL,
    agent_config_json TEXT NOT NULL,
    output_schema_json TEXT,
    runtime_capability_json TEXT NOT NULL,
    provider_account_id TEXT,
    identity_override_json TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_prepared_ordinary_launches_job
ON prepared_ordinary_launches(job_id);

CREATE INDEX idx_prepared_ordinary_launches_session_turn
ON prepared_ordinary_launches(session_id, turn_id);
