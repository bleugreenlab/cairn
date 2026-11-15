-- lineage: shared
-- Focus and open-PR refusals share the durable job hold and its claim.
-- Widen the CHECK by rebuilding; no table references these hold rows.
CREATE TABLE provider_limit_holds_new (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id) ON DELETE CASCADE,
    backend TEXT NOT NULL,
    account_id TEXT,
    -- Unix seconds; NULL means no known automatic resolution boundary.
    retry_not_before INTEGER,
    boundary_source TEXT NOT NULL
        CHECK (boundary_source IN ('rate_limit_event', 'result_text', 'account_switch',
                                   'credits_depleted', 'unknown', 'supply_reset', 'recheck',
                                   'fleet_slot', 'thread_focus', 'thread_open_prs')),
    observed_text TEXT,
    observed_count INTEGER NOT NULL DEFAULT 1,
    launch_attempts INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'resolved', 'cancelled', 'superseded')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    resolved_at INTEGER,
    resume_gate TEXT,
    boundary_account_id TEXT,
    kind TEXT NOT NULL DEFAULT 'yield' CHECK (kind IN ('yield', 'launch', 'resume')),
    subject TEXT,
    resume_json TEXT,
    CHECK (kind <> 'yield' OR (run_id IS NOT NULL AND turn_id IS NOT NULL)),
    CHECK (kind <> 'resume' OR turn_id IS NOT NULL)
);

INSERT INTO provider_limit_holds_new (
    id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source,
    observed_text, observed_count, launch_attempts, state, created_at, updated_at,
    resolved_at, resume_gate, boundary_account_id, kind, subject
)
SELECT
    id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source,
    observed_text, observed_count, launch_attempts, state, created_at, updated_at,
    resolved_at, resume_gate, boundary_account_id, kind, subject
FROM provider_limit_holds;

DROP TABLE provider_limit_holds;
ALTER TABLE provider_limit_holds_new RENAME TO provider_limit_holds;

CREATE UNIQUE INDEX idx_provider_limit_holds_one_active
    ON provider_limit_holds(job_id) WHERE state = 'active';

CREATE INDEX idx_provider_limit_holds_due
    ON provider_limit_holds(retry_not_before)
    WHERE state = 'active' AND retry_not_before IS NOT NULL;

CREATE INDEX idx_provider_limit_holds_job_id ON provider_limit_holds(job_id);
CREATE INDEX idx_provider_limit_holds_run_id ON provider_limit_holds(run_id);
CREATE INDEX idx_provider_limit_holds_turn_id ON provider_limit_holds(turn_id);

CREATE INDEX idx_provider_limit_holds_thread_boundary
    ON provider_limit_holds(boundary_source, created_at, id) WHERE state = 'active';
