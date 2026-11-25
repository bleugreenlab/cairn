-- lineage: shared
-- cairn/7546: the run queue is the one admission authority. A job the queue
-- holds below its running line rests on a `queue` hold; the thread focus,
-- open-PR and fleet-slot boundaries are gone, and so is the hold queue key
-- that ordered them. Rebuild to change the CHECK. An active hold on a removed
-- boundary still owns its node (a yielded head, a pending launch, a resting
-- continuation), so it becomes a queue hold, which the queue releases when it
-- admits the execution; settled rows on removed boundaries are dropped.
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
                                   'queue')),
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
    launch_failures_json TEXT NOT NULL DEFAULT '[]',
    CHECK (kind <> 'yield' OR (run_id IS NOT NULL AND turn_id IS NOT NULL)),
    CHECK (kind <> 'resume' OR turn_id IS NOT NULL)
);

INSERT INTO provider_limit_holds_new (
    id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source,
    observed_text, observed_count, launch_attempts, state, created_at, updated_at,
    resolved_at, resume_gate, boundary_account_id, kind, subject, resume_json,
    launch_failures_json
)
SELECT
    id, job_id, run_id, turn_id, backend, account_id,
    CASE WHEN boundary_source IN ('fleet_slot', 'thread_focus', 'thread_open_prs')
         THEN NULL ELSE retry_not_before END,
    CASE WHEN boundary_source IN ('fleet_slot', 'thread_focus', 'thread_open_prs')
         THEN 'queue' ELSE boundary_source END,
    observed_text, observed_count, launch_attempts, state, created_at, updated_at,
    resolved_at, resume_gate, boundary_account_id, kind, subject, resume_json,
    launch_failures_json
FROM provider_limit_holds
WHERE state = 'active'
   OR boundary_source NOT IN ('fleet_slot', 'thread_focus', 'thread_open_prs');

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

-- The queue releases an execution's queue holds by boundary.
CREATE INDEX idx_provider_limit_holds_boundary
    ON provider_limit_holds(boundary_source, created_at, id) WHERE state = 'active';

-- Probe resolved queue-resume claims retaining input, then seek each job's head.
CREATE INDEX idx_provider_limit_holds_resolved_queue_resume
    ON provider_limit_holds(job_id, turn_id, id)
    WHERE state = 'resolved' AND kind = 'resume' AND resume_json IS NOT NULL
      AND boundary_source = 'queue';
