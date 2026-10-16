-- The yielded turn remains unfinished until its provider boundary is resolved.
CREATE TABLE provider_limit_holds (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
    backend TEXT NOT NULL,
    account_id TEXT,
    -- Unix seconds; NULL means no known automatic resolution boundary.
    retry_not_before INTEGER,
    -- account_switch: the session was repinned to another eligible account and
    -- the work resumes on it now. credits_depleted: no replacement, and credits
    -- do not refill on an observable clock.
    boundary_source TEXT NOT NULL
        CHECK (boundary_source IN ('rate_limit_event', 'result_text', 'account_switch',
                                   'credits_depleted', 'unknown')),
    observed_text TEXT,
    observed_count INTEGER NOT NULL DEFAULT 1,
    launch_attempts INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'resolved', 'cancelled', 'superseded')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    resolved_at INTEGER
);

CREATE UNIQUE INDEX idx_provider_limit_holds_one_active
    ON provider_limit_holds(job_id) WHERE state = 'active';

CREATE INDEX idx_provider_limit_holds_due
    ON provider_limit_holds(retry_not_before)
    WHERE state = 'active' AND retry_not_before IS NOT NULL;
