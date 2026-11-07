-- lineage: shared
-- cairn/6069: one hold for a node that cannot run for want of supply, whichever
-- way it met that. `kind` says how:
--
--   yield   a provider limit landed mid-turn; the yielded head owns the wait
--           (every row before this migration).
--   launch  a cold launch was refused before any session ran: selection or
--           session routing found no account that can serve its model now.
--           It has no turn of its own, and a run only once the refusal is
--           recorded on one, so `run_id` and `turn_id` become nullable.
--   resume  a continuation was refused before spawn; `turn_id` is the refused
--           head the node rests on.
--
-- `subject` names what a launch or resume hold waits for supply of, a
-- `provider/model` or an allotment, so a held node can say so; NULL on a yield.
-- `boundary_source` admits two more readings: `supply_reset` (the soonest reset
-- a refusal declared) and `recheck` (none declared; the hold re-checks on a
-- bounded clock). `fleet_slot` marks a hold whose work supply can serve again
-- but whose fleet is full; it asks again shortly, oldest first. A CHECK cannot be widened nor NOT NULL dropped in place, so
-- the table is rebuilt the way 0313 and 0355 rebuilt a shared table: every
-- column, row and index carries over, and existing rows become `yield`. Nothing
-- references this table, so the rebuild needs no foreign-key toggle.

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
                                   'fleet_slot')),
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
    CHECK (kind <> 'yield' OR (run_id IS NOT NULL AND turn_id IS NOT NULL)),
    CHECK (kind <> 'resume' OR turn_id IS NOT NULL)
);

INSERT INTO provider_limit_holds_new (
    id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source,
    observed_text, observed_count, launch_attempts, state, created_at, updated_at,
    resolved_at, resume_gate, boundary_account_id, kind
)
SELECT
    id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source,
    observed_text, observed_count, launch_attempts, state, created_at, updated_at,
    resolved_at, resume_gate, boundary_account_id, 'yield'
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
