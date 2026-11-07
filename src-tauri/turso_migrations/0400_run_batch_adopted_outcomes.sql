-- lineage: shared
-- cairn/5536: a run batch that outlives a host restart delivers its real
-- result once, and publishes its commit.
--
-- An executor retains a finished attempt's result until the runner
-- acknowledges it, and sends it again on its next link. A runner process
-- that did not grant the attempt holds no in-memory binding for it, so the
-- result is adopted into this table when an unsettled durable row still
-- claims its batch (a pending `agent_waits` run-batch wait, or a running
-- `run_batch_detached` row), and only then acknowledged. One driver settles
-- each row exactly once: it publishes the batch's commit through the ordinary
-- publication path and delivers the composed result to the wait or the
-- detached row.
--
-- `state`:
--   received    adopted, not yet claimed by a driver.
--   publishing  a driver in `claimed_by_incarnation` is settling it. A row
--               left here by a runner process that has since ended is claimed
--               again by the next one; publication finds a landing the
--               earlier process made by its key in the store's operation log.
--   published   settled: `report` holds the composed result and
--               `published_commit` the commit it landed, if any.
--   delivered   the result reached the wait or the detached row.
-- `machine` is the executor's public name, so the batch's lifecycle can name
-- where it ran.
CREATE TABLE run_batch_adopted_outcomes (
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    executor_id TEXT NOT NULL,
    machine TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'received' CHECK (state IN ('received', 'publishing', 'published', 'delivered')),
    claimed_by_incarnation TEXT,
    published_commit TEXT,
    report TEXT,
    received_ms INTEGER NOT NULL,
    settled_ms INTEGER,
    PRIMARY KEY (request_id, attempt_id)
);

CREATE INDEX idx_run_batch_adopted_outcomes_open
ON run_batch_adopted_outcomes(request_id)
WHERE state <> 'delivered';

CREATE INDEX idx_run_batch_adopted_outcomes_job ON run_batch_adopted_outcomes(job_id);

-- The run-batch waits still parked, which every adoption and every sweep of
-- this runner reads; the predicate is the one those reads spell.
CREATE INDEX idx_agent_waits_pending_run_batch
ON agent_waits(created_at)
WHERE state = 'pending' AND kind = 'run_batch';

-- What settles and publishes a detached batch from an adopted result: the
-- data half of the batch's settlement, as the call that detached it held it.
ALTER TABLE run_batch_detached ADD COLUMN settlement_json TEXT;
