-- lineage: shared
-- cairn/6227: one durable row per run-batch request, written when the call
-- arrives, so `cairn://p/<project>/requests/<id>` reads a batch's whole
-- lifecycle from the store by any id it was called by.
--
-- Before this a batch that answered inline, was refused, or died with its
-- call left nothing but a log line: `run_batch_detached` and `agent_waits`
-- hold only batches that detached or parked. This row is written for every
-- batch at receipt, gains what the batch became and where it ran, and gets
-- exactly one end.
--
-- `end`:
--   answered   the call carried the batch's result.
--   resumed    the call parked, and the result resumed its turn.
--   detached   the call answered with a handle; `run_batch_detached` holds
--              the rest of the batch's life.
--   refused    the batch never ran; `end_detail` says why and names the
--              candidates.
--   cancelled  `end_detail` names who cancelled it.
--   dropped    the call's future was dropped before it answered;
--              `end_detail` is the stage the batch had reached.
-- NULL with an older `incarnation` than the runner's: the runner process
-- ended with the call still open.
CREATE TABLE run_batch_requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    tool_use_id TEXT,
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    session_id TEXT,
    turn_id TEXT,
    project_id TEXT NOT NULL,
    commits INTEGER NOT NULL DEFAULT 0,
    received_ms INTEGER NOT NULL,
    -- The runner process that received it: the string `execution_queue`
    -- journals as `runner_incarnation`.
    incarnation TEXT,
    -- What the batch became once its items resolved.
    label TEXT,
    item_count INTEGER,
    sequential INTEGER,
    declared_timeout_ms INTEGER,
    verdict_branch TEXT,
    cwd TEXT,
    scratch TEXT,
    selector_json TEXT,
    -- Where it ran, sampled from the machine's reports while it ran.
    attempt_id TEXT,
    executor_id TEXT,
    machine TEXT,
    generation INTEGER,
    cell_id TEXT,
    pid INTEGER,
    started_ms INTEGER,
    peak_cpu_milli INTEGER,
    peak_resident_bytes INTEGER,
    last_progress_ms INTEGER,
    output_bytes INTEGER,
    ended_ms INTEGER,
    "end" TEXT CHECK ("end" IN ('answered', 'resumed', 'detached', 'refused', 'cancelled', 'dropped')),
    end_detail TEXT
);

CREATE INDEX idx_run_batch_requests_tool_use ON run_batch_requests(tool_use_id);
CREATE INDEX idx_run_batch_requests_run ON run_batch_requests(run_id, received_ms);
CREATE INDEX idx_run_batch_requests_job ON run_batch_requests(job_id, received_ms);

-- A tool call id resolves to the wait it parked under, for batches that
-- predate the ledger. The existing tool-use index covers only open waits.
CREATE INDEX idx_agent_waits_tool_use ON agent_waits(tool_use_id);

-- A request id resolves to the wait that carries it in its condition, for a
-- batch whose only durable trace is that wait.
CREATE INDEX idx_agent_waits_request ON agent_waits(json_extract(condition_json, '$.request_id'));

-- An adopted result's attempt id resolves to its request.
CREATE INDEX idx_run_batch_adopted_outcomes_attempt ON run_batch_adopted_outcomes(attempt_id);
