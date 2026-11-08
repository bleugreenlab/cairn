-- lineage: shared
-- cairn/6426: a detached batch that no machine has been handed yet is
-- `parked`, not `running`, and says what it waits for.
--
-- A Codex nested call never parks, so its batch is recorded detached from the
-- inline window on. A batch that was still waiting to be placed then (a
-- committing batch waiting for its job's home while another batch runs
-- there) read `running`, named nothing, and held its branch's base advance
-- for hours before it had started.
--
-- `state`:
--   parked   not yet handed to a machine; `waiting_on` says for what. It holds
--            no branch (`holds_branch` is 0) until it is admitted, when it
--            becomes `running`.
--   running, settled, lost  as in 0375.
-- `waiting_on`: what a parked batch waits for, in a sentence; null otherwise.
CREATE TABLE run_batch_detached_next (
    request_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    label TEXT,
    identity TEXT,
    commits INTEGER NOT NULL DEFAULT 0,
    holds_branch INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'running' CHECK (state IN ('parked', 'running', 'settled', 'lost')),
    verdict TEXT,
    output TEXT,
    commit_id TEXT,
    reason TEXT NOT NULL,
    lost_evidence TEXT,
    created_ms INTEGER NOT NULL,
    settled_ms INTEGER,
    notified_ms INTEGER,
    settlement_json TEXT,
    waiting_on TEXT
);

INSERT INTO run_batch_detached_next
    (request_id, job_id, run_id, label, identity, commits, holds_branch, state,
     verdict, output, commit_id, reason, lost_evidence, created_ms, settled_ms,
     notified_ms, settlement_json, waiting_on)
SELECT request_id, job_id, run_id, label, identity, commits, holds_branch, state,
       verdict, output, commit_id, reason, lost_evidence, created_ms, settled_ms,
       notified_ms, settlement_json, NULL
FROM run_batch_detached;

DROP TABLE run_batch_detached;
ALTER TABLE run_batch_detached_next RENAME TO run_batch_detached;

CREATE INDEX idx_run_batch_detached_job ON run_batch_detached(job_id, created_ms);

CREATE INDEX idx_run_batch_detached_running
ON run_batch_detached(job_id)
WHERE state IN ('parked', 'running');
