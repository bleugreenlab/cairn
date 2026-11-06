-- lineage: shared
-- cairn/6145: a `run` batch that outlives its call without a suspension.
--
-- When a call cannot suspend (its carrier cannot resume it in place, or the
-- project store was busy past the answer deadline), the call answers with a
-- handle and the batch keeps running instead of being cancelled. This row is
-- the batch's durable life from that moment: written before the call
-- answers, settled by the batch's own result, and read by the node's
-- `cairn:~/batches/<request_id>` handle, the job's result notice, the reissue
-- refusal, and the replay mutation boundary.
--
-- It lives beside `agent_waits` in the run's owning database because the
-- mutation boundary reads both in one bracket.
--
-- `state`:
--   running  the batch has not reported. A runner restart does not change it:
--            a remote batch may still be running, and only its report or
--            evidence that it can no longer report settles it.
--   settled  its result arrived; `verdict`, `output` and `commit_id` hold it.
--   lost     evidence (`lost_evidence`) shows it can no longer report.
-- `holds_branch`: whether the batch can write the branch or runs in the
-- node's residence. A verdict-only batch or a host batch never holds it.
-- `notified_ms`: when the result notice was queued for the job.
CREATE TABLE run_batch_detached (
    request_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    label TEXT,
    identity TEXT,
    commits INTEGER NOT NULL DEFAULT 0,
    holds_branch INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'settled', 'lost')),
    verdict TEXT,
    output TEXT,
    commit_id TEXT,
    reason TEXT NOT NULL,
    lost_evidence TEXT,
    created_ms INTEGER NOT NULL,
    settled_ms INTEGER,
    notified_ms INTEGER
);

CREATE INDEX idx_run_batch_detached_job ON run_batch_detached(job_id, created_ms);

CREATE INDEX idx_run_batch_detached_running
ON run_batch_detached(job_id)
WHERE state = 'running';
