-- CAIRN-3232: a turn may park N of its calls at once.
--
-- The original index bounded a job to one ACTIVE ROW, which collapsed two
-- different statements into one constraint: "a job parks one turn at a time"
-- (a real invariant, enforced by the job running one turn at a time) and "a
-- turn parks one call" (an accident of expressing that invariant on `job_id`
-- alone). A provider routinely emits several tool calls in one assistant event,
-- so the second statement cost every concurrent long `run` batch but the first
-- its suspension. Re-key the bound to the call.
--
-- Which turn a job parks is deliberately NOT an index question anymore. A row
-- left behind by a turn the job has already moved past is superseded in
-- `durable_suspend::supersede_abandoned`, which is where the evidence for
-- abandonment lives; an index can only see states, not which turn is current.
--
-- The bound this claims is real rather than nominal because `tool_use_id` is
-- declared NOT NULL (0115): SQL treats NULLs as distinct within a unique index,
-- so a nullable column here would have admitted unbounded active rows per job.
-- Every active row carries a claimed provider call id, and the sole insert site
-- binds it from a non-optional `String`.
DROP INDEX IF EXISTS idx_agent_waits_one_pending_job;
CREATE UNIQUE INDEX idx_agent_waits_one_pending_call
  ON agent_waits(job_id, tool_use_id)
  WHERE state IN ('pending', 'resolving');
