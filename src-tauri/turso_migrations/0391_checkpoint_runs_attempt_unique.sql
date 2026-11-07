-- lineage: shared
-- checkpoint runs: one row per attempt

-- record_checkpoint_run numbers an attempt by counting the job's rows inside its
-- write. Two concurrent records each count the same rows and insert different
-- ids, so under BEGIN CONCURRENT both committed the same attempt and the cap
-- undercounted. A key over (job_id, attempt) makes the second record conflict
-- and recount (cairn/6166; docs/database.md, "Write skew").
CREATE UNIQUE INDEX idx_checkpoint_runs_job_attempt ON checkpoint_runs (job_id, attempt);
