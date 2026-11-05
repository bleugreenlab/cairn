-- lineage: shared
-- cairn/6004: a thread's children census (`resources::thread_seed`) reads an
-- unresolved child's check verdicts per job, oldest first, and nothing indexed
-- `check_result_observations.job_id`, so it hash-joined the whole table: 4.0 s
-- cold under one pooled read on the 11-04 clone. Covering, because Turso
-- assembles the whole record to read any column and an observation's row carries
-- its output tail.
CREATE INDEX IF NOT EXISTS idx_check_result_observations_job
    ON check_result_observations (job_id, ran_at, check_name, verdict);
