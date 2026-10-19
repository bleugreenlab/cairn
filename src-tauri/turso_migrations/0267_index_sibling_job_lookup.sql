-- Index the two lookups base_advance::load_sibling_jobs makes on every base
-- advance: every merge, every external advance of a base branch, and every
-- durable retry of a parked reconcile (cairn/5119).
--
-- The statement selects the jobs cut from a base branch that are still live:
-- on an unresolved issue, and either still running or still holding an open
-- pull request (base_advance::LIVE_JOB_PREDICATE). Before this migration
-- neither half had an index to seek:
--
-- 1. merge_requests(project_id, source_branch). The open-PR test asks "does a
--    PR in this project come from this job's branch". The only index whose
--    leading column is the project was idx_mr_project_status, and the status
--    term is `NOT IN (...)`, which does not seek, so for every settled job the
--    probe read every merge request in the project and compared source_branch
--    row by row. On production (9.8k settled jobs on `main`, 3.5k merge
--    requests in the project) that was ~60 s per call, stepped on a runtime
--    worker. With both columns bound the probe is one seek: source_branch is
--    effectively unique per PR.
--
-- 2. jobs(project_id, base_branch). The outer selection had no index on either
--    column and scanned the whole jobs table. Smaller today, same growth curve.
--    The re-point after a branch merges (reparent_children_of_merged_branch)
--    selects the same population and benefits identically.
--
-- base_advance::tests::sibling_job_statement_seeks_both_lookups pins the plan
-- for the exact statement text. Turso keeps no table statistics, so a plan is
-- the only evidence an index is load-bearing rather than dead weight.
CREATE INDEX IF NOT EXISTS idx_mr_project_source_branch
    ON merge_requests(project_id, source_branch);
CREATE INDEX IF NOT EXISTS idx_jobs_project_base_branch
    ON jobs(project_id, base_branch);
