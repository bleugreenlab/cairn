-- lineage: private
-- cairn/7244: a suite's tree as measured at width on each machine, so the
-- build-jobs model prices only the widths a machine has measured, and a wider
-- grant runs alone until it has been.
--
-- The typical job (0449) is a median crate taken with no regard to what ran
-- beside it; a wide compile holds several large rustc at once, and its tree
-- reached 54.7 GiB at 20 jobs on bginf-sp2 where largest + 19 typical priced
-- about 30. Each column is rise-only over the runs in this machine context:
--   widest_census_jobs       the most build jobs a measured run was granted
--   widest_tree_peak_bytes   the largest tree peak at that width
--   upper_job_marginal_bytes the largest (tree peak - largest job) / (jobs - 1)
--                            of a run at two or more jobs
--   marginal_jobs, marginal_tree_peak_bytes
--                            the jobs and tree peak of the run that set it
-- NULL until a run here was measured at a known width, which reads as a
-- suite measured at one job: any wider grant then runs alone.
ALTER TABLE suite_memory_evidence ADD COLUMN widest_census_jobs INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN widest_tree_peak_bytes INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN upper_job_marginal_bytes INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN marginal_jobs INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN marginal_tree_peak_bytes INTEGER;
