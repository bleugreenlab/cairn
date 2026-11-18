-- lineage: private
-- cairn/7332 (cairn/7303 step 3): a compile runs at its machine's one compile
-- slot width, so a suite's figure on a machine is the process tree it was
-- measured to hold there at that width, and no per-width model is needed.
--   slot_jobs             the build jobs the runs that taught the figure were
--                         granted: the machine's slot width
--   slot_tree_peak_bytes  the tree peak those runs reached, rising at once and
--                         decaying slowly within one width
-- A run at another width replaces both only when it ran cold, and the reader
-- uses them only while slot_jobs is the machine's current slot width, so a
-- figure from another width is never read as current. NULL until a run here
-- was measured at a known width.
--
-- The widest-width and marginal columns (0467) priced elastic widths, and the
-- per-job census (0449) priced a width from per-process figures; nothing reads
-- them once every compile here runs at one width.
ALTER TABLE suite_memory_evidence ADD COLUMN slot_jobs INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN slot_tree_peak_bytes INTEGER;
ALTER TABLE suite_memory_evidence DROP COLUMN widest_census_jobs;
ALTER TABLE suite_memory_evidence DROP COLUMN widest_tree_peak_bytes;
ALTER TABLE suite_memory_evidence DROP COLUMN upper_job_marginal_bytes;
ALTER TABLE suite_memory_evidence DROP COLUMN marginal_jobs;
ALTER TABLE suite_memory_evidence DROP COLUMN marginal_tree_peak_bytes;
ALTER TABLE suite_memory_evidence DROP COLUMN upper_job_largest_bytes;
ALTER TABLE suite_memory_evidence DROP COLUMN upper_job_typical_bytes;
ALTER TABLE suite_memory_evidence DROP COLUMN job_census_samples;
