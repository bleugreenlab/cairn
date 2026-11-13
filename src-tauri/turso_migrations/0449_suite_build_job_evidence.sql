-- lineage: private
-- cairn/5685: the build jobs a command is granted at spawn are sized by what
-- its suite's build jobs have been measured to hold on the machine, instead of
-- a fixed 2.5 GiB a job.
--
-- A run's build-job processes are its tree's members that grew past 128 MiB,
-- each at its own peak. upper_job_largest_bytes is the largest such process
-- and upper_job_typical_bytes their upper median, each rise-only over the runs
-- in this machine context that ran enough job processes for a median to speak
-- for a typical job; job_census_samples counts those runs. NULL until one has,
-- which keeps the executor on its assumed reserve.
ALTER TABLE suite_memory_evidence ADD COLUMN upper_job_largest_bytes INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN upper_job_typical_bytes INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN job_census_samples INTEGER NOT NULL DEFAULT 0;
