-- lineage: shared
-- Plan-freshness checkpoints. A turn records the carried base its prompt's
-- freshness evidence was measured against; the job's column advances to it
-- only once that turn reaches a process, so a re-entry repeats the comparison
-- until a newly carried base has actually been delivered.
ALTER TABLE turns ADD COLUMN plan_notice_base_commit TEXT;
ALTER TABLE jobs ADD COLUMN plan_checked_base_commit TEXT;
