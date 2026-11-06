-- lineage: private
-- cairn/6147: the run that sealed a held delta. Republication asks the
-- publication authority whether that run's session may still publish, so a
-- delta held before its issue resolved stays refused after a reopen, which
-- continues the job in a new session and leaves the old one closed. NULL for
-- rows held before this column existed; those are judged by job and issue.
ALTER TABLE held_publications ADD COLUMN run_id TEXT;
