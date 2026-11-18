-- lineage: private
-- cairn/6262: a platform's largest single process is no longer rise-only
-- forever. It is superseded once enough completions that built their work
-- from nothing, on source newer than the figure was measured on, each report
-- a smaller one; the platform's figure then becomes the largest of what they
-- reported (`resource_profiles::PEAK_SUPERSEDED_AFTER`).
--
-- A platform's streak is spread over its rows (one per machine context) and
-- read across them: the standing source is the newest of
-- peak_tree_committed_at_unix_ms, the count the sum of lower_from_nothing,
-- and the smaller figure the largest lower_process_peak_bytes with its name
-- and the newest lower_tree_committed_at_unix_ms. A raise or a supersession
-- clears every row's streak. Times are the committer time of the source the
-- work ran, in milliseconds; NULL where nothing dated it, which a dated
-- completion counts as older.
ALTER TABLE suite_memory_evidence ADD COLUMN peak_tree_committed_at_unix_ms INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN lower_from_nothing INTEGER NOT NULL DEFAULT 0;
ALTER TABLE suite_memory_evidence ADD COLUMN lower_process_peak_bytes INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN lower_process_peak_name TEXT;
ALTER TABLE suite_memory_evidence ADD COLUMN lower_tree_committed_at_unix_ms INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN peak_tree_committed_at_unix_ms INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN lower_from_nothing INTEGER NOT NULL DEFAULT 0;
ALTER TABLE command_resource_profiles ADD COLUMN lower_process_peak_bytes INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN lower_process_peak_name TEXT;
ALTER TABLE command_resource_profiles ADD COLUMN lower_tree_committed_at_unix_ms INTEGER;
