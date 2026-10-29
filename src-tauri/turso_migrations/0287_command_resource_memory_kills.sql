-- An out-of-memory kill of a profiled command on one machine context: the
-- victim's resident size at death is a floor on the command's demand there,
-- kept apart from upper_peak_rss_bytes because the saturation quarantine that
-- guards a process tree's peak must never discard it.
--
-- oom_alone_machine_bytes is the machine's memory when a kill happened while
-- nothing else of Cairn's ran there, and is cleared by the next success: such a
-- kill says the machine cannot hold the command at all.
ALTER TABLE command_resource_profiles ADD COLUMN oom_victim_rss_bytes INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN oom_killed_at_unix_ms INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN oom_alone_machine_bytes INTEGER;
