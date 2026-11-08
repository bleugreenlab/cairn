-- lineage: private
-- cairn/6496: a suite's out-of-memory kill ends a machine's fit for the suite
-- only when it says something about one process on that machine.
--
-- oom_kill_count now counts only kills while the victim ran alone (no command
-- co-resident at its start, no batch sibling beside it), at
-- oom_machine_total_bytes, under oom_kill_memory_brake: whether the machine's
-- memory brake existed then. Placement ignores alone kills under a brake state
-- other than the machine's current one. Rows counted before this migration
-- keep their count with a NULL brake, which reads as recorded before the brake
-- existed: none of them recorded whether the victim was alone, and every one
-- predates the brake.
--
-- The oom_crowded_* columns record kills while other commands shared the
-- machine: how many, the most commands co-resident at one, the brake state
-- and time of the latest. They are evidence for admission's owed accounting
-- and the memory brake, and never end a machine's fit.
--
-- A successful run of the suite in the context, or the operator declaring the
-- executor repaired, empties both.
ALTER TABLE suite_memory_evidence ADD COLUMN oom_kill_memory_brake INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN oom_crowded_kill_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE suite_memory_evidence ADD COLUMN oom_crowded_most_co_resident INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN oom_crowded_memory_brake INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN oom_crowded_killed_at_unix_ms INTEGER;
