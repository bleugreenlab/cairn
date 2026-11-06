-- lineage: private
-- cairn/6089: memory fit judges the largest single process a command holds,
-- because a process tree's total grows with the build width a machine grants
-- while one compiler's working set does not.
--
-- upper_process_peak_bytes is the largest single process any completion of
-- this identity held in this machine context (resident plus swapped where the
-- executor reads swap), rise-only until the identity's version changes;
-- process_peak_name is what it was (`rustc`).
--
-- suite_memory_evidence is the same high-water mark per suite (a configured
-- check's name, else the command class, prefixed by the project), so a
-- never-seen identity of a known suite is judged on the suite's evidence. It
-- also counts out-of-memory kills of the suite per machine context at the
-- machine total they happened on: two kills at one total end fit there.
--
-- resident_kills_learned names each resident kill (one a machine reports on
-- its health, not on a result) already folded in, by machine context, kill
-- time and pid: a machine keeps re-reporting its last kills, by count rather
-- than age, and two kills named in one census share a millisecond, so neither
-- a time nor a watermark is an identity. The runner learns only kills less
-- than a day old; rows older than two days are pruned as new ones arrive, so
-- an identity outlives every replay that could count it again.
ALTER TABLE command_resource_profiles ADD COLUMN upper_process_peak_bytes INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN process_peak_name TEXT;

CREATE TABLE suite_memory_evidence (
    suite_key TEXT NOT NULL,
    executor_class TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    upper_process_peak_bytes INTEGER,
    process_peak_name TEXT,
    oom_kill_count INTEGER NOT NULL DEFAULT 0,
    oom_machine_total_bytes INTEGER,
    updated_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (suite_key, executor_class, os, arch)
);

CREATE TABLE resident_kills_learned (
    executor_class TEXT NOT NULL,
    killed_at_unix_ms INTEGER NOT NULL,
    pid INTEGER NOT NULL,
    PRIMARY KEY (executor_class, killed_at_unix_ms, pid)
);
