-- lineage: private
-- Keep each causal observation intact; historical aggregate maxima are not events.
ALTER TABLE command_resource_profiles ADD COLUMN oom_event_json TEXT;
ALTER TABLE suite_memory_evidence ADD COLUMN oom_event_json TEXT;
-- A clean observation remains a tombstone against delayed/replayed kills.
ALTER TABLE suite_memory_evidence ADD COLUMN oom_policy_observed_at_unix_ms INTEGER NOT NULL DEFAULT 0;

CREATE TABLE memory_kill_events (
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('command', 'suite')),
    scope_key TEXT NOT NULL,
    executor_class TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    toolchain_fingerprint TEXT NOT NULL,
    killed_at_unix_ms INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (scope_kind, scope_key, executor_class, os, arch,
                 toolchain_fingerprint, killed_at_unix_ms, event_json)
);

-- Start-time occupancy cannot prove isolation when the kernel killed a process.
-- Retire only the unsupported host-size inference, retaining victim maxima,
-- kill timestamps and suite counters. No exact legacy event association exists
-- in these aggregates, so their causal JSON remains NULL (bound unknown).
UPDATE command_resource_profiles SET oom_alone_machine_bytes = NULL
WHERE oom_alone_machine_bytes IS NOT NULL;
