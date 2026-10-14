CREATE TABLE executor_registry_new (
    device_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    logical_cores INTEGER NOT NULL,
    toolchains TEXT NOT NULL DEFAULT '[]',
    projects_served TEXT NOT NULL DEFAULT '[]',
    current_load INTEGER NOT NULL,
    warm_commits TEXT NOT NULL DEFAULT '[]',
    connection_generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    last_seen INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (device_id, executor_id)
);
INSERT INTO executor_registry_new (device_id, executor_id, display_name, os, arch, logical_cores, toolchains, projects_served, current_load, warm_commits, connection_generation, status, last_seen, expires_at, updated_at)
SELECT device_id, executor_id, display_name, os, arch, logical_cores, toolchains, projects_served, current_load, warm_commits, connection_generation, status, last_seen, expires_at, updated_at FROM executor_registry;
DROP TABLE executor_registry;
ALTER TABLE executor_registry_new RENAME TO executor_registry;
