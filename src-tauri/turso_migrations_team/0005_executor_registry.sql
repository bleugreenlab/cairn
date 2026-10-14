CREATE TABLE IF NOT EXISTS executor_registry (
    device_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    logical_cores INTEGER NOT NULL,
    toolchains TEXT NOT NULL DEFAULT '[]',
    projects_served TEXT NOT NULL DEFAULT '[]',
    slot_capacity INTEGER NOT NULL,
    current_load INTEGER NOT NULL,
    warm_commits TEXT NOT NULL DEFAULT '[]',
    connection_generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    last_seen INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (device_id, executor_id)
);
