-- Machine-local slowdown curves learned from executions with stated start load.
CREATE TABLE command_contention_profiles (
    executor_class TEXT NOT NULL,
    compile_jobs INTEGER NOT NULL,
    light_jobs INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    recent_multiplier_millis TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (executor_class, compile_jobs, light_jobs)
);