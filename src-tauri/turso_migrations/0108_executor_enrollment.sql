CREATE TABLE executor_enrollment_grants (
    token_hash TEXT PRIMARY KEY NOT NULL,
    runner_device_id TEXT NOT NULL,
    expected_executor_id TEXT,
    expected_device_id TEXT,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER,
    created_at INTEGER NOT NULL
);

CREATE TABLE executor_enrollments (
    executor_id TEXT PRIMARY KEY NOT NULL,
    device_id TEXT NOT NULL,
    runner_device_id TEXT NOT NULL,
    credential_hash TEXT NOT NULL UNIQUE,
    enrolled_at INTEGER NOT NULL,
    revoked_at INTEGER,
    updated_at INTEGER NOT NULL
);
