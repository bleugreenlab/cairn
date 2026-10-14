-- Enrollment credentials are shared transport plumbing. Device kind scopes every
-- lookup while preserving existing executor rows and executor-only name claims.
DROP INDEX IF EXISTS executor_enrollments_active_name;

ALTER TABLE executor_enrollment_grants RENAME TO executor_enrollment_grants_legacy;
CREATE TABLE executor_enrollment_grants (
    token_hash TEXT PRIMARY KEY NOT NULL,
    device_kind TEXT NOT NULL CHECK (device_kind IN ('executor', 'companion')),
    runner_device_id TEXT NOT NULL,
    expected_enrolled_endpoint_id TEXT,
    expected_device_id TEXT,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER,
    created_at INTEGER NOT NULL
);
INSERT INTO executor_enrollment_grants
SELECT token_hash, 'executor', runner_device_id, expected_executor_id,
       expected_device_id, expires_at, consumed_at, created_at
FROM executor_enrollment_grants_legacy;
DROP TABLE executor_enrollment_grants_legacy;

ALTER TABLE executor_enrollments RENAME TO executor_enrollments_legacy;
CREATE TABLE executor_enrollments (
    device_kind TEXT NOT NULL CHECK (device_kind IN ('executor', 'companion')),
    enrolled_endpoint_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    runner_device_id TEXT NOT NULL,
    credential_hash TEXT NOT NULL UNIQUE,
    enrolled_at INTEGER NOT NULL,
    revoked_at INTEGER,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    previous_credential_hash TEXT,
    previous_expires_at INTEGER,
    executor_name TEXT,
    PRIMARY KEY (device_kind, enrolled_endpoint_id),
    CHECK (device_kind = 'executor' OR executor_name IS NULL)
);
INSERT INTO executor_enrollments
SELECT 'executor', executor_id, device_id, runner_device_id, credential_hash,
       enrolled_at, revoked_at, updated_at, expires_at,
       previous_credential_hash, previous_expires_at, executor_name
FROM executor_enrollments_legacy;
DROP TABLE executor_enrollments_legacy;

CREATE UNIQUE INDEX executor_enrollments_active_name
    ON executor_enrollments (executor_name)
    WHERE device_kind = 'executor' AND executor_name IS NOT NULL AND revoked_at IS NULL;