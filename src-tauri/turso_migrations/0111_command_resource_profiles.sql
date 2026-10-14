-- Machine-local learned resource aggregates for canonical check command identities.
-- Deliberately contains no commands, environments, credentials, or filesystem paths.
CREATE TABLE command_resource_profiles (
    identity_version INTEGER NOT NULL,
    command_identity TEXT NOT NULL,
    executor_class TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    toolchain_fingerprint TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    upper_peak_rss_bytes INTEGER,
    upper_disk_delta_bytes INTEGER,
    upper_duration_ms INTEGER,
    confidence_millis INTEGER NOT NULL,
    PRIMARY KEY (
        identity_version, command_identity, executor_class,
        os, arch, toolchain_fingerprint
    )
);
