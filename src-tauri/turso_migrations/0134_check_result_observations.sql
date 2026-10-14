-- Immutable, commit-addressable check observations and environment-safe hot reuse.
-- Legacy cache rows are retained with an empty environment and schema version zero;
-- those identities are deliberately never reusable by the observation API.

CREATE TABLE check_result_observations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    commit_sha TEXT NOT NULL,
    tree_hash TEXT NOT NULL,
    check_name TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    environment_fingerprint TEXT NOT NULL,
    exit_code INTEGER NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('passed', 'failed')),
    failure_kind TEXT,
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    reusable INTEGER NOT NULL CHECK (reusable IN (0, 1)),
    non_reusable_reason TEXT,
    parser_version INTEGER NOT NULL,
    result_schema_version INTEGER NOT NULL,
    ran_at INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    job_id TEXT,
    run_id TEXT,
    cadence TEXT NOT NULL,
    executor_id TEXT,
    executor_device_id TEXT,
    executor_connection_generation INTEGER,
    executor_slot_id TEXT,
    executor_lease_epoch INTEGER,
    executor_started_at_unix_ms INTEGER,
    executor_finished_at_unix_ms INTEGER,
    runner_build_id TEXT,
    toolchain_fingerprint TEXT,
    output_tail TEXT NOT NULL
);

CREATE INDEX idx_check_result_observations_project_commit
    ON check_result_observations(project_id, commit_sha, check_name, environment_fingerprint);

CREATE TRIGGER check_result_observations_immutable
BEFORE UPDATE ON check_result_observations
BEGIN SELECT RAISE(ABORT, 'check result observations are immutable'); END;

CREATE TABLE check_test_results (
    observation_id TEXT NOT NULL REFERENCES check_result_observations(id) ON DELETE CASCADE,
    test_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'skipped')),
    duration_ms INTEGER,
    attempt_count INTEGER,
    failure_excerpt TEXT,
    skip_reason TEXT,
    declaration_source TEXT,
    flaky INTEGER NOT NULL DEFAULT 0 CHECK (flaky IN (0, 1)),
    PRIMARY KEY (observation_id, test_id)
);

CREATE TRIGGER check_test_results_immutable
BEFORE UPDATE ON check_test_results
BEGIN SELECT RAISE(ABORT, 'check test results are immutable'); END;

CREATE TABLE check_result_commit_aliases (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    commit_sha TEXT NOT NULL,
    check_name TEXT NOT NULL,
    environment_fingerprint TEXT NOT NULL,
    result_schema_version INTEGER NOT NULL,
    source_observation_id TEXT NOT NULL REFERENCES check_result_observations(id) ON DELETE CASCADE,
    tree_hash TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('fresh', 'cached')),
    evaluated_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, commit_sha, check_name, environment_fingerprint, result_schema_version)
);

CREATE TRIGGER check_result_commit_aliases_immutable
BEFORE UPDATE ON check_result_commit_aliases
BEGIN SELECT RAISE(ABORT, 'check result commit aliases are immutable'); END;

ALTER TABLE check_result_cache RENAME TO check_result_cache_legacy;

CREATE TABLE check_result_cache (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    tree_hash TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    check_name TEXT NOT NULL,
    environment_fingerprint TEXT NOT NULL DEFAULT '',
    result_schema_version INTEGER NOT NULL DEFAULT 0,
    source_observation_id TEXT REFERENCES check_result_observations(id) ON DELETE CASCADE,
    exit_code INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    output_tail TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    ran_at INTEGER NOT NULL,
    target_results_json TEXT,
    job_id TEXT,
    cached INTEGER,
    failure_kind TEXT,
    executor_id TEXT,
    executor_device_id TEXT,
    executor_connection_generation INTEGER,
    executor_slot_id TEXT,
    executor_lease_epoch INTEGER,
    executor_started_at_unix_ms INTEGER,
    executor_finished_at_unix_ms INTEGER,
    toolchain_fingerprint TEXT,
    infra_failure_streak INTEGER NOT NULL DEFAULT 0,
    infra_escalated_at INTEGER,
    PRIMARY KEY (project_id, check_name, input_hash, environment_fingerprint, result_schema_version)
);

INSERT INTO check_result_cache (
    project_id, tree_hash, input_hash, check_name, exit_code, passed, output_tail,
    duration_ms, ran_at, target_results_json, job_id, cached, failure_kind,
    executor_id, executor_device_id, executor_connection_generation,
    executor_slot_id, executor_lease_epoch, executor_started_at_unix_ms,
    executor_finished_at_unix_ms, toolchain_fingerprint, infra_failure_streak,
    infra_escalated_at
)
SELECT project_id, tree_hash, input_hash, check_name, exit_code, passed, output_tail,
       duration_ms, ran_at, target_results_json, job_id, cached, failure_kind,
       executor_id, executor_device_id, executor_connection_generation,
       executor_slot_id, executor_lease_epoch, executor_started_at_unix_ms,
       executor_finished_at_unix_ms, toolchain_fingerprint, infra_failure_streak,
       infra_escalated_at
FROM check_result_cache_legacy;

DROP TABLE check_result_cache_legacy;

CREATE INDEX idx_check_result_cache_project_tree ON check_result_cache(project_id, tree_hash);
CREATE INDEX idx_check_result_cache_project_ran_at ON check_result_cache(project_id, ran_at DESC);
CREATE INDEX idx_check_result_cache_job ON check_result_cache(job_id);
CREATE INDEX idx_check_result_cache_executor_generation
    ON check_result_cache(project_id, executor_id, executor_connection_generation);
CREATE INDEX idx_check_result_cache_project_check_recency
    ON check_result_cache(project_id, check_name, ran_at DESC, tree_hash DESC);
