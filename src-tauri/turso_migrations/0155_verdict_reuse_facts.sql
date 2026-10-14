-- Queryable execution facts for project-declared verdict trust sets.
-- Legacy rows remain NULL and therefore diagnostic-only.
ALTER TABLE check_result_observations ADD COLUMN verdict_platform TEXT;
ALTER TABLE check_result_observations ADD COLUMN verdict_arch TEXT;
ALTER TABLE check_result_observations ADD COLUMN verdict_environment_hash TEXT;

ALTER TABLE check_result_cache ADD COLUMN verdict_platform TEXT;
ALTER TABLE check_result_cache ADD COLUMN verdict_arch TEXT;
ALTER TABLE check_result_cache ADD COLUMN verdict_environment_hash TEXT;

CREATE INDEX idx_check_result_observations_reusable_lookup
    ON check_result_observations(
        project_id, check_name, input_hash, result_schema_version, ran_at DESC
    );
