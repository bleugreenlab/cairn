ALTER TABLE check_result_cache
ADD COLUMN row_family TEXT NOT NULL DEFAULT 'verdict'
CHECK (row_family IN ('verdict', 'infra_suppression'));

UPDATE check_result_cache
SET row_family = 'infra_suppression'
WHERE environment_fingerprint LIKE 'infra:%'
  AND result_schema_version = 0;
