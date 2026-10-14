ALTER TABLE executor_enrollments ADD COLUMN superseded_credential_hash TEXT;
ALTER TABLE executor_enrollments ADD COLUMN superseded_at INTEGER;
