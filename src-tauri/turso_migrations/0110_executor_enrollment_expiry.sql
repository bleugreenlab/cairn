ALTER TABLE executor_enrollments ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0;

-- Bound the transition window for credentials created before expiry existed.
UPDATE executor_enrollments
SET expires_at = CASE
    WHEN revoked_at IS NULL THEN unixepoch() + 604800
    ELSE updated_at
END;

ALTER TABLE executor_enrollments ADD COLUMN previous_credential_hash TEXT;
ALTER TABLE executor_enrollments ADD COLUMN previous_expires_at INTEGER;
