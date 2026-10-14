-- Preserve every turn-end check arm while keeping one explicit current pointer per job.
CREATE TABLE turn_end_check_attempts (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    state TEXT NOT NULL,
    reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_turn_end_check_attempts_job_created
    ON turn_end_check_attempts(job_id, created_at, id);
ALTER TABLE jobs ADD COLUMN turn_end_check_attempt_id TEXT
    REFERENCES turn_end_check_attempts(id);
