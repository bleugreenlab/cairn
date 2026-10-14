-- Migration: bind terminals to executor-hosted lifetime processes.
ALTER TABLE job_terminals ADD COLUMN lease_id TEXT;
ALTER TABLE job_terminals ADD COLUMN lease_incarnation_id TEXT;
ALTER TABLE job_terminals ADD COLUMN lease_epoch INTEGER;
ALTER TABLE job_terminals ADD COLUMN process_generation INTEGER;
CREATE INDEX idx_job_terminals_lease_id ON job_terminals(lease_id) WHERE lease_id IS NOT NULL;