-- lineage: shared
-- Candidate discovery reads only narrow job facts before loading snapshots.
CREATE INDEX idx_jobs_status_execution ON jobs(status, execution_id);

-- Suppress only the historical resolution that was intentionally repaired
-- quietly. A later unresolve increments resolution_generation and is eligible
-- for the normal Resolved notice again.
ALTER TABLE issues ADD COLUMN quiet_failure_generation INTEGER;
UPDATE issues SET quiet_failure_generation = resolution_generation WHERE status = 'failed';

-- A launch can fail before a run exists. Keep its bounded reason on the work
-- turn, rather than manufacturing a run solely to carry error text.
ALTER TABLE turns ADD COLUMN failure_reason TEXT;
