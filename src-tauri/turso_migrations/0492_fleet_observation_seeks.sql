-- lineage: shared
-- Absence incidents read just the newest lane for one executor, without sorting.
CREATE INDEX idx_check_observations_executor_latest
ON check_result_observations(executor_id, ran_at DESC, id DESC);

-- Generation-end attribution reads only infrastructure failures in its time window.
CREATE INDEX idx_check_observations_infrastructure_generation
ON check_result_observations(executor_id, executor_connection_generation, ran_at)
WHERE failure_kind = 'infrastructure';
