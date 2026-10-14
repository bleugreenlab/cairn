-- Serve the durable newest-result-per-check projection for one job without
-- scanning every historical result for that job (CAIRN-4398).
--
-- The correlated NOT EXISTS query partitions rows by `(job_id, check_name)` and
-- compares recency by `ran_at`, then `tree_hash`, then `input_hash`. The existing
-- job-only index reaches the job but leaves both the outer projection and every
-- correlated newer-row probe walking that job's complete result history.
--
-- Keep this column order synchronized with
-- `execution::cache::latest_check_results_for_job`. The descending order is the
-- query's exact newest-first tuple and the final input hash makes tie-breaking
-- deterministic without returning to the table.
--
-- Shared project state, composed once into both lineages.
CREATE INDEX IF NOT EXISTS idx_check_result_cache_job_check_recency
    ON check_result_cache(
        job_id,
        check_name,
        ran_at DESC,
        tree_hash DESC,
        input_hash DESC
    );