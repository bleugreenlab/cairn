-- Index the recency ranking that selects the latest verdict per check.
--
-- `list_latest_check_results_for_project` picks one row per `check_name` ordered
-- by `(ran_at DESC, tree_hash DESC)` within a project. No existing index carries
-- that shape: the primary key is `(project_id, check_name, input_hash)`, so a
-- lookup could reach a check's rows but never their recency order, and the
-- planner fell back to scanning and sorting the whole project's rows.
--
-- The cache grows a few rows per commit and never prunes (one real project held
-- 11,472 rows across 23 checks), so this read is on the write-check planning
-- critical path of every source-touching commit and must not scale with history.
-- Column order matches the query's PARTITION BY plus its ordering inside the
-- partition, so the winning row per check is a seek rather than a sort.
--
-- Shared project state, composed once into both lineages.

CREATE INDEX IF NOT EXISTS idx_check_result_cache_project_check_recency
    ON check_result_cache(project_id, check_name, ran_at DESC, tree_hash DESC);
