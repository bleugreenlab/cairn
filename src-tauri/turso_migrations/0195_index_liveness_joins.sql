-- Index two of the joins the attention-push liveness predicate walks on every
-- drain, and a drain happens on every MCP tool call (CAIRN-4181).
--
-- The third join, issue -> jobs -> artifacts, needed no new index: 0106 already
-- created idx_jobs_issue_id. It was simply never used, because the predicate
-- asked one issue at a time (`WHERE j.issue_id = ?`) and for that shape Turso
-- prefers to drive from artifacts by type. Only the batched set form
-- (`WHERE j.issue_id IN (...) AND EXISTS (...)`) drives from the issue. That is
-- a query-shape fix, not a schema one, and it lives in
-- orchestrator/attention_push.rs.
--
-- Both plans below were measured against the real schema with
-- EXPLAIN QUERY PLAN; the storage::migrations test
-- migration_0195_indexes_attention_push_liveness_joins asserts each one so they
-- cannot silently regress. Turso keeps no table statistics, so a plan is the
-- only evidence an index is load-bearing rather than dead weight.

-- 1. issues(number). Resolving a whole backlog's `cairn://p/<key>/<n>` refs to
--    issue rows wants one statement with the numbers bound as a set. The
--    existing UNIQUE (project_id, number) index only seeks when BOTH columns
--    are equalities -- Turso will not seek it for `number IN (...)`, falling
--    back to idx_issues_project_id and loading every issue in the project
--    (~4.9k rows in production). Indexing `number` alone lets the set form seek
--    once per number ("SEARCH i USING INDEX idx_issues_number (number=?)"),
--    with the project matched on the join.
CREATE INDEX IF NOT EXISTS idx_issues_number ON issues(number);

-- 2. merge_requests(issue_id), non-partial. `idx_mr_issue` existed already, but
--    as a PARTIAL index (WHERE issue_id IS NOT NULL) -- and Turso declines to
--    use a partial index, so the first and cheapest review arm ("is there an
--    open unmerged PR for this issue") has been a full SCAN of merge_requests
--    (~3.4k rows) once per push per drain, all along. The partial predicate
--    excluded nothing worth excluding either: it drops exactly the rows nobody
--    can look up by issue_id. Replaced in place, under the same name, with a
--    plain index the planner will actually seek.
DROP INDEX IF EXISTS idx_mr_issue;
CREATE INDEX IF NOT EXISTS idx_mr_issue ON merge_requests(issue_id);
