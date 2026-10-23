-- Index the two per-execution lookups the gate census makes on every surface
-- that lists executions or summarizes an issue, the issue route among them
-- (cairn/5206).
--
-- 1. execution_gates(execution_id). The census asks "does this execution have
--    any gates" (an EXISTS in its row statement and in gated_unreleased_sql!)
--    and reads each listed execution's gates. The only index on the column was
--    idx_execution_gates_unsatisfied, which is partial on satisfied_at IS NULL,
--    so neither lookup could use it and both scanned the table per execution.
--
-- 2. execution_supersessions(superseded_execution_id). The census names the
--    successor that fenced a won't-start execution with a correlated subquery
--    per row, which had no index on that column and scanned the table.
--
-- execution::gates::census::tests::census_statements_never_scan and
-- gates::store::tests::gates_for_executions_seek_by_execution pin the plans.
-- Turso keeps no table statistics, so a plan is the only evidence an index is
-- load-bearing rather than dead weight.
CREATE INDEX IF NOT EXISTS idx_execution_gates_execution
    ON execution_gates(execution_id);
CREATE INDEX IF NOT EXISTS idx_execution_supersessions_superseded
    ON execution_supersessions(superseded_execution_id);
