-- lineage: shared
-- cairn/7718: the run queue's facts read takes only the executions that can
-- matter to its list: those the list already holds, sought by id through
-- idx_executions_hold_facts, and those that can join it (running or paused,
-- `runnable_execution_status_sql!`), scanned from the partial
-- idx_executions_live. Each gains the columns that read takes, so neither
-- half assembles an execution's row (cairn/5894). idx_executions_run_queue led
-- with lifecycle, so the read walked every canonical execution ever filed; it
-- has no other reader and goes. The live index's predicate is spelled exactly
-- as before, which is what lets Turso substitute it (docs/database.md).
DROP INDEX idx_executions_run_queue;
DROP INDEX idx_executions_hold_facts;
CREATE INDEX idx_executions_hold_facts
    ON executions(id, issue_id, seq, lifecycle, released_at, runner_device_id,
                  release_override_json, status, started_at, queue_state);
DROP INDEX idx_executions_live;
CREATE INDEX idx_executions_live
    ON executions(issue_id, started_at, seq, status, id, lifecycle, runner_device_id,
                  released_at, queue_state)
 WHERE status IN ('running','paused');
