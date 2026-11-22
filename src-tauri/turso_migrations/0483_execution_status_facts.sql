-- lineage: shared
-- Completion repair reads cached status without assembling snapshot-bearing rows.
DROP INDEX idx_executions_hold_facts;
CREATE INDEX idx_executions_hold_facts
    ON executions(id, issue_id, seq, lifecycle, released_at, runner_device_id,
                  release_override_json, status);
