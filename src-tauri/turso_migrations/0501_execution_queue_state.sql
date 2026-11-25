-- lineage: shared
-- cairn/7546: an execution's place relative to its runner's run queue line, as
-- the queue last decided it: 'queued' (in the list, below the line), 'parked'
-- (out of the list), or NULL (running, or not a queue member). The run queue
-- is its only writer; the issue status projection reads it to derive the
-- `queued` and `parked` statuses.
ALTER TABLE executions ADD COLUMN queue_state TEXT
    CHECK (queue_state IS NULL OR queue_state IN ('queued', 'parked'));
-- Every column the run queue's membership read takes, so it seeks live
-- executions by lifecycle and never assembles an execution's row (whose
-- snapshot can run to hundreds of kilobytes).
CREATE INDEX idx_executions_run_queue
    ON executions(lifecycle, runner_device_id, id, issue_id, status, released_at,
                  started_at, queue_state);
