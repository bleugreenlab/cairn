-- A machine whose build root is on a detachable volume now declines work that
-- needs a cell with a reason of its own, `build_volume_absent`, while that
-- volume is not mounted. The journal's CHECK names every decline reason and
-- cannot be widened in place, so the table is rebuilt with the new value and
-- its rows copied across. Nothing references this table, and it is
-- runner-local, so no team lineage carries it.
CREATE TABLE execution_queue_rebuilt (
    event_id TEXT PRIMARY KEY NOT NULL,
    entry_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    runner_incarnation TEXT NOT NULL,
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'queued', 'offered', 'claimed', 'completed', 'cancelled', 'expired'
    )),
    executor_id TEXT,
    generation INTEGER,
    decline_reason TEXT CHECK (decline_reason IN (
        'capacity', 'pressure', 'disk', 'constraint', 'unknown', 'project_provisioning',
        'build_volume_absent'
    )),
    detail TEXT,
    constraints_json TEXT NOT NULL,
    scoring_json TEXT NOT NULL,
    horizon_unix_ms INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (entry_id, sequence)
);

INSERT INTO execution_queue_rebuilt (
    event_id, entry_id, sequence, runner_incarnation, request_id, attempt_id,
    project_id, state, executor_id, generation, decline_reason, detail,
    constraints_json, scoring_json, horizon_unix_ms, created_at
)
SELECT
    event_id, entry_id, sequence, runner_incarnation, request_id, attempt_id,
    project_id, state, executor_id, generation, decline_reason, detail,
    constraints_json, scoring_json, horizon_unix_ms, created_at
FROM execution_queue;

DROP TABLE execution_queue;

ALTER TABLE execution_queue_rebuilt RENAME TO execution_queue;

CREATE INDEX idx_execution_queue_entry
ON execution_queue(entry_id, sequence);

CREATE INDEX idx_execution_queue_live
ON execution_queue(runner_incarnation, state, created_at DESC);

CREATE TRIGGER execution_queue_immutable
BEFORE UPDATE ON execution_queue
BEGIN
    SELECT RAISE(ABORT, 'execution queue events are immutable');
END;
