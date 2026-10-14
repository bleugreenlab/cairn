-- The runner's account of how every execution bound: what was offered to which
-- machine, what claimed it, and what declined it and why.
--
-- A journal, not a queue. The live queue is in memory and authoritative --
-- routing has to be instant, an entry's waiter is an in-process future, and a
-- connection generation is an in-memory registry no SQL transaction can
-- arbitrate. Nothing is ever read back out of this to reconstruct an entry.
-- Rows left non-terminal by a previous runner incarnation are closed as expired
-- at start rather than resurrected.
--
-- Runner-local by construction: `runner_incarnation` names one process of one
-- installation, so these rows never enter team sync.
CREATE TABLE execution_queue (
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
        'capacity', 'pressure', 'disk', 'constraint', 'unknown'
    )),
    detail TEXT,
    constraints_json TEXT NOT NULL,
    scoring_json TEXT NOT NULL,
    horizon_unix_ms INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (entry_id, sequence)
);

CREATE INDEX idx_execution_queue_entry
ON execution_queue(entry_id, sequence);

-- The start-up sweep asks exactly this: which entries of some OTHER incarnation
-- are still non-terminal?
CREATE INDEX idx_execution_queue_live
ON execution_queue(runner_incarnation, state, created_at DESC);

CREATE TRIGGER execution_queue_immutable
BEFORE UPDATE ON execution_queue
BEGIN
    SELECT RAISE(ABORT, 'execution queue events are immutable');
END;
