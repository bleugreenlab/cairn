-- CAIRN-3290: make `events(run_id, sequence)` unique.
--
-- `sequence` is the transcript's ordering key, but nothing enforced it. Backend
-- readers carried hand-rolled counters that could not see the slot `open_stream`
-- reserves in `message_streams` for an in-flight message, and orchestrator paths
-- computed `MAX(sequence) + 1` in a read separated from their insert. Both wrote
-- duplicates; at authoring time a real workspace held 52,350 duplicated slots
-- across 6,341 of 12,228 runs, still accruing at ~11% of new events.
--
-- Allocation now happens inside the same write transaction as the insert, so no
-- new duplicate can be written. This migration repairs history so the invariant
-- becomes enforceable, then enforces it.
--
-- The repair preserves ORDER, not density. Within a run, rows are ordered by
-- (sequence, created_at, id). Every row after the first in a duplicated slot is
-- an "extra"; every row is shifted up by the number of extras at or before it.
-- That map is strictly increasing, so it is injective: duplicates separate into
-- adjacent slots in their original order, and no row is reordered or discarded.
-- Gaps are deliberately left alone — a reserved slot whose stream aborted never
-- becomes an event, so gaps are legitimate and always have been.

-- Narrow to the colliding slots first. This is one grouped pass over the
-- existing (run_id, sequence) index; every later step probes it instead of
-- re-scanning the whole table, which is what keeps a first-launch repair on a
-- million-event database in seconds rather than minutes.
CREATE TABLE _event_sequence_dup_slots (
    run_id   TEXT    NOT NULL,
    sequence INTEGER NOT NULL
);

INSERT INTO _event_sequence_dup_slots (run_id, sequence)
SELECT run_id, sequence FROM events
GROUP BY run_id, sequence HAVING COUNT(*) > 1;

CREATE INDEX _event_sequence_dup_slots_key
    ON _event_sequence_dup_slots(run_id, sequence);

CREATE TABLE _event_sequence_extras (
    run_id     TEXT    NOT NULL,
    sequence   INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    id         TEXT    NOT NULL
);

INSERT INTO _event_sequence_extras (run_id, sequence, created_at, id)
SELECT e.run_id, e.sequence, e.created_at, e.id
FROM events e
JOIN _event_sequence_dup_slots d
  ON d.run_id = e.run_id AND d.sequence = e.sequence
WHERE EXISTS (
    SELECT 1 FROM events p
    WHERE p.run_id = e.run_id
      AND p.sequence = e.sequence
      AND (p.created_at < e.created_at
           OR (p.created_at = e.created_at AND p.id < e.id))
);

CREATE INDEX _event_sequence_extras_key
    ON _event_sequence_extras(run_id, sequence, created_at, id);

-- A row below its run's first extra always shifts by zero, so the shift is only
-- computed from that point up.
CREATE TABLE _event_sequence_runs (
    run_id      TEXT    NOT NULL PRIMARY KEY,
    first_extra INTEGER NOT NULL
);

INSERT INTO _event_sequence_runs (run_id, first_extra)
SELECT run_id, MIN(sequence) FROM _event_sequence_extras GROUP BY run_id;

-- Drop the ordering index BEFORE the bulk rewrite. Everything above is done
-- reading it, and leaving it in place would make the rewrite maintain an index
-- that is replaced immediately afterwards.
DROP INDEX idx_events_run_sequence;

-- The shift is computed inline against the frozen `_event_sequence_extras`
-- snapshot, never against `events` itself, so this UPDATE never reads a value it
-- is concurrently rewriting.
UPDATE events
SET sequence = sequence + (
    SELECT COUNT(*) FROM _event_sequence_extras x
    WHERE x.run_id = events.run_id
      AND (x.sequence < events.sequence
           OR (x.sequence = events.sequence AND x.created_at < events.created_at)
           OR (x.sequence = events.sequence AND x.created_at = events.created_at
               AND x.id <= events.id))
)
WHERE run_id IN (SELECT run_id FROM _event_sequence_runs)
  AND sequence >= (
      SELECT r.first_extra FROM _event_sequence_runs r WHERE r.run_id = events.run_id
  );

CREATE UNIQUE INDEX idx_events_run_sequence ON events(run_id, sequence);

DROP TABLE _event_sequence_runs;
DROP TABLE _event_sequence_extras;
DROP TABLE _event_sequence_dup_slots;
