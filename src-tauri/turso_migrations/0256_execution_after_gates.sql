-- `after:` gates on an execution.
--
-- An execution may be filed with a list of event predicates. While any of them
-- is unmet the execution is HELD: it mints no jobs, no admission request, and
-- consumes no slot -- but the row exists from the moment it is filed, so the
-- issue's executions list and every census can name what it is waiting on.
--
-- Predicates are level-resolvable: each answers "is this true now?" by SQL
-- against this database, so a dropped event costs one tick of latency rather
-- than stranding an execution forever. Satisfaction is LATCHED in
-- `satisfied_at`: release is a one-way door, and a check that goes green and
-- later red must not un-start work that already started.
--
-- `released_at` on `executions` is the other half of the held predicate. "Has
-- at least one gate AND released_at IS NULL" is the whole definition of held,
-- which is why the status projection can derive it rather than anyone setting
-- it, and why the guarded UPDATE on `released_at` is an exactly-once release
-- primitive.
--
-- No backfill: `execution_gates` is new, so no existing execution has a gate
-- and none is held. The projection's new first branch is a no-op for every row
-- that predates this migration.
CREATE TABLE execution_gates (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    -- Authored order, so a census renders an execution's gates the way its
    -- filer wrote them rather than in insertion-race order.
    seq INTEGER NOT NULL,
    -- One canonicalized TriggerClause -- the same shared predicate vocabulary
    -- routes and thread definition graphs already use.
    predicate_json TEXT NOT NULL,
    -- NULL = unmet. Latched once observed true; never cleared.
    satisfied_at INTEGER,
    -- The most recent resolution: 'satisfied' | 'refuted' | 'no_evidence'.
    -- "Never true in practice" is the median case for some predicates, so a
    -- held execution must be able to say WHICH state each gate is in; a gate
    -- that is merely quiet is indistinguishable from one that is invisible.
    last_outcome TEXT,
    -- Permanently unsatisfiable. Foreclosure does not release or cancel: the
    -- execution stays held and the filing thread is woken for judgment.
    foreclosed_at INTEGER,
    foreclosed_reason TEXT,
    created_at INTEGER NOT NULL
);

-- The sweep's access path: unsatisfied gates, by execution. The predicate is
-- in the index so the scan needs no per-row table fetch.
CREATE INDEX idx_execution_gates_unsatisfied
    ON execution_gates(execution_id) WHERE satisfied_at IS NULL;

-- Plain ADD COLUMNs, deliberately: `executions` is a synced replica table and a
-- create-copy-drop-rename rebuild of one breaks Turso sync triggers.
--
-- NULL while held. The guarded transition on this column
-- (`... WHERE id = ?1 AND released_at IS NULL`) is the exactly-once release
-- primitive, and it composes with the canonical-lineage guard from 0230.
ALTER TABLE executions ADD COLUMN released_at INTEGER;
-- Set only on a manual early release: {principal, at}. Its presence is what
-- distinguishes "the predicates came true" from "someone decided not to wait".
ALTER TABLE executions ADD COLUMN release_override_json TEXT;
