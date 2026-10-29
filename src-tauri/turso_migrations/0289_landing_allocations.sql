-- lineage: private
-- cairn/5222: the record of every sequential identifier the store bound when
-- a branch landed on its project's default branch. A branch authors a
-- migration as NEXT_name.sql and a protocol version as `/// Version NEXT`; the
-- landing allocates the next free value against the tip it lands on, renames
-- and rewrites the branch in one sealed commit, and writes one row here per
-- value once the landing succeeds. Private: a landing runs against this
-- installation's store.
--
-- `kind` is the `landing.identifiers` key that allocated the value (migration,
-- executor-protocol), `name` is what carries it on the default branch
-- (0289_landing_allocations, Version 158), and `provisional` is what the
-- branch authored (NEXT_landing_allocations, NEXT, or a number the landing
-- moved). One value may appear twice only if a merge made outside Cairn raced
-- the landing between its bind and its merge, so this is not a key.
CREATE TABLE landing_allocations (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    value INTEGER NOT NULL,
    name TEXT NOT NULL,
    provisional TEXT NOT NULL,
    issue_id TEXT,
    branch TEXT NOT NULL,
    landed_commit TEXT,
    allocated_at INTEGER NOT NULL
);

CREATE INDEX idx_landing_allocations_value
    ON landing_allocations(project_id, kind, value);

-- The replay-time resolver this replaces kept its recovery floors here. The
-- store no longer renumbers at replay, so nothing reads or writes them.
DROP INDEX IF EXISTS idx_contended_identifier_obligations_merge_gate;
DROP TABLE IF EXISTS contended_identifier_obligations;
