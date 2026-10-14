-- Migration: archival blob store for content-addressed system-prompt segments
--
-- The assembled system prompt (~23 KB) is recorded as a `system:prompt` event on
-- every run and is the largest repeated blob in the db. At worktree teardown its
-- static segments (backend base prompt, cairn prompt, workspace instructions,
-- agent content) are replaced with content-addressed references into this table;
-- the per-run dynamic tail (orientation) stays inline on the event. Identical
-- segments across runs and executions collapse to a single row, so the scheme
-- pays for itself the second time a segment recurs.
--
-- Rows are shared across executions and are intentionally NOT foreign-keyed to
-- executions: deleting an execution must not cascade a blob another execution
-- still references. Garbage collection (a refcount sweep of unreferenced hashes)
-- is out of scope here and not yet implemented.
CREATE TABLE archival_blobs (
    hash TEXT PRIMARY KEY NOT NULL,  -- sha-256 (hex) of the original segment bytes
    content BLOB NOT NULL,           -- zstd-compressed segment bytes
    created_at INTEGER NOT NULL
);
