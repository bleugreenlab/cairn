-- Migration: archival storage
--
-- Foundations for session archival via git coordinates (CAIRN-1538). The hot
-- path keeps recording events `full`; at worktree teardown events are rewritten
-- to git coordinates with a zstd backstop, and an execution-scoped range pack
-- preserves the gc-at-risk objects needed to reconstruct them.

-- Per-execution range packfile: the objects reachable from the worktree tip but
-- not from the durable pack anchor or the default branch. `pack`/`pack_idx` are
-- NULL when the range was empty (everything was already durable and resolves
-- from the project repo's object database).
CREATE TABLE execution_history (
    execution_id TEXT PRIMARY KEY NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    base_sha TEXT NOT NULL,   -- the pack anchor the range was built against
    tip_sha TEXT NOT NULL,    -- worktree HEAD at teardown
    pack BLOB,                -- NULL when the range was empty (everything already durable)
    pack_idx BLOB
);

-- Per-event storage mode. `full` keeps the original inline `data`; `gitcoord`
-- reconstructs the rendered output from a commit + path; `zstd` falls back to a
-- compressed copy of the original `data` when a coordinate read drifts.
ALTER TABLE events ADD COLUMN storage_mode TEXT CHECK(storage_mode IN ('full','gitcoord','zstd')) DEFAULT 'full';

-- Full 40-hex commit a gitcoord event's content is addressed against.
ALTER TABLE events ADD COLUMN content_commit TEXT;

-- sha-256 of the original rendered output, captured at rewrite time as a drift
-- tripwire. Never used for reconstruction — only to detect when a coordinate
-- read no longer matches what was originally recorded.
ALTER TABLE events ADD COLUMN content_render_sha TEXT;

-- zstd-compressed copy of the original `data`, populated for the `zstd` backstop.
ALTER TABLE events ADD COLUMN data_blob BLOB;

-- Codec marker for `data_blob`, tagged per row so decompression stays stable
-- across future codec additions.
ALTER TABLE events ADD COLUMN codec TEXT CHECK(codec IN ('none','zstd_v1')) DEFAULT 'none';
