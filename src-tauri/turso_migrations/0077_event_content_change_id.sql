-- CAIRN-1964: durable jj change-id provenance for archived event coordinates.
--
-- At worktree teardown, archival forward-maps each event's recorded (and under
-- jj, possibly-rewritten) commit to the current in-pack commit-id via jj's
-- stable change-id. This column records that change-id alongside content_commit
-- as durable provenance. It is orthogonal to the reconstruction-shape contract
-- (the six columns encode/decode owns) and is never consumed by reconstruction,
-- which resolves purely by content_commit. NULL for plain-git worktrees and for
-- non-git-addressed shapes (zstd/blobbed/full).
ALTER TABLE events ADD COLUMN content_change_id TEXT;
