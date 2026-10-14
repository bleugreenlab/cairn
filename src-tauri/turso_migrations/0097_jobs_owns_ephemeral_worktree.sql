-- Migration: mark jobs that own a throwaway ephemeral task worktree (CAIRN-2476)
--
-- Shared across the private and team lineages. `owns_ephemeral_worktree` is set
-- to 1 for a task job spawned by an ambient (no-worktree / Branch: main) parent:
-- such a task cannot run in the user's live checkout and has no parent worktree
-- to inherit, so it gets its own worktree off the default branch (isolation),
-- and that worktree is reclaimed the moment the task job reaches a terminal
-- status. The marker is the explicit teardown trigger read by the finalize
-- reclaim and the worktree GC backstop, so ownership is never re-derived from
-- parent state that may have changed. A plain ADD COLUMN keeps this sync-safe; do
-- not rebuild synced replica tables (see TEAM_TAIL in migrations.rs).

ALTER TABLE jobs ADD COLUMN owns_ephemeral_worktree INTEGER NOT NULL DEFAULT 0;
