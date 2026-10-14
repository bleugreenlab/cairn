-- Migration: job pack anchor
--
-- Durable archival anchor captured at worktree creation: the nearest ancestor
-- commit that is reachable from the project's default branch. Set alongside
-- base_commit. NULL when unresolvable -- archival later treats NULL as
-- zstd-only and never guesses.
ALTER TABLE jobs ADD COLUMN pack_anchor TEXT;
