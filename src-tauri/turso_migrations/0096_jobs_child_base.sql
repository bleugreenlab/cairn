-- Migration: per-job child-routing mode (CAIRN-2460)
--
-- Shared across the private and team lineages. `child_base` records the
-- resolved child-routing mode of the agent node that produced this job:
-- NULL / 'parent-branch' (default) routes child issues onto the parent's live
-- integration branch; 'default-branch' routes them onto the project default
-- branch (the "coordinator on main" pattern). Resolved once at job creation and
-- read back by issues::relations::resolve_parent_branch, so both the child's
-- base-branch resolution and its pack-anchor resolution see one value. A plain
-- ADD COLUMN keeps this sync-safe; do not rebuild synced replica tables (see
-- TEAM_TAIL in migrations.rs).

ALTER TABLE jobs ADD COLUMN child_base TEXT;
