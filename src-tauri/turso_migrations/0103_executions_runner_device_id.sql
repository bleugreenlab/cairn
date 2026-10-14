-- Migration: stamp which machine owns a team execution (CAIRN-2629)
--
-- Shared across the private and team lineages. `runner_device_id` records the
-- stable per-machine device id (the anon-device UUID, independent of sign-in)
-- that OWNS an execution: only that machine claims and runs its jobs. Stamped at
-- execution creation (defaulting to the creating machine), synced so every
-- member agrees on the owner by construction, and re-readable so a reassigned or
-- returning-stale owner refuses at the claim guard. NULL for every legacy row
-- (the ownership guard treats a NULL owner as "anyone may run", preserving the
-- pre-feature behavior). A plain ADD COLUMN keeps this sync-safe; do not rebuild
-- synced replica tables (see TEAM_TAIL in migrations.rs).

ALTER TABLE executions ADD COLUMN runner_device_id TEXT;
