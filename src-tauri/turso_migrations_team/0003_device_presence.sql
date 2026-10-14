-- Migration: per-machine device presence for team runner selection (CAIRN-2629)
--
-- Team-only (no private counterpart): a synced row per member machine, upserted
-- on a cadence by each runner, advertising which project keys that machine has a
-- LOCAL clone for (`project_routes.local_repo_path`, a private-per-machine
-- overlay). Peers read it to build the runner picker at execution start, to
-- validate that a chosen device can actually run a project, and to render
-- "waiting for <device>" when an owner is offline (a stale `last_seen`).
-- Timestamps are in SECONDS (the replica convention). `project_keys` is a JSON
-- array of strings.
--
-- Mirrors the team-only `labels` catch-up (0002): a fresh replica gets this from
-- the edited team head 0001 snapshot, an existing replica gets it from this
-- tracked migration, and the migration-vs-sync race is covered by the idempotent
-- `ensure_device_presence_table` runtime repair at team-open (db.rs).

CREATE TABLE IF NOT EXISTS device_presence (
    device_id TEXT PRIMARY KEY NOT NULL,
    device_name TEXT NOT NULL,
    last_seen INTEGER NOT NULL,
    project_keys TEXT NOT NULL DEFAULT '[]',
    updated_at INTEGER NOT NULL
);
