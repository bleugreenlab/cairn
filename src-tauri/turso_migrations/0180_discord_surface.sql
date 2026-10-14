-- CAIRN-4006: machine-local durable lifecycle for Discord containers.
CREATE TABLE discord_surface (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guild_id TEXT NOT NULL,
  surface_kind TEXT NOT NULL CHECK (
    surface_kind IN ('project_category', 'thread_channel', 'unmanaged_home', 'issue_thread')
  ),
  project_key TEXT NOT NULL,
  target_uri TEXT,
  parent_surface_id INTEGER REFERENCES discord_surface(id),
  remote_channel_id TEXT,
  seed_message_id TEXT,
  desired_state TEXT NOT NULL DEFAULT 'active' CHECK (desired_state IN ('active', 'locked')),
  observed_state TEXT NOT NULL DEFAULT 'absent' CHECK (
    observed_state IN ('absent', 'creating', 'active', 'archived', 'locking', 'locked', 'failed')
  ),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at INTEGER NOT NULL,
  CHECK (
    (surface_kind IN ('project_category', 'unmanaged_home') AND target_uri IS NULL)
    OR (surface_kind IN ('thread_channel', 'issue_thread') AND target_uri IS NOT NULL)
  )
);

CREATE UNIQUE INDEX idx_discord_surface_target
  ON discord_surface(guild_id, surface_kind, target_uri)
  WHERE target_uri IS NOT NULL;
CREATE UNIQUE INDEX idx_discord_surface_project_category
  ON discord_surface(guild_id, project_key)
  WHERE surface_kind = 'project_category';
CREATE UNIQUE INDEX idx_discord_surface_unmanaged_home
  ON discord_surface(guild_id, project_key)
  WHERE surface_kind = 'unmanaged_home';
CREATE UNIQUE INDEX idx_discord_surface_remote
  ON discord_surface(guild_id, remote_channel_id)
  WHERE remote_channel_id IS NOT NULL;
CREATE INDEX idx_discord_surface_reconcile
  ON discord_surface(next_attempt_at, desired_state, observed_state);