-- lineage: private
-- rebuild-fk-off: Conserved(discord_surface), Conserved(channel_conversation_binding), Conserved(channel_inbound)
-- The ambient Discord channel (cairn/4882): one channel per project category
-- that is bound to no single thread. Unaddressed text there becomes a
-- `channel_message` fact for routes to act on.
--
-- `channel_conversation_binding` gains the `ambient` kind. An ambient row
-- targets the project root, carries exactly MESSAGE_CLASS_AGENT_TEXT (8) so
-- threads can reply there and no question, permission, or notification ever
-- does, and is never focused. One per conversation.
--
-- `discord_surface` gains the `ambient_home` kind, which targets the project
-- root, so the existing (guild, kind, target) unique index makes it one per
-- project per guild. `parent_surface_id` references the table itself, so the
-- rebuild runs with foreign keys off.
--
-- `channel_inbound` now records every admitted message, not just rejected and
-- unsolicited ones: an admitted row carries the conversation, the target it
-- was delivered into (the project root for unaddressed ambient text), and the
-- kind of binding that resolved it. Admitted is `target_uri IS NOT NULL`;
-- rows from before 0176 carry a NULL rejection reason too. The table is rebuilt
-- rather than altered because its rejection CHECK never admitted
-- `unbound_conversation`, the reason the router stores for a message in an
-- unbound Discord channel; the router's `INSERT OR IGNORE` silently dropped
-- every such row.
-- All three tables are runner-local channel state, so no team lineage carries
-- this.

CREATE TABLE channel_conversation_binding_rebuilt (
  provider TEXT NOT NULL,
  conversation TEXT NOT NULL,
  target_uri TEXT NOT NULL,
  binding_kind TEXT NOT NULL CHECK (binding_kind IN ('follow', 'structural', 'ambient')),
  message_classes INTEGER NOT NULL CHECK (message_classes >= 0),
  followed_at INTEGER NOT NULL,
  cursor_rowid INTEGER NOT NULL DEFAULT 0 CHECK (cursor_rowid >= 0),
  suppressed_updates INTEGER NOT NULL DEFAULT 0 CHECK (suppressed_updates >= 0),
  selected_at INTEGER,
  PRIMARY KEY (provider, conversation, target_uri),
  -- 8 is MESSAGE_CLASS_AGENT_TEXT: an ambient row carries agent text only.
  CHECK (binding_kind != 'ambient' OR (message_classes = 8 AND selected_at IS NULL))
);

INSERT INTO channel_conversation_binding_rebuilt (
  provider, conversation, target_uri, binding_kind, message_classes,
  followed_at, cursor_rowid, suppressed_updates, selected_at
)
SELECT provider, conversation, target_uri, binding_kind, message_classes,
  followed_at, cursor_rowid, suppressed_updates, selected_at
FROM channel_conversation_binding;

DROP TABLE channel_conversation_binding;
ALTER TABLE channel_conversation_binding_rebuilt RENAME TO channel_conversation_binding;
CREATE UNIQUE INDEX channel_conversation_binding_focus
  ON channel_conversation_binding(provider, conversation) WHERE selected_at IS NOT NULL;
CREATE UNIQUE INDEX channel_conversation_binding_structural
  ON channel_conversation_binding(provider, conversation) WHERE binding_kind = 'structural';
CREATE UNIQUE INDEX channel_conversation_binding_ambient
  ON channel_conversation_binding(provider, conversation) WHERE binding_kind = 'ambient';

CREATE TABLE discord_surface_rebuilt (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guild_id TEXT NOT NULL,
  surface_kind TEXT NOT NULL CHECK (
    surface_kind IN ('project_category', 'thread_channel', 'unmanaged_home', 'issue_thread', 'ambient_home')
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
    OR (surface_kind IN ('thread_channel', 'issue_thread', 'ambient_home') AND target_uri IS NOT NULL)
  )
);

INSERT INTO discord_surface_rebuilt (
  id, guild_id, surface_kind, project_key, target_uri, parent_surface_id,
  remote_channel_id, seed_message_id, desired_state, observed_state,
  attempt_count, next_attempt_at, last_error, updated_at
)
SELECT id, guild_id, surface_kind, project_key, target_uri, parent_surface_id,
  remote_channel_id, seed_message_id, desired_state, observed_state,
  attempt_count, next_attempt_at, last_error, updated_at
FROM discord_surface;

DROP TABLE discord_surface;
ALTER TABLE discord_surface_rebuilt RENAME TO discord_surface;
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

CREATE TABLE channel_inbound_rebuilt (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  provider_guid TEXT,
  sender TEXT NOT NULL,
  text TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  acknowledged_at INTEGER,
  rejection_reason TEXT
    CHECK (rejection_reason IN ('allowlist', 'policy', 'unmatched', 'unbound_conversation')),
  conversation TEXT,
  target_uri TEXT,
  binding_kind TEXT CHECK (binding_kind IN ('structural', 'follow', 'ambient', 'none')),
  UNIQUE (channel, provider_guid)
);

INSERT INTO channel_inbound_rebuilt (
  id, channel, provider_guid, sender, text, received_at, acknowledged_at, rejection_reason
)
SELECT id, channel, provider_guid, sender, text, received_at, acknowledged_at, rejection_reason
FROM channel_inbound;

DROP TABLE channel_inbound;
ALTER TABLE channel_inbound_rebuilt RENAME TO channel_inbound;
CREATE INDEX idx_channel_inbound_received ON channel_inbound(channel, received_at DESC);
CREATE INDEX idx_channel_inbound_target ON channel_inbound(target_uri);
