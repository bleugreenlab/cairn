-- lineage: private
-- rebuild-fk-off: Reshaped(channel_conversation_binding), Conserved(channel_inbound)
-- cairn/6870: retire channel following and focus.
--
-- Follow and focus lived only as `binding_kind = 'follow'` rows on
-- `channel_conversation_binding`: focus was `selected_at`, the follow stream's
-- cursor `cursor_rowid` / `suppressed_updates`. The 0144 tables they started in
-- (`channel_thread_follow`, `channel_thread_focus`) were already copied here and
-- dropped by 0181, so nothing else remains to retire.
--
-- The follow rows are deleted, and the binding table is rebuilt without the
-- follow-only columns and the `follow` kind; `followed_at` survives as
-- `bound_at`, since a structural or ambient row is bound, not followed. The
-- binding loses rows on purpose, so it is Reshaped.
--
-- `channel_inbound` keeps every row and column. Its `binding_kind` CHECK drops
-- `follow`: a historical row admitted through a follow becomes `none`, and keeps
-- its `target_uri`, the record of where it went. `rejection_reason` gains
-- `no_default_thread`, stored when bare text has no reply-to, no recent
-- operator-message sender, and the default project has no open general thread.
-- Neither CHECK can change in place, so the table is rebuilt.
--
-- No table holds a foreign key into either, and both are runner-local channel
-- state, so no team lineage carries this.

DELETE FROM channel_conversation_binding WHERE binding_kind = 'follow';

CREATE TABLE channel_conversation_binding_rebuilt (
  provider TEXT NOT NULL,
  conversation TEXT NOT NULL,
  target_uri TEXT NOT NULL,
  binding_kind TEXT NOT NULL CHECK (binding_kind IN ('structural', 'ambient')),
  message_classes INTEGER NOT NULL CHECK (message_classes >= 0),
  bound_at INTEGER NOT NULL,
  PRIMARY KEY (provider, conversation, target_uri),
  -- 8 is MESSAGE_CLASS_AGENT_TEXT: an ambient row carries agent text only.
  CHECK (binding_kind != 'ambient' OR message_classes = 8)
);

INSERT INTO channel_conversation_binding_rebuilt (
  provider, conversation, target_uri, binding_kind, message_classes, bound_at
)
SELECT provider, conversation, target_uri, binding_kind, message_classes, followed_at
FROM channel_conversation_binding;

DROP TABLE channel_conversation_binding;
ALTER TABLE channel_conversation_binding_rebuilt RENAME TO channel_conversation_binding;
CREATE UNIQUE INDEX channel_conversation_binding_structural
  ON channel_conversation_binding(provider, conversation) WHERE binding_kind = 'structural';
CREATE UNIQUE INDEX channel_conversation_binding_ambient
  ON channel_conversation_binding(provider, conversation) WHERE binding_kind = 'ambient';
CREATE INDEX idx_channel_binding_target ON channel_conversation_binding(provider, target_uri);

CREATE TABLE channel_inbound_rebuilt (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  provider_guid TEXT,
  sender TEXT NOT NULL,
  text TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  acknowledged_at INTEGER,
  rejection_reason TEXT
    CHECK (rejection_reason IN (
      'allowlist', 'policy', 'unmatched', 'unbound_conversation', 'no_default_thread'
    )),
  conversation TEXT,
  target_uri TEXT,
  binding_kind TEXT CHECK (binding_kind IN ('structural', 'ambient', 'none')),
  addressed_by TEXT,
  message_guid TEXT,
  UNIQUE (channel, provider_guid)
);

INSERT INTO channel_inbound_rebuilt (
  id, channel, provider_guid, sender, text, received_at, acknowledged_at,
  rejection_reason, conversation, target_uri, binding_kind, addressed_by, message_guid
)
SELECT id, channel, provider_guid, sender, text, received_at, acknowledged_at,
  rejection_reason, conversation, target_uri,
  CASE binding_kind WHEN 'follow' THEN 'none' ELSE binding_kind END,
  addressed_by, message_guid
FROM channel_inbound;

DROP TABLE channel_inbound;
ALTER TABLE channel_inbound_rebuilt RENAME TO channel_inbound;
CREATE INDEX idx_channel_inbound_received ON channel_inbound(channel, received_at DESC);
CREATE INDEX idx_channel_inbound_target ON channel_inbound(target_uri);
CREATE INDEX idx_channel_inbound_conversation ON channel_inbound(conversation, received_at);
