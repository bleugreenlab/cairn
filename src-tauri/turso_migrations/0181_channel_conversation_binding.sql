CREATE TABLE channel_conversation_binding (
  provider TEXT NOT NULL,
  conversation TEXT NOT NULL,
  target_uri TEXT NOT NULL,
  binding_kind TEXT NOT NULL CHECK (binding_kind IN ('follow', 'structural')),
  message_classes INTEGER NOT NULL CHECK (message_classes >= 0),
  followed_at INTEGER NOT NULL,
  cursor_rowid INTEGER NOT NULL DEFAULT 0 CHECK (cursor_rowid >= 0),
  suppressed_updates INTEGER NOT NULL DEFAULT 0 CHECK (suppressed_updates >= 0),
  selected_at INTEGER,
  PRIMARY KEY (provider, conversation, target_uri)
);
CREATE UNIQUE INDEX channel_conversation_binding_focus
  ON channel_conversation_binding(provider, conversation) WHERE selected_at IS NOT NULL;
CREATE UNIQUE INDEX channel_conversation_binding_structural
  ON channel_conversation_binding(provider, conversation) WHERE binding_kind = 'structural';

INSERT INTO channel_conversation_binding (
  provider, conversation, target_uri, binding_kind, message_classes,
  followed_at, cursor_rowid, suppressed_updates, selected_at
)
SELECT follow.channel,
  CASE follow.channel
    WHEN 'imessage' THEN 'imessage:legacy'
    WHEN 'telegram' THEN 'telegram:0'
    WHEN 'discord' THEN 'discord:0/0'
    ELSE follow.channel || ':legacy'
  END,
  follow.thread_uri, 'follow', 7, follow.followed_at, follow.cursor_rowid,
  follow.suppressed_updates, focus.selected_at
FROM channel_thread_follow follow
LEFT JOIN channel_thread_focus focus
  ON focus.channel = follow.channel AND focus.thread_uri = follow.thread_uri
WHERE follow.channel IN ('imessage', 'telegram', 'discord')
  AND follow.thread_uri != 'cairn:~' AND follow.thread_uri NOT LIKE 'cairn:~/%';

DROP TABLE channel_thread_focus;
DROP TABLE channel_thread_follow;