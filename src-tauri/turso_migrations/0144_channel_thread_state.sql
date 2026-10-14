-- Durable iMessage thread-follow state. This belongs to the private channel
-- lineage: it describes one operator's Messages account, not shared project data.
CREATE TABLE channel_thread_follow (
  channel TEXT NOT NULL,
  thread_uri TEXT NOT NULL,
  followed_at INTEGER NOT NULL,
  cursor_rowid INTEGER NOT NULL DEFAULT 0 CHECK (cursor_rowid >= 0),
  PRIMARY KEY (channel, thread_uri)
);

CREATE TABLE channel_thread_focus (
  channel TEXT PRIMARY KEY,
  thread_uri TEXT NOT NULL,
  selected_at INTEGER NOT NULL
);
