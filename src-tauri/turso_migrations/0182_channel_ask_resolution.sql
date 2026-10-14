-- One canonical ask may be rendered by several providers and conversations.
-- The resolution row is the global first-answer-wins claim; outbound rows retain
-- provider-specific delivery and cleanup state.
CREATE TABLE channel_ask_resolution (
  binding_ref TEXT PRIMARY KEY,
  action_ref TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('question', 'permission')),
  answer TEXT NOT NULL,
  winner_provider TEXT NOT NULL,
  winner_conversation TEXT NOT NULL,
  resolved_at INTEGER NOT NULL,
  domain_resolved_at INTEGER
);

CREATE INDEX idx_channel_ask_resolution_action ON channel_ask_resolution(action_ref, domain_resolved_at);

-- Domain effects are separately leased from immutable answers. A crashed worker's
-- lease expires, allowing a later channel sweep to retry without accepting a
-- second answer or invoking the effect concurrently.
CREATE TABLE channel_ask_action (
  action_ref TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('question', 'permission')),
  lease_token TEXT,
  lease_until INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  domain_resolved_at INTEGER
);

CREATE TABLE channel_outbound_rebuilt (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('question', 'permission', 'review', 'route')),
  binding_ref TEXT NOT NULL,
  conversation TEXT NOT NULL,
  job_id TEXT,
  rendered_text TEXT NOT NULL,
  rendering TEXT NOT NULL CHECK (rendering IN ('poll', 'text')),
  options_json TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'cleanup_pending', 'resolved', 'failed', 'expired')),
  provider_guid TEXT,
  caption_guid TEXT,
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  resolved_at INTEGER,
  last_error TEXT,
  UNIQUE (channel, conversation, kind, binding_ref)
);

INSERT INTO channel_outbound_rebuilt
SELECT * FROM channel_outbound;
DROP TABLE channel_outbound;
ALTER TABLE channel_outbound_rebuilt RENAME TO channel_outbound;
CREATE INDEX idx_channel_outbound_status_created ON channel_outbound(channel, status, created_at);
CREATE UNIQUE INDEX idx_channel_outbound_provider_guid ON channel_outbound(channel, provider_guid) WHERE provider_guid IS NOT NULL;
CREATE UNIQUE INDEX idx_channel_outbound_caption_guid ON channel_outbound(channel, caption_guid) WHERE caption_guid IS NOT NULL;