-- CAIRN-3373: host-local external-channel delivery and inbound persistence.
CREATE TABLE channel_outbound (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('question', 'permission', 'review')),
  binding_ref TEXT NOT NULL,
  conversation TEXT NOT NULL,
  job_id TEXT,
  rendered_text TEXT NOT NULL,
  rendering TEXT NOT NULL CHECK (rendering IN ('poll', 'text')),
  options_json TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'resolved', 'failed')),
  provider_guid TEXT,
  caption_guid TEXT,
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  resolved_at INTEGER,
  last_error TEXT,
  UNIQUE (channel, kind, binding_ref)
);
CREATE INDEX idx_channel_outbound_status_created ON channel_outbound(channel, status, created_at);
CREATE UNIQUE INDEX idx_channel_outbound_provider_guid ON channel_outbound(channel, provider_guid) WHERE provider_guid IS NOT NULL;
CREATE UNIQUE INDEX idx_channel_outbound_caption_guid ON channel_outbound(channel, caption_guid) WHERE caption_guid IS NOT NULL;

CREATE TABLE channel_cursor (
  channel TEXT PRIMARY KEY,
  since_rowid INTEGER NOT NULL CHECK (since_rowid >= 0)
);

CREATE TABLE channel_inbound (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  provider_guid TEXT,
  sender TEXT NOT NULL,
  text TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  acknowledged_at INTEGER,
  UNIQUE (channel, provider_guid)
);
CREATE INDEX idx_channel_inbound_received ON channel_inbound(channel, received_at DESC);
