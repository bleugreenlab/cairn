-- Route channel sinks use the standing delivery ledger, so route intents must be
-- a first-class ledger kind rather than bypassing its claim/retry machinery.
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
  status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'resolved', 'failed', 'expired')),
  provider_guid TEXT,
  caption_guid TEXT,
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  resolved_at INTEGER,
  last_error TEXT,
  UNIQUE (channel, kind, binding_ref)
);

INSERT INTO channel_outbound_rebuilt (id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, options_json, status, provider_guid, caption_guid, created_at, sent_at, resolved_at, last_error)
  SELECT id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, options_json, status, provider_guid, caption_guid, created_at, sent_at, resolved_at, last_error FROM channel_outbound;

DROP TABLE channel_outbound;
ALTER TABLE channel_outbound_rebuilt RENAME TO channel_outbound;

CREATE INDEX idx_channel_outbound_status_created ON channel_outbound(channel, status, created_at);
CREATE UNIQUE INDEX idx_channel_outbound_provider_guid ON channel_outbound(channel, provider_guid) WHERE provider_guid IS NOT NULL;
CREATE UNIQUE INDEX idx_channel_outbound_caption_guid ON channel_outbound(channel, caption_guid) WHERE caption_guid IS NOT NULL;