-- Agent-authored free text to a bound channel conversation.
--
-- A direct agent message is its own outbound kind, `message`, rather than a
-- `route`: route rows carry a serialized route-sink submission, write route
-- firing history, and are recovered under presence deferral, none of which
-- applies to an agent speaking in a conversation. The kind CHECK cannot be
-- widened in place, so the table is rebuilt with every current column
-- (including the 0211 retry columns) and its rows copied across. Nothing holds a
-- foreign key into channel_outbound, and it is runner-local, so no team lineage
-- carries it.
CREATE TABLE channel_outbound_rebuilt (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('question', 'permission', 'review', 'route', 'message')),
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
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER,
  UNIQUE (channel, conversation, kind, binding_ref)
);

INSERT INTO channel_outbound_rebuilt (
  id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering,
  options_json, status, provider_guid, caption_guid, created_at, sent_at,
  resolved_at, last_error, attempts, next_attempt_at
)
SELECT
  id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering,
  options_json, status, provider_guid, caption_guid, created_at, sent_at,
  resolved_at, last_error, attempts, next_attempt_at
FROM channel_outbound;

DROP TABLE channel_outbound;
ALTER TABLE channel_outbound_rebuilt RENAME TO channel_outbound;
CREATE INDEX idx_channel_outbound_status_created ON channel_outbound(channel, status, created_at);
CREATE UNIQUE INDEX idx_channel_outbound_provider_guid ON channel_outbound(channel, provider_guid) WHERE provider_guid IS NOT NULL;
CREATE UNIQUE INDEX idx_channel_outbound_caption_guid ON channel_outbound(channel, caption_guid) WHERE caption_guid IS NOT NULL;

-- Agent text is a fourth per-conversation delivery grant (bit 8). A Discord
-- structural surface is a thread's or issue's own channel, so it speaks there by
-- default; the reconciler installs new rows with the bit, and this carries it
-- onto the rows it already installed. A structural row an operator deliberately
-- silenced (classes 0) stays silent, and follow rows -- the operator's pocket --
-- are untouched.
UPDATE channel_conversation_binding
   SET message_classes = message_classes | 8
 WHERE provider = 'discord'
   AND binding_kind = 'structural'
   AND message_classes != 0;
