-- lineage: private
-- rebuild-fk-off: none
-- cairn/6869: messages addressed to the operator, at cairn://operator.
--
-- A message is one `operator_messages` row: the claim, the channel the sender
-- chose for it (`via`), the default it will act on without a reply, and the
-- headline row it is linked to. Seen and answered are facts of the message, so
-- they live here; delivery is the transport's, so it stays on the one
-- `channel_outbound` row a phone `via` writes (kind `operator`, binding_ref
-- `operator:{id}`), and is derived from it rather than copied.
--
-- `channel_outbound` gains the `operator` kind and `read_at`, the transport's
-- read receipt. The kind CHECK cannot be widened in place, so the table is
-- rebuilt with every current column and its rows copied across, as 0238 did.
-- Nothing holds a foreign key into channel_outbound.
CREATE TABLE channel_outbound_rebuilt (
  id TEXT PRIMARY KEY,
  channel TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('question', 'permission', 'review', 'route', 'message', 'operator')),
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
  last_attempt_at INTEGER,
  read_at INTEGER,
  UNIQUE (channel, conversation, kind, binding_ref)
);

INSERT INTO channel_outbound_rebuilt (
  id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering,
  options_json, status, provider_guid, caption_guid, created_at, sent_at,
  resolved_at, last_error, attempts, next_attempt_at, last_attempt_at
)
SELECT
  id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering,
  options_json, status, provider_guid, caption_guid, created_at, sent_at,
  resolved_at, last_error, attempts, next_attempt_at, last_attempt_at
FROM channel_outbound;

DROP TABLE channel_outbound;
ALTER TABLE channel_outbound_rebuilt RENAME TO channel_outbound;
CREATE INDEX idx_channel_outbound_status_created ON channel_outbound(channel, status, created_at);
CREATE UNIQUE INDEX idx_channel_outbound_provider_guid ON channel_outbound(channel, provider_guid) WHERE provider_guid IS NOT NULL;
CREATE UNIQUE INDEX idx_channel_outbound_caption_guid ON channel_outbound(channel, caption_guid) WHERE caption_guid IS NOT NULL;
CREATE INDEX idx_channel_outbound_conversation ON channel_outbound(conversation, created_at);

CREATE TABLE operator_messages (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  sender_home TEXT NOT NULL,
  sender_job_id TEXT NOT NULL,
  via TEXT NOT NULL CHECK (via IN ('app', 'imessage', 'telegram')),
  content TEXT NOT NULL CHECK (length(content) BETWEEN 1 AND 400),
  default_text TEXT CHECK (default_text IS NULL OR length(default_text) BETWEEN 1 AND 400),
  headline TEXT,
  headline_thread_id TEXT,
  headline_row_id INTEGER,
  created_at INTEGER NOT NULL,
  seen_at INTEGER,
  seen_via TEXT CHECK (seen_via IN ('receipt', 'cursor', 'app')),
  answered_at INTEGER,
  answered_via TEXT CHECK (answered_via IN ('reply', 'app')),
  answer_ref TEXT
);
CREATE INDEX operator_messages_project ON operator_messages(project_id);
CREATE INDEX operator_messages_sender ON operator_messages(sender_home, created_at);
CREATE INDEX operator_messages_headline ON operator_messages(headline_thread_id, headline_row_id) WHERE seen_at IS NULL;
CREATE INDEX operator_messages_waiting ON operator_messages(project_id) WHERE default_text IS NOT NULL;
