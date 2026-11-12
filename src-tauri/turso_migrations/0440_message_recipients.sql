-- lineage: shared
-- cairn/5537: a direct message's addressees live in `message_recipients`, one
-- row per recipient, so one stored message can go to several addresses. The
-- row names the recipient's job, the durable addressee delivery already uses
-- (`attention_pushes.recipient`); the run that was live at send is kept only as
-- provenance. `position` 0 is the append target, then the cc entries in the
-- order the sender named them. `wake` is what this recipient's own mute made
-- of the sender's urgency; history written before this migration recorded
-- none. `created_at` copies the message's, so a recipient's history seeks and
-- orders on this table alone.
CREATE TABLE message_recipients (
    message_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    recipient_job_id TEXT,
    recipient_run_id TEXT,
    created_at INTEGER NOT NULL,
    wake TEXT CHECK (wake IN ('passive', 'wake', 'interrupt')),
    PRIMARY KEY (message_id, position)
);
CREATE INDEX idx_message_recipients_job
    ON message_recipients (recipient_job_id, created_at, message_id);

-- Every addressed direct becomes its recipient's row. A direct whose run row
-- is gone keeps the run id and has no job; a direct that never named a
-- recipient run gets no row.
INSERT OR IGNORE INTO message_recipients
    (message_id, position, recipient_job_id, recipient_run_id, created_at, wake)
SELECT m.id, 0, r.job_id, m.recipient_run_id, m.created_at, NULL
  FROM messages m
  LEFT JOIN runs r ON r.id = m.recipient_run_id
 WHERE m.channel_type = 'direct' AND m.recipient_run_id IS NOT NULL;

-- Dropped in place rather than by a table rebuild, so the Turso sync triggers
-- on the shared `messages` table and its search triggers stay intact.
DROP INDEX IF EXISTS idx_messages_recipient;
ALTER TABLE messages DROP COLUMN recipient_run_id;
