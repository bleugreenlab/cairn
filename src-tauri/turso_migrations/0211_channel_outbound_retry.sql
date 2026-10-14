-- CAIRN-4477: make channel_outbound a durable retry queue.
ALTER TABLE channel_outbound ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE channel_outbound ADD COLUMN next_attempt_at INTEGER;