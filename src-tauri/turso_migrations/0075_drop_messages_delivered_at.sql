-- CAIRN-1906: drop the retired messages.delivered_at column.
--
-- The direct-message delivered_at pull path was retired by the push-queue
-- migration (CAIRN-1900); its last reader, the cairn watch external-reply
-- catch-up cursor, now keys on the reply message's own created_at. Drop the
-- partial index first (it references the column), then the column. No trigger or
-- view references delivered_at, so the column drop is a clean in-place ALTER.

DROP INDEX IF EXISTS idx_messages_pending_directs;
ALTER TABLE messages DROP COLUMN delivered_at;
