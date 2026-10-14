-- CAIRN-1196: Queue mid-turn direct messages and inject them at the next prompt boundary.
--
-- Per-message delivery state lets the message handler (a) persist a direct
-- message without trying to resume the recipient mid-turn and (b) lets the
-- two injection paths (Claude hook additionalContext, Cairn-tool result
-- augmentation) atomically claim pending directs so they aren't delivered
-- twice. Channel messages don't use this column — they continue with the
-- per-process in-memory cursor.

ALTER TABLE messages ADD COLUMN delivered_at INTEGER DEFAULT NULL;

-- Pending-direct pull lives on the recipient run and runs on every Claude
-- tool boundary + every Cairn tool dispatch, so it wants a fast lookup.
CREATE INDEX idx_messages_pending_directs
    ON messages(recipient_run_id, delivered_at)
    WHERE channel_type = 'direct' AND delivered_at IS NULL;
