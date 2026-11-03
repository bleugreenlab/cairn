-- lineage: shared
-- cairn/5619: a run's channel catch-up, and the pending-delivery chips that
-- preview it, read one channel's messages past a session cursor, newest first.
-- `idx_messages_channel` orders a channel by `created_at`, so it cannot serve the
-- rowid order the cursor is defined in, and with the channel bound as parameters
-- Turso preferred `idx_messages_channel_created` on `channel_type` alone: every
-- issue message in the workspace per call (55-160 ms p50 on the 11-02 production
-- clone). This index's implicit trailing rowid is the cursor's order, so the read
-- walks one channel newest first and stops at its limit (0.1-0.5 ms).
--
-- Shared: `messages` is ProjectScoped and both lineages carry it. On the 11-02
-- clone (152k messages) the build took 2.3 s warm and 16 s cold, which is this
-- migration's one-time startup cost.
CREATE INDEX IF NOT EXISTS idx_messages_channel_seq
    ON messages (channel_type, channel_id);
