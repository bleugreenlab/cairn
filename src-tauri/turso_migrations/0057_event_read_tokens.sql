-- Per-event token cache for the lean read-transcript projection (CAIRN-1593).
--
-- Keyed by the immutable durable event id, so a row is permanently valid once
-- written — no watermark/staleness columns are needed (unlike the skyline
-- cache). The presence of a row also signals "this event is a read tool-result"
-- to the projection: hot query paths LEFT JOIN this table and strip the read
-- body + attach counts wherever a row exists. ON DELETE CASCADE drops the cache
-- row when the originating event is removed (e.g. archival rewrites).
CREATE TABLE event_read_tokens (
    event_id      TEXT PRIMARY KEY NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    segments_json TEXT NOT NULL,    -- ordered [{target, tokens}] in read section order
    total_tokens  INTEGER NOT NULL,
    created_at    INTEGER NOT NULL
);
