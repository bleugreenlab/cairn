-- CAIRN-4911: stamp each provider attempt on its outbound row.
--
-- A failed row is channel status only while its latest attempt is later than
-- the channel's latest successful send, so an attempt needs a real time:
-- claiming a delivery stamps it, and recording the failure overwrites it.
-- Existing rows stay NULL and fall back to their schedule or creation.
ALTER TABLE channel_outbound ADD COLUMN last_attempt_at INTEGER;

-- A retired route was failed with a retry scheduled at i64::MAX, a schedule no
-- sweep honors and one that would read as a current failure forever. A row
-- that is never re-sent now carries no schedule.
UPDATE channel_outbound
   SET next_attempt_at = NULL
 WHERE status = 'failed' AND next_attempt_at = 9223372036854775807;
