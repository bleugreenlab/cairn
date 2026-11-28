-- lineage: shared
-- cairn/7752: where each recently served skyline state's prefix ends, so the
-- events after it are sought rather than counted past.
--
-- A JSON array of `{eventRevision, eventCount, edge}`, newest last, written by
-- the skyline reader with the bars (`SkylinePosition` in the transport's
-- skyline commands). A row without it still serves: its next tail is read by
-- count as before, and the store that follows records the position.
ALTER TABLE session_skyline_cache ADD COLUMN event_positions TEXT;
