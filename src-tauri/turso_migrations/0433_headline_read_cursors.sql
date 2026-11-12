-- lineage: private
-- cairn/6866: how far the operator has read each thread's headlines.
--
-- A thread's headlines live in its own store (thread migration 0006); this is
-- the one fact about them that is the reader's, not the thread's: the highest
-- headline id the operator has been shown. A line with a greater id is new to
-- him, and the thread's seed says so, which is how "unread" and "unanswered"
-- stop looking the same to a thread.
--
-- The cursor is per reader, not per device: the question a thread asks is
-- "does he know", and he reads on more than one screen. `reader` is the literal
-- `operator` the system already attributes operator authorship to, until an
-- operator principal id exists to carry instead. `seen_on` ('desktop' | 'ios')
-- records where the last advance came from, for diagnosis only.
--
-- Private, like `thread_read_positions`: one person's reading on this install,
-- which a replica must not let a teammate clear. Writes only move
-- `seen_through_id` forward (MAX of old and new), and the writer clamps it to
-- the store's newest id, so a cursor never claims a line not yet written.
--
-- The key leads with `thread_id` so a thread's delete cascades by seeking it
-- rather than walking every reader's rows; a (reader, thread) lookup binds
-- both columns and seeks it all the same.
CREATE TABLE headline_read_cursors (
  reader          TEXT NOT NULL,
  thread_id       TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
  seen_through_id INTEGER NOT NULL,
  seen_at         INTEGER NOT NULL,
  seen_on         TEXT,
  PRIMARY KEY (thread_id, reader)
);
