-- Per-thread transcript read cursor for the local operator (CAIRN-4195).
--
-- A sidebar thread row shows how many transcript entries have landed since this
-- operator last actually looked at the thread. That marker is a property of THIS
-- person at THIS install, not of the thread: replicating it would let a
-- teammate's reading clear your unread badge, and there is no second operator on
-- a private database to disambiguate. So the table is private-only, classified
-- `Private(PrivateReason::IdentityCredential)`-adjacent under RunnerTransient's
-- sibling reasoning and asserted absent from the team projection by
-- `team_schema_matches_private`.
--
-- The watermark is a global `events.rowid`, not a timestamp. Event rowids are
-- monotonic per insert and are already the cursor the transcript delta walks
-- (`runs::queries::list_events_for_session_delta`), so the same number means the
-- same position in both places, survives session rotation (a rotated successor
-- appends higher rowids under the same job), and cannot be skewed by a client
-- clock. The frontend never supplies it: `mark_thread_viewed` selects the
-- server's own latest eligible rowid and advances monotonically.
--
-- The thread id is the key rather than the session or job id precisely so a
-- rename or a session rotation leaves the position where it was.
CREATE TABLE IF NOT EXISTS thread_read_positions (
  thread_id                TEXT PRIMARY KEY NOT NULL
                             REFERENCES threads(id) ON DELETE CASCADE,
  -- Highest `events.rowid` this operator has been shown. 0 means "nothing read",
  -- which is also the answer for a thread with no row at all, so a missing row
  -- and a fresh row agree.
  acknowledged_event_rowid INTEGER NOT NULL DEFAULT 0,
  updated_at               INTEGER NOT NULL
);
