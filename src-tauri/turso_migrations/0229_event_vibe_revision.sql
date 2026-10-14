-- The skyline delta protocol asks one question per read: has anything the
-- reader holds changed? For events the answer is sound from aggregates alone —
-- they append and are never rewritten, so (COUNT, MAX(created_at)) summarizes
-- them exactly. For vibes it was not, and the protocol was built on the
-- assumption that it was (CAIRN-4710).
--
-- A vibe write is an UPSERT keyed on event_id: re-embedding an event replaces
-- its color in place. That holds COUNT(*) constant, and because created_at is
-- epoch SECONDS while the embed worker persists a whole batch inside one
-- second, it can hold MAX(created_at) constant too. Every replacement after the
-- first in a given second was therefore invisible to both aggregates, and a
-- reader — along with the persisted bar cache behind it — kept the stale color
-- indefinitely despite the write having emitted its invalidation.
--
-- `revision` is a value that moves on every write instead. The writer assigns
-- it MAX(revision)+1 over the table, through an upsert that updates the row in
-- place rather than deleting and re-inserting it, so the assigned value is
-- strictly greater than every existing one INCLUDING the row being replaced.
-- (A delete-then-insert form cannot promise that: dropping the row that holds
-- the maximum lowers it, and max+1 hands back the same number.)
ALTER TABLE event_vibes ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;

-- The writer derives each new revision from the newest existing one, and the
-- embed worker persists a whole batch one row at a time. Without this index
-- that derivation is a full table scan per write, so a batch of M vibes against
-- N accumulated ones costs O(M*N) — a write-side regression traded for the
-- read-side win, which is no trade at all. The writer reads the edge with
-- `ORDER BY revision DESC LIMIT 1` rather than `MAX(revision)` because that is
-- the form this engine answers by seeking the index rather than walking it (see
-- docs/database.md); `event_vibe_revision_is_assigned_from_the_index_edge`
-- asserts the plan so a future rewrite cannot quietly reintroduce the scan.
CREATE INDEX idx_event_vibes_revision ON event_vibes(revision);

-- The skyline cache records the vibe watermark it was built at, so that column
-- now holds a revision rather than a timestamp. Rename it to say what it is.
ALTER TABLE session_skyline_cache RENAME COLUMN latest_vibe_created_at TO vibe_revision;

-- Every cached row's stored value is a timestamp under the old meaning, which
-- would compare against a revision as an absurdly distant watermark. Drop them:
-- the cache is purely derived and rebuilds transparently on the next read.
DELETE FROM session_skyline_cache;
