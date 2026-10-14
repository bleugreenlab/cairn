-- Bound Project Home's transcript selection before JSON extraction (CAIRN-4621).
--
-- Home merges a project's thread transcripts into one paged timeline. Its two
-- event branches ask for "the newest renderable prose events belonging to these
-- thread runs", and before this migration the schema could not answer that
-- without opening every row:
--
--   SCAN events AS e USING INDEX idx_events_run_type_created
--   SEARCH tr USING INDEX ephemeral_subquery_t5 (run_id=?)
--   USE SORTER FOR ORDER BY
--
-- `idx_events_run_type_created` leads with `run_id`, so it can locate one run's
-- entries but carries nothing about whether a row is prose. With no table
-- statistics to tell it the thread runs are the small side, Turso inverted the
-- join: it walked the WHOLE events index -- 1.7M rows on the reported
-- workspace -- fetched each row from the table to reach `data`, ran
-- `json_type`/`json_extract` over it to decide the row was never a candidate,
-- and only then probed thread membership. The per-branch `ORDER BY ... LIMIT`
-- bounded the ANSWER; nothing bounded the SCAN. Fifteen of those running
-- concurrently is the 555-1509% CPU and 2.9 GiB RSS the live investigation saw.
--
-- The predicate that decides renderability is the expensive part, so it moves
-- into the index. A partial index stores only the rows that satisfy it, and
-- Turso evaluates that predicate at write time rather than per read, so the
-- branch walks candidates directly and never deserializes a row it will
-- discard. Judged by index-only-ness rather than selectivity: the win is the
-- table fetch and JSON parse that no longer happen per candidate row.
--
-- The predicate must stay written exactly as `resources::home::HOME_PROSE_SHAPE`
-- spells it, modulo that query's `e.` alias. Turso substitutes a partial index
-- only when the query's WHERE terms imply the index's, so a rewording on either
-- side silently drops Home back to the full corpus scan above while keeping
-- every entry correct. `home_prose_shape_matches_its_index` pins the two
-- spellings together, and `home_plan_never_scans_the_event_corpus` asserts the
-- resulting plan, since Turso keeps no table statistics and the plan is the only
-- evidence the index is load-bearing.
--
-- `id` and `turn_id` ride along as trailing columns so the candidate walk is
-- index-only: the branch needs `turn_id` to reach the turn a candidate belongs
-- to, and reading it from the index is what keeps the table out of the loop.
--
-- An archived (`zstd`) row qualifies on its envelope alone. Its prose lives in a
-- compressed blob that SQL cannot open, so the index is deliberately generous
-- there and `home_entry_body` drops the empties after decompressing -- the same
-- bargain the pre-existing query struck, preserved exactly. A `zstd` row also
-- never reaches the JSON functions: its `storage_mode` fails the left branch's
-- first test, and the `AND` short-circuits there.
--
-- The `CASE` guards the JSON functions rather than trusting the corpus, and it
-- cannot be flattened into an `AND` chain. Turso raises `malformed JSON` rather
-- than returning NULL, and its `AND` evaluates both sides rather than
-- short-circuiting, so `json_valid(data) AND json_type(data, ...)` still reaches
-- `json_type` with the unparseable body; `CASE` is the one form that
-- short-circuits.
--
-- This matters because a partial index's predicate is evaluated on the WRITE
-- path, once per event inserted: a form that raises would not fail a read, it
-- would make the row unstorable. `events.data` is always valid JSON today -- the
-- `search_events_*` triggers already reject anything else, and all 1.7M rows of
-- the reported corpus parse -- so this index is not what holds that invariant up
-- and does not change what the database accepts. It simply declines to become a
-- second, differently-worded reason to refuse an agent's transcript if the
-- search index's rules ever move.
-- `the_prose_predicate_tolerates_a_body_that_is_not_json` is the regression test.
--
-- Measured with `home_cost_at_reported_scale`, over a corpus built to the row
-- counts observed on the reported workspace (776 thread runs holding 19,316
-- assistant and 1,409 user entries, 31,700 direct and 318 thread messages,
-- 36,076 attention pushes, 1.7M events overall):
--
--   one page of Home       before  9.93 s      after  99 ms
--   the page below it                          after 110 ms
--   building this index    once    2.56 s
--
-- The live workspace stores far larger event bodies than that corpus does, and
-- the bodies are what the old query carried through its sorter, so the gap there
-- is wider than 100x -- the reported symptom was >30 s.
--
-- Building this index reads the `events` table once, parsing each row's JSON.
-- That is the 2.56 s above: one-time work on the first launch after upgrading,
-- against a read cost that was being repaid on every Home fetch and every write
-- invalidation behind it.
DROP INDEX IF EXISTS idx_events_run_type_created;

CREATE INDEX IF NOT EXISTS idx_events_home_prose
    ON events(run_id, event_type, created_at, id, turn_id)
    WHERE ((CASE WHEN json_valid(data) AND (storage_mode IS NULL OR storage_mode='full') THEN json_type(data,'$.content')='text' AND trim(json_extract(data,'$.content'))<>'' ELSE 0 END) OR (storage_mode='zstd' AND data_blob IS NOT NULL AND codec IS NOT NULL));

-- The user branch hides entries that were delivered as attention pushes. With no
-- index on the column it joins, that `NOT EXISTS` had to search the whole
-- `attention_pushes` table (36k rows on the reported workspace) for every
-- candidate event it tested.
CREATE INDEX IF NOT EXISTS idx_attention_pushes_delivered_event
    ON attention_pushes(delivered_event_id);

-- The direct-message branch wants the newest direct messages regardless of
-- channel. `idx_messages_channel` leads (channel_type, channel_id, created_at),
-- so seeking `channel_type='direct'` leaves `created_at` unordered and the
-- branch sorted every direct message the workspace had ever recorded to find
-- fifty. This index orders them, so the walk stops once the page is full.
CREATE INDEX IF NOT EXISTS idx_messages_channel_created
    ON messages(channel_type, created_at, id);
