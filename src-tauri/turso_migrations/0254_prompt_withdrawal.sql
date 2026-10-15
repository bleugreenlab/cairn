-- cairn/4955: a question can be withdrawn rather than answered.
--
-- An operator's cold Compact kills the session a pending question belongs to,
-- and nothing may answer into that dead session afterwards. `prompts` has no
-- status column (pending means `response IS NULL`), and writing a sentinel into
-- `response` would fabricate an answer, so withdrawal is its own pair of
-- columns: when, and the typed `WithdrawalCause` (snake_case) that explains it.
-- Every pending-question predicate excludes a withdrawn row, and the answer
-- path refuses one with that cause.
--
-- `prompts` is shared project state, so both lineages carry the columns. Both
-- are additive ALTERs, never a rebuild: the team replica's sync triggers ride on
-- this table.
ALTER TABLE prompts ADD COLUMN withdrawn_at INTEGER;
ALTER TABLE prompts ADD COLUMN withdrawn_cause TEXT;
