-- Order turns by recency without sorting the whole table (CAIRN-3810).
--
-- The semantic lane's sweep asks for the newest turns still lacking a vector,
-- which is an ORDER BY created_at DESC with a small LIMIT. Without this index
-- every pass sorts the entire turns table to return a page of sixty-four.
-- `turns` is a shared table, so the index is composed into both lineages.

CREATE INDEX idx_turns_created_at ON turns (created_at);
