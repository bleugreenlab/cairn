-- lineage: shared
-- cairn/5218: carry each request's prompt-length tier through the token_rollup
-- fold. Haiku 5.5 bills a request whose prompt (input + cache reads + cache
-- writes) is over 100k tokens at five times its short rate, and the rollup sums
-- requests, so it now also stores the part of each component sum that came from
-- long-prompt requests. The long_* columns are subsets of their unprefixed
-- siblings, never added to them. The threshold belongs to the price table
-- (cairn-analytics `pricing::LONG_PROMPT_TOKENS`); the fold reads it.
--
-- SHARED: token_rollup is in both lineages (team_schema_matches_private).
-- Clearing every watermark makes the fold re-derive each run with the new
-- columns filled; the private sibling migration reopens the one-time
-- historical backfill that runs that fold. Existing rows are left in place:
-- the fold replaces a run's rows wholesale, so until it reaches a run, that
-- run keeps reading as it did (long columns 0, priced at the short tier).

ALTER TABLE token_rollup ADD COLUMN long_input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE token_rollup ADD COLUMN long_cache_read_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE token_rollup ADD COLUMN long_cache_create_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE token_rollup ADD COLUMN long_output_tokens INTEGER NOT NULL DEFAULT 0;

DELETE FROM token_rollup_runs;
