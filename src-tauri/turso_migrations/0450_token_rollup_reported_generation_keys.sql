-- lineage: shared
-- cairn/7127: a launch that named a bare Claude alias (`fable`, `opus`) now
-- keys token_rollup.model by the generation the provider reported serving each
-- event (`claude-fable-5-1`), not by the alias, so moving an alias to a new
-- generation no longer re-prices the usage of the old one. The stored key no
-- longer depends on which generation an alias currently means; readers fold
-- the current generation onto its alias at read time.
--
-- Clearing the watermarks makes the fold re-derive every run under the new
-- key; the private sibling reopens the one-time backfill that first fills
-- events.reported_model for alias launches' events stored before that column,
-- then runs the fold. Existing rows read as before until the fold replaces
-- each run's rows wholesale.
--
-- SHARED: token_rollup_runs is in both lineages.

DELETE FROM token_rollup_runs;
