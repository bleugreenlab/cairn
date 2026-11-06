-- lineage: shared
-- cairn/6032: the provider account that served each billable event, stored on
-- the event and carried into token_rollup as a grouped dimension, so token
-- spend can be read per account.
--
-- The transcript insert seam stamps events.account_id on rollup-relevant
-- events (assistant, result%, or a priced event) from runs.account_id at
-- insert time: the account that launched the process now serving the run.
-- Freezing it on the event, rather than joining token_rollup to
-- runs.account_id, is what keeps a run whose account changed mid-run (a cold
-- restart onto another account) split correctly, and what a re-fold reads.
--
-- token_rollup_runs is deliberately NOT cleared. Every existing event has a
-- NULL account, and a NULL account leaves the rollup id unchanged, so a
-- re-fold would rebuild exactly the rows that already exist. Rows from before
-- this change stay unattributed; nothing is guessed.
--
-- SHARED: events and token_rollup are in both lineages; a plain ADD COLUMN
-- keeps it sync-safe.

ALTER TABLE events ADD COLUMN account_id TEXT;

ALTER TABLE token_rollup ADD COLUMN account_id TEXT;
