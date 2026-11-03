-- lineage: shared
-- cairn/5979: the model the provider reported serving each billable event,
-- stored as a column so the token_rollup fold can attribute an event whose job
-- selected no model (long-lived thread jobs that predate stored selections).
-- The model already sits in the event's JSON body, but archival rewrites that
-- body to an envelope over a compressed blob SQL cannot open, so a re-fold
-- reading the JSON would silently lose the attribution. A column survives
-- archival, like the token columns. Written by the transcript insert seam for
-- top-level assistant and result events; the reopened historical backfill
-- fills it for runs of model-less jobs, reconstructing archived bodies.
--
-- The fold also now keys token_rollup.model canonically (one key per model:
-- `Opus`, `opus[1m]` and `claude-opus-5-5` are all `opus`), so every run is
-- re-derived: clearing the watermarks makes the fold revisit each run, and
-- the private sibling reopens the one-time backfill that runs it. Existing
-- rows read as before until the fold replaces each run's rows wholesale.
--
-- SHARED: events and token_rollup are in both lineages; a plain ADD COLUMN
-- keeps it sync-safe.

ALTER TABLE events ADD COLUMN reported_model TEXT;

DELETE FROM token_rollup_runs;
