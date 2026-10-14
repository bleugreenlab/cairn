-- CAIRN-2481: schema + workflow-tag carriers for the ephemeral call primitive.
--
-- jobs.output_contract holds the JSON-serialized DelegatedOutputContract for a
-- node-less run (an ephemeral call, and going forward a child task), so the
-- artifact-write handler can resolve a per-run output schema without a recipe
-- node. NULL preserves the prior behavior (fall back to the `return` preset).
--
-- runs.label / runs.phase are durable, queryable workflow tags for a call run:
-- the future workflow monitoring panel groups per-call rows by these. No UI
-- consumes them yet.
--
-- jobs and runs are ProjectScoped shared tables, so this is a SHARED_TAIL
-- migration reaching both the private and team lineages identically.
ALTER TABLE jobs ADD COLUMN output_contract TEXT;
ALTER TABLE runs ADD COLUMN label TEXT;
ALTER TABLE runs ADD COLUMN phase TEXT;
