-- Migration: record which commit DEFINED the check a result describes (CAIRN-3333)
--
-- Shared across the private and team lineages. A check result already names the
-- commit whose content was evaluated; it could not name the commit whose
-- `.cairn/config.yaml` declared the check that produced it. Those were assumed
-- to be the same tree and were not: a check defined only on one agent branch
-- executed inside a sibling job's cadence, against the sibling's tree, and the
-- resulting row was indistinguishable from one the sibling's own config asked
-- for. `defined_by_commit_sha` makes that shape visible from the row alone.
--
-- On `check_result_observations` it is the defining commit of the executed
-- definition. On `check_result_commit_aliases` it is the defining commit at the
-- ALIASED commit -- for a cache hit, the evaluated commit's own definition,
-- which the reuse was admitted under; the linked source observation keeps its
-- own. On `check_result_cache` it is the defining commit of the row's current
-- source observation, so a direct query of the hot table diagnoses a leak
-- without a join.
--
-- Nullable with no backfill: historical rows have no truthful answer, and
-- inventing one would be the same false equivalence this column exists to
-- expose. Reuse refuses rows whose provenance is absent, so a null is
-- diagnostic-only rather than silently trusted.
--
-- Plain ADD COLUMNs keep this sync-safe; do not rebuild synced replica tables
-- (see TEAM_TAIL in migrations.rs).

ALTER TABLE check_result_observations ADD COLUMN defined_by_commit_sha TEXT;
ALTER TABLE check_result_commit_aliases ADD COLUMN defined_by_commit_sha TEXT;
ALTER TABLE check_result_cache ADD COLUMN defined_by_commit_sha TEXT;
