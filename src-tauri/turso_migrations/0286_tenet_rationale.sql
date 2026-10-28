-- A tenet's statement and its rationale are separate fields (cairn/5433).
--
-- The statement is the one sentence the operator answers yes or no at a
-- glance; `rationale` carries what the operator may open beneath it: why,
-- the alternatives ruled out, and the canonical URIs where it was argued.
--
-- Tenets are immutable, so nothing is backfilled: an existing row keeps its
-- statement exactly as proposed and has no rationale. A migrated ruling's
-- original rationale stays where it was imported, in `legacy_json`.
ALTER TABLE tenets ADD COLUMN rationale TEXT;
