-- lineage: shared
-- No recipe requires a tenet any more (cairn/5452). A thread holds a filing on
-- a tenet only by naming a `tenet_verdict` clause in its own `after`, so the
-- per-project list of recipes a thread could file only behind a tenet has no
-- reader. Held executions keep their gate rows untouched: those are ordinary
-- `execution_gates` clauses and never read this column.
--
-- Dropped in place rather than by a table rebuild, so the Turso sync triggers
-- on the shared `projects` table stay intact.
ALTER TABLE projects DROP COLUMN tenet_gated_recipes;
