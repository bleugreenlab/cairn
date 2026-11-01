-- lineage: shared
-- cairn/5778: when a durable suspension entered `resolving`. A row that stays
-- there past a bound with no live resolver is stuck, and the periodic sweep
-- re-drives it; the same bound decides when a node shows it as stuck. Rows
-- written before this column read their age from `created_at`.
ALTER TABLE agent_waits ADD COLUMN resolving_at INTEGER;
