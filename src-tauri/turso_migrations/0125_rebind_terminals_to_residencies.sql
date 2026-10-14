-- Migration: bind terminals to the residency that holds their cell.
--
-- A terminal is a process inside an execution environment, not the holder of a
-- lease of its own, so its durable fence is now (holder, incarnation, cell
-- epoch). The holder is stored in its compact `class:id` form, which is what
-- `ResidencyHolder::storage_key` produces and `parse_storage_key` reads back.
--
-- The `lease_id` / `lease_incarnation_id` / `lease_epoch` columns added by 0113
-- are left in place and dormant. `job_terminals` is carried by the synced team
-- lineage, where a create-copy-drop-rename column removal breaks Turso sync
-- triggers; nothing reads or writes them after this change, and every fence they
-- recorded is dead anyway, because the cells behind them are retired when this
-- build first adopts.
ALTER TABLE job_terminals ADD COLUMN residency_holder TEXT;
ALTER TABLE job_terminals ADD COLUMN residency_incarnation_id TEXT;
ALTER TABLE job_terminals ADD COLUMN cell_epoch INTEGER;
CREATE INDEX idx_job_terminals_residency_holder ON job_terminals(residency_holder) WHERE residency_holder IS NOT NULL;
