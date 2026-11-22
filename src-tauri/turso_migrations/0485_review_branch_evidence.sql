-- lineage: shared
-- A wave's branch-advance evidence survives its mutable lifecycle reason.
ALTER TABLE turn_end_check_attempts ADD COLUMN branch_advance_reason TEXT NOT NULL DEFAULT '';
