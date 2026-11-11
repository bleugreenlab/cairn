-- lineage: shared
-- cairn/6771: a review wave's wall time, measured against the project's
-- `checkBudgets.reviewWaveSeconds`. The wall runs from the lanes' submission
-- (after the wave stops deferring to its node's own work) to the last lane's
-- verdict, so queue and capacity waits count and the settle wait does not.
--
-- `settle_ms` is that deferral, reported beside the wall and never charged.
-- `budget_ms` is the budget the wave's own commit declared; NULL means none.
-- `lane_observations_json` is the JSON array of observation ids this wave's
-- submitted lanes recorded, which is how one wave's rows are cited together:
-- observations are immutable, and a reused lane's row belongs to the wave
-- that ran it, so the link lives on the attempt rather than the observation.
--
-- All NULL on attempts recorded before this column existed and on waves that
-- submitted nothing (every lane answered from recorded verdicts).
ALTER TABLE turn_end_check_attempts ADD COLUMN settle_ms INTEGER;
ALTER TABLE turn_end_check_attempts ADD COLUMN lanes_submitted_at_ms INTEGER;
ALTER TABLE turn_end_check_attempts ADD COLUMN lanes_finished_at_ms INTEGER;
ALTER TABLE turn_end_check_attempts ADD COLUMN budget_ms INTEGER;
ALTER TABLE turn_end_check_attempts ADD COLUMN lane_observations_json TEXT;
