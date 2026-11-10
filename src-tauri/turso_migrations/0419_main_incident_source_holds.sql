-- lineage: shared
-- A main observation whose timing or host-condition reds wait for a second
-- host's row stays an unacknowledged incident source until they are decided
-- (execution/checks_main/corroboration.rs). `held_until` is when the wait ends
-- without one (Unix seconds); NULL is a source that is not waiting. Leaving the
-- source unprocessed keeps main_incident_source_retention_guard protecting the
-- observation, so the held evidence is the canonical row itself.
ALTER TABLE main_incident_sources ADD COLUMN held_until INTEGER;
