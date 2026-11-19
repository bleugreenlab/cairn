-- lineage: shared
-- cairn/7348: every transcript event a run owns carries that run's session.
--
-- An open chat follows its session through forward deltas selected by
-- `events.session_id`. Writers outside the provider stream (operator request
-- cards, provider-limit resume notices, synthetic error events) stored a NULL
-- session, so a live reader skipped them and they appeared only after a cold
-- reload. The insert seam now fills the session from the run; this backfills the
-- rows written before it, so they appear on every session read as well.
--
-- `idx_events_session_id` keys the NULL rows, so only they are visited, and each
-- probes `runs` by primary key. Events of a run that never had a session stay
-- as they are: no session reader exists to miss them.
UPDATE events
SET session_id = (SELECT r.session_id FROM runs r WHERE r.id = events.run_id)
WHERE session_id IS NULL
  AND EXISTS (
      SELECT 1 FROM runs r WHERE r.id = events.run_id AND r.session_id IS NOT NULL
  );
