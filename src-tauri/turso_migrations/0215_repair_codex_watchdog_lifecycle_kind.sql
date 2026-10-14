CREATE TABLE codex_watchdog_lifecycle_rebuilt (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    provider_turn_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
      'armed', 'phase_transition', 'heartbeat', 'disarmed', 'expired',
      'worker_panicked', 'worker_exited_unexpectedly', 'recovery_claimed',
      'successor_reserved', 'terminalization_succeeded', 'terminalization_failed',
      'successor_started', 'successor_failed'
    )),
    phase TEXT CHECK (phase IS NULL OR phase IN ('provider_progress', 'tool_outstanding', 'post_tool_continuation')),
    reason TEXT,
    details_json TEXT,
    successor_run_id TEXT REFERENCES runs(id),
    successor_turn_id TEXT,
    created_at INTEGER NOT NULL
);

INSERT INTO codex_watchdog_lifecycle_rebuilt (
  id, run_id, session_id, provider_turn_id, generation, kind, phase, reason,
  details_json, successor_run_id, successor_turn_id, created_at
)
SELECT
  id, run_id, session_id, provider_turn_id, generation, kind, phase, reason,
  details_json, successor_run_id, successor_turn_id, created_at
FROM codex_watchdog_lifecycle;

DROP TABLE codex_watchdog_lifecycle;
ALTER TABLE codex_watchdog_lifecycle_rebuilt RENAME TO codex_watchdog_lifecycle;

CREATE INDEX idx_codex_watchdog_lifecycle_identity
  ON codex_watchdog_lifecycle(run_id, session_id, provider_turn_id, generation, created_at);