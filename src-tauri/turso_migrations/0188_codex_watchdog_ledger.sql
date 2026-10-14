-- Host-local ownership and append-only evidence for Codex provider-turn watchdogs.
CREATE TABLE codex_watchdog_leases (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    provider_turn_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    runner_boot_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('provider_progress', 'tool_outstanding', 'post_tool_continuation')),
    phase_deadline_at INTEGER NOT NULL,
    last_provider_progress_at INTEGER NOT NULL,
    last_worker_heartbeat_at INTEGER NOT NULL,
    armed_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'expired', 'recovery_claimed', 'disarmed', 'terminalized', 'recovery_failed')),
    terminal_reason TEXT,
    successor_run_id TEXT REFERENCES runs(id),
    successor_turn_id TEXT,
    PRIMARY KEY (run_id, session_id, provider_turn_id)
);

CREATE INDEX idx_codex_watchdog_leases_active_deadline
  ON codex_watchdog_leases(state, phase_deadline_at, last_worker_heartbeat_at);
CREATE INDEX idx_codex_watchdog_leases_session
  ON codex_watchdog_leases(session_id, updated_at DESC);

CREATE TABLE codex_watchdog_lifecycle (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    provider_turn_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
      'armed', 'phase_transition', 'heartbeat', 'disarmed', 'expired',
      'worker_panicked', 'worker_exited_unexpectedly', 'recovery_claimed',
      'terminalization_succeeded', 'terminalization_failed',
      'successor_started', 'successor_failed'
    )),
    phase TEXT CHECK (phase IS NULL OR phase IN ('provider_progress', 'tool_outstanding', 'post_tool_continuation')),
    reason TEXT,
    details_json TEXT,
    successor_run_id TEXT REFERENCES runs(id),
    successor_turn_id TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_codex_watchdog_lifecycle_identity
  ON codex_watchdog_lifecycle(run_id, session_id, provider_turn_id, generation, created_at);
