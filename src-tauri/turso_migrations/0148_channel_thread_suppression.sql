-- Preserve the one catch-up summary across channel/router restarts after
-- followed-thread updates were intentionally dropped while the operator was active.
ALTER TABLE channel_thread_follow
  ADD COLUMN suppressed_updates INTEGER NOT NULL DEFAULT 0 CHECK (suppressed_updates >= 0);