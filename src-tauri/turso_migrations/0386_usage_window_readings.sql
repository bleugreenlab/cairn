-- lineage: private
-- cairn/6032: a time series of each provider account's usage windows, one row
-- per reading that changed, so a later fit (cairn/6031) can measure how fast
-- each window drains. Written by every usage refresh that produced real
-- windows for a known account (probes, post-launch probes, live Codex events,
-- manual and pool refreshes); a reading identical to the window's previous one
-- is skipped.
--
-- PRIVATE: accounts and their credentials belong to this install, like
-- model_requests (0315).
--
-- window_id is the provider window's stable id (`weekly_opus-opus`);
-- window_name is its family name (`Session`, `Week`, `Opus`, `5-hour`), which
-- is not guaranteed unique. Times are unix seconds. Pruned on insert by
-- cairn-db storage::usage_readings: rows older than 60 days and outside the
-- window's last 8 resets go.

CREATE TABLE usage_window_readings (
  id                   INTEGER PRIMARY KEY,
  provider             TEXT NOT NULL,
  account_id           TEXT NOT NULL,
  window_id            TEXT NOT NULL,
  window_name          TEXT NOT NULL,
  scope                TEXT NOT NULL,
  scope_target         TEXT,
  remaining_percent    REAL NOT NULL,
  window_duration_mins INTEGER,
  resets_at            INTEGER,
  captured_at          INTEGER NOT NULL,
  source               TEXT NOT NULL
);

CREATE INDEX idx_usage_window_readings_window
  ON usage_window_readings(account_id, provider, window_id, captured_at);
