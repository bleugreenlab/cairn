-- Versioned, maintained summary for dashboard tool analytics. The retained
-- dimensions are exactly those consumed by the UI; measures remain additive,
-- except max_duration_s which composes with MAX.
CREATE TABLE tool_analytics_hourly (
    id                  TEXT PRIMARY KEY,
    schema_version      INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    bucket_start        INTEGER NOT NULL,
    project_id          TEXT,
    run_id              TEXT NOT NULL,
    session_id          TEXT,
    verb                TEXT NOT NULL,
    target_scheme       TEXT NOT NULL,
    target_kind         TEXT NOT NULL,
    target_path         TEXT,
    invocation_count    INTEGER NOT NULL,
    error_count         INTEGER NOT NULL,
    duration_count      INTEGER NOT NULL,
    duration_error_count INTEGER NOT NULL,
    duration_sum_s      INTEGER NOT NULL,
    max_duration_s      INTEGER NOT NULL
);

CREATE INDEX idx_tool_analytics_hourly_scope
    ON tool_analytics_hourly(project_id, bucket_start);
CREATE INDEX idx_tool_analytics_hourly_bucket
    ON tool_analytics_hourly(bucket_start);
CREATE INDEX idx_tool_analytics_hourly_run
    ON tool_analytics_hourly(run_id);

-- Fact and summary changes are one SQLite statement/transaction. A failed
-- summary update therefore rolls the fact mutation back to the savepoint, so
-- the caller's logged rollup failure cannot commit a stale materialization.
CREATE TRIGGER tool_analytics_after_insert AFTER INSERT ON tool_invocations BEGIN
  INSERT INTO tool_analytics_hourly (
    id, schema_version, bucket_start, project_id, run_id, session_id, verb,
    target_scheme, target_kind, target_path, invocation_count, error_count,
    duration_count, duration_error_count, duration_sum_s, max_duration_s)
  VALUES (
    printf('%d:%s:%s:%s:%s:%s:%s', (NEW.ts / 3600) * 3600, hex(NEW.run_id),
      hex(COALESCE((SELECT session_id FROM runs WHERE id = NEW.run_id), '')),
      hex(NEW.verb), hex(NEW.target_scheme), hex(NEW.target_kind),
      hex(COALESCE(NEW.target_path, ''))),
    1, (NEW.ts / 3600) * 3600,
    COALESCE((SELECT project_id FROM runs WHERE id = NEW.run_id),
      (SELECT j.project_id FROM runs r JOIN jobs j ON j.id = r.job_id WHERE r.id = NEW.run_id)),
    NEW.run_id, (SELECT session_id FROM runs WHERE id = NEW.run_id), NEW.verb,
    NEW.target_scheme, NEW.target_kind, NEW.target_path, 1, NEW.is_error,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN 1 ELSE 0 END,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN NEW.is_error ELSE 0 END,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN NEW.result_ts - NEW.ts ELSE 0 END,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN NEW.result_ts - NEW.ts ELSE 0 END)
  ON CONFLICT(id) DO UPDATE SET
    invocation_count = invocation_count + 1,
    error_count = error_count + excluded.error_count,
    duration_count = duration_count + excluded.duration_count,
    duration_error_count = duration_error_count + excluded.duration_error_count,
    duration_sum_s = duration_sum_s + excluded.duration_sum_s,
    max_duration_s = MAX(max_duration_s, excluded.max_duration_s);
END;

CREATE TRIGGER tool_analytics_before_update BEFORE UPDATE OF is_error, result_ts ON tool_invocations BEGIN
  UPDATE tool_analytics_hourly SET
    invocation_count = invocation_count - 1,
    error_count = error_count - OLD.is_error,
    duration_count = duration_count - CASE WHEN OLD.result_ts IS NOT NULL AND OLD.result_ts >= OLD.ts THEN 1 ELSE 0 END,
    duration_error_count = duration_error_count - CASE WHEN OLD.result_ts IS NOT NULL AND OLD.result_ts >= OLD.ts THEN OLD.is_error ELSE 0 END,
    duration_sum_s = duration_sum_s - CASE WHEN OLD.result_ts IS NOT NULL AND OLD.result_ts >= OLD.ts THEN OLD.result_ts - OLD.ts ELSE 0 END,
    max_duration_s = CASE WHEN OLD.result_ts IS NOT NULL AND OLD.result_ts - OLD.ts = max_duration_s THEN 0 ELSE max_duration_s END
  WHERE id = printf('%d:%s:%s:%s:%s:%s:%s', (OLD.ts / 3600) * 3600, hex(OLD.run_id),
    hex(COALESCE((SELECT session_id FROM runs WHERE id = OLD.run_id), '')),
    hex(OLD.verb), hex(OLD.target_scheme), hex(OLD.target_kind), hex(COALESCE(OLD.target_path, '')));
  DELETE FROM tool_analytics_hourly WHERE invocation_count = 0;
END;

CREATE TRIGGER tool_analytics_after_update AFTER UPDATE OF is_error, result_ts ON tool_invocations BEGIN
  INSERT INTO tool_analytics_hourly (
    id, schema_version, bucket_start, project_id, run_id, session_id, verb,
    target_scheme, target_kind, target_path, invocation_count, error_count,
    duration_count, duration_error_count, duration_sum_s, max_duration_s)
  SELECT printf('%d:%s:%s:%s:%s:%s:%s', (NEW.ts / 3600) * 3600, hex(NEW.run_id),
      hex(COALESCE(r.session_id, '')), hex(NEW.verb), hex(NEW.target_scheme),
      hex(NEW.target_kind), hex(COALESCE(NEW.target_path, ''))),
    1, (NEW.ts / 3600) * 3600, COALESCE(r.project_id, j.project_id), NEW.run_id,
    r.session_id, NEW.verb, NEW.target_scheme, NEW.target_kind, NEW.target_path,
    1, NEW.is_error,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN 1 ELSE 0 END,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN NEW.is_error ELSE 0 END,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN NEW.result_ts - NEW.ts ELSE 0 END,
    CASE WHEN NEW.result_ts IS NOT NULL AND NEW.result_ts >= NEW.ts THEN NEW.result_ts - NEW.ts ELSE 0 END
  FROM runs r LEFT JOIN jobs j ON j.id = r.job_id WHERE r.id = NEW.run_id
  ON CONFLICT(id) DO UPDATE SET
    invocation_count = invocation_count + 1, error_count = error_count + excluded.error_count,
    duration_count = duration_count + excluded.duration_count,
    duration_error_count = duration_error_count + excluded.duration_error_count,
    duration_sum_s = duration_sum_s + excluded.duration_sum_s,
    max_duration_s = MAX(max_duration_s, excluded.max_duration_s);
  UPDATE tool_analytics_hourly SET max_duration_s = COALESCE((
    SELECT MAX(ti.result_ts - ti.ts) FROM tool_invocations ti
    WHERE ti.run_id = NEW.run_id AND (ti.ts / 3600) * 3600 = (NEW.ts / 3600) * 3600
      AND ti.verb = NEW.verb AND ti.target_scheme = NEW.target_scheme
      AND ti.target_kind = NEW.target_kind AND ti.target_path IS NEW.target_path
      AND ti.result_ts IS NOT NULL AND ti.result_ts >= ti.ts
  ), 0)
  WHERE id = printf('%d:%s:%s:%s:%s:%s:%s', (NEW.ts / 3600) * 3600, hex(NEW.run_id),
    hex(COALESCE((SELECT session_id FROM runs WHERE id = NEW.run_id), '')),
    hex(NEW.verb), hex(NEW.target_scheme), hex(NEW.target_kind), hex(COALESCE(NEW.target_path, '')));
END;

-- Invalidates the old materialization explicitly. Startup's incremental tool
-- backfill sees every run as stale and rebuilds both atomic facts and summaries.
DELETE FROM tool_invocation_runs;