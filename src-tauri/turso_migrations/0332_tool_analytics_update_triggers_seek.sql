-- lineage: shared
-- cairn/5907: the two triggers a tool result's `UPDATE tool_invocations` fires
-- walked whole tables inside the transcript writer's transaction. Every
-- `tool_result` event pays this, and with the page cache cleared on every write
-- each walked page is a read: a production clone measured 84 s for one cold
-- update and 278 ms warm, against 0.35 s and 23 ms after this migration.
--
-- `tool_analytics_before_update` swept `DELETE ... WHERE invocation_count = 0`
-- over the whole summary table (a full scan: no index carries the count). The
-- only row that can reach zero is the bucket the trigger has just decremented,
-- since inserts start at one and nothing else writes the table, so the sweep is
-- scoped to that bucket's key, which the primary key answers with one seek.
--
-- `tool_analytics_after_update` recomputes the bucket's longest duration with
-- a `MAX` over the run's invocations in that bucket. Without statistics the
-- planner chose `idx_tool_invocations_shape (verb, target_scheme, target_kind)`,
-- three equality columns, over `idx_tool_invocations_run (run_id)`, so it walked
-- every invocation of that tool shape across all runs (143k rows for a Rust
-- file read) where one run holds at most a few hundred. `INDEXED BY` names the
-- run index. `cairn-analytics`' `tool_result_update_seeks_its_buckets` pins
-- both plans.
DROP TRIGGER IF EXISTS tool_analytics_before_update;

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
  DELETE FROM tool_analytics_hourly
  WHERE id = printf('%d:%s:%s:%s:%s:%s:%s', (OLD.ts / 3600) * 3600, hex(OLD.run_id),
    hex(COALESCE((SELECT session_id FROM runs WHERE id = OLD.run_id), '')),
    hex(OLD.verb), hex(OLD.target_scheme), hex(OLD.target_kind), hex(COALESCE(OLD.target_path, '')))
    AND invocation_count = 0;
END;

DROP TRIGGER IF EXISTS tool_analytics_after_update;

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
    SELECT MAX(ti.result_ts - ti.ts) FROM tool_invocations ti INDEXED BY idx_tool_invocations_run
    WHERE ti.run_id = NEW.run_id AND (ti.ts / 3600) * 3600 = (NEW.ts / 3600) * 3600
      AND ti.verb = NEW.verb AND ti.target_scheme = NEW.target_scheme
      AND ti.target_kind = NEW.target_kind AND ti.target_path IS NEW.target_path
      AND ti.result_ts IS NOT NULL AND ti.result_ts >= ti.ts
  ), 0)
  WHERE id = printf('%d:%s:%s:%s:%s:%s:%s', (NEW.ts / 3600) * 3600, hex(NEW.run_id),
    hex(COALESCE((SELECT session_id FROM runs WHERE id = NEW.run_id), '')),
    hex(NEW.verb), hex(NEW.target_scheme), hex(NEW.target_kind), hex(COALESCE(NEW.target_path, '')));
END;
