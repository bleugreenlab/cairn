ALTER TABLE github_app ADD COLUMN relay_health_state TEXT;
ALTER TABLE github_app ADD COLUMN relay_health_reason TEXT;
ALTER TABLE github_app ADD COLUMN relay_first_failure_at TEXT;
ALTER TABLE github_app ADD COLUMN relay_last_failure_at TEXT;
ALTER TABLE github_app ADD COLUMN relay_consecutive_failures INTEGER;
ALTER TABLE github_app ADD COLUMN relay_failing_event_id TEXT;
ALTER TABLE github_app ADD COLUMN relay_failing_event_at TEXT;
ALTER TABLE github_app ADD COLUMN relay_last_successful_delivery_at TEXT;