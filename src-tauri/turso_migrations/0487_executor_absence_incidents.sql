-- lineage: private
-- The bounded runner journal is evidence, not the incident's lifetime.
-- Keep machine facts, episode watermarks and a crash-safe notice outbox after
-- runner_events retention. Forgetting enrollment stops monitoring; it does not
-- erase the explanation of a prior absence.
CREATE TABLE executor_absence_incidents (
    executor_id TEXT PRIMARY KEY NOT NULL,
    state_json TEXT NOT NULL
);
CREATE TABLE executor_absence_notices (
    idempotency_key TEXT PRIMARY KEY NOT NULL,
    executor_id TEXT NOT NULL,
    content TEXT NOT NULL,
    delivered_at_ms INTEGER
);
CREATE INDEX idx_executor_absence_notices_pending
ON executor_absence_notices(delivered_at_ms)
WHERE delivered_at_ms IS NULL;
