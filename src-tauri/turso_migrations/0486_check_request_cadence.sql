-- lineage: shared
-- cairn/7439: the cadence that filed a check request, when a cadence did
-- (`handoff` for a PR-artifact write's lane, `carried` for a base-advance
-- carried head's write lane); NULL for a request someone asked for. Code that
-- must know a row is a handoff lane reads this column, never `requester_ref`,
-- which is a display label.
ALTER TABLE check_requests ADD COLUMN cadence TEXT;
CREATE INDEX idx_check_requests_job_cadence
    ON check_requests(requester_job_id, cadence, suite)
    WHERE cadence IS NOT NULL;
