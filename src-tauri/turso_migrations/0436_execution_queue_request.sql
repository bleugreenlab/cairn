-- lineage: private
-- cairn/6991: read one request's journal rows without a pass over the journal.
--
-- A batch an earlier runner placed is settled from the report of the machine
-- that claimed it, and when its durable row does not name that machine the
-- journal's `claimed` event does (`run::detached::holder_of`). The journal is
-- append-only and never pruned (over a million rows on a busy runner), and no
-- existing index leads with the request, so without this each such lookup was
-- a full scan.
CREATE INDEX idx_execution_queue_request ON execution_queue(request_id);
