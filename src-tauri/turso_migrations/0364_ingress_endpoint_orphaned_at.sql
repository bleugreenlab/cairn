-- lineage: private
-- cairn/5969: an endpoint left behind when the operator re-registers a relay
-- channel the api rejects. Its row is revoked, since nothing on this install
-- receives its events any more, and `orphaned_at` (epoch milliseconds) says
-- why: its sender still posts to a URL on the old channel and has to be
-- reconfigured. Settings lists it as orphaned, with a way to delete it.
ALTER TABLE ingress_endpoints ADD COLUMN orphaned_at INTEGER;
