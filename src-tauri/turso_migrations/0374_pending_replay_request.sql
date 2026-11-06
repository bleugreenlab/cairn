-- lineage: shared
-- cairn/6197: an explicit replay request is AUTHORITY, not a page annotation.
--
-- `replay_requested_at` and its tip/destination siblings say that an agent
-- asked; they are cleared whenever a conflict is re-recorded and carry no
-- identity, so a request deferred by the worker (lock contention, a held
-- mutation boundary) left nothing the next pass could act on. These columns
-- hold the request itself on the item queued at its destination: an identity
-- the consuming pass compares against, so a newer request is never consumed by
-- an older pass; how the branch's conflict resolves; the stated reason for
-- dropping incoming work; and when a completed pass served it. Unserved means
-- `replay_request_id IS NOT NULL AND replay_served_at IS NULL`. A row written
-- before this migration has no id and is never authority.
--
-- `replay_result_json` is the first terminal verdict for the request, frozen
-- when it is served: the requester may be waiting on a pass another worker
-- ran, and a later pass over the same item must not rewrite what that pass
-- did. NULL on a request served only by being superseded.
ALTER TABLE jj_reconcile_items ADD COLUMN replay_request_id TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN replay_resolution TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN replay_drop_incoming_reason TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN replay_served_at INTEGER;
ALTER TABLE jj_reconcile_items ADD COLUMN replay_result_json TEXT;
