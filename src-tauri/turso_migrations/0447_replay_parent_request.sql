-- lineage: shared
-- cairn/7095: a take-committed-tip request lands the resolution on the base its
-- session was recorded against, then judges the base's newer commits as a
-- following round: a plain request onto the live base, queued as a child of
-- the request that landed. `replay_parent_request_id` names that parent, whose
-- authority the child carries; NULL for every request an agent made itself.
ALTER TABLE jj_reconcile_items ADD COLUMN replay_parent_request_id TEXT;
