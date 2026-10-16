-- cairn/4987: what each path of a committed conflict resolution was resolved
-- AGAINST.
--
-- A resolution an agent commits is a linear commit: it carries the incoming
-- side's content without that side in its ancestry, so every later merge-base
-- still names the stale fork. `resolution_basis_json` records, per path, the
-- merge base and incoming side that path was last resolved against, as a JSON
-- array of {"path", "base", "theirs"}. A later replay carries each resolution
-- forward against what the agent actually saw, instead of restoring it whole
-- over a newer base. NULL for a session whose branch has resolved nothing.
ALTER TABLE jj_reconcile_items ADD COLUMN resolution_basis_json TEXT;
-- The tip a take-committed-tip request was assessed at, and the destination it
-- was queued onto. The worker refuses a tip that moved after the request, and
-- the rebase page names the destination actually queued rather than the one
-- the session was recorded against.
ALTER TABLE jj_reconcile_items ADD COLUMN replay_requested_tip TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN replay_requested_destination TEXT;
