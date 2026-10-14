-- Change-triggered review pushes (CAIRN-1889).
--
-- A review push must fire only when the reviewable state CHANGED since the last
-- review push to that recipient — not on every producer work-turn idle. The
-- review creator stamps a lightweight fingerprint of the reviewable content (an
-- open PR's diffstat + identity, or a plan/create-pr artifact's version and
-- updated_at) and skips creating a new review push when the latest review push
-- for the recipient already carries the same fingerprint.
--
-- The column is nullable: only review pushes carry a fingerprint. Question,
-- permission, resolved, catch-up, and direct pushes are event-triggered (created
-- at the open/terminal moment, not at every idle), so they need no fingerprint
-- and leave this NULL.

ALTER TABLE attention_pushes ADD COLUMN fingerprint TEXT;
