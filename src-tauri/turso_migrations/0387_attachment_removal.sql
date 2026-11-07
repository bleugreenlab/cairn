-- lineage: shared
-- Removal of a stored object, as opposed to retirement (cairn/6318).
--
-- Retirement (`retired_at`, 0227) stops an address resolving and leaves the
-- bytes where they are. Removal deletes the bytes this database holds and
-- leaves a tombstone in their place: when, by whom, why, and the authorization
-- decision that allowed it. The tombstone is what every address that once
-- named the object answers with afterwards, so a transcript that referenced it
-- renders the record of its removal rather than an unexplained absence.
--
-- Additive columns only: `attachments` is a synced shared table, and a
-- create-copy-drop rebuild breaks Turso sync triggers on team replicas.
ALTER TABLE attachments ADD COLUMN removed_at INTEGER;
ALTER TABLE attachments ADD COLUMN removed_by TEXT;
ALTER TABLE attachments ADD COLUMN removal_reason TEXT;
ALTER TABLE attachments ADD COLUMN removal_decision_id TEXT;
