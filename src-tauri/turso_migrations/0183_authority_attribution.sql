-- Typed decision attribution for the private authority journal.
-- Actor references are independently indexed for audit lookup; appearance snapshots
-- remain opaque evidence and are interpreted only through cairn-common identity types.
ALTER TABLE authority_grants ADD COLUMN actor_principal_json TEXT;
ALTER TABLE authority_grants ADD COLUMN appearance_snapshot_json TEXT;
ALTER TABLE authorization_events ADD COLUMN actor_principal_json TEXT;
ALTER TABLE authorization_events ADD COLUMN appearance_snapshot_json TEXT;

CREATE INDEX idx_authority_grants_actor
  ON authority_grants(workspace_id, actor_principal_json, created_at DESC);
CREATE INDEX idx_authorization_events_actor
  ON authorization_events(workspace_id, actor_principal_json, decided_at DESC);
