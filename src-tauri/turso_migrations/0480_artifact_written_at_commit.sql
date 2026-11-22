-- lineage: shared
-- Provenance belongs to each stored version, not to its authored JSON schema.
ALTER TABLE artifacts ADD COLUMN written_at_commit TEXT;
