-- Pin each backend conversation to the provider account that owns it.
-- Nullable keeps non-Claude sessions and pre-existing conversations unchanged.
-- Sessions are shared project state, so this migration is composed into both lineages.
ALTER TABLE sessions ADD COLUMN account_id TEXT;
