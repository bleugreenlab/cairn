ALTER TABLE github_app ADD COLUMN relay_unmapped_mention_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE github_app ADD COLUMN relay_last_unmapped_event_id TEXT;
ALTER TABLE github_app ADD COLUMN relay_last_unmapped_repository TEXT;
ALTER TABLE github_app ADD COLUMN relay_last_unmapped_at TEXT;