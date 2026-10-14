ALTER TABLE session_skyline_cache ADD COLUMN vibe_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE session_skyline_cache ADD COLUMN latest_vibe_created_at INTEGER NOT NULL DEFAULT 0;
