-- Real per-generation dollar cost for metered backends (OpenRouter).
-- NULL for subscription backends (Claude/Codex), whose cost stays
-- price-table-derived in analytics.
ALTER TABLE events ADD COLUMN cost_usd REAL;
