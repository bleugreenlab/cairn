-- Anonymous device identity for the cloud /embed gateway.
--
-- When no account is connected, the app self-registers a user-less device JWT
-- so embedding (vibe coloring, recommender, etc.) still works logged-out. This
-- single-row table (mirroring `account`) persists that token.
CREATE TABLE anon_device (
    device_id      TEXT PRIMARY KEY NOT NULL,
    jwt_encrypted  TEXT,
    jwt_expires_at INTEGER,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);
