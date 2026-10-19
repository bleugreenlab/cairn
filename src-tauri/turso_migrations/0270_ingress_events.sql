-- cairn/4910: generic external ingress, first slice.
--
-- `ingress_events` is the canonical local record of every event an ingress
-- endpoint delivered over the relay channel, delivered or not. The fact log's
-- `ingress` rows are pointers into it, and their `body` is hydrated from here.
-- It replaces `webhook_events`, which no code path ever wrote.
DROP INDEX IF EXISTS idx_webhook_events_pr;
DROP INDEX IF EXISTS idx_webhook_events_processed;
DROP TABLE IF EXISTS webhook_events;

CREATE TABLE ingress_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_id TEXT NOT NULL,
    -- The endpoint's name and project when the event arrived; NULL when the
    -- endpoint was unknown to this install.
    endpoint_name TEXT,
    project_id TEXT,
    relay_event_id INTEGER NOT NULL,
    delivery_id TEXT,
    -- The delivery identity: the endpoint's delivery id, or the relay row id
    -- for a delivery that named none.
    delivery_key TEXT NOT NULL,
    type TEXT NOT NULL,
    verification TEXT,
    sender_json TEXT,
    -- The event body, bounded; NULL for an event that could not be opened.
    body TEXT,
    received_at TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'undeliverable')),
    note TEXT,
    UNIQUE (endpoint_id, delivery_key)
);
CREATE INDEX idx_ingress_events_endpoint ON ingress_events(endpoint_id, id);
CREATE INDEX idx_ingress_events_recorded ON ingress_events(recorded_at);

-- This install's view of the ingress endpoints it minted. The api holds the
-- authoritative row (and any secret); this mirror is what the relay consumer
-- resolves an event's endpoint against and what the endpoints resource reads.
CREATE TABLE ingress_endpoints (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    project_id TEXT,
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    topic TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    -- The grant as the api last reported it (RFC 3339), refreshed on read.
    granted_at TEXT,
    created_at INTEGER NOT NULL,
    revoked_at INTEGER
);
CREATE INDEX idx_ingress_endpoints_project ON ingress_endpoints(project_id, name);

-- How many events the relay consumer has skipped since its last successful
-- delivery, beside the existing failing-event evidence on the channel's row.
ALTER TABLE github_app ADD COLUMN relay_undeliverable_since_healthy INTEGER NOT NULL DEFAULT 0;
