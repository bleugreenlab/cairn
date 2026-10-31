-- lineage: private
-- cairn/5129: the relay channel belongs to the install, not to the GitHub App.
--
-- Every ingress endpoint and the App's webhooks arrive on one channel, so its
-- id, secret, keypair, shared cursor and delivery health move from
-- `github_app` to a single-row `relay_channel`. The values copy verbatim: the
-- at-rest key is derived from the machine id and a per-field domain, neither
-- of which depends on the table. The four `relay_unmapped_*` counters stay on
-- `github_app`; they count GitHub mentions that map to no project, a GitHub
-- routing statistic rather than channel health.
CREATE TABLE relay_channel (
    id TEXT PRIMARY KEY DEFAULT 'default',
    channel_id TEXT,
    -- Encrypted at rest (cairn-relay-secret-v1).
    secret TEXT,
    public_key TEXT,
    -- Encrypted at rest (cairn-relay-key-v1).
    private_key_encrypted TEXT,
    key_rotated_at TEXT,
    pending_public_key TEXT,
    pending_private_key_encrypted TEXT,
    -- The shared cursor, `received_at|id`.
    last_event_sync TEXT,
    -- When a sync last drained the channel completely. A cursor behind the
    -- api's retention horizon lost events to pruning only if no drain has
    -- happened since the horizon.
    drained_at TEXT,
    health_state TEXT,
    health_reason TEXT,
    first_failure_at TEXT,
    last_failure_at TEXT,
    consecutive_failures INTEGER,
    failing_event_id TEXT,
    failing_event_at TEXT,
    last_successful_delivery_at TEXT,
    undeliverable_since_healthy INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

INSERT INTO relay_channel (
    id, channel_id, secret, public_key, private_key_encrypted, key_rotated_at,
    pending_public_key, pending_private_key_encrypted, last_event_sync,
    health_state, health_reason, first_failure_at, last_failure_at,
    consecutive_failures, failing_event_id, failing_event_at,
    last_successful_delivery_at, undeliverable_since_healthy, created_at, updated_at,
    drained_at
)
SELECT 'default', relay_channel_id, relay_secret, relay_public_key,
       relay_private_key_encrypted, relay_key_rotated_at, relay_pending_public_key,
       relay_pending_private_key_encrypted, last_event_sync, relay_health_state,
       relay_health_reason, relay_first_failure_at, relay_last_failure_at,
       relay_consecutive_failures, relay_failing_event_id, relay_failing_event_at,
       relay_last_successful_delivery_at, relay_undeliverable_since_healthy,
       created_at, updated_at,
       -- The runner this replaces was polling up to the upgrade, so the
       -- channel was drained now; an idle channel is not a retention gap.
       strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
FROM github_app
WHERE id = 'default' AND relay_channel_id IS NOT NULL AND relay_secret IS NOT NULL;

ALTER TABLE github_app DROP COLUMN relay_channel_id;
ALTER TABLE github_app DROP COLUMN relay_secret;
ALTER TABLE github_app DROP COLUMN relay_public_key;
ALTER TABLE github_app DROP COLUMN relay_private_key_encrypted;
ALTER TABLE github_app DROP COLUMN relay_key_rotated_at;
ALTER TABLE github_app DROP COLUMN relay_pending_public_key;
ALTER TABLE github_app DROP COLUMN relay_pending_private_key_encrypted;
ALTER TABLE github_app DROP COLUMN last_event_sync;
ALTER TABLE github_app DROP COLUMN relay_health_state;
ALTER TABLE github_app DROP COLUMN relay_health_reason;
ALTER TABLE github_app DROP COLUMN relay_first_failure_at;
ALTER TABLE github_app DROP COLUMN relay_last_failure_at;
ALTER TABLE github_app DROP COLUMN relay_consecutive_failures;
ALTER TABLE github_app DROP COLUMN relay_failing_event_id;
ALTER TABLE github_app DROP COLUMN relay_failing_event_at;
ALTER TABLE github_app DROP COLUMN relay_last_successful_delivery_at;
ALTER TABLE github_app DROP COLUMN relay_undeliverable_since_healthy;

-- What the endpoints mirror needs for an external endpoint: the URL a sender
-- posts to, and the deliveries the api refused for it (bad signature, over its
-- rate or body cap), both as the api last reported them on refresh.
ALTER TABLE ingress_endpoints ADD COLUMN url TEXT;
ALTER TABLE ingress_endpoints ADD COLUMN rejected_count INTEGER NOT NULL DEFAULT 0;
