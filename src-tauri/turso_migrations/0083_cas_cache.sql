-- Migration: per-team content-store local cache (CAIRN-2188)
--
-- A team run's heavy archived content -- the per-execution range pack and the
-- content-addressed system-prompt/init blobs -- lives in the shared per-team
-- content store, NOT in the synced replica. On reconstruction a member fetches
-- those bytes by hash and read-through caches them here, keyed by the same
-- content hash, so repeat reconstructs are offline. Content-addressed, so a
-- cached row is safe to keep indefinitely.
--
-- PRIVATE-ONLY by design: fetched bytes must never land on the synced team
-- replica, so this table is not part of the team lineage. It is classified
-- `Private(PrivateReason::RebuildableCache)` in `TABLE_SCOPES` (migrations.rs),
-- and the `team_schema_matches_private` projection test proves the team lineage
-- is exactly the tables that classify into it. Garbage collection of
-- unreferenced cache rows is out of scope, matching `archival_blobs`.
CREATE TABLE cas_cache (
    hash TEXT PRIMARY KEY NOT NULL,  -- sha-256 (hex) content-store key
    content BLOB NOT NULL,           -- the verbatim store-object bytes for `hash`
    fetched_at INTEGER NOT NULL
);
