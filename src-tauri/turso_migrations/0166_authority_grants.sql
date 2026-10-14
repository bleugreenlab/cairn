-- Journaled authority grants and the append-only decision journal that cites
-- them (CAIRN-3803).
--
-- Both tables are PRIVATE, and that is a deliberate authorization decision
-- rather than a storage convenience.
--
-- A grant records what THIS operator approved on THIS install. Replicating it
-- to a team replica would carry it to every teammate's machine, where it would
-- authorize their agents to install MCP servers and write capability-bearing
-- settings into their own ~/.cairn — a blast-radius expansion arriving through
-- the storage layer rather than through any policy decision, which is exactly
-- what this subsystem exists to prevent.
--
-- Keeping them private also makes one database the answer everywhere: the mint,
-- the authorization check, the grant listing, and revocation all use it, so a
-- grant can never be enforceable while being invisible and unrevocable.
-- Authorization for a team run still works: the run executes on this runner, so
-- this install is the right place to record what its operator allowed.
--
-- The scope is stored twice on purpose. `scope_json` is authoritative and is
-- compared structurally in Rust; `scope_key`/`place_kind`/`action` are the
-- derived, injective shorthand columns that let a lookup narrow candidates on an
-- index without a JSON parse. A query filters on the key; the match is confirmed
-- on the structure.
CREATE TABLE authority_grants (
  id               TEXT PRIMARY KEY,
  scope_key        TEXT NOT NULL,
  place_kind       TEXT NOT NULL,
  action           TEXT NOT NULL,
  scope_json       TEXT NOT NULL,
  -- Versioned closed envelope; an unknown version is refused on load rather
  -- than parsed leniently, since dropping a constraint we cannot read would
  -- silently widen an approval the operator narrowed.
  constraints_json TEXT NOT NULL,
  principal_json   TEXT NOT NULL,
  audience_json    TEXT NOT NULL,
  -- Denormalized from the audience so the hot lookup is a single indexed scan.
  workspace_id     TEXT NOT NULL,
  lifetime_kind    TEXT NOT NULL CHECK(lifetime_kind IN ('once','turn','session','standing')),
  -- The request/turn/session this grant is anchored to. Standing grants are
  -- unanchored by definition; the CHECK makes that the only legal NULL, so an
  -- anchored lifetime can never lose its anchor and silently become standing.
  lifetime_anchor  TEXT,
  provenance_json  TEXT NOT NULL,
  created_at       INTEGER NOT NULL,
  expires_at       INTEGER,
  consumed_at      INTEGER,
  revoked_at       INTEGER,
  revoked_by       TEXT,
  CHECK((lifetime_kind = 'standing') = (lifetime_anchor IS NULL))
);

CREATE INDEX idx_authority_grants_lookup
  ON authority_grants(workspace_id, scope_key, created_at DESC);

-- Append-only. Every approval-required decision lands here, including the ones
-- that prompted or refused, so "why did this not need approval" and "who
-- allowed this" are both answerable after the fact. Direct decisions are
-- deliberately absent: journaling ordinary work would bury the boundary
-- crossings this table exists to make visible.
CREATE TABLE authorization_events (
  id             TEXT PRIMARY KEY,
  scope_key      TEXT NOT NULL,
  place_kind     TEXT NOT NULL,
  action         TEXT NOT NULL,
  scope_json     TEXT NOT NULL,
  mutation       TEXT NOT NULL CHECK(mutation IN ('create','update','delete')),
  summary        TEXT NOT NULL,
  outcome        TEXT NOT NULL CHECK(outcome IN ('allowed_by_grant','approval_required','forbidden')),
  reason         TEXT NOT NULL,
  principal_json TEXT NOT NULL,
  audience_json  TEXT NOT NULL,
  workspace_id   TEXT NOT NULL,
  run_id         TEXT,
  -- The permission request or action URI that occasioned the decision.
  request_uri    TEXT,
  -- The grant an `allowed_by_grant` outcome cites. Intentionally not a foreign
  -- key: the journal is history and must remain readable and exact even if the
  -- grants table is later pruned or re-rooted.
  grant_id       TEXT,
  decided_at     INTEGER NOT NULL
);

CREATE INDEX idx_authorization_events_scope
  ON authorization_events(workspace_id, scope_key, decided_at DESC);

CREATE INDEX idx_authorization_events_grant
  ON authorization_events(grant_id);
