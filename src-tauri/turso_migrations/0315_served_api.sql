-- lineage: private
-- cairn/5712: Cairn's served OpenAI-compatible API for outside clients.
--
-- Both tables are PRIVATE to this install, like the grants they sit beside
-- (0166). A client record is this operator's decision about who may spend this
-- install's models; replicating it would hand a teammate's install a key it
-- never issued. One database also answers authentication, listing, revocation
-- and the usage journal, so a key can never authenticate while being
-- invisible or unrevocable.
--
-- served_api_clients: one row per client key. The key itself is never stored:
-- `key_hash` is the lowercase hex SHA-256 of the trimmed key, the scheme bgllm
-- (and LiteLLM before it) used, so imported keys keep working unchanged. `id`
-- is the immutable id the client's External principal names; `name` is an
-- alias for people, unique among live keys. `key_hint` holds the last four
-- characters of a key Cairn minted; an imported key has none.
CREATE TABLE served_api_clients (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  name         TEXT NOT NULL,
  key_hash     TEXT NOT NULL UNIQUE CHECK(length(key_hash) = 64),
  key_hint     TEXT,
  -- `minted`, or `imported:<origin>` (`imported:bgllm`).
  source       TEXT NOT NULL,
  created_at   INTEGER NOT NULL,
  revoked_at   INTEGER,
  revoked_by   TEXT
);

CREATE UNIQUE INDEX idx_served_api_clients_live_name
  ON served_api_clients(workspace_id, name) WHERE revoked_at IS NULL;

-- model_requests: one row per served-API request, refusals included, written
-- when the reply ends or the client goes away. It never stores a prompt or a
-- reply. `principal_json` and `appearance_json` are the authenticated client's
-- External principal and its appearance snapshot; both are NULL for a request
-- that did not authenticate. `grant_id` is the standing grant that allowed a
-- remote provider. Times are unix milliseconds.
CREATE TABLE model_requests (
  id                  TEXT PRIMARY KEY,
  ts                  INTEGER NOT NULL,
  door                TEXT NOT NULL,
  client_id           TEXT,
  principal_json      TEXT,
  appearance_json     TEXT,
  model_asked         TEXT,
  model               TEXT,
  provider            TEXT,
  account_id          TEXT,
  sent_model          TEXT,
  status              INTEGER NOT NULL,
  stream              INTEGER NOT NULL,
  input_tokens        INTEGER,
  output_tokens       INTEGER,
  cached_tokens       INTEGER,
  cost                REAL,
  latency_ms          INTEGER NOT NULL,
  wait_ms             INTEGER,
  upstream_request_id TEXT,
  grant_id            TEXT,
  outcome             TEXT NOT NULL CHECK(outcome IN ('answered','refused','upstream_error','client_closed')),
  detail              TEXT
);

CREATE INDEX idx_model_requests_client_ts ON model_requests(client_id, ts DESC);
CREATE INDEX idx_model_requests_ts ON model_requests(ts);
