-- lineage: private
-- cairn/5730: models by their real name, and what verifying a recipe on a pool
-- measured.
--
-- `inference_models` holds one row per model, keyed by its Hugging Face
-- repository id, as the cairn-inference facet's serde form in `body_json`
-- (the record-row pattern of 0268). A recipe must serve a registered model,
-- so every model name the existing recipes already serve is registered here
-- with its facts unknown; reading its config later fills them in.
--
-- `inference_verifications` is every verification run: a recipe loaded on a
-- pool, its health checked and a short benchmark run, recording the stage it
-- reached, why it failed if it did, its load time, the memory each machine
-- held, the context and KV cache the engine reported, and the speeds the
-- benchmark measured. `recipe_digest` is the sha256 of the recipe's canonical
-- JSON when it ran, so a row whose recipe has since changed reads as stale.
-- Pruned to the newest runs per recipe and pool on insert. Times are epoch
-- milliseconds.
CREATE TABLE inference_models (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('bginf', 'cairn')),
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO inference_models (id, source, body_json, created_at, updated_at)
SELECT name, 'cairn', json_object('name', name), MIN(created_at), MAX(updated_at)
  FROM (SELECT json_extract(body_json, '$.model.name') AS name, created_at, updated_at
          FROM inference_recipes)
 WHERE name IS NOT NULL
 GROUP BY name;

CREATE TABLE inference_verifications (
    id TEXT PRIMARY KEY,
    recipe_id TEXT NOT NULL,
    recipe_digest TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    machines_json TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('running', 'passed', 'failed')),
    stage TEXT NOT NULL,
    reason TEXT,
    load_ms INTEGER,
    footprint_json TEXT,
    context_tokens INTEGER,
    kv_tokens INTEGER,
    ttft_ms INTEGER,
    decode_tps REAL,
    prefill_tps REAL,
    embed_tps REAL,
    engine_version TEXT
);

CREATE INDEX idx_inference_verifications_recipe_pool
    ON inference_verifications (recipe_id, pool_id, started_at);
