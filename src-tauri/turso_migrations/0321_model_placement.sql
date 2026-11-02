-- lineage: private
-- cairn/5728 (tenets 1336, 1337): execution's placement runs local models.
-- A running model is a resident placement holds on a pool of machines it
-- detects from the links executors report, so the hand-written pool records
-- and the pool controller's per-pool record retire.
--
-- What survives of them moves to three tables:
--
-- `inference_machines`: each machine's network address, the one fact a pool
-- record held that nothing detects (a deployment's API base and health URL are
-- its head's address plus its own port and paths). Same shape as the other
-- inference record tables, so authoring, adoption and the YAML export treat a
-- machine like a profile or a deployment.
--
-- `model_pool_preferences`: per detected pool, keyed by its member names
-- sorted and joined with `+`, the model it places when idle (by real name)
-- and the operator's override of its idle window in seconds. A pool record's
-- default carries over as its warm model, and the controller's operator-set
-- model idle window as the override.
--
-- `model_pins`: standing leases on residents, one per deployment, with the
-- model's real name and who holds it. The controller's pin carries over, one
-- row per deployment in its set.
--
-- `inference_requests` stays as it is; its `pool` column now holds the pool
-- key. Times are epoch milliseconds.
CREATE TABLE inference_machines (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('bginf', 'cairn')),
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE model_pool_preferences (
    pool_key TEXT PRIMARY KEY,
    warm_model TEXT,
    idle_override_secs INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE TABLE model_pins (
    deployment_id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    holder TEXT NOT NULL,
    at INTEGER NOT NULL
);

INSERT OR IGNORE INTO inference_machines (id, source, body_json, created_at, updated_at)
SELECT json_extract(m.value, '$.name'),
       p.source,
       json_object(
           'name', json_extract(m.value, '$.name'),
           'source', p.source,
           'address', json_extract(m.value, '$.address')
       ),
       p.created_at,
       p.updated_at
  FROM inference_pools AS p, json_each(json_extract(p.body_json, '$.machines')) AS m
 WHERE json_extract(m.value, '$.address') IS NOT NULL
   AND json_extract(m.value, '$.address') != '';

INSERT OR IGNORE INTO model_pool_preferences (pool_key, warm_model, idle_override_secs, updated_at)
SELECT (SELECT group_concat(name, '+')
          FROM (SELECT json_extract(m.value, '$.name') AS name
                  FROM json_each(json_extract(p.body_json, '$.machines')) AS m
                 ORDER BY name)),
       (SELECT json_extract(r.body_json, '$.model.name')
          FROM inference_deployments AS d
          JOIN inference_profiles AS r ON r.id = json_extract(d.body_json, '$.profile')
         WHERE d.id = json_extract(p.body_json, '$.default[0]')),
       (SELECT json_extract(c.body_json, '$.settings.modelIdleSecs')
          FROM inference_pool_control AS c
         WHERE c.pool = p.id),
       p.updated_at
  FROM inference_pools AS p;

DELETE FROM model_pool_preferences
 WHERE warm_model IS NULL AND idle_override_secs IS NULL;

INSERT OR IGNORE INTO model_pins (deployment_id, model, holder, at)
SELECT s.value,
       COALESCE(
           (SELECT json_extract(r.body_json, '$.model.name')
              FROM inference_deployments AS d
              JOIN inference_profiles AS r ON r.id = json_extract(d.body_json, '$.profile')
             WHERE d.id = s.value),
           s.value
       ),
       json_extract(c.body_json, '$.pin.holder'),
       json_extract(c.body_json, '$.pin.at')
  FROM inference_pool_control AS c, json_each(json_extract(c.body_json, '$.pin.set')) AS s
 WHERE json_extract(c.body_json, '$.pin.holder') IS NOT NULL;

DROP TABLE inference_pool_control;
DROP TABLE inference_pools;
