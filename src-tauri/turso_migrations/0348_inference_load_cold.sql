-- lineage: private
-- cairn/5977: whether a finished load downloaded what it serves before serving
-- it (its head saw a container image pull in the start's output). Such a load
-- says nothing about what a reload from the machine's cache costs, so the
-- windows a pool derives from load times pass over it while a load from cache
-- is on record. Every load recorded before this reads as from cache.
ALTER TABLE inference_deployment_loads ADD COLUMN cold INTEGER NOT NULL DEFAULT 0;
