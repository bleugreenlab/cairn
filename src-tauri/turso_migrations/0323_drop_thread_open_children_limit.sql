-- lineage: shared
-- cairn/5830: the thread focus cap is the workspace `maxOpenChildrenPerThread`
-- alone; a thread no longer carries its own limit.
ALTER TABLE threads DROP COLUMN max_open_children;
-- Capacity holds now store their facts as JSON in `detail` (the gate census
-- renders them). The prose rows the queue wrote are cleared, and the next
-- sweep rewrites them. Authored `after:` gates always have a NULL `detail`.
UPDATE execution_gates SET detail = NULL WHERE detail IS NOT NULL;
