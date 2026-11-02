-- lineage: shared
-- cairn/5802: the execution queue. A capacity hold (fleet slot, thread focus)
-- is an execution gate row the system files; `detail` carries its live reason
-- ("waiting for a fleet slot: 6 of 6 in progress (2nd in line)"), rewritten as
-- the queue moves. NULL on every authored `after:` gate, whose prose is
-- rendered from its predicate.
ALTER TABLE execution_gates ADD COLUMN detail TEXT;
-- A thread's own limit on open started children, overriding the workspace
-- setting `maxOpenChildrenPerThread`. NULL inherits it.
ALTER TABLE threads ADD COLUMN max_open_children INTEGER;
