-- lineage: shared
-- The terminal claim pins the issue incarnation in the same UPDATE. NULL is
-- legacy evidence, valid only before the issue has ever been reopened.
ALTER TABLE merge_requests ADD COLUMN issue_resolution_generation INTEGER;
