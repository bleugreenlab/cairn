-- lineage: shared
-- retire thread branches
-- Threads never commit (tenet #1416), so the numbered branch ledger retires. The
-- jobs and merge requests these rows pointed at are kept: a backing job is an
-- ordinary completed thread task, and its merge request stays addressable through
-- it. `threads.next_branch_number` stays as a dormant column, because `threads`
-- is synced and a create-copy-drop-rename column removal breaks its sync triggers.
DROP TABLE thread_branches;
