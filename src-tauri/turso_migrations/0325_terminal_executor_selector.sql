-- lineage: shared
-- cairn/5406: the `executor` a terminal was created with, as the same JSON the
-- create takes. NULL when the create named no machine. Recovery reads it back so
-- a terminal that comes back is placed by the rule its create was: on a machine
-- that satisfies it, or not at all, with the refusal in `exit_reason`.
ALTER TABLE job_terminals ADD COLUMN executor_selector TEXT;
