-- The account whose credential a run's process was launched with. A provider
-- request is attributed to its run, so this is how a continuation learns which
-- account the last request ran on after the session pin has moved. NULL for
-- runs launched before this column existed, and for a backend with no account.
ALTER TABLE runs ADD COLUMN account_id TEXT;

-- Why automatic resolution is withheld even when the boundary is known.
-- 'tool_in_flight': the limit landed while a tool call had no recorded result,
-- so resuming could repeat it; only an explicit continue resumes. NULL: none.
-- Additive rather than a widened boundary_source CHECK, because widening a CHECK
-- needs a create-copy-drop-rename rebuild, which breaks the team replica's sync
-- triggers on this shared table.
ALTER TABLE provider_limit_holds ADD COLUMN resume_gate TEXT;

-- Set when no eligible account remained and retry_not_before is another
-- account's recorded reset: the earliest across the accounts the routing policy
-- may select. NULL: the boundary is this hold's own account's.
ALTER TABLE provider_limit_holds ADD COLUMN boundary_account_id TEXT;
