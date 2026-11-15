-- lineage: shared
-- An addressed interrupt can resolve timed waits. Persist its cause so crash
-- replay does not turn an addressed response into a system re-entry.
ALTER TABLE agent_waits ADD COLUMN resume_cause TEXT NOT NULL DEFAULT 'system'
    CHECK (resume_cause IN ('system', 'addressed'));
