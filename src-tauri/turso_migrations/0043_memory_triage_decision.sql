-- Canon v2 resolution decisions recorded while triage memories are claimed.

ALTER TABLE memories ADD COLUMN triage_decision TEXT
  CHECK (triage_decision IN ('promote','discard','defer'));
ALTER TABLE memories ADD COLUMN deferred_scope TEXT;
ALTER TABLE memories ADD COLUMN deferred_scope_value TEXT;
