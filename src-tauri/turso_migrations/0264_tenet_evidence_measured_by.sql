-- Tenet evidence is judged through the models facet's `decide` verb
-- (cairn/5064), so an evidence row's confidence is a measured probability:
-- the chosen option's probability as a decision model reports it (`native`)
-- or as a chat model's next-token logprobs measure it (`logprobs`). NULL
-- marks a row written before this, whose confidence a generating model
-- reported about itself.
ALTER TABLE tenet_evidence ADD COLUMN measured_by TEXT
  CHECK (measured_by IS NULL OR measured_by IN ('native','logprobs'));

-- A skipped event now carries `last_error` exactly when it was skipped
-- because nothing configured could answer a decision; the startup pass
-- re-queues those. Clear what earlier failed attempts left on events that
-- were later skipped for an ordinary reason, so none is mistaken for one.
UPDATE tenet_evidence_events SET last_error = NULL WHERE state = 'skipped';
