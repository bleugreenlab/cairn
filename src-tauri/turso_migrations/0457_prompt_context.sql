-- lineage: shared
-- cairn/5519: a question carries its context. One `context` per ask, beside
-- `questions`: what the question is about in the asker's own words, with the
-- canonical URIs it concerns, so neither the operator answering it nor the
-- asker after compaction needs it explained again. NULL for an ask without
-- one, and for every row from before. Additive only: the team replica's sync
-- triggers ride on `prompts`, so it is never rebuilt.
ALTER TABLE prompts ADD COLUMN context TEXT;
