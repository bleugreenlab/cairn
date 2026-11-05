-- lineage: shared
-- cairn/4963: what the shared host-wait handoff needs to know about a question.
-- `prompts` is shared schema, so both lineages gain the columns.
--
-- `resume_error`: why an answered question's resume was refused, mirroring
-- `permission_requests.resume_error` (0250). NULL when the answer resumed the
-- job, was queued for it, or needed no resume.
ALTER TABLE prompts ADD COLUMN resume_error TEXT;

-- `background`: the ask was non-blocking (`blocking: false`), so its turn did
-- not suspend and no waiter anywhere owns its answer. A NULL `tool_use_id` does
-- not say this, because a blocking ask with no correlation id stores NULL too.
-- Rows from before this column read as blocking, which is how they behaved.
ALTER TABLE prompts ADD COLUMN background INTEGER NOT NULL DEFAULT 0;
