-- lineage: shared
-- cairn/5387: the commit a landed merge request's source branch is expected to
-- hold once the landing is recorded (the folded/published commit when the
-- landing moved the source bookmark onto it, otherwise the exact source head
-- the landing accepted). Branch cleanup deletes a merged source branch only
-- while its bookmark still holds exactly this commit; NULL (history, or a
-- route that could not name it) falls back to a real ancestry proof.
ALTER TABLE merge_requests ADD COLUMN merged_source_commit TEXT;
