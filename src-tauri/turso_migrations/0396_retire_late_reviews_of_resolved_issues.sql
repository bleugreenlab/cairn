-- lineage: shared
-- Converge reviews minted after the resolution-time retirement by older builds.
-- Keep the same scope as the original resolved-review backfill: only pending
-- reviews of merged or closed issues; delivered and retired history is untouched.
UPDATE attention_pushes
   SET retired_at = unixepoch(),
       retirement_reason = 'review_resolved'
 WHERE delivered_event_id IS NULL
   AND retired_at IS NULL
   AND "key" IN (
         SELECT 'review:cairn://p/' || lower(p."key") || '/' || i.number
           FROM issues i JOIN projects p ON p.id = i.project_id
          WHERE i.status IN ('merged', 'closed'));
