-- lineage: shared
-- Retire every pending `review:` push whose issue is already merged or closed,
-- with the reason `review_resolved`.
--
-- A review push names an issue as awaiting review, and a resolved issue awaits
-- nothing. The drain resolver could not always prove that: a merged issue whose
-- `create-pr` artifact was written again after its PR opened reads as a rerun
-- waiting on its PR, which the resolver suspends rather than retires, so those
-- pushes stayed pending and kept surfacing as Review items in the watcher's
-- composer. The resolution now retires its issue's review pushes in the same
-- write, and this converges the rows older builds left behind once.
--
-- Rows are retired, never deleted, so they stay for audit. Delivered rows are
-- untouched. The key is matched against the canonical issue URI, whose project
-- segment is lowercased (`canonicalize_uri_identity`), and the list of resolved
-- issues' keys is built once and seeked through `idx_attention_pushes_key`.
UPDATE attention_pushes
   SET retired_at = unixepoch(),
       retirement_reason = 'review_resolved'
 WHERE delivered_event_id IS NULL
   AND retired_at IS NULL
   AND "key" IN (
         SELECT 'review:cairn://p/' || lower(p."key") || '/' || i.number
           FROM issues i JOIN projects p ON p.id = i.project_id
          WHERE i.status IN ('merged', 'closed'));
