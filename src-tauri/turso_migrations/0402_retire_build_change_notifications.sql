-- lineage: private
-- A rebuild notice is now one camp_notice fact per project, addressed to base
-- camp and fanned out to its participants by role (cairn/6255), so the
-- per-recipient notice rows and the passive push each one fed are retired.
--
-- A pending build-change push can only render from the row this drops, and its
-- referent (a rebuild that already happened) is no longer owed: the next
-- rebuild reaches every participant through base camp. Retirement's reason
-- vocabulary has no entry for that, and widening a synced table's CHECK would
-- need a rebuild, so the few pending rows are deleted instead. Delivered pushes
-- are untouched; the transcript already holds what each one said.
DELETE FROM attention_pushes
 WHERE "key" = 'build-change'
   AND delivered_event_id IS NULL
   AND retired_at IS NULL;
DROP TABLE build_change_notifications;

-- The notice a changed build still owes, recorded in the same write that
-- advances the build identity, so a crash before every project's base camp
-- was told leaves the obligation in place for the next boot to finish. Both
-- null when nothing is owed.
ALTER TABLE app_boot_state ADD COLUMN pending_from_version TEXT;
ALTER TABLE app_boot_state ADD COLUMN pending_from_build_id TEXT;
