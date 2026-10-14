-- CAIRN-3293: retire the snapshotted child-attention subscriptions.
--
-- A child issue's attention now routes to whichever node currently drives the
-- parent issue, derived from `issues.parent_issue_id` at wake time
-- (`wakes::coordinating_job_for_child_issue`). The rows this deletes were the
-- old materialization of that answer: one `issue` subscription minted at
-- parent-edge creation, naming whichever job coordinated the parent at that
-- instant, and never moved afterwards.
--
-- Leaving them would preserve exactly the defect the derivation removes. A
-- retired coordinator still holds its row, so it stays in the watcher set and
-- keeps receiving pushes for children a newer coordinator execution now owns --
-- the "wrong-recipient" half of the bug, which is what let a superseded
-- coordinator merge a child out from under the live one.
--
-- The rule is the row's *effect*, not its state. `watcher_jobs_for_issue`
-- includes every non-`unsubscribed` row, so:
--
--   * `active` and `muted` rows INCLUDE the job in the watcher set, and must go.
--     A `muted` one is not a weaker case: the push creators still write a
--     `passive` row for that recipient, which rides along the next time the
--     retired coordinator runs and shows it gates it no longer owns.
--   * `unsubscribed` rows only ever SUPPRESS, so they are kept. Deleting one
--     would resurrect the derived watch for a job that explicitly refused it --
--     and unlike a mute, a refusal is safe whether or not the job is still the
--     coordinator (inert if not, honored if so).
--
-- A mute is not evidence of intent to keep watching after losing ownership. It
-- was scoped to the watch the node held at the time, and `created_by = 'system'`
-- says the row itself was machine-minted, not chosen; a mute that landed on this
-- key mutated a default rather than recording a subscription. Deliberate
-- `created_by = 'agent'`/`'user'` rows are untouched at any state, so a
-- coordinator that explicitly subscribed to or muted a child keeps that row.
--
-- Scope is pinned to the default fact-kind set exactly as `fact_kinds_json`
-- serializes it (sorted, deduplicated). `user`, `peer` (job defaults) and
-- `process` (terminal) subscriptions are untouched -- they are not derivable from
-- a parent edge and remain materialized.

DELETE FROM wake_subscriptions
WHERE created_by = 'system'
  AND source_kind = 'issue'
  AND state IN ('active', 'muted')
  AND one_shot = 0
  AND fact_kinds_json = '["message","permission","question","resolved","review"]';
