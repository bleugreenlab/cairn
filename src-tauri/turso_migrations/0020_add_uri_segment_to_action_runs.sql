-- Migration: add uri_segment to action_runs
--
-- Gives each action_run a stable, addressable URI segment, mirroring the
-- stored-segment identity model used for jobs (CAIRN-1125) and prompts. This
-- lets the cairn:// resource layer address an action node (notably a `pr`
-- node) at the bare URI cairn://p/PROJ/N/EXEC/<segment>, resolve it back, and
-- surface it in the issue node-tree and `cairn watch` detail URIs (CAIRN-1222).
--
-- New rows allocate their segment at insert time via the shared node_segments
-- allocator, deduped across both jobs and action_runs for the execution. For
-- existing rows, recover the bare segment from the recipe node id, which is
-- conventionally "<segment>-<n>" (e.g. "pr-1" -> "pr"); fall back to the
-- recipe_node_id verbatim.

ALTER TABLE action_runs ADD COLUMN uri_segment TEXT;

UPDATE action_runs
SET uri_segment = CASE
    WHEN recipe_node_id GLOB '*-[0-9]'
        THEN substr(recipe_node_id, 1, length(recipe_node_id) - 2)
    WHEN recipe_node_id GLOB '*-[0-9][0-9]'
        THEN substr(recipe_node_id, 1, length(recipe_node_id) - 3)
    ELSE recipe_node_id
END
WHERE uri_segment IS NULL;

CREATE INDEX idx_action_runs_uri_segment ON action_runs(uri_segment);
