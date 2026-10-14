-- Normalize OpenAI-style usage into disjoint prompt components. Historical
-- Codex and OpenRouter rows stored input_tokens inclusive of cache reads.
UPDATE events
SET input_tokens = MAX(input_tokens - COALESCE(cache_read_tokens, 0), 0)
WHERE input_tokens IS NOT NULL
  AND COALESCE(cache_read_tokens, 0) > 0
  AND session_id IN (
      SELECT id
      FROM sessions
      WHERE LOWER(backend) IN ('codex', 'openrouter')
  );

-- The rollup was folded under the old backend-specific total formula. Clearing
-- its watermark makes the startup backfill re-derive every run canonically.
DELETE FROM token_rollup;
DELETE FROM token_rollup_runs;
UPDATE analytics_rollup_backfill_state SET backfilled_at = NULL;
