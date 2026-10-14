-- Phrase-based terminal-output wake subscriptions.
--
-- An agent can subscribe to a running terminal and resume when a literal phrase
-- appears in its output (e.g. a dev server's "ready" line) without polling.
-- match_phrase holds the literal substring to watch for; it is NULL for every
-- other subscription kind. Output subscriptions also carry the terminal_exit
-- fact kind (see fact_kinds_json) so a terminal that exits before the phrase
-- appears still wakes the waiting subscriber instead of stranding it. The match
-- is one_shot, so it can never re-fire on already-observed output.
ALTER TABLE wake_subscriptions ADD COLUMN match_phrase TEXT;
