-- Migration: jobs needs_fresh_session
--
-- Set when a mid-run prompt edit lands on a node whose session is already
-- spawned. The system prompt is assembled once at session spawn and never
-- rebuilt on resume, so applying an edited prompt requires a fresh session. The
-- continue path consumes this flag between turns and rotates to a new
-- same-backend session, so the node's next turn rebuilds the prompt without
-- orphaning a live turn's output.
ALTER TABLE jobs ADD COLUMN needs_fresh_session INTEGER NOT NULL DEFAULT 0;
