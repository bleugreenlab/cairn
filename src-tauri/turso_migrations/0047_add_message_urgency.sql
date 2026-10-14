-- Add canonical delivery urgency for direct messages.
-- NULL preserves legacy semantics: direct messages default to steer.
ALTER TABLE messages ADD COLUMN urgency TEXT;
