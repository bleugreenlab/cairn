ALTER TABLE jobs ADD COLUMN memory_review_state TEXT CHECK (memory_review_state IN ('sent', 'done'));
ALTER TABLE jobs ADD COLUMN memory_review_requested_turn INTEGER;
