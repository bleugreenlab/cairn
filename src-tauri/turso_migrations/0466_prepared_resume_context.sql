-- lineage: private
-- A queued launch can lose supply after restart. Keep its originating cause
-- and inline continuation input with the durable preparation, not only RAM.
ALTER TABLE prepared_ordinary_launches ADD COLUMN resume_json TEXT;
