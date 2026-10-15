-- cairn/4936: why an answered permission's resume was refused. NULL when the
-- answer resumed the job (or needed no resume). Set by the slow-path driver when
-- it settles a successor turn nothing could continue into.
ALTER TABLE permission_requests ADD COLUMN resume_error TEXT;
