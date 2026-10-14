-- The Cairn account that served a response, so usage can be attributed to it.
-- NULL for a failed attempt and for backends that do not name one.
ALTER TABLE response_invocations ADD COLUMN account_id TEXT;
