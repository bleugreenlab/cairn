-- lineage: private
-- cairn/7650: an operator message (`cairn://operator/messages/{n}`) and an
-- operator request (`cairn://requests/{n}`) are addressed by a short number,
-- numbered across this install's database, in place of their storage ids,
-- which stay the primary keys. Both tables are install-local, so the numbers
-- are UNIQUE; `address_counters` and the stamping trigger work as the shared
-- migration before this one describes.
ALTER TABLE operator_messages ADD COLUMN number INTEGER;
ALTER TABLE requests ADD COLUMN number INTEGER;

CREATE TABLE address_number_backfill (
    id TEXT PRIMARY KEY,
    number INTEGER NOT NULL
);

INSERT INTO address_number_backfill (id, number)
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at, id) FROM operator_messages;
UPDATE operator_messages
   SET number = (SELECT b.number FROM address_number_backfill b WHERE b.id = operator_messages.id);
INSERT INTO address_counters (kind, scope, last)
SELECT 'operator_message', '', MAX(number) FROM address_number_backfill HAVING COUNT(*) > 0;
DELETE FROM address_number_backfill;

-- A declined request is immutable once numbered; the number is stamped once.
DROP TRIGGER request_decline_immutable;
INSERT INTO address_number_backfill (id, number)
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at, id) FROM requests;
UPDATE requests
   SET number = (SELECT b.number FROM address_number_backfill b WHERE b.id = requests.id);
INSERT INTO address_counters (kind, scope, last)
SELECT 'operator_request', '', MAX(number) FROM address_number_backfill HAVING COUNT(*) > 0;
DROP TABLE address_number_backfill;
CREATE TRIGGER request_decline_immutable BEFORE UPDATE ON requests
 WHEN OLD.state='declined' AND OLD.number IS NOT NULL
 BEGIN SELECT RAISE(ABORT,'decline decision is immutable'); END;

CREATE UNIQUE INDEX idx_operator_messages_number ON operator_messages(number);
CREATE UNIQUE INDEX idx_requests_number ON requests(number);

CREATE TRIGGER operator_messages_number AFTER INSERT ON operator_messages
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('operator_message', '', 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE operator_messages
       SET number = (SELECT last FROM address_counters
                      WHERE kind = 'operator_message' AND scope = '')
     WHERE id = NEW.id;
END;

CREATE TRIGGER requests_number AFTER INSERT ON requests
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('operator_request', '', 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE requests
       SET number = (SELECT last FROM address_counters
                      WHERE kind = 'operator_request' AND scope = '')
     WHERE id = NEW.id;
END;
