-- lineage: shared
-- cairn/7650: a short number is the canonical address segment of a check
-- request (`check-requests/{n}`), a check observation
-- (`check-observations/{n}`), a stored message (`messages/{n}`) and a run
-- batch (`batches/{n}`), in place of the UUID or hex storage id, which stays
-- the primary key.
--
-- `address_counters` holds the last number handed out per (kind, scope). Each
-- table's AFTER INSERT trigger bumps its counter and stamps the row in the
-- insert's own statement, so a number is allocated in the row's transaction,
-- and a counter never moves back, so a deleted row's number is never reused.
--
-- Scopes: a check request and an observation number within their project, a
-- run batch within the job (node or task) that called it, and a message within
-- the database that holds it. A direct message's project is known only from
-- its recipient rows, written after the message row, so no per-project count
-- is available to its insert; the project address selects the database, so a
-- database-wide number is still unique and in filing order in every project.
--
-- A team replica numbers its own rows. Two runners writing one replica before
-- a pull can mint the same number (the exposure issue numbers already have),
-- so these indexes are not UNIQUE: a duplicate must never fail a sync replay,
-- and a lookup by number takes the earliest row.
CREATE TABLE address_counters (
    kind TEXT NOT NULL,
    scope TEXT NOT NULL,
    last INTEGER NOT NULL,
    PRIMARY KEY (kind, scope)
);

ALTER TABLE check_requests ADD COLUMN number INTEGER;
ALTER TABLE check_result_observations ADD COLUMN number INTEGER;
ALTER TABLE messages ADD COLUMN number INTEGER;
ALTER TABLE run_batch_requests ADD COLUMN number INTEGER;

-- Existing rows are numbered in creation order within their scope.
CREATE TABLE address_number_backfill (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    number INTEGER NOT NULL
);

INSERT INTO address_number_backfill (id, scope, number)
SELECT id, project_id,
       ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY accepted_at_ms, id)
  FROM check_requests;
UPDATE check_requests
   SET number = (SELECT b.number FROM address_number_backfill b WHERE b.id = check_requests.id);
INSERT INTO address_counters (kind, scope, last)
SELECT 'check_request', scope, MAX(number) FROM address_number_backfill GROUP BY scope;
DELETE FROM address_number_backfill;

DROP TRIGGER check_result_observations_immutable;
INSERT INTO address_number_backfill (id, scope, number)
SELECT id, project_id,
       ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY ran_at, id)
  FROM check_result_observations;
UPDATE check_result_observations
   SET number = (SELECT b.number FROM address_number_backfill b
                  WHERE b.id = check_result_observations.id);
INSERT INTO address_counters (kind, scope, last)
SELECT 'check_observation', scope, MAX(number) FROM address_number_backfill GROUP BY scope;
DELETE FROM address_number_backfill;
-- Immutable once numbered: the insert trigger's own stamp is the one update a
-- row ever takes.
CREATE TRIGGER check_result_observations_immutable
BEFORE UPDATE ON check_result_observations
WHEN OLD.number IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'check result observations are immutable'); END;

INSERT INTO address_number_backfill (id, scope, number)
SELECT id, '', ROW_NUMBER() OVER (ORDER BY created_at, id)
  FROM messages;
UPDATE messages
   SET number = (SELECT b.number FROM address_number_backfill b WHERE b.id = messages.id);
INSERT INTO address_counters (kind, scope, last)
SELECT 'message', scope, MAX(number) FROM address_number_backfill GROUP BY scope;
DELETE FROM address_number_backfill;

INSERT INTO address_number_backfill (id, scope, number)
SELECT request_id, job_id,
       ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY received_ms, request_id)
  FROM run_batch_requests;
UPDATE run_batch_requests
   SET number = (SELECT b.number FROM address_number_backfill b
                  WHERE b.id = run_batch_requests.request_id);
INSERT INTO address_counters (kind, scope, last)
SELECT 'run_batch', scope, MAX(number) FROM address_number_backfill GROUP BY scope;
DROP TABLE address_number_backfill;

CREATE INDEX idx_check_requests_number ON check_requests(project_id, number);
CREATE INDEX idx_check_result_observations_number ON check_result_observations(project_id, number);
CREATE INDEX idx_messages_number ON messages(number);
CREATE INDEX idx_run_batch_requests_number ON run_batch_requests(job_id, number);

CREATE TRIGGER check_requests_number AFTER INSERT ON check_requests
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('check_request', NEW.project_id, 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE check_requests
       SET number = (SELECT last FROM address_counters
                      WHERE kind = 'check_request' AND scope = NEW.project_id)
     WHERE id = NEW.id;
END;

CREATE TRIGGER check_result_observations_number AFTER INSERT ON check_result_observations
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('check_observation', NEW.project_id, 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE check_result_observations
       SET number = (SELECT last FROM address_counters
                      WHERE kind = 'check_observation' AND scope = NEW.project_id)
     WHERE id = NEW.id;
END;

CREATE TRIGGER messages_number AFTER INSERT ON messages
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('message', '', 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE messages
       SET number = (SELECT last FROM address_counters WHERE kind = 'message' AND scope = '')
     WHERE id = NEW.id;
END;

CREATE TRIGGER run_batch_requests_number AFTER INSERT ON run_batch_requests
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('run_batch', NEW.job_id, 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE run_batch_requests
       SET number = (SELECT last FROM address_counters
                      WHERE kind = 'run_batch' AND scope = NEW.job_id)
     WHERE request_id = NEW.request_id;
END;
