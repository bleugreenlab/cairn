CREATE TABLE search_outbox (
    id TEXT PRIMARY KEY NOT NULL,
    source_table TEXT NOT NULL,
    source_id TEXT NOT NULL,
    content_type TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'applied')),
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_search_outbox_status_created
    ON search_outbox(status, created_at);

CREATE TRIGGER search_issues_insert AFTER INSERT ON issues BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;

CREATE TRIGGER search_issues_update AFTER UPDATE ON issues
WHEN OLD.title != NEW.title OR OLD.description IS NOT NEW.description OR OLD.project_id != NEW.project_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;

CREATE TRIGGER search_issues_delete AFTER DELETE ON issues BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', OLD.id, 'issue', 'delete', 'pending', unixepoch());
END;

CREATE TRIGGER search_comments_insert AFTER INSERT ON comments BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'comments', NEW.id, 'comment', 'upsert', 'pending', NEW.created_at);
END;

CREATE TRIGGER search_comments_update AFTER UPDATE ON comments
WHEN OLD.content != NEW.content OR OLD.issue_id != NEW.issue_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'comments', NEW.id, 'comment', 'upsert', 'pending', NEW.created_at);
END;

CREATE TRIGGER search_comments_delete AFTER DELETE ON comments BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'comments', OLD.id, 'comment', 'delete', 'pending', unixepoch());
END;

CREATE TRIGGER search_artifacts_insert AFTER INSERT ON artifacts
WHEN NEW.job_id IS NOT NULL BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'artifacts', NEW.id, 'artifact', 'upsert', 'pending', NEW.updated_at);
END;

CREATE TRIGGER search_artifacts_update AFTER UPDATE ON artifacts
WHEN OLD.data != NEW.data OR OLD.job_id IS NOT NEW.job_id OR OLD.artifact_type != NEW.artifact_type BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'artifacts', NEW.id, 'artifact', 'upsert', 'pending', NEW.updated_at);
END;

CREATE TRIGGER search_artifacts_delete AFTER DELETE ON artifacts BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'artifacts', OLD.id, 'artifact', 'delete', 'pending', unixepoch());
END;

CREATE TRIGGER search_events_insert AFTER INSERT ON events
WHEN NEW.event_type IN ('text', 'tool_result', 'user') AND json_extract(NEW.data, '$.content') IS NOT NULL BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'events', NEW.id, 'event', 'upsert', 'pending', NEW.created_at);
END;

CREATE TRIGGER search_events_update AFTER UPDATE ON events
WHEN NEW.event_type IN ('text', 'tool_result', 'user') AND OLD.data != NEW.data AND json_extract(NEW.data, '$.content') IS NOT NULL BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'events', NEW.id, 'event', 'upsert', 'pending', NEW.created_at);
END;

CREATE TRIGGER search_events_delete AFTER DELETE ON events BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'events', OLD.id, 'event', 'delete', 'pending', unixepoch());
END;

CREATE TRIGGER search_messages_insert AFTER INSERT ON messages BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;

CREATE TRIGGER search_messages_update AFTER UPDATE ON messages
WHEN OLD.content != NEW.content OR OLD.channel_type != NEW.channel_type OR OLD.channel_id IS NOT NEW.channel_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;

CREATE TRIGGER search_messages_delete AFTER DELETE ON messages BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', OLD.id, 'message', 'delete', 'pending', unixepoch());
END;
