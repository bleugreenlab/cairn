-- lineage: private
-- The operator's own closes in the notification center. Every entry there is
-- derived from a live source row; a dismissal is the one thing the center
-- stores, keyed by the entry's stable key so a new occurrence of the same
-- cause (a new key) is open again.
CREATE TABLE operator_notification_dismissals (
    entry_key TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    dismissed_at INTEGER NOT NULL
);

CREATE INDEX idx_operator_notification_dismissals_project
    ON operator_notification_dismissals (project_id);
