CREATE TABLE annotation_message_links (
    annotation_id TEXT NOT NULL REFERENCES annotations(id) ON DELETE CASCADE,
    message_id    TEXT NOT NULL,
    job_id        TEXT,
    created_at    INTEGER NOT NULL,
    delivered_at  INTEGER,
    PRIMARY KEY (annotation_id, message_id)
);

CREATE INDEX idx_anno_links_message ON annotation_message_links(message_id);
CREATE INDEX idx_anno_links_annotation ON annotation_message_links(annotation_id);
CREATE INDEX idx_anno_links_job ON annotation_message_links(job_id);
