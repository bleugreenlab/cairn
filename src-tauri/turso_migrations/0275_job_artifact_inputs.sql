-- Which stored artifact version each job was launched with, per input. A job's
-- launch prompt embeds (or references) the latest version of each upstream
-- artifact at that moment; this row is the structural record of that version,
-- advanced each time a later revision is delivered to the job as a message.
CREATE TABLE job_artifact_inputs (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    producer_job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    output_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, producer_job_id, output_name)
);
CREATE INDEX idx_job_artifact_inputs_producer ON job_artifact_inputs(producer_job_id, output_name);
