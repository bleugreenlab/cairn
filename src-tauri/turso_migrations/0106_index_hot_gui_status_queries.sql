-- Hot GUI status reads join jobs by issue, list jobs by execution, and correlate
-- message streams by turn. Without these indexes each refresh scans hot tables.
CREATE INDEX idx_jobs_issue_id ON jobs (issue_id);
CREATE INDEX idx_jobs_execution_id ON jobs (execution_id);
CREATE INDEX idx_message_streams_turn_id ON message_streams (turn_id);
