-- lineage: shared
-- rebuild-fk-off: Conserved(permission_requests)
-- cairn/7649: one tool call may raise several permission requests in turn.
--
-- A `write` whose items each cross an authority boundary (or each escape the
-- fence) raises one card per item: the first suspends the call, and once it is
-- answered the re-driven call passes that item and raises the next card under
-- the same tool call. `UNIQUE (run_id, tool_use_id)` refused that second card,
-- so the call failed with a storage error instead of asking. What the code
-- relies on is narrower: at most one PENDING request owns a tool call's
-- continuation (every lookup by run and tool call filters status='pending').
-- The table-level key cannot be dropped in place, so the table is rebuilt
-- without it, every column in its order, then its indexes and trigger as they
-- stood, and the pending-only key is added.
CREATE TABLE permission_requests_pending_per_call_rebuild (
    id TEXT NOT NULL PRIMARY KEY,
    run_id TEXT NOT NULL,
    tool_use_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_input TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    response TEXT,
    created_at INTEGER NOT NULL,
    responded_at INTEGER,
    turn_id TEXT,
    job_id TEXT,
    uri_segment TEXT,
    resolution_id TEXT,
    resolution_surface TEXT CHECK (resolution_surface IN ('resource_patch','channel_reply','remote_intent','non_operator_invoke','local_invoke','authenticated_operator','authenticated_desktop','legacy_unknown')),
    resolution_provider TEXT,
    resolution_conversation TEXT,
    resolution_actor TEXT,
    resume_error TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (turn_id) REFERENCES turns(id)
);
INSERT INTO permission_requests_pending_per_call_rebuild (
    id, run_id, tool_use_id, tool_name, tool_input, status, response, created_at,
    responded_at, turn_id, job_id, uri_segment, resolution_id, resolution_surface,
    resolution_provider, resolution_conversation, resolution_actor, resume_error
)
SELECT id, run_id, tool_use_id, tool_name, tool_input, status, response, created_at,
       responded_at, turn_id, job_id, uri_segment, resolution_id, resolution_surface,
       resolution_provider, resolution_conversation, resolution_actor, resume_error
  FROM permission_requests;
DROP TABLE permission_requests;
ALTER TABLE permission_requests_pending_per_call_rebuild RENAME TO permission_requests;
CREATE INDEX idx_permission_requests_run_id ON permission_requests (run_id);
CREATE INDEX idx_permission_requests_status ON permission_requests (status);
CREATE INDEX idx_permission_requests_turn_id ON permission_requests (turn_id);
CREATE UNIQUE INDEX idx_permission_requests_job_uri_segment ON permission_requests (job_id, uri_segment)
    WHERE job_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE UNIQUE INDEX idx_permission_requests_pending_tool_use ON permission_requests (run_id, tool_use_id)
    WHERE status = 'pending';
CREATE TRIGGER validate_permission_provenance_update BEFORE UPDATE OF resolution_surface,resolution_provider,resolution_conversation,resolution_actor ON permission_requests
WHEN (NEW.resolution_surface = 'channel_reply' AND (COALESCE(NEW.resolution_provider,'') = '' OR COALESCE(NEW.resolution_conversation,'') = '' OR COALESCE(NEW.resolution_actor,'') = ''))
  OR (NEW.resolution_surface <> 'channel_reply' AND (NEW.resolution_provider IS NOT NULL OR NEW.resolution_conversation IS NOT NULL))
  OR (NEW.resolution_surface = 'resource_patch' AND COALESCE(NEW.resolution_actor,'') = '')
BEGIN SELECT RAISE(ABORT, 'invalid permission provenance'); END;
