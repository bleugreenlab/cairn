-- lineage: shared
-- Probe resolved thread-resume claims retaining input, then seek each job's head.
CREATE INDEX idx_provider_limit_holds_resolved_thread_resume
ON provider_limit_holds(job_id, turn_id, id)
WHERE state = 'resolved' AND kind = 'resume' AND resume_json IS NOT NULL
  AND boundary_source IN ('thread_focus', 'thread_open_prs');
