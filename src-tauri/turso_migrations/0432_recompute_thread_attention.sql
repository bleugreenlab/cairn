-- lineage: private
-- cairn/4947: `threads.attention` is a persisted projection, and answering a
-- thread job's question or permission request recomputed only the issue. Threads
-- kept reading `needs_input` or `needs_authorization` after their last prompt was
-- answered. The answer and withdrawal paths now recompute the thread in the same
-- transaction; this converges the rows older builds left behind once, from the
-- same three facts `recompute_thread_attention_conn` derives it from, in its
-- order: an open question, then a pending permission request, then a blocked
-- job. Only rows whose value changes are written, so a thread that was already
-- right keeps its `updated_at`.
UPDATE threads
   SET attention = (
         CASE
           WHEN EXISTS (
             SELECT 1 FROM prompts p
               JOIN runs r ON p.run_id = r.id
               JOIN jobs j ON r.job_id = j.id
              WHERE j.thread_id = threads.id
                AND p.response IS NULL AND p.withdrawn_at IS NULL)
             THEN 'needs_input'
           WHEN EXISTS (
             SELECT 1 FROM permission_requests pr
               JOIN runs r ON pr.run_id = r.id
               JOIN jobs j ON r.job_id = j.id
              WHERE j.thread_id = threads.id AND pr.status = 'pending')
             THEN 'needs_authorization'
           WHEN EXISTS (
             SELECT 1 FROM jobs j
              WHERE j.thread_id = threads.id AND j.status = 'blocked')
             THEN 'needs_approval'
           ELSE 'none'
         END),
       updated_at = unixepoch()
 WHERE attention != (
         CASE
           WHEN EXISTS (
             SELECT 1 FROM prompts p
               JOIN runs r ON p.run_id = r.id
               JOIN jobs j ON r.job_id = j.id
              WHERE j.thread_id = threads.id
                AND p.response IS NULL AND p.withdrawn_at IS NULL)
             THEN 'needs_input'
           WHEN EXISTS (
             SELECT 1 FROM permission_requests pr
               JOIN runs r ON pr.run_id = r.id
               JOIN jobs j ON r.job_id = j.id
              WHERE j.thread_id = threads.id AND pr.status = 'pending')
             THEN 'needs_authorization'
           WHEN EXISTS (
             SELECT 1 FROM jobs j
              WHERE j.thread_id = threads.id AND j.status = 'blocked')
             THEN 'needs_approval'
           ELSE 'none'
         END);
