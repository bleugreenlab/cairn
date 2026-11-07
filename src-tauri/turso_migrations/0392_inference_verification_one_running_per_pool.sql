-- lineage: private
-- inference verifications: at most one running per pool

-- A verification claims its pool by checking that none is running there and
-- inserting its own row. Under BEGIN CONCURRENT two claims each read the pool
-- idle and insert different ids, so both committed and two verifications drove
-- one pool. A key over the running rows per pool makes the second claim
-- conflict and re-read (cairn/6166; docs/database.md, "Write skew").
CREATE UNIQUE INDEX idx_inference_verifications_running_pool
    ON inference_verifications (pool_id) WHERE outcome = 'running';
