-- A merge request is "local" when it was created without a GitHub remote — a
-- durable, creation-time fact. It was previously inferred from a missing
-- github_pr_number, which wrongly classified a remote PR as local during the
-- window between opening the PR and GitHub returning its number. Store it.
ALTER TABLE merge_requests ADD COLUMN is_local INTEGER NOT NULL DEFAULT 0;

-- Backfill settled rows with the old inference: a row that never received a
-- GitHub PR number was local. New rows set is_local explicitly at creation.
UPDATE merge_requests SET is_local = 1 WHERE github_pr_number IS NULL;
