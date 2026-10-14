-- The PR head commit SHA: the precise content key for a review push's change
-- fingerprint (CAIRN-1891). The diffstat (additions/deletions) can collide across
-- two different commits with the same +/-, missing a real re-review; the head SHA
-- cannot. Nullable: populated from the GitHub webhook payload
-- (pull_request.head.sha); rows without a webhook (e.g. local PRs) fall back to
-- the diffstat fingerprint.
ALTER TABLE merge_requests ADD COLUMN head_sha TEXT;
