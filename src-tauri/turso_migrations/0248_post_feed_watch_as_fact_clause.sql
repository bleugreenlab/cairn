-- cairn/4913: a Posts watch is the fact clause {fact: post_created}.
--
-- Posts now reach their readers through the fact log. A `{subscribe:{kind:"posts"}}`
-- watch compiles to a `fact` subscription on that clause's canonical JSON, which
-- the log worker's index lane treats as an ordinary consumer under the one
-- jurisdiction rule every consumer gets. This rewrites the watches written
-- before that, in place, keeping each row's id and state: a muted watch stays
-- muted (and rides along), an unsubscribed one stays a refusal.
--
-- A ref'd `post` row never received anything (the retired feed path read only
-- the ref-less watch) and its kind no longer exists, so it is deleted. A job
-- with two ref-less rows (different fact kinds) would collide on the unique
-- scope index once both are rewritten, so only one survives, chosen by the
-- greatest id: deterministic, and either row says the job watches Posts.

DELETE FROM wake_subscriptions WHERE source_kind = 'post' AND source_ref IS NOT NULL;

DELETE FROM wake_subscriptions
 WHERE source_kind = 'post'
   AND id NOT IN (SELECT MAX(id) FROM wake_subscriptions WHERE source_kind = 'post' GROUP BY job_id);

DELETE FROM wake_subscriptions
 WHERE source_kind = 'post'
   AND job_id IN (SELECT job_id FROM wake_subscriptions
                   WHERE source_kind = 'fact'
                     AND source_ref = '{"fact":"post_created"}'
                     AND fact_kinds_json = '["post_created"]');

UPDATE wake_subscriptions
   SET source_kind = 'fact',
       source_ref = '{"fact":"post_created"}',
       fact_kinds_json = '["post_created"]'
 WHERE source_kind = 'post';
