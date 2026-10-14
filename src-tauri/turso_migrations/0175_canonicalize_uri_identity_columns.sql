-- URI text used as identity must follow the same lowercase-project canon as
-- newly minted URIs. Prose and provenance are intentionally not rewritten.
-- The expression changes only the project segment, including when the URI is
-- embedded behind a key prefix such as `review:`.

DROP INDEX idx_attention_pushes_supersede;

UPDATE attention_pushes
SET key = substr(key, 1, instr(key, 'cairn://p/') + 9)
       || lower(substr(key, instr(key, 'cairn://p/') + 10,
          CASE instr(substr(key, instr(key, 'cairn://p/') + 10), '/')
            WHEN 0 THEN length(key)
            ELSE instr(substr(key, instr(key, 'cairn://p/') + 10), '/') - 1 END))
       || substr(key, instr(key, 'cairn://p/') + 10 +
          CASE instr(substr(key, instr(key, 'cairn://p/') + 10), '/')
            WHEN 0 THEN length(key)
            ELSE instr(substr(key, instr(key, 'cairn://p/') + 10), '/') - 1 END)
WHERE instr(key, 'cairn://p/') > 0;

DELETE FROM attention_pushes
WHERE delivered_event_id IS NULL
  AND rowid NOT IN (
    SELECT MAX(rowid) FROM attention_pushes
    WHERE delivered_event_id IS NULL GROUP BY recipient, key
  );

CREATE UNIQUE INDEX idx_attention_pushes_supersede
  ON attention_pushes(recipient, key) WHERE delivered_event_id IS NULL;

UPDATE attention_pushes SET content_ref =
  substr(content_ref, 1, instr(content_ref, 'cairn://p/') + 9)
  || lower(substr(content_ref, instr(content_ref, 'cairn://p/') + 10,
     CASE instr(substr(content_ref, instr(content_ref, 'cairn://p/') + 10), '/') WHEN 0 THEN length(content_ref) ELSE instr(substr(content_ref, instr(content_ref, 'cairn://p/') + 10), '/') - 1 END))
  || substr(content_ref, instr(content_ref, 'cairn://p/') + 10 + CASE instr(substr(content_ref, instr(content_ref, 'cairn://p/') + 10), '/') WHEN 0 THEN length(content_ref) ELSE instr(substr(content_ref, instr(content_ref, 'cairn://p/') + 10), '/') - 1 END)
WHERE instr(content_ref, 'cairn://p/') > 0;

-- Merge case aliases before updating unique identity columns. The newest
-- subscription owns state; suppressed rows are repointed before losers vanish.
UPDATE suppressed_wakes SET subscription_id = (
  SELECT winner.id FROM wake_subscriptions winner
  JOIN wake_subscriptions loser ON loser.id = suppressed_wakes.subscription_id
  WHERE winner.job_id = loser.job_id AND winner.source_kind = loser.source_kind
    AND lower(COALESCE(winner.source_ref, '')) = lower(COALESCE(loser.source_ref, ''))
    AND COALESCE(winner.fact_kinds_json, '') = COALESCE(loser.fact_kinds_json, '')
  ORDER BY winner.updated_at DESC, winner.rowid DESC LIMIT 1
) WHERE subscription_id IS NOT NULL;
DELETE FROM wake_subscriptions AS loser WHERE EXISTS (
  SELECT 1 FROM wake_subscriptions winner
  WHERE winner.job_id=loser.job_id AND winner.source_kind=loser.source_kind
    AND lower(COALESCE(winner.source_ref,''))=lower(COALESCE(loser.source_ref,''))
    AND COALESCE(winner.fact_kinds_json,'')=COALESCE(loser.fact_kinds_json,'')
    AND (winner.updated_at > loser.updated_at
      OR (winner.updated_at = loser.updated_at AND winner.rowid > loser.rowid))
);
UPDATE wake_subscriptions SET
  source_ref = CASE WHEN instr(source_ref, 'cairn://p/') > 0 THEN replace(source_ref, 'cairn://p/' || substr(source_ref, instr(source_ref, 'cairn://p/') + 10, instr(substr(source_ref, instr(source_ref, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(source_ref, instr(source_ref, 'cairn://p/') + 10, instr(substr(source_ref, instr(source_ref, 'cairn://p/') + 10) || '/', '/') - 1))) ELSE source_ref END,
  mute_until_ref = CASE WHEN instr(mute_until_ref, 'cairn://p/') > 0 THEN replace(mute_until_ref, 'cairn://p/' || substr(mute_until_ref, instr(mute_until_ref, 'cairn://p/') + 10, instr(substr(mute_until_ref, instr(mute_until_ref, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(mute_until_ref, instr(mute_until_ref, 'cairn://p/') + 10, instr(substr(mute_until_ref, instr(mute_until_ref, 'cairn://p/') + 10) || '/', '/') - 1))) ELSE mute_until_ref END;

-- Pending digest aliases collapse into the newest row without losing counts.
UPDATE suppressed_wakes SET occurrences = (
  SELECT SUM(s2.occurrences) FROM suppressed_wakes s2
  WHERE s2.job_id=suppressed_wakes.job_id AND s2.source_kind=suppressed_wakes.source_kind
    AND lower(COALESCE(s2.source_ref,''))=lower(COALESCE(suppressed_wakes.source_ref,''))
    AND COALESCE(s2.fact_kind,'')=COALESCE(suppressed_wakes.fact_kind,'')
    AND s2.content IS NULL AND s2.delivered_at IS NULL
) WHERE content IS NULL AND delivered_at IS NULL;
DELETE FROM suppressed_wakes WHERE content IS NULL AND delivered_at IS NULL AND rowid NOT IN (
  SELECT MAX(rowid) FROM suppressed_wakes WHERE content IS NULL AND delivered_at IS NULL
  GROUP BY job_id, source_kind, lower(COALESCE(source_ref,'')), COALESCE(fact_kind,'')
);
UPDATE suppressed_wakes SET
  source_ref = CASE WHEN instr(source_ref, 'cairn://p/') > 0 THEN replace(source_ref, 'cairn://p/' || substr(source_ref, instr(source_ref, 'cairn://p/') + 10, instr(substr(source_ref, instr(source_ref, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(source_ref, instr(source_ref, 'cairn://p/') + 10, instr(substr(source_ref, instr(source_ref, 'cairn://p/') + 10) || '/', '/') - 1))) ELSE source_ref END,
  latest_detail_uri = CASE WHEN instr(latest_detail_uri, 'cairn://p/') > 0 THEN replace(latest_detail_uri, 'cairn://p/' || substr(latest_detail_uri, instr(latest_detail_uri, 'cairn://p/') + 10, instr(substr(latest_detail_uri, instr(latest_detail_uri, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(latest_detail_uri, instr(latest_detail_uri, 'cairn://p/') + 10, instr(substr(latest_detail_uri, instr(latest_detail_uri, 'cairn://p/') + 10) || '/', '/') - 1))) ELSE latest_detail_uri END;

-- A follow survivor carries the furthest cursor and original follow time.
UPDATE channel_thread_follow SET
  cursor_rowid = (SELECT MAX(f2.cursor_rowid) FROM channel_thread_follow f2 WHERE f2.channel=channel_thread_follow.channel AND lower(f2.thread_uri)=lower(channel_thread_follow.thread_uri)),
  followed_at = (SELECT MIN(f2.followed_at) FROM channel_thread_follow f2 WHERE f2.channel=channel_thread_follow.channel AND lower(f2.thread_uri)=lower(channel_thread_follow.thread_uri));
DELETE FROM channel_thread_follow WHERE rowid NOT IN (
  SELECT MAX(rowid) FROM channel_thread_follow GROUP BY channel, lower(thread_uri)
);
UPDATE channel_thread_follow SET thread_uri = replace(thread_uri, 'cairn://p/' || substr(thread_uri, 11, instr(substr(thread_uri, 11) || '/', '/') - 1), 'cairn://p/' || lower(substr(thread_uri, 11, instr(substr(thread_uri, 11) || '/', '/') - 1))) WHERE thread_uri LIKE 'cairn://p/%';
UPDATE channel_thread_focus SET thread_uri = replace(thread_uri, 'cairn://p/' || substr(thread_uri, 11, instr(substr(thread_uri, 11) || '/', '/') - 1), 'cairn://p/' || lower(substr(thread_uri, 11, instr(substr(thread_uri, 11) || '/', '/') - 1))) WHERE thread_uri LIKE 'cairn://p/%';
-- Preserve the strongest external-delivery fence before aliases collapse.
UPDATE channel_outbound SET status = CASE
  WHEN EXISTS (SELECT 1 FROM channel_outbound c2 WHERE c2.channel=channel_outbound.channel AND c2.kind=channel_outbound.kind AND lower(c2.binding_ref)=lower(channel_outbound.binding_ref) AND c2.status='resolved') THEN 'resolved'
  WHEN EXISTS (SELECT 1 FROM channel_outbound c2 WHERE c2.channel=channel_outbound.channel AND c2.kind=channel_outbound.kind AND lower(c2.binding_ref)=lower(channel_outbound.binding_ref) AND c2.status='sent') THEN 'sent'
  WHEN EXISTS (SELECT 1 FROM channel_outbound c2 WHERE c2.channel=channel_outbound.channel AND c2.kind=channel_outbound.kind AND lower(c2.binding_ref)=lower(channel_outbound.binding_ref) AND c2.status='failed') THEN 'failed'
  WHEN EXISTS (SELECT 1 FROM channel_outbound c2 WHERE c2.channel=channel_outbound.channel AND c2.kind=channel_outbound.kind AND lower(c2.binding_ref)=lower(channel_outbound.binding_ref) AND c2.status='expired') THEN 'expired'
  ELSE 'pending' END;
DELETE FROM channel_outbound WHERE rowid NOT IN (
  SELECT MAX(rowid) FROM channel_outbound GROUP BY channel, kind, lower(binding_ref)
);
UPDATE channel_outbound SET binding_ref = replace(binding_ref, 'cairn://p/' || substr(binding_ref, instr(binding_ref, 'cairn://p/') + 10, instr(substr(binding_ref, instr(binding_ref, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(binding_ref, instr(binding_ref, 'cairn://p/') + 10, instr(substr(binding_ref, instr(binding_ref, 'cairn://p/') + 10) || '/', '/') - 1))) WHERE instr(binding_ref, 'cairn://p/') > 0;
UPDATE route_firings SET fact_identity = replace(fact_identity, 'cairn://p/' || substr(fact_identity, instr(fact_identity, 'cairn://p/') + 10, instr(substr(fact_identity, instr(fact_identity, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(fact_identity, instr(fact_identity, 'cairn://p/') + 10, instr(substr(fact_identity, instr(fact_identity, 'cairn://p/') + 10) || '/', '/') - 1))) WHERE instr(fact_identity, 'cairn://p/') > 0;
UPDATE route_firings SET sink_ref = replace(sink_ref, 'cairn://p/' || substr(sink_ref, instr(sink_ref, 'cairn://p/') + 10, instr(substr(sink_ref, instr(sink_ref, 'cairn://p/') + 10) || '/', '/') - 1), 'cairn://p/' || lower(substr(sink_ref, instr(sink_ref, 'cairn://p/') + 10, instr(substr(sink_ref, instr(sink_ref, 'cairn://p/') + 10) || '/', '/') - 1))) WHERE instr(sink_ref, 'cairn://p/') > 0;