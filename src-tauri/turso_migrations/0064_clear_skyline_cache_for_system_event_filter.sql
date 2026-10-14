-- Clear the derived session skyline cache so existing rows rebuild without bars
-- for non-rendered `system:*` bookkeeping events (thinking-token tallies, status
-- pings, hook lifecycle, init handshakes). build_skyline_bars now emits a system
-- bar only for the subtypes the transcript renders (message, error,
-- compact_boundary, prompt); the cache is keyed only by event/vibe watermarks,
-- which do not change when the bar set changes, so the stale phantom bars would
-- otherwise persist on completed sessions. The cache is purely derived and safe
-- to drop; load_or_build_session_skyline_bars transparently rebuilds on read.
DELETE FROM session_skyline_cache;
