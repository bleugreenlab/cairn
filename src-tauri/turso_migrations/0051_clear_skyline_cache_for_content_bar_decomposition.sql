-- Clear the derived session skyline cache so existing rows rebuild with the new
-- per-segment (thinking / content / tools) bar decomposition. The cache is keyed
-- only by event/vibe watermarks, which do not change when the bar-building logic
-- changes, so stale content-less bars would otherwise persist until a session
-- gains a new event. The cache is purely derived and safe to drop;
-- load_or_build_session_skyline_bars transparently rebuilds each session on read.
DELETE FROM session_skyline_cache;
