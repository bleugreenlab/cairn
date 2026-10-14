-- CAIRN-1647: pivot attention items from typed content blobs to URI-resolution
-- delivery.
--
-- An item no longer stores serialized per-kind content. Instead it stores a
-- small `fingerprint` (a stable change-detection string used purely for the
-- open/bump/unchanged idempotency decision) plus its existing `detail_uri`. At
-- delivery the briefing resolves `detail_uri` through the same in-process read
-- that backs `cairn read [uri]`, yielding the rendered content AND its
-- affordance/actions block inline. This removes the duplicate, drift-prone
-- content modelling and makes the chat-window message digest fall out for free.
--
-- Add the fingerprint column (default '' so existing rows stay valid; a real
-- fingerprint differs, so any still-open item simply bumps once on its next
-- emission) and drop the now-unused content blob.
ALTER TABLE attention_items ADD COLUMN fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE attention_items DROP COLUMN content_json;
