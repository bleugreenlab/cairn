-- lineage: private
-- cairn/7108: replyTo on a conversation append.
--
-- An inbound message's own provider id (`chat:message` on Telegram,
-- `channel:message` on Discord, the message guid on iMessage) is what a later
-- native reply references. It gets its own column: `provider_guid` on an
-- inbound row is the id of the message it replied to, and it carries the
-- table's dedup fence. Rows from before this column read NULL and cannot be
-- replied to natively.
--
-- An outbound row records the Cairn message id it replies to (`reply_to`) and
-- the provider id it referenced natively (`reply_to_guid`), which is NULL when
-- the provider has no native reply and it was sent plainly.
ALTER TABLE channel_inbound ADD COLUMN message_guid TEXT;
ALTER TABLE channel_outbound ADD COLUMN reply_to TEXT;
ALTER TABLE channel_outbound ADD COLUMN reply_to_guid TEXT;
