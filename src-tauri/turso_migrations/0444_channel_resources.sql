-- lineage: private
-- cairn/6772: channels as legible resources.
--
-- `addressed_by` keeps the address an ambient message opened with (`@channels`,
-- `<#1537606844975943712>`), which the router consumes before delivering the
-- rest. Without it the conversation view could not say why a message reached the
-- thread it did. Rows from before this column read NULL.
--
-- `cairn://channels/{provider}/{conversation}` reads one conversation's inbound
-- and outbound rows in time order and joins each inbound message to the route
-- firings its `channel_message` fact caused (by fact identity), and
-- `cairn://channels/discord/{project}/{name}` resolves to its binding by
-- target, so each of those lookups gets an index.
ALTER TABLE channel_inbound ADD COLUMN addressed_by TEXT;
CREATE INDEX idx_channel_inbound_conversation ON channel_inbound(conversation, received_at);
CREATE INDEX idx_channel_outbound_conversation ON channel_outbound(conversation, created_at);
CREATE INDEX idx_channel_binding_target ON channel_conversation_binding(provider, target_uri);
CREATE INDEX idx_route_firings_fact_identity ON route_firings(fact_identity);
