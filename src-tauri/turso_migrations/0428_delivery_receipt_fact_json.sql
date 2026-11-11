-- lineage: shared
-- cairn/6510: the facts of a fact that has no row of its own, and a
-- recipient's receipts found by the row they share.
--
-- A receipt references its fact's durable content (`fact_row_id`) and never
-- copies a body. A one-shot rebuild subscription's firing
-- (`rebuild-wake:<subscription id>`) is a fact with no other home: the
-- subscription row is spent in the same transaction that writes the
-- receipt, and a boot that changed no build has no camp fact to point at.
-- `fact_json` holds that firing's structured facts (the target, the commit it
-- resolved to, the boot's commit, version and build) so the drain renders
-- the same line after a restart. NULL for every receipt whose fact is a row.
ALTER TABLE delivery_receipts ADD COLUMN fact_json TEXT;

-- A recipient's receipts that share a row are one fact: a new receipt joins
-- its queued sibling's wake, and the drain settles one a delivered sibling
-- already carried. Both look siblings up by recipient and row.
CREATE INDEX idx_delivery_receipts_row ON delivery_receipts(recipient_kind, recipient_id, fact_row_id, state);
