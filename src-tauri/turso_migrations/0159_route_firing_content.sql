-- A route firing's meaning is the content that moved through it, so the journal
-- snapshots that content at fire time instead of holding a ref into another
-- table. `channel_outbound` is provider-account state with its own lifetime and
-- retention, and a message sink's target compacts; either way the firing row
-- must stay readable after its referent is gone.
ALTER TABLE route_firings ADD COLUMN fact_summary TEXT;
ALTER TABLE route_firings ADD COLUMN payload_text TEXT;

-- Existing channel firings still have their ledger row, so the delivered text is
-- recoverable exactly once, here. `binding_ref` is the route's own composite key
-- (`route:<route id>:<fact identity>`), which is why this join is exact rather
-- than heuristic. Rows whose ledger entry is already gone stay NULL and render
-- as unrecorded.
UPDATE route_firings
SET payload_text = (
  SELECT substr(o.rendered_text, 1, 2000)
  FROM channel_outbound o
  WHERE o.kind = 'route'
    AND o.binding_ref = 'route:' || route_firings.route_id || ':' || route_firings.fact_identity
)
WHERE sink_kind = 'channel';
