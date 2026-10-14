-- A channel-answered gate whose domain effect was refused because only an
-- operator may approve it is not a transient failure. The sweep that applies
-- these effects had no way to say so: it released the lease, logged, and tried
-- again on the next tick forever (CAIRN-4364 -- four such actions produced
-- ~112,000 identical 'will retry' lines in one day).
--
-- Parking is that classification made durable. A parked action is invisible to
-- the cadence sweep and waits for an event: any surface that leases the action
-- again -- an operator answering through the desktop or the permissions
-- resource -- clears the park as part of taking it.
--
-- No backfill. The actions already stuck predate the column, and the first
-- sweep after this migration parks each one exactly once through the code path,
-- which also withdraws the answer that cannot be applied. Marking them parked
-- here would silence them while leaving that answer holding the gate.
ALTER TABLE channel_ask_action ADD COLUMN parked_at INTEGER;
ALTER TABLE channel_ask_action ADD COLUMN parked_reason TEXT;
