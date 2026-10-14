//! The `run` verb's time contract, stated once for every crate that must agree
//! about it.
//!
//! Three layers once derived a run item's budget independently — the cairn-cmd
//! HTTP callback socket, the executor batch item, and the host-local fallback —
//! and disagreed by a factor of six. The socket was the smallest, so it died
//! first and discarded every byte a full test suite had produced while the suite
//! itself kept running. These constants exist so "how long may this take" has
//! exactly one answer: cairn-cmd sizes its socket from the grace window and
//! advertises the ceiling in the `run` tool contract, cairn-core enforces both.

/// How long a `run` call stays synchronous. A batch that settles inside this
/// window returns from the original call; past it the call suspends durably and
/// the agent resumes with the same completed result. Grace governs the SHAPE of
/// the call and nothing else — it never bounds an item.
pub const RUN_GRACE_WINDOW_MS: u64 = 120_000;

/// The absolute bound on one `run` batch, and therefore on any single item in
/// it. An item that omits `timeout` is bounded only by this, so a real build or
/// test suite runs to completion; an explicit `timeout` is honored up to it.
///
/// There is deliberately no second, smaller bound underneath. A quieter
/// ten-minute cap once sat here and killed a no-timeout `bun run test:rust`
/// halfway through with no way for the agent to tell that from a real failure.
/// A batch that genuinely reaches six hours is a process that should have been a
/// terminal, and it fails loudly saying exactly that.
pub const RUN_BATCH_CEILING_MS: u32 = 6 * 60 * 60 * 1_000;

/// [`RUN_BATCH_CEILING_MS`] in whole hours, for the prose that advertises it to
/// agents. The advertised text is generated from this so the documented maximum
/// and the enforced one cannot drift apart.
pub const RUN_BATCH_CEILING_HOURS: u32 = RUN_BATCH_CEILING_MS / (60 * 60 * 1_000);
