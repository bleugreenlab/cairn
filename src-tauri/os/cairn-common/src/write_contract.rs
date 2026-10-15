//! The `write` verb's time contract, stated once for every crate that must
//! agree about it. The sibling of [`run_contract`](crate::run_contract), and
//! written for the same reason: a budget stated independently in two places is a
//! place for two layers to disagree.
//!
//! A file-target `write` serializes on the project's jj store lock, so its host
//! budget is dominated by how long it may wait for that lock. Under a base
//! advance that wait is real, not theoretical. cairn-cmd sizes its HTTP socket
//! from this constant plus a margin; cairn-core waits on the lock for exactly
//! this long. Because they are the same fact, they cannot drift.

/// How long a file-target `write` may wait for the project store lock before it
/// gives up and reports the contention.
///
/// This is the host budget the cairn-cmd callback socket wraps. The socket must
/// sit strictly above it: a socket that fires first hands the agent a transport
/// error for a write the host then goes on to land, and the agent's natural next
/// move is to re-issue a batch that has already been applied. That is one of the
/// deliveries the write-replay guard exists to absorb (CAIRN-3264) — but a
/// guard against a race is not a reason to keep the race.
pub const WRITE_STORE_LOCK_WAIT_MS: u64 = 600_000;

/// How long a committing `write` or `run` waits inline for the when:write
/// checks of the commit it just sealed before answering with the checks marked
/// pending.
///
/// The commit is the call's result; the checks are commentary on it. A wave
/// that runs past this keeps running in the background and lands its verdicts
/// at `cairn:~/checks`. Without this bound a hung or slow wave held the result
/// open until the callback socket gave up, and the agent saw a transport
/// failure for a write that had already committed.
pub const WRITE_CHECK_INLINE_WAIT_MS: u64 = 120_000;
