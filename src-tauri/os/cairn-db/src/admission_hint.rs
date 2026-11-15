//! A hint that evidence a held execution reads may have moved.
//!
//! The gate sweep lives in `cairn-core`; workflow and session transition
//! writers schedule [`evidence_changed`] with `TrackedConnection::on_commit`
//! so a rolled-back transition cannot race the sweep. Core installs the
//! coalescing notifier as the hook. A hint carries nothing: a host with no hook
//! drops it, and the scheduler's periodic sweep releases whatever it would have.

use std::cell::Cell;
use std::sync::OnceLock;

static HOOK: OnceLock<fn()> = OnceLock::new();

thread_local! {
    static SENT: Cell<u64> = const { Cell::new(0) };
}

/// Install the hook. The first installation wins; there is one sweep per
/// process.
pub fn install(hook: fn()) {
    let _ = HOOK.set(hook);
}

/// Say that something a held execution might be waiting on has moved.
pub fn evidence_changed() {
    SENT.with(|sent| sent.set(sent.get() + 1));
    if let Some(hook) = HOOK.get() {
        hook();
    }
}

/// How many hints this thread has sent. A hint is otherwise unobservable (a
/// host with no hook drops it), so a test reads this around a write to prove
/// the writer nudged; per thread, so parallel tests do not see each other's.
pub fn sent_on_this_thread() -> u64 {
    SENT.with(Cell::get)
}
