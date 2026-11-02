//! A hint that evidence a held execution reads may have moved.
//!
//! The gate sweep lives in `cairn-core`, but some of what frees a hold is
//! written here: a job leaving `pending`/`running` and a turn yielding free a
//! fleet slot, and a job ending can drop its issue out of its thread's
//! open-children count; a capacity-held execution waits on either. Those
//! writers call [`evidence_changed`]; `cairn-core` installs the sweep's
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
