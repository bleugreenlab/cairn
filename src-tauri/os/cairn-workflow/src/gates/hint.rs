//! A hint that evidence a held execution reads may have moved.
//!
//! The gate sweep lives in `cairn-core`, but some of what frees a hold is
//! written here: a job leaving `pending`/`running` and a turn yielding free a
//! fleet slot, which a capacity-held execution is waiting for. Those writers
//! call [`evidence_changed`]; `cairn-core` installs the sweep's coalescing
//! notifier as the hook. A hint carries nothing: a host with no hook drops it,
//! and the scheduler's periodic sweep releases whatever it would have.

use std::sync::OnceLock;

static HOOK: OnceLock<fn()> = OnceLock::new();

/// Install the hook. The first installation wins; there is one sweep per
/// process.
pub fn install(hook: fn()) {
    let _ = HOOK.set(hook);
}

/// Say that something a held execution might be waiting on has moved.
pub fn evidence_changed() {
    if let Some(hook) = HOOK.get() {
        hook();
    }
}
