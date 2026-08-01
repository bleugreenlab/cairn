//! The boundary between a run this host process owns and one it inherited.
//!
//! Three places need this answer and must never disagree about it: the
//! tool-serving fence in [`crate::dispatch`], which refuses calls for runs it does
//! not own; the startup sweep in [`crate::runs::queries`], which stops surviving
//! processes and marks their runs crashed; and the in-session reaper in
//! [`crate::runs::reap`], which settles run rows this host has no process for. A
//! fence that refuses a run while the sweep considers it live — or worse, a sweep
//! that signals a process the fence would have served — is the same class of
//! process/recording disagreement this whole mechanism exists to close, so the
//! rule lives here once (CAIRN-3287).

/// The moment a run began, for every question about whether it is still ours or
/// still alive.
///
/// `started_at` is stamped on the single permitted `Starting -> Live` transition
/// (`backends::run_state::transition_run_to_live`) and every spawn inserts its own
/// `runs` row. `started_at` is NULL only while a run is still `starting` — which
/// is exactly the row both callers below are asking about — and `created_at` is
/// NOT NULL and precedes it by at most the spawn itself, so it is the fallback.
pub(crate) fn run_began_at(started_at: Option<i64>, created_at: i64) -> i64 {
    started_at.unwrap_or(created_at)
}

/// Whether a run was spawned before `boot_at`, and therefore belongs to a
/// predecessor host process rather than this one.
///
/// The comparison is strict: these are whole seconds, so a run spawned during the
/// boot second carries `started_at == boot_at` and is ours.
pub(crate) fn predates_host_boot(started_at: Option<i64>, created_at: i64, boot_at: i64) -> bool {
    run_began_at(started_at, created_at) < boot_at
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_from_before_boot_is_a_predecessors() {
        assert!(predates_host_boot(Some(100), 99, 200));
    }

    #[test]
    fn a_run_from_the_boot_second_onward_is_ours() {
        assert!(!predates_host_boot(Some(200), 199, 200));
        assert!(!predates_host_boot(Some(201), 200, 200));
    }

    #[test]
    fn a_starting_run_falls_back_to_created_at() {
        assert!(predates_host_boot(None, 100, 200));
        assert!(!predates_host_boot(None, 250, 200));
    }
}
