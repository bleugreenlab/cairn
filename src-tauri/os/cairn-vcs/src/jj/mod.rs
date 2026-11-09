//! Jujutsu (jj) driver for the runner-owned branch store.
//!
//! Cairn maintains one runner-owned jj store per repository. The store is the
//! branch graph and logical-head authority; agent processes live in scratch and
//! never own a jj workspace. Executor materializations are disposable
//! projections whose deltas publish back through the runner's compare-and-swap
//! transaction.
//!
//! jj opens `$EDITOR` for `describe`/`commit`/`squash` and writes user config
//! under `~/.config/jj` unless redirected; every command here forces
//! `EDITOR=true`/`JJ_EDITOR=true` and points `JJ_CONFIG` at a Cairn-managed file.

mod advance;
mod bookmark;
mod catch_up;
mod combine;
mod conflict;
mod diff;
mod emptied;
mod env;
mod errors;
mod export;
mod heal;
mod in_place;
mod landing_gate;
mod leased_push;
mod maintenance;
mod merge;
mod object_lease;
mod packed_refs;
mod publish;
mod rebuild;
mod reconcile;
mod recovery;
mod retire;
mod seal;
mod three_way;
mod workspace;
mod worktree;

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

pub use advance::*;
pub use bookmark::*;
pub use catch_up::*;
pub use combine::{TwinStack, VersionBase};
pub use conflict::*;
pub use diff::*;
pub use emptied::{drop_empty_commits, DroppedEmpty};
pub use env::*;
pub use errors::*;
pub use export::*;
pub use heal::*;
pub use in_place::in_place_blocking;
pub use landing_gate::*;
pub use leased_push::{
    confirm_leased_push, publish_landing_atomic, publish_landing_atomic_unrecorded,
    push_bookmark_expecting, push_commit_expecting_unrecorded, push_managed_branch_unrecorded,
    push_refs_leased, readback_leased_push, readback_leased_push_exact, record_leased_push,
    record_origin_observation, retire_remote_branch_unrecorded, AcceptedHeads, AcceptedRef,
    LandingPublication, LeasedPush, LeasedPushError, LeasedRefRole, LeasedRefUpdate, TargetProof,
    RECORD_COMMITTED_UNVERIFIED,
};
pub use maintenance::*;
pub use merge::*;
pub use object_lease::StoreObjectLease;
pub use packed_refs::{
    take_packed_refs_waited, with_packed_refs_patience, with_packed_refs_patience_async, PackedRefs,
};
pub use publish::*;
pub use rebuild::{rebuild_flagged_branch, RebuildOutcome};
pub use reconcile::*;
pub use recovery::{
    interrupted_repair_coordinates, repair_branch_recovery, validate_store_recovery,
    BookmarkRecovery, RecoveryCoordinates, RecoveryScope, RepairVerdict, RollbackFailurePhase,
    StoreValidation, SurvivorRecord,
};
pub use retire::{
    read_local_branch_state, retire_bookmark_expecting, LocalBranchState, Retirement,
};
pub use seal::*;
pub use three_way::*;
pub use workspace::*;
pub use worktree::*;

// Crate-internal helpers shared across jj submodules; not part of the public
// jj API.
pub(crate) use env::quote_fileset;
pub(crate) use errors::LOST_SEAL_MSG;
#[cfg(test)]
pub(crate) use packed_refs::at_step;
pub(crate) use packed_refs::{
    packed_refs_lock_error, reached_step, run_store_rewrite, CONTENTION_DIAGNOSIS, STALE_DIAGNOSIS,
};

// Referenced only by the jj test suite (unused in non-test builds).
#[cfg(test)]
pub(crate) use reconcile::restore_bookmark;
#[cfg(test)]
pub(crate) use seal::sealed_commit_is_lost;
#[cfg(test)]
pub(crate) use worktree::{parse_grep_z, parse_ls_tree};
