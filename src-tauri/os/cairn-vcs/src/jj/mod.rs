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

mod bookmark;
mod combine;
mod conflict;
mod diff;
mod env;
mod errors;
mod export;
mod heal;
mod landing_gate;
mod maintenance;
mod merge;
mod packed_refs;
mod publish;
mod reconcile;
mod seal;
mod three_way;
mod workspace;
mod worktree;

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

pub use bookmark::*;
pub use combine::{TwinStack, VersionBase};
pub use conflict::*;
pub use diff::*;
pub use env::*;
pub use errors::*;
pub use export::*;
pub use heal::*;
pub use landing_gate::*;
pub use maintenance::*;
pub use merge::*;
pub use packed_refs::{
    take_packed_refs_waited, with_packed_refs_patience, with_packed_refs_patience_async, PackedRefs,
};
pub use publish::*;
pub use reconcile::*;
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
