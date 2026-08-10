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
mod conflict;
mod diff;
mod env;
mod errors;
mod export;
mod merge;
mod publish;
mod reconcile;
mod seal;
mod three_way;
mod workspace;
mod worktree;

#[cfg(test)]
pub(crate) mod tests;

pub use bookmark::*;
pub use conflict::*;
pub use diff::*;
pub use env::*;
pub use errors::*;
pub use export::*;
pub use merge::*;
pub use publish::*;
pub use reconcile::*;
pub use seal::*;
pub use three_way::*;
pub use workspace::*;
pub use worktree::*;

// Crate-internal helpers shared across jj submodules; not part of the public
// jj API.
pub(crate) use conflict::revset_descends_from;
pub(crate) use diff::parse_git_diff;
pub(crate) use env::quote_fileset;
pub(crate) use errors::{CONFLICTED_BRANCH_SEAL_MSG, LOST_SEAL_MSG};
pub(crate) use worktree::sealed_tree_hash_via_git;

// Referenced only by the jj test suite (unused in non-test builds).
#[cfg(test)]
pub(crate) use reconcile::restore_bookmark;
#[cfg(test)]
pub(crate) use seal::sealed_commit_is_lost;
#[cfg(test)]
pub(crate) use worktree::parse_ls_tree;
