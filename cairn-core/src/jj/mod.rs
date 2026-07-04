//! Jujutsu (jj) driver: the all-jj VCS substrate for agent worktrees.
//!
//! Cairn provisions **one shared jj store per jj-managed project** (a single
//! commit graph and operation log), backed by the project's existing `.git` so
//! commits stay in the project's object database (pushable, readable by git
//! tooling against the project) and the user's working checkout is never
//! touched. Each job's working directory is a `jj workspace` off that one store:
//! physically isolated files over one shared graph, which is what gives
//! cross-sibling auto-rebase, the entire reason to move off git.
//!
//! Workspaces created by `jj workspace add` are non-colocated: a workspace dir
//! carries a `.jj` and **no `.git`**. Branch-keyed tooling cannot read the git
//! branch inside such a dir, so Cairn records the real branch in a marker that is
//! invisible to the working-copy commit (`<workspace>/.jj/cairn-branch` — jj
//! never snapshots its own metadata dir) and `resolveBranch` reads it. See
//! `docs/jj-migration.md`.
//!
//! jj opens `$EDITOR` for `describe`/`commit`/`squash` and writes user config
//! under `~/.config/jj` unless redirected; every command here forces
//! `EDITOR=true`/`JJ_EDITOR=true` and points `JJ_CONFIG` at a Cairn-managed file.

mod bookmark;
mod conflict;
mod diff;
mod env;
mod errors;
mod merge;
mod reconcile;
mod seal;
mod workspace;
mod worktree;

#[cfg(test)]
mod tests;

pub use bookmark::*;
pub use conflict::*;
pub use diff::*;
pub use env::*;
pub use errors::*;
pub use merge::*;
pub use reconcile::*;
pub use seal::*;
pub use workspace::*;
pub use worktree::*;

// Crate-internal helpers shared across jj submodules; not part of the public
// jj API.
pub(crate) use conflict::revset_descends_from;
pub(crate) use diff::parse_git_diff;
pub(crate) use env::quote_fileset;
pub(crate) use errors::{CONFLICTED_BRANCH_SEAL_MSG, LOST_SEAL_MSG};
pub(crate) use seal::scoped_dirty;
pub(crate) use worktree::sealed_tree_hash_via_git;

// Referenced only by the jj test suite (unused in non-test builds).
#[cfg(test)]
pub(crate) use env::populate_auto_track_expr;
#[cfg(test)]
pub(crate) use reconcile::restore_bookmark;
#[cfg(test)]
pub(crate) use seal::sealed_commit_is_lost;
#[cfg(test)]
pub(crate) use worktree::parse_ls_tree;
