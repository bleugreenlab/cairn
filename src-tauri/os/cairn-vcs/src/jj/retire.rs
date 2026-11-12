//! Retiring a branch's local copy by compare-and-swap.
//!
//! A branch is retired only as the caller saw it. The bookmark (the store's
//! answer) and the git ref (what the backing repository holds, which an
//! unexported store move can leave behind) are read together, from one settled
//! operation, and the delete re-reads both and refuses, before changing
//! anything, when either moved. The bookmark is removed by one jj-lib
//! transaction that touches nothing else, never through the jj CLI, which can
//! import or export refs as a side effect and so overwrite a git ref the caller
//! has not judged. The git ref is then deleted against the exact value read. A
//! failure after the bookmark is gone is reported as partial, never undone.

use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::{RefName, RemoteRefSymbol};
use jj_lib::repo::ReadonlyRepo;
use std::path::Path;
use std::sync::Arc;

use super::store_git_dir;

/// What one branch's local copies hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBranchState {
    /// The store bookmark.
    pub bookmark: Option<String>,
    /// `refs/heads/<branch>` in the store's backing repository.
    pub git_ref: Option<String>,
    /// The store's tracked `<branch>@origin`: the old value a remote delete of
    /// the branch leases on. `None` when it is absent or untracked.
    pub tracked_origin: Option<String>,
    /// The store's recorded `<branch>@origin`, tracked or not: the last value
    /// this store pushed there or took from there, and so the exact lease a
    /// publication of the branch sends. `None` when it is absent or conflicted.
    pub origin: Option<String>,
}

/// How a retirement ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retirement {
    /// Both local copies are gone.
    Retired,
    /// A local copy moved since the caller's read; nothing was changed.
    Refused(String),
    /// The bookmark was removed but the git ref could not be; the ref stays.
    Partial(String),
}

/// Read `branch`'s bookmark, git ref and tracked origin copy. The caller holds
/// the store's object lease.
pub fn read_local_branch_state(store: &Path, branch: &str) -> Result<LocalBranchState, String> {
    block_on(async {
        let repo = settled_repo(store).await?;
        state_at(&repo, store, branch)
    })
}

/// Every local bookmark the store's one settled operation records, read from
/// its view without a jj process. The caller holds the store's object lease.
pub fn local_bookmark_names(store: &Path) -> Result<std::collections::BTreeSet<String>, String> {
    block_on(async {
        let repo = settled_repo(store).await?;
        Ok(repo
            .view()
            .local_bookmarks()
            .map(|(name, _)| name.as_str().to_owned())
            .collect())
    })
}

/// Remove `branch`'s bookmark and git ref if they still hold exactly what
/// `expected` says. The caller holds the branch reserved and the store lock,
/// so no Cairn writer moves either between this read and the removal.
pub fn retire_bookmark_expecting(
    store: &Path,
    branch: &str,
    expected: &LocalBranchState,
) -> Result<Retirement, String> {
    block_on(async {
        let repo = settled_repo(store).await?;
        let now = state_at(&repo, store, branch)?;
        if now.bookmark != expected.bookmark || now.git_ref != expected.git_ref {
            return Ok(Retirement::Refused(format!(
                "`{branch}` moved since it was judged (bookmark {:?} -> {:?}, git ref {:?} -> {:?})",
                expected.bookmark, now.bookmark, expected.git_ref, now.git_ref
            )));
        }
        if expected.bookmark.is_some() {
            let mut transaction = repo.start_transaction();
            transaction
                .repo_mut()
                .set_local_bookmark_target(RefName::new(branch), RefTarget::absent());
            transaction
                .commit(format!("retire branch {branch}"))
                .await
                .map_err(|error| format!("retire the bookmark `{branch}`: {error}"))?;
        }
        if let Some(git_ref) = expected.git_ref.as_deref() {
            if let Err(error) = delete_git_ref(store, branch, git_ref) {
                return Ok(Retirement::Partial(format!(
                    "the bookmark `{branch}` is retired, but refs/heads/{branch} was kept: {error}"
                )));
            }
        }
        Ok(Retirement::Retired)
    })
}

/// The store at its one operation head. A store whose operation log is split
/// is refused rather than merged: retiring from a merged view would publish a
/// reconciliation nobody judged.
async fn settled_repo(store: &Path) -> Result<Arc<ReadonlyRepo>, String> {
    let workspace = crate::load_workspace(store)?;
    let loader = workspace.repo_loader();
    let heads = loader
        .op_heads_store()
        .get_op_heads()
        .await
        .map_err(|error| format!("read operation heads: {error}"))?;
    let [head] = heads.as_slice() else {
        return Err(format!(
            "the store has {} operation heads; a branch is retired only from one settled operation",
            heads.len()
        ));
    };
    let operation = loader
        .load_operation(head)
        .await
        .map_err(|error| format!("load the store's operation: {error}"))?;
    loader
        .load_at(&operation)
        .await
        .map_err(|error| format!("load the store at its operation: {error}"))
}

fn state_at(repo: &ReadonlyRepo, store: &Path, branch: &str) -> Result<LocalBranchState, String> {
    let bookmark = match repo
        .view()
        .get_local_bookmark(RefName::new(branch))
        .as_resolved()
    {
        Some(Some(id)) => Some(id.hex()),
        Some(None) => None,
        None => return Err(format!("the bookmark `{branch}` is conflicted")),
    };
    let origin = repo.view().get_remote_bookmark(RemoteRefSymbol {
        name: branch.as_ref(),
        remote: "origin".as_ref(),
    });
    let recorded_origin = origin.target.as_normal().map(|id| id.hex());
    let tracked_origin = origin
        .is_tracked()
        .then(|| recorded_origin.clone())
        .flatten();
    Ok(LocalBranchState {
        bookmark,
        git_ref: git_ref_commit(store, branch)?,
        tracked_origin,
        origin: recorded_origin,
    })
}

fn backing_git_dir(store: &Path) -> Result<std::path::PathBuf, String> {
    store_git_dir(store)
        .ok_or_else(|| format!("resolve the backing repository of {}", store.display()))
}

fn git_ref_commit(store: &Path, branch: &str) -> Result<Option<String>, String> {
    let output = cairn_platform::env::git()
        .arg("--git-dir=.")
        .args([
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}^{{commit}}"),
        ])
        .current_dir(backing_git_dir(store)?)
        .output()
        .map_err(|error| format!("read refs/heads/{branch}: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if output.status.success() && !stdout.is_empty() {
        return Ok(Some(stdout));
    }
    // `--verify --quiet` reports a missing ref as exit 1 with nothing on
    // stderr; anything else is a failure to read, not an absence.
    if output.status.code() == Some(1) && output.stderr.is_empty() {
        return Ok(None);
    }
    Err(format!(
        "read refs/heads/{branch}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn delete_git_ref(store: &Path, branch: &str, expected: &str) -> Result<(), String> {
    let output = cairn_platform::env::git()
        .arg("--git-dir=.")
        .args([
            "update-ref",
            "-d",
            &format!("refs/heads/{branch}"),
            expected,
        ])
        .current_dir(backing_git_dir(store)?)
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

/// Run jj-lib's async API to completion from synchronous code. jj-lib's local
/// repository I/O needs no reactor, so a plain executor drives it; unlike a
/// Tokio runtime, it may be entered from a thread already inside one (an async
/// caller, or a section handed off with `block_in_place`).
fn block_on<T>(future: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
    futures::executor::block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case that panicked: called from inside a running Tokio runtime.
    #[test]
    fn the_executor_runs_inside_a_tokio_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let answer = runtime.block_on(async { block_on(async { Ok::<_, String>(7) }) });
        assert_eq!(answer, Ok(7));
    }
}
