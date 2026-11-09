//! A project's default branch is a declared-base relationship.
//!
//! A job that records no base of its own advances against the project default,
//! so changing the default changes that job's effective base. Every writer of
//! the default therefore holds the project's jj store guard, the same guard that
//! base preparation and publication hold while they read the relationship and
//! install against it. A default change can then land only before an installer
//! reads the relationship or after it publishes, never between the two.
//!
//! The default lives in two places: the checkout's `.cairn/config.yaml`
//! `defaultBranch`, which wins, and `projects.default_branch`. Writers keep the
//! column equal to the effective default
//! ([`crate::config::project_settings::effective_default_branch`]), so readers of
//! the bare column agree with readers that consult the config.
//!
//! Lock order: store guard, then the settings-file lock taken inside
//! [`mutate_project_settings`], then a short SQL-only transaction. A busy store
//! refuses the whole change before the file or the row is touched.

use std::path::Path;
use std::time::Duration;

use crate::config::project_settings::{mutate_project_settings, ProjectSettingsFile};
use crate::orchestrator::{JjStoreGuard, Orchestrator, StoreLockClass};
use crate::storage::LocalDb;

/// How the config file's `defaultBranch` follows a default change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigDefault {
    /// Declare the new default in the config file.
    Declare,
    /// Rewrite `defaultBranch` only where the file already declares one. The
    /// column alone would otherwise change while the declaration still won.
    FollowIfDeclared,
}

/// A requested change of a project's default branch.
#[derive(Clone, Copy, Debug)]
pub struct DefaultBranchChange<'a> {
    pub branch: &'a str,
    pub config: ConfigDefault,
}

/// The store guard a default change runs under: lent by a caller that already
/// holds this project's guard, or acquired for the change.
enum StoreWitness<'a> {
    Lent(&'a JjStoreGuard),
    Owned(JjStoreGuard),
}

impl StoreWitness<'_> {
    fn guard(&self) -> &JjStoreGuard {
        match self {
            StoreWitness::Lent(guard) => guard,
            StoreWitness::Owned(guard) => guard,
        }
    }
}

/// Reuse `held` when it guards this project's store: the lock is not
/// reentrant, so a write batch that already holds it must lend it. A guard on
/// another store does not cover this one, and the bounded acquisition keeps a
/// cross-store wait a refusal rather than a deadlock.
async fn store_witness<'a>(
    orch: &Orchestrator,
    repo_path: &Path,
    held: Option<&'a JjStoreGuard>,
    timeout: Duration,
) -> Result<StoreWitness<'a>, String> {
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    if let Some(guard) = held.filter(|guard| guard.covers(&store)) {
        return Ok(StoreWitness::Lent(guard));
    }
    let guard = crate::mcp::vcs::acquire_store_lock(
        orch,
        Some(&store),
        StoreLockClass::Foreground,
        "project default-branch change",
        timeout,
    )
    .await?;
    Ok(StoreWitness::Owned(
        guard.expect("a named store always yields a guard"),
    ))
}

/// Mutate a project's settings file and, when `default_branch` is set, change
/// its default under the store guard: the file first, then the row.
///
/// `held` is the guard the caller already holds, if any; a write batch passes
/// the guard it holds for its own store. Settings changes that leave the
/// default alone take no guard.
pub async fn write_project_settings<T>(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &Path,
    default_branch: Option<DefaultBranchChange<'_>>,
    held: Option<&JjStoreGuard>,
    mutate: impl FnOnce(&mut ProjectSettingsFile) -> Result<T, String>,
) -> Result<T, String> {
    write_project_settings_within(
        orch,
        db,
        project_id,
        repo_path,
        default_branch,
        held,
        crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        mutate,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn write_project_settings_within<T>(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &Path,
    default_branch: Option<DefaultBranchChange<'_>>,
    held: Option<&JjStoreGuard>,
    timeout: Duration,
    mutate: impl FnOnce(&mut ProjectSettingsFile) -> Result<T, String>,
) -> Result<T, String> {
    let witness = match default_branch {
        Some(_) => Some(store_witness(orch, repo_path, held, timeout).await?),
        None => None,
    };
    let output = mutate_project_settings(repo_path, |config| {
        let output = mutate(config)?;
        if let Some(change) = default_branch {
            if change.config == ConfigDefault::Declare || config.default_branch.is_some() {
                config.default_branch = Some(change.branch.to_string());
            }
        }
        Ok(output)
    })
    .map_err(|error| error.to_string())?;
    if let (Some(change), Some(witness)) = (default_branch, witness.as_ref()) {
        crate::projects::crud::set_default_branch_db(
            db,
            witness.guard(),
            project_id,
            change.branch,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(output)
}

#[cfg(test)]
#[path = "default_branch_tests.rs"]
mod tests;
