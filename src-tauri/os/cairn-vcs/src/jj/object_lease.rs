//! In-process exclusion between detached objects and destructive maintenance.
//!
//! The runner is the sole store owner. Manual external jj/Git garbage collection
//! is not part of its publication protocol. Automatic keep refs protect writes
//! only by mtime; retaining the full store prevents reclamation between candidate
//! commit writes, including operation/index metadata, without renewing timestamps.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[derive(Default)]
struct State {
    readers: usize,
    reclaiming: bool,
}

type Shared = Arc<Mutex<State>>;

fn state(git_dir: &Path) -> Result<Shared, String> {
    static STORES: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<State>>>>> = OnceLock::new();
    let key = git_dir
        .canonicalize()
        .map_err(|error| format!("canonicalize object lease store: {error}"))?;
    let mut stores = STORES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| "object lease registry poisoned".to_string())?;
    stores.retain(|_, state| state.strong_count() != 0);
    let entry = stores.entry(key).or_default();
    if let Some(shared) = entry.upgrade() {
        return Ok(shared);
    }
    let shared = Arc::new(Mutex::new(State::default()));
    *entry = Arc::downgrade(&shared);
    Ok(shared)
}

#[derive(Debug)]
pub(super) enum LeaseError {
    Busy,
    Repository(String),
}

/// Protect unpublished objects and their jj metadata from Cairn maintenance.
///
/// Acquire before constructing objects and retain through their last use,
/// including any temporary operation rollback and publication/recovery window.
/// This is in-process reclamation exclusion, not a writer lock or a durable pin:
/// callers must separately retain a named Git root for restart recovery, and
/// acquire a lease before recovery races with maintenance after restart.
pub struct StoreObjectLease {
    _lease: ObjectLease,
}

impl StoreObjectLease {
    pub fn for_store(store: &Path) -> Result<Self, String> {
        let git_dir = super::store_git_dir(store)
            .ok_or_else(|| format!("resolve object lease store {}", store.display()))?;
        let lease = ObjectLease::acquire(&git_dir).map_err(|error| match error {
            LeaseError::Busy => "store maintenance is active".to_string(),
            LeaseError::Repository(error) => error,
        })?;
        Ok(Self { _lease: lease })
    }
}

impl std::fmt::Debug for StoreObjectLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreObjectLease")
            .finish_non_exhaustive()
    }
}

pub(super) struct ObjectLease(Shared);

impl ObjectLease {
    pub(super) fn acquire(git_dir: &Path) -> Result<Self, LeaseError> {
        let shared = state(git_dir).map_err(LeaseError::Repository)?;
        {
            let mut state = shared
                .lock()
                .map_err(|_| LeaseError::Repository("object lease poisoned".to_string()))?;
            if state.reclaiming {
                return Err(LeaseError::Busy);
            }
            state.readers += 1;
        }
        Ok(Self(shared))
    }
}

impl Drop for ObjectLease {
    fn drop(&mut self) {
        // Poisoning must not silently grant reclamation while objects are live.
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.readers -= 1;
    }
}

pub(super) struct Reclamation(Shared);

impl Reclamation {
    pub(super) fn acquire(git_dir: &Path) -> Result<Self, String> {
        let shared = state(git_dir)?;
        {
            let mut state = shared
                .lock()
                .map_err(|_| "object lease poisoned".to_string())?;
            if state.readers != 0 || state.reclaiming {
                return Err(
                    "store reclamation deferred: detached advance or maintenance is active"
                        .to_string(),
                );
            }
            state.reclaiming = true;
        }
        Ok(Self(shared))
    }

    pub(super) fn for_store(store: &Path) -> Result<Self, String> {
        let git_dir = super::store_git_dir(store)
            .ok_or_else(|| format!("resolve object lease store {}", store.display()))?;
        Self::acquire(&git_dir)
    }
}

impl Drop for Reclamation {
    fn drop(&mut self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.reclaiming = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_store_lease_shares_aliases_and_detached_maintenance_exclusion() {
        let root = tempfile::TempDir::new().unwrap();
        let checkout = root.path().join("checkout");
        let git_dir = checkout.join(".git");
        let store = root.path().join("store");
        std::fs::create_dir_all(&git_dir).unwrap();
        let backend = store.join(".jj/repo/store");
        std::fs::create_dir_all(&backend).unwrap();
        std::fs::write(backend.join("git_target"), git_dir.to_str().unwrap()).unwrap();
        std::fs::create_dir(store.join("child")).unwrap();
        let alias = store.join("child/..");
        let public = crate::jj::StoreObjectLease::for_store(&alias).unwrap();
        let detached = ObjectLease::acquire(&git_dir).unwrap();
        assert!(Reclamation::for_store(&store).is_err());
        drop(public);
        assert!(
            Reclamation::for_store(&alias).is_err(),
            "public and detached leases share one reader count"
        );
        drop(detached);
        let maintenance = Reclamation::for_store(&alias).unwrap();
        assert!(crate::jj::StoreObjectLease::for_store(&store)
            .unwrap_err()
            .contains("maintenance"));
        drop(maintenance);
        let _public = crate::jj::StoreObjectLease::for_store(&store).unwrap();
    }

    #[test]
    fn canonical_aliases_share_the_reclamation_gate() {
        let directory = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(directory.path().join("child")).unwrap();
        let alias = directory.path().join("child").join("..");
        let lease = ObjectLease::acquire(&alias).unwrap();
        assert!(Reclamation::acquire(directory.path()).is_err());
        drop(lease);
        let reclamation = Reclamation::acquire(directory.path()).unwrap();
        assert!(ObjectLease::acquire(&alias).is_err());
        drop(reclamation);
        let _lease = ObjectLease::acquire(directory.path()).unwrap();
    }

    #[test]
    fn lease_excludes_reclamation_without_blocking_other_stores() {
        let first = tempfile::TempDir::new().unwrap();
        let second = tempfile::TempDir::new().unwrap();
        let lease = ObjectLease::acquire(first.path()).unwrap();
        let other_lease = ObjectLease::acquire(first.path()).unwrap();
        assert!(Reclamation::acquire(first.path()).is_err());
        let other_store = Reclamation::acquire(second.path()).unwrap();
        assert!(ObjectLease::acquire(second.path()).is_err());
        drop(other_store);
        let _other = ObjectLease::acquire(second.path()).unwrap();
        drop(lease);
        assert!(Reclamation::acquire(first.path()).is_err());
        drop(other_lease);
        let reclamation = Reclamation::acquire(first.path()).unwrap();
        assert!(ObjectLease::acquire(first.path()).is_err());
        drop(reclamation);
        let _lease = ObjectLease::acquire(first.path()).unwrap();
    }
}
