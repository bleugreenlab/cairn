//! One process's view of `credentials.yaml`: the durable file plus the copy
//! this process routes from.

use std::path::{Path, PathBuf};
use std::sync::{Arc, LockResult, Mutex, MutexGuard};

use crate::store;
use crate::CredentialStore;

/// The credential store as one process holds it.
///
/// Clones share the same in-memory copy. Every write goes to the file under
/// `config_dir` first, and the copy is replaced with what was written, stamped
/// with the root it lives under so managed-profile paths resolve against it.
#[derive(Clone)]
pub struct CredentialStoreHandle {
    config_dir: PathBuf,
    cached: Arc<Mutex<Option<CredentialStore>>>,
}

impl CredentialStoreHandle {
    pub fn new(config_dir: PathBuf, initial: Option<CredentialStore>) -> Self {
        Self {
            config_dir,
            cached: Arc::new(Mutex::new(initial)),
        }
    }

    /// The configuration root `credentials.yaml` lives in. Also the root of
    /// the machine-wide credential leases.
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// This process's copy, for callers that must hold it across a persist
    /// (see [`Self::persist_locked`]).
    pub fn lock(&self) -> LockResult<MutexGuard<'_, Option<CredentialStore>>> {
        self.cached.lock()
    }

    /// This process's copy of the store.
    pub fn get(&self) -> Option<CredentialStore> {
        self.cached.lock().ok().and_then(|guard| guard.clone())
    }

    /// Write a store held under [`Self::lock`], then stamp where it now lives.
    pub fn persist_locked(&self, store: &mut CredentialStore) -> Result<(), String> {
        store::save_credential_store(&self.config_dir, store)?;
        store.config_dir = self.config_dir.clone();
        Ok(())
    }

    /// Save the full store to disk and update in-memory state.
    ///
    /// Persistence only, and deliberately so. Model discovery is a consequence
    /// of *credentials* changing, not of the store being written, so the few
    /// paths that change credentials ask for a catalog refresh themselves.
    pub fn save(&self, mut store: CredentialStore) -> Result<(), String> {
        store::save_credential_store(&self.config_dir, &store)?;
        // Stamp where it now lives, so the in-memory copy resolves profile
        // paths against the same root it was just written to.
        store.config_dir = self.config_dir.clone();
        if let Ok(mut guard) = self.cached.lock() {
            *guard = Some(store);
        }
        Ok(())
    }

    /// Change one part of the durable store under the store lease, against the
    /// file's current contents rather than this process's copy.
    ///
    /// [`Self::save`] writes a whole store, which is correct for a caller that
    /// just derived it from a fresh read and wrong for one holding a copy
    /// another process has since superseded. Credentials are the case where
    /// that difference destroys something: reverting a rotated OAuth token
    /// restores a value the provider has already retired. Use this whenever
    /// the change is "this field, as it stands now".
    ///
    /// The in-memory copy is replaced with the store as written, so this
    /// process also picks up whatever else landed while it was not looking.
    pub fn update<T>(
        &self,
        mutate: impl FnOnce(&mut CredentialStore) -> Result<T, String>,
    ) -> Result<T, String> {
        let (outcome, mut store) =
            store::update_credential_store(&self.config_dir, self.get(), mutate)?;
        store.config_dir = self.config_dir.clone();
        if let Ok(mut guard) = self.cached.lock() {
            *guard = Some(store);
        }
        Ok(outcome)
    }

    /// The store as it stands on disk, adopted as this process's copy.
    ///
    /// [`Self::get`] answers from memory, which is what nearly every caller
    /// wants and what a credential refresher must not trust: another Cairn
    /// process sharing this `CAIRN_HOME` may have published a rotated token
    /// since this copy was taken, and acting on the copy would spend a
    /// credential the provider has already retired. A home with no
    /// `credentials.yaml` has nothing newer to offer, so the copy stands.
    pub fn durable(&self) -> Option<CredentialStore> {
        match store::load_credential_store(&self.config_dir) {
            Ok(Some(mut store)) => {
                store.config_dir = self.config_dir.clone();
                if let Ok(mut guard) = self.cached.lock() {
                    *guard = Some(store.clone());
                }
                Some(store)
            }
            Ok(None) => self.get(),
            Err(e) => {
                log::warn!("Failed to re-read credentials.yaml; using this process's copy: {e}");
                self.get()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_store(user_id: &str) -> CredentialStore {
        CredentialStore {
            user_id: user_id.to_string(),
            accounts: Vec::new(),
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: PathBuf::new(),
        }
    }

    #[test]
    fn a_saved_store_is_stamped_with_the_root_it_was_written_to() {
        let home = tempfile::tempdir().unwrap();
        let handle = CredentialStoreHandle::new(home.path().to_path_buf(), None);
        handle.save(empty_store("first")).unwrap();
        let cached = handle.get().unwrap();
        assert_eq!(cached.user_id, "first");
        assert_eq!(cached.config_dir, home.path());
    }

    #[test]
    fn durable_adopts_what_another_process_published() {
        let home = tempfile::tempdir().unwrap();
        let ours = CredentialStoreHandle::new(home.path().to_path_buf(), None);
        ours.save(empty_store("ours")).unwrap();
        // A second process sharing the home publishes over our copy.
        let theirs = CredentialStoreHandle::new(home.path().to_path_buf(), None);
        theirs.save(empty_store("theirs")).unwrap();
        assert_eq!(ours.get().unwrap().user_id, "ours");
        assert_eq!(ours.durable().unwrap().user_id, "theirs");
        assert_eq!(ours.get().unwrap().user_id, "theirs");
    }

    #[test]
    fn update_mutates_the_file_rather_than_the_stale_copy() {
        let home = tempfile::tempdir().unwrap();
        let ours = CredentialStoreHandle::new(home.path().to_path_buf(), None);
        ours.save(empty_store("ours")).unwrap();
        CredentialStoreHandle::new(home.path().to_path_buf(), None)
            .save(empty_store("theirs"))
            .unwrap();
        let seen = ours
            .update(|store| {
                let seen = store.user_id.clone();
                store.user_id = format!("{seen}+updated");
                Ok(seen)
            })
            .unwrap();
        assert_eq!(seen, "theirs");
        let cached = ours.get().unwrap();
        assert_eq!(cached.user_id, "theirs+updated");
        assert_eq!(cached.config_dir, home.path());
    }
}
