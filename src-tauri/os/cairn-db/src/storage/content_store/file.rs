//! The private database's content store: content-addressed objects in a
//! directory on the local filesystem.
//!
//! This is where the private database's heavy archival bytes live once they
//! leave `cairn.db`: the framed per-execution range packs and the system-prompt
//! and system-init segment blobs, each a file named by the sha256 of its bytes.
//! It is the private counterpart of the team's brokered store, and archival and
//! reconstruction reach both through the one [`ContentStore`] trait.
//!
//! # Layout
//!
//! ```text
//! <root>/CAIRN-ARCHIVE          marker, written when the store creates <root>
//! <root>/objects/ab/abcdef...   one object per sha256, fanned out by its first byte
//! <root>/tmp/<random>           in-flight writes, renamed into objects/
//! ```
//!
//! # Offline
//!
//! A root without its marker, or one that cannot be read, is offline: every
//! `get` and `put` returns [`ContentStoreError::Offline`] naming the root, and
//! readers degrade to their labeled stubs. The store creates the root (and its
//! marker) only when it opens, and only when the root is absent and its parent
//! exists. A configured volume that is not mounted therefore reads as offline
//! instead of being silently replaced by an empty directory, and a root that
//! goes away while the process runs stays offline rather than being recreated
//! empty under the objects that point into it.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;

use super::{content_hash, ContentStore, ContentStoreError};

/// The marker file whose presence makes a root an archive.
pub const ARCHIVE_MARKER: &str = "CAIRN-ARCHIVE";

/// The marker's content: the layout format this store reads and writes.
const MARKER_CONTENT: &str = "cairn content archive\nformat 1\n";

const OBJECTS_DIR: &str = "objects";
const TMP_DIR: &str = "tmp";

/// A content-addressed store over a filesystem directory. Every filesystem
/// call runs on the blocking pool.
pub struct FileContentStore {
    root: PathBuf,
    /// Hashes whose object already failed its integrity check, so a corrupt
    /// object read in a loop logs once.
    warned: Mutex<HashSet<String>>,
}

impl FileContentStore {
    /// Open the store at `root`, creating it when it is absent and its parent
    /// exists, and clearing writes a crash left in `tmp/`. Never fails: a root
    /// that cannot be used opens offline, and [`Self::status`] says why.
    ///
    /// Create only where no archive has been before: a database that already
    /// points into an archive opens it with [`Self::open_existing`], so a
    /// missing archive reads as offline instead of as an empty one.
    pub fn open(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        if let Err(error) = create_if_absent(&root) {
            log::warn!("archive: could not create {}: {error}", root.display());
        }
        Self::open_existing(root)
    }

    /// Open the store at `root` without ever creating it: an absent root is
    /// offline.
    pub fn open_existing(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        if online(&root).is_ok() {
            clear_tmp(&root);
        }
        Self {
            root,
            warned: Mutex::new(HashSet::new()),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `Ok` when the root carries its marker and can be read.
    pub fn status(&self) -> Result<(), ContentStoreError> {
        online(&self.root)
    }

    fn warn_once(&self, hash: &str) {
        let first = self
            .warned
            .lock()
            .map(|mut warned| warned.insert(hash.to_string()))
            .unwrap_or(true);
        if first {
            log::warn!(
                "archive object {hash} in {} failed its integrity check; treating as absent",
                self.root.display()
            );
        }
    }
}

#[async_trait]
impl ContentStore for FileContentStore {
    fn local_root(&self) -> Option<&Path> {
        Some(&self.root)
    }

    async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), ContentStoreError> {
        let root = self.root.clone();
        let hash = hash.to_string();
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || put_blocking(&root, &hash, &bytes))
            .await
            .map_err(|error| ContentStoreError::Io(format!("archive put task failed: {error}")))?
    }

    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, ContentStoreError> {
        let root = self.root.clone();
        let key = hash.to_string();
        let read = tokio::task::spawn_blocking(move || get_blocking(&root, &key))
            .await
            .map_err(|error| {
                ContentStoreError::Io(format!("archive get task failed: {error}"))
            })??;
        match read {
            Read::Absent => Ok(None),
            Read::Corrupt => {
                self.warn_once(hash);
                Ok(None)
            }
            Read::Verified(bytes) => Ok(Some(bytes)),
        }
    }
}

enum Read {
    Absent,
    Corrupt,
    Verified(Vec<u8>),
}

fn io(what: &str, path: &Path, error: std::io::Error) -> ContentStoreError {
    ContentStoreError::Io(format!("{what} {}: {error}", path.display()))
}

fn create_if_absent(root: &Path) -> std::io::Result<()> {
    if root.symlink_metadata().is_ok() {
        return Ok(());
    }
    let Some(parent) = root.parent().filter(|parent| parent.is_dir()) else {
        return Ok(());
    };
    // Build the root beside its final name and rename it into place, so a crash
    // never leaves a root without its marker (which would read as offline
    // forever).
    let staging = parent.join(format!(
        ".{}.creating-{}",
        root.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "archive".into()),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir(&staging)?;
    std::fs::create_dir(staging.join(OBJECTS_DIR))?;
    std::fs::create_dir(staging.join(TMP_DIR))?;
    let mut marker = std::fs::File::create(staging.join(ARCHIVE_MARKER))?;
    marker.write_all(MARKER_CONTENT.as_bytes())?;
    marker.sync_all()?;
    drop(marker);
    match std::fs::rename(&staging, root) {
        Ok(()) => {
            sync_dir(parent);
            Ok(())
        }
        // Another opener won the race; its root is as good as ours.
        Err(_) if online(root).is_ok() => {
            let _ = std::fs::remove_dir_all(&staging);
            Ok(())
        }
        Err(error) => {
            let _ = std::fs::remove_dir_all(&staging);
            Err(error)
        }
    }
}

fn online(root: &Path) -> Result<(), ContentStoreError> {
    match std::fs::metadata(root.join(ARCHIVE_MARKER)) {
        Ok(meta) if meta.is_file() => Ok(()),
        _ => Err(ContentStoreError::Offline {
            path: root.to_path_buf(),
        }),
    }
}

fn clear_tmp(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root.join(TMP_DIR)) else {
        return;
    };
    for entry in entries.flatten() {
        let _ = std::fs::remove_file(entry.path());
    }
}

/// The object path for `hash`. A key is a lowercase hex digest, which is also
/// what keeps it from naming anything outside `objects/`.
fn object_path(root: &Path, hash: &str) -> Result<PathBuf, ContentStoreError> {
    let well_formed = hash.len() >= 4
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !well_formed {
        return Err(ContentStoreError::Io(format!(
            "archive key {hash:?} is not a lowercase hex digest"
        )));
    }
    Ok(root.join(OBJECTS_DIR).join(&hash[..2]).join(hash))
}

fn get_blocking(root: &Path, hash: &str) -> Result<Read, ContentStoreError> {
    online(root)?;
    let path = object_path(root, hash)?;
    match std::fs::read(&path) {
        Ok(bytes) if content_hash(&bytes) == hash => Ok(Read::Verified(bytes)),
        Ok(_) => Ok(Read::Corrupt),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Read::Absent),
        Err(error) => Err(io("reading archive object", &path, error)),
    }
}

fn verified_at(path: &Path, hash: &str) -> bool {
    std::fs::read(path).is_ok_and(|bytes| content_hash(&bytes) == hash)
}

fn put_blocking(root: &Path, hash: &str, bytes: &[u8]) -> Result<(), ContentStoreError> {
    online(root)?;
    let path = object_path(root, hash)?;
    // An object that cannot verify could never be read back: refuse it here
    // rather than store bytes under a key they do not hash to.
    if content_hash(bytes) != hash {
        return Err(ContentStoreError::Io(format!(
            "archive put for {hash}: the bytes do not hash to their key"
        )));
    }
    if verified_at(&path, hash) {
        return Ok(());
    }
    let fanout = path
        .parent()
        .expect("an object path has a fan-out directory");
    std::fs::create_dir_all(fanout).map_err(|e| io("creating", fanout, e))?;
    let tmp_dir = root.join(TMP_DIR);
    std::fs::create_dir_all(&tmp_dir).map_err(|e| io("creating", &tmp_dir, e))?;
    let tmp = tmp_dir.join(uuid::Uuid::new_v4().simple().to_string());
    let written = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    if let Err(error) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(io("writing", &tmp, error));
    }
    match std::fs::rename(&tmp, &path) {
        Ok(()) => {}
        // A concurrent put of the same object landed first (a rename onto an
        // existing file fails on Windows). Its bytes are ours.
        Err(_) if verified_at(&path, hash) => {
            let _ = std::fs::remove_file(&tmp);
        }
        Err(error) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(io("renaming into", &path, error));
        }
    }
    sync_dir(fanout);
    Ok(())
}

/// Make a rename durable. Directories cannot be opened for sync on Windows,
/// where the rename is already durable once it returns.
fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(handle) = std::fs::File::open(dir) {
        let _ = handle.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn store_in(dir: &tempfile::TempDir) -> FileContentStore {
        FileContentStore::open(dir.path().join("archive"))
    }

    #[tokio::test]
    async fn round_trips_and_put_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        assert!(store.status().is_ok());
        assert!(dir.path().join("archive").join(ARCHIVE_MARKER).is_file());

        let hash = content_hash(b"hello");
        assert_eq!(store.get(&hash).await.unwrap(), None);
        store.put(&hash, b"hello").await.unwrap();
        store.put(&hash, b"hello").await.unwrap();
        assert_eq!(store.get(&hash).await.unwrap().unwrap(), b"hello");
        let object = dir
            .path()
            .join("archive/objects")
            .join(&hash[..2])
            .join(&hash);
        assert!(object.is_file());
    }

    #[tokio::test]
    async fn a_put_whose_bytes_do_not_hash_to_the_key_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let error = store.put(&content_hash(b"one"), b"two").await.unwrap_err();
        assert!(matches!(error, ContentStoreError::Io(_)), "{error}");
        let traversal = store.get("../../etc/passwd").await.unwrap_err();
        assert!(matches!(traversal, ContentStoreError::Io(_)));
    }

    #[tokio::test]
    async fn a_corrupted_object_reads_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let hash = content_hash(b"payload");
        store.put(&hash, b"payload").await.unwrap();
        let object = object_path(store.root(), &hash).unwrap();
        std::fs::write(&object, b"tampered").unwrap();
        assert_eq!(store.get(&hash).await.unwrap(), None);
        assert_eq!(store.get(&hash).await.unwrap(), None);
        // A re-put repairs it.
        store.put(&hash, b"payload").await.unwrap();
        assert_eq!(store.get(&hash).await.unwrap().unwrap(), b"payload");
    }

    #[tokio::test]
    async fn a_root_without_its_marker_is_offline_for_get_and_put() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("archive");
        std::fs::create_dir(&root).unwrap();
        let store = FileContentStore::open(&root);
        let hash = content_hash(b"x");
        let offline = ContentStoreError::Offline { path: root.clone() };
        assert_eq!(store.status().unwrap_err(), offline);
        assert_eq!(store.get(&hash).await.unwrap_err(), offline);
        assert_eq!(store.put(&hash, b"x").await.unwrap_err(), offline);
        assert_eq!(
            offline.to_string(),
            format!("archive offline: {}", root.display())
        );
        assert!(
            !root.join(ARCHIVE_MARKER).exists(),
            "an existing root is never adopted"
        );
    }

    #[tokio::test]
    async fn a_root_whose_parent_is_gone_is_offline_and_never_created() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("unmounted-volume").join("archive");
        let store = FileContentStore::open(&root);
        let hash = content_hash(b"x");
        assert!(matches!(
            store.get(&hash).await,
            Err(ContentStoreError::Offline { .. })
        ));
        assert!(matches!(
            store.put(&hash, b"x").await,
            Err(ContentStoreError::Offline { .. })
        ));
        assert!(!root.exists());
        assert!(!dir.path().join("unmounted-volume").exists());
    }

    #[tokio::test]
    async fn open_existing_never_creates_a_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("archive");
        let store = FileContentStore::open_existing(&root);
        assert!(matches!(
            store.status(),
            Err(ContentStoreError::Offline { .. })
        ));
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn a_root_removed_while_open_stays_offline() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let hash = content_hash(b"kept");
        store.put(&hash, b"kept").await.unwrap();
        std::fs::rename(store.root(), dir.path().join("moved")).unwrap();
        assert!(matches!(
            store.get(&hash).await,
            Err(ContentStoreError::Offline { .. })
        ));
        assert!(!store.root().exists(), "not recreated empty");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_of_one_hash_all_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(store_in(&dir));
        let bytes = vec![7u8; 256 * 1024];
        let hash = content_hash(&bytes);
        let puts = (0..16).map(|_| {
            let store = store.clone();
            let bytes = bytes.clone();
            let hash = hash.clone();
            tokio::spawn(async move { store.put(&hash, &bytes).await })
        });
        for put in puts.collect::<Vec<_>>() {
            put.await.unwrap().unwrap();
        }
        assert_eq!(store.get(&hash).await.unwrap().unwrap(), bytes);
        let leftovers = std::fs::read_dir(store.root().join(TMP_DIR))
            .unwrap()
            .count();
        assert_eq!(leftovers, 0);
    }

    #[tokio::test]
    async fn a_stray_tmp_file_is_ignored_then_cleaned_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let stray = store.root().join(TMP_DIR).join("crashed-write");
        std::fs::write(&stray, b"half").unwrap();
        let hash = content_hash(b"half");
        assert_eq!(store.get(&hash).await.unwrap(), None);
        drop(store);
        let reopened = store_in(&dir);
        assert!(!stray.exists());
        assert!(reopened.status().is_ok());
    }
}
