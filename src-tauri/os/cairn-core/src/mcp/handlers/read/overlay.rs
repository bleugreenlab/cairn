use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cairn_codec::objects::{
    NameStatusKind, ObjectReadError, ObjectReadLimits, ObjectStore, TreeEntryKind,
};

use super::object_read::{visible_entries, ContentEntry};

const CORRECTION_CACHE_CAPACITY: usize = 64;
const BLOB_CACHE_CAPACITY: usize = 256 * 1024 * 1024;
const QUERY_BLOB_BYTE_CAPACITY: usize = 512 * 1024 * 1024;
const SYMLINK_MODE: u16 = 0o120000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileRecord {
    blob_sha: String,
    mode: u16,
}

#[derive(Debug)]
struct BaseSnapshot {
    commit_id: String,
    files: BTreeMap<String, FileRecord>,
}

#[derive(Debug, Default)]
struct CoordinateCorrections {
    whiteouts: BTreeSet<String>,
    replacements: BTreeMap<String, FileRecord>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BlobCacheDiagnostics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub oversize_rejections: u64,
    pub resident_bytes: usize,
}

struct BlobCacheInner {
    entries: HashMap<String, Arc<[u8]>>,
    order: VecDeque<String>,
    resident_bytes: usize,
}

struct BlobCache {
    capacity: usize,
    inner: Mutex<BlobCacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    oversize_rejections: AtomicU64,
}

impl BlobCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::new(BlobCacheInner {
                entries: HashMap::new(),
                order: VecDeque::new(),
                resident_bytes: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            oversize_rejections: AtomicU64::new(0),
        }
    }

    fn get_or_read(
        &self,
        sha: &str,
        store: &ObjectStore,
        limits: &ObjectReadLimits,
    ) -> Result<Arc<[u8]>, ObjectReadError> {
        if limits.cancellation.load(Ordering::Relaxed) {
            return Err(ObjectReadError::Cancelled);
        }
        if Instant::now() >= limits.deadline {
            return Err(ObjectReadError::DeadlineExceeded);
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some(bytes) = inner.entries.get(sha).cloned() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            if let Some(position) = inner.order.iter().position(|entry| entry == sha) {
                inner.order.remove(position);
            }
            inner.order.push_back(sha.to_string());
            drop(inner);
            if bytes.len() > limits.max_blob_bytes {
                return Err(ObjectReadError::BlobLimitExceeded {
                    size: bytes.len(),
                    limit: limits.max_blob_bytes,
                });
            }
            return Ok(bytes);
        }
        drop(inner);

        self.misses.fetch_add(1, Ordering::Relaxed);
        let bytes: Arc<[u8]> = store.blob(sha, limits)?.into();
        if bytes.len() > self.capacity {
            self.oversize_rejections.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }

        let mut inner = self.inner.lock().unwrap();
        if let Some(existing) = inner.entries.get(sha).cloned() {
            return Ok(existing);
        }
        while inner.resident_bytes + bytes.len() > self.capacity {
            let Some(oldest) = inner.order.pop_front() else {
                break;
            };
            if let Some(evicted) = inner.entries.remove(&oldest) {
                inner.resident_bytes -= evicted.len();
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        inner.resident_bytes += bytes.len();
        inner.order.push_back(sha.to_string());
        inner.entries.insert(sha.to_string(), bytes.clone());
        Ok(bytes)
    }

    fn diagnostics(&self) -> BlobCacheDiagnostics {
        BlobCacheDiagnostics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            oversize_rejections: self.oversize_rejections.load(Ordering::Relaxed),
            resident_bytes: self.inner.lock().unwrap().resident_bytes,
        }
    }
}

struct CorrectionCache {
    entries: HashMap<(String, String), Arc<CoordinateCorrections>>,
    order: VecDeque<(String, String)>,
}

impl CorrectionCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, key: &(String, String)) -> Option<Arc<CoordinateCorrections>> {
        self.entries.get(key).cloned()
    }

    fn insert(
        &mut self,
        key: (String, String),
        value: Arc<CoordinateCorrections>,
    ) -> Arc<CoordinateCorrections> {
        if let Some(existing) = self.entries.get(&key) {
            return existing.clone();
        }
        while self.entries.len() >= CORRECTION_CACHE_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, value.clone());
        value
    }
}

struct ProjectOverlay {
    repository_path: PathBuf,
    published: Mutex<Option<Arc<BaseSnapshot>>>,
    desired_base: Mutex<String>,
    refreshing: AtomicBool,
    corrections: Mutex<CorrectionCache>,
    blobs: BlobCache,
}

impl ProjectOverlay {
    fn new(repository_path: PathBuf) -> Self {
        Self {
            repository_path,
            published: Mutex::new(None),
            desired_base: Mutex::new(String::new()),
            refreshing: AtomicBool::new(false),
            corrections: Mutex::new(CorrectionCache::new()),
            blobs: BlobCache::new(BLOB_CACHE_CAPACITY),
        }
    }

    fn snapshot(
        self: &Arc<Self>,
        desired_commit: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Arc<BaseSnapshot>, ObjectReadError> {
        *self.desired_base.lock().unwrap() = desired_commit.to_string();
        if let Some(snapshot) = self.published.lock().unwrap().clone() {
            if snapshot.commit_id != desired_commit {
                self.start_refresh(desired_commit.to_string(), limits.clone());
            }
            return Ok(snapshot);
        }

        let snapshot = Arc::new(build_snapshot(
            &self.repository_path,
            desired_commit,
            limits,
        )?);
        let mut published = self.published.lock().unwrap();
        if let Some(existing) = published.as_ref() {
            return Ok(existing.clone());
        }
        *published = Some(snapshot.clone());
        Ok(snapshot)
    }

    fn start_refresh(self: &Arc<Self>, desired_commit: String, mut limits: ObjectReadLimits) {
        if self.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        limits.deadline = Instant::now() + std::time::Duration::from_secs(30);
        limits.cancellation = Arc::new(AtomicBool::new(false));
        let state = self.clone();
        std::thread::spawn(move || {
            if let Ok(snapshot) = build_snapshot(&state.repository_path, &desired_commit, &limits) {
                let still_desired =
                    state.desired_base.lock().unwrap().as_str() == desired_commit.as_str();
                if still_desired {
                    *state.published.lock().unwrap() = Some(Arc::new(snapshot));
                }
            }
            state.refreshing.store(false, Ordering::Release);
        });
    }

    fn corrections(
        &self,
        base: &str,
        head: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Arc<CoordinateCorrections>, ObjectReadError> {
        let key = (base.to_string(), head.to_string());
        if let Some(cached) = self.corrections.lock().unwrap().get(&key) {
            return Ok(cached);
        }
        let store = ObjectStore::new(&self.repository_path, None)
            .map_err(|error| ObjectReadError::InvalidPath(error.to_string()))?;
        let mut corrections = CoordinateCorrections::default();
        for status in store.name_status(base, head, limits)? {
            match status.kind {
                NameStatusKind::Deleted => {
                    corrections.whiteouts.insert(status.path);
                }
                NameStatusKind::Added | NameStatusKind::Modified => {
                    corrections.whiteouts.insert(status.path.clone());
                    if let (Some(blob_sha), Some(mode)) = (status.new_oid, status.new_mode) {
                        if mode != SYMLINK_MODE {
                            corrections
                                .replacements
                                .insert(status.path, FileRecord { blob_sha, mode });
                        }
                    }
                }
            }
        }
        Ok(self
            .corrections
            .lock()
            .unwrap()
            .insert(key, Arc::new(corrections)))
    }

    fn entries(
        self: &Arc<Self>,
        base_commit: &str,
        head_commit: &str,
        prefix: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<ContentEntry>, ObjectReadError> {
        let snapshot = self.snapshot(base_commit, limits)?;
        let corrections = self.corrections(&snapshot.commit_id, head_commit, limits)?;
        let mut budget = AggregateBudget::new(limits);
        for _ in &snapshot.files {
            budget.entry()?;
        }
        for _ in &corrections.whiteouts {
            budget.entry()?;
        }
        for _ in &corrections.replacements {
            budget.entry()?;
        }
        let mut merged = snapshot.files.clone();
        for path in &corrections.whiteouts {
            merged.remove(path);
        }
        for (path, replacement) in &corrections.replacements {
            merged.insert(path.clone(), replacement.clone());
        }

        let entries: Vec<_> = merged
            .into_iter()
            .map(|(path, record)| ContentEntry {
                path,
                oid: record.blob_sha,
                mode: record.mode,
            })
            .collect();
        let store = ObjectStore::new(&self.repository_path, None)
            .map_err(|error| ObjectReadError::InvalidPath(error.to_string()))?;
        visible_entries(entries, prefix, |sha| {
            self.blobs.get_or_read(sha, &store, limits)
        })
    }

    fn load_entries(
        &self,
        entries: &[ContentEntry],
        limits: &ObjectReadLimits,
    ) -> Result<Vec<(String, Vec<u8>)>, ObjectReadError> {
        let store = ObjectStore::new(&self.repository_path, None)
            .map_err(|error| ObjectReadError::InvalidPath(error.to_string()))?;
        let mut budget = QueryByteBudget::new(limits);
        entries
            .iter()
            .map(|entry| {
                let bytes = self.blobs.get_or_read(&entry.oid, &store, limits)?;
                budget.bytes(bytes.len())?;
                Ok((entry.path.clone(), bytes.to_vec()))
            })
            .collect()
    }
}

fn build_snapshot(
    repository_path: &Path,
    commit_id: &str,
    limits: &ObjectReadLimits,
) -> Result<BaseSnapshot, ObjectReadError> {
    let store = ObjectStore::new(repository_path, None)
        .map_err(|error| ObjectReadError::InvalidPath(error.to_string()))?;
    let files = store
        .walk_commit(commit_id, "", limits)?
        .into_iter()
        .filter(|item| item.kind == TreeEntryKind::Blob && item.mode != SYMLINK_MODE)
        .map(|item| {
            (
                item.path,
                FileRecord {
                    blob_sha: item.oid,
                    mode: item.mode,
                },
            )
        })
        .collect();
    Ok(BaseSnapshot {
        commit_id: commit_id.to_string(),
        files,
    })
}

struct AggregateBudget<'a> {
    limits: &'a ObjectReadLimits,
    entries: usize,
}

impl<'a> AggregateBudget<'a> {
    fn new(limits: &'a ObjectReadLimits) -> Self {
        Self { limits, entries: 0 }
    }

    fn check(&self) -> Result<(), ObjectReadError> {
        if self.limits.cancellation.load(Ordering::Relaxed) {
            return Err(ObjectReadError::Cancelled);
        }
        if Instant::now() >= self.limits.deadline {
            return Err(ObjectReadError::DeadlineExceeded);
        }
        Ok(())
    }

    fn entry(&mut self) -> Result<(), ObjectReadError> {
        self.check()?;
        self.entries += 1;
        if self.entries > self.limits.max_entries {
            return Err(ObjectReadError::EntryLimitExceeded {
                limit: self.limits.max_entries,
            });
        }
        Ok(())
    }
}

struct QueryByteBudget<'a> {
    limits: &'a ObjectReadLimits,
    bytes: usize,
}

impl<'a> QueryByteBudget<'a> {
    fn new(limits: &'a ObjectReadLimits) -> Self {
        Self { limits, bytes: 0 }
    }

    fn bytes(&mut self, amount: usize) -> Result<(), ObjectReadError> {
        if self.limits.cancellation.load(Ordering::Relaxed) {
            return Err(ObjectReadError::Cancelled);
        }
        if Instant::now() >= self.limits.deadline {
            return Err(ObjectReadError::DeadlineExceeded);
        }
        self.bytes = self.bytes.saturating_add(amount);
        if self.bytes > QUERY_BLOB_BYTE_CAPACITY {
            return Err(ObjectReadError::AggregateBlobLimitExceeded {
                size: self.bytes,
                limit: QUERY_BLOB_BYTE_CAPACITY,
            });
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ProjectOverlayRegistry {
    projects: Mutex<HashMap<(String, PathBuf), Arc<ProjectOverlay>>>,
}

impl ProjectOverlayRegistry {
    fn state(&self, project_id: &str, repository_path: &Path) -> Arc<ProjectOverlay> {
        let key = (project_id.to_string(), repository_path.to_path_buf());
        let mut projects = self.projects.lock().unwrap();
        projects
            .entry(key)
            .or_insert_with(|| Arc::new(ProjectOverlay::new(repository_path.to_path_buf())))
            .clone()
    }

    pub(super) fn entries(
        &self,
        project_id: &str,
        repository_path: &Path,
        base_commit: &str,
        head_commit: &str,
        prefix: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<ContentEntry>, ObjectReadError> {
        self.state(project_id, repository_path)
            .entries(base_commit, head_commit, prefix, limits)
    }

    pub(super) fn load_entries(
        &self,
        project_id: &str,
        repository_path: &Path,
        entries: &[ContentEntry],
        limits: &ObjectReadLimits,
    ) -> Result<Vec<(String, Vec<u8>)>, ObjectReadError> {
        self.state(project_id, repository_path)
            .load_entries(entries, limits)
    }

    pub(crate) fn files(
        &self,
        project_id: &str,
        repository_path: &Path,
        base_commit: &str,
        head_commit: &str,
        prefix: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<(String, Vec<u8>)>, ObjectReadError> {
        let state = self.state(project_id, repository_path);
        let result = state
            .entries(base_commit, head_commit, prefix, limits)
            .and_then(|entries| state.load_entries(&entries, limits));
        let diagnostics = state.blobs.diagnostics();
        tracing::debug!(
            project_id,
            hits = diagnostics.hits,
            misses = diagnostics.misses,
            evictions = diagnostics.evictions,
            oversize_rejections = diagnostics.oversize_rejections,
            resident_bytes = diagnostics.resident_bytes,
            "project overlay blob cache"
        );
        result
    }

    #[cfg(test)]
    pub(crate) fn diagnostics(
        &self,
        project_id: &str,
        repository_path: &Path,
    ) -> Option<BlobCacheDiagnostics> {
        self.projects
            .lock()
            .unwrap()
            .get(&(project_id.to_string(), repository_path.to_path_buf()))
            .map(|state| state.blobs.diagnostics())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_codec::testutil::{commit_all, git, init_repo, write_file};
    use std::time::Duration;

    fn paths(files: &[(String, Vec<u8>)]) -> Vec<&str> {
        files.iter().map(|(path, _)| path.as_str()).collect()
    }

    #[test]
    fn visible_metadata_loads_only_ignore_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, ".gitignore", b"ignored.txt\n");
        write_file(repo, "visible.txt", b"ordinary");
        write_file(repo, "ignored.txt", b"ignored");
        git(repo, &["add", "-f", "."]);
        let commit = commit_all(repo, "metadata");
        let registry = ProjectOverlayRegistry::default();

        let entries = registry
            .entries(
                "project",
                repo,
                &commit,
                &commit,
                "",
                &ObjectReadLimits::default(),
            )
            .unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            [".gitignore", "visible.txt"]
        );
        let diagnostics = registry.diagnostics("project", repo).unwrap();
        assert_eq!(diagnostics.misses, 1);
        assert_eq!(diagnostics.resident_bytes, b"ignored.txt\n".len());
    }

    #[test]
    fn merged_view_applies_whiteouts_ignore_edits_case_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, ".gitignore", b"base-hidden.txt\n");
        write_file(repo, "base-hidden.txt", b"becomes visible");
        write_file(repo, "head-hidden.txt", b"becomes hidden");
        write_file(repo, "old-name.txt", b"rename needle");
        write_file(repo, "regular-to-link.txt", b"regular");
        write_file(repo, "binary.bin", b"needle\0binary");
        git(repo, &["add", "-f", "."]);
        let base = commit_all(repo, "base");

        write_file(repo, ".gitignore", b"head-hidden.txt\n");
        std::fs::rename(repo.join("old-name.txt"), repo.join("new-name.txt")).unwrap();
        write_file(repo, "Foo.rs", b"fn upper() {}\n");
        std::fs::remove_file(repo.join("regular-to-link.txt")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("new-name.txt", repo.join("regular-to-link.txt")).unwrap();
        git(repo, &["add", "-f", "."]);
        // Build the second case-distinct tree entry through the index so this
        // fixture remains valid on case-insensitive macOS filesystems.
        write_file(repo, "lower-content", b"fn lower() {}\n");
        let lower_blob = git(repo, &["hash-object", "-w", "lower-content"])
            .trim()
            .to_string();
        std::fs::remove_file(repo.join("lower-content")).unwrap();
        git(
            repo,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("100644,{lower_blob},foo.rs"),
            ],
        );
        git(repo, &["commit", "-q", "-m", "head"]);
        let head = git(repo, &["rev-parse", "HEAD"]).trim().to_string();

        let registry = ProjectOverlayRegistry::default();
        let files = registry
            .files(
                "project",
                repo,
                &base,
                &head,
                "",
                &ObjectReadLimits::default(),
            )
            .unwrap();
        assert_eq!(
            paths(&files),
            [
                ".gitignore",
                "Foo.rs",
                "base-hidden.txt",
                "binary.bin",
                "foo.rs",
                "new-name.txt",
            ]
        );
        assert_eq!(
            files
                .iter()
                .find(|(path, _)| path == "binary.bin")
                .unwrap()
                .1,
            b"needle\0binary"
        );
        assert!(!paths(&files).contains(&"old-name.txt"));
        assert!(!paths(&files).contains(&"regular-to-link.txt"));
        assert!(!paths(&files).contains(&"head-hidden.txt"));
    }

    #[test]
    fn blob_cache_is_byte_bounded_deduplicated_and_enforces_hit_limits() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a", b"aa");
        write_file(repo, "b", b"bb");
        let commit = commit_all(repo, "blobs");
        let store = ObjectStore::new(repo, None).unwrap();
        let items = store
            .walk_commit(&commit, "", &ObjectReadLimits::default())
            .unwrap();
        let a = &items.iter().find(|item| item.path == "a").unwrap().oid;
        let b = &items.iter().find(|item| item.path == "b").unwrap().oid;
        let cache = BlobCache::new(3);
        let limits = ObjectReadLimits::default();

        cache.get_or_read(a, &store, &limits).unwrap();
        cache.get_or_read(a, &store, &limits).unwrap();
        cache.get_or_read(b, &store, &limits).unwrap();
        let diagnostics = cache.diagnostics();
        assert_eq!(diagnostics.hits, 1);
        assert_eq!(diagnostics.misses, 2);
        assert_eq!(diagnostics.evictions, 1);
        assert_eq!(diagnostics.resident_bytes, 2);

        let mut hit_limits = limits.clone();
        hit_limits.max_blob_bytes = 1;
        assert!(matches!(
            cache.get_or_read(b, &store, &hit_limits),
            Err(ObjectReadError::BlobLimitExceeded { size: 2, limit: 1 })
        ));

        let bypass = BlobCache::new(1);
        assert_eq!(
            bypass.get_or_read(a, &store, &limits).unwrap().as_ref(),
            b"aa"
        );
        assert_eq!(bypass.diagnostics().oversize_rejections, 1);
        assert_eq!(bypass.diagnostics().resident_bytes, 0);
    }

    #[test]
    fn aggregate_limits_abort_overlay_without_partial_success() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a", &vec![b'a'; 17 * 1024 * 1024]);
        write_file(repo, "b", &vec![b'b'; 17 * 1024 * 1024]);
        let commit = commit_all(repo, "base");
        let registry = ProjectOverlayRegistry::default();

        let mut limits = ObjectReadLimits {
            max_blob_bytes: 32 * 1024 * 1024,
            ..ObjectReadLimits::default()
        };
        let files = registry
            .files("project", repo, &commit, &commit, "", &limits)
            .unwrap();
        assert_eq!(
            files.iter().map(|(_, bytes)| bytes.len()).sum::<usize>(),
            34 * 1024 * 1024
        );

        limits.max_blob_bytes = 16 * 1024 * 1024;
        assert!(matches!(
            registry.files("project", repo, &commit, &commit, "", &limits),
            Err(ObjectReadError::BlobLimitExceeded { .. })
        ));
        limits.max_blob_bytes = usize::MAX;
        limits.deadline = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            registry
                .files("project", repo, &commit, &commit, "", &limits)
                .unwrap_err(),
            ObjectReadError::DeadlineExceeded
        );
        limits.deadline = Instant::now() + Duration::from_secs(1);
        limits.cancellation.store(true, Ordering::Relaxed);
        assert_eq!(
            registry
                .files("project", repo, &commit, &commit, "", &limits)
                .unwrap_err(),
            ObjectReadError::Cancelled
        );
    }

    #[test]
    fn base_refresh_keeps_pinned_snapshot_immutable() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "base.txt", b"B");
        let base = commit_all(repo, "base");
        write_file(repo, "next.txt", b"B prime");
        let next = commit_all(repo, "next");
        let state = Arc::new(ProjectOverlay::new(repo.to_path_buf()));
        let limits = ObjectReadLimits::default();

        let pinned = state.snapshot(&base, &limits).unwrap();
        let during_refresh = state.snapshot(&next, &limits).unwrap();
        assert_eq!(pinned.commit_id, base);
        assert_eq!(during_refresh.commit_id, base);
        assert!(pinned.files.contains_key("base.txt"));
        assert!(!pinned.files.contains_key("next.txt"));

        let deadline = Instant::now() + Duration::from_secs(5);
        while state.refreshing.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let published = state.snapshot(&next, &limits).unwrap();
        assert_eq!(published.commit_id, next);
        assert!(published.files.contains_key("base.txt"));
        assert!(published.files.contains_key("next.txt"));
        assert_eq!(pinned.commit_id, base);
        assert!(!pinned.files.contains_key("next.txt"));
    }

    #[test]
    fn advancing_head_uses_coordinate_specific_corrections_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "value.txt", b"base");
        let base = commit_all(repo, "base");
        write_file(repo, "value.txt", b"head one");
        let head_one = commit_all(repo, "head one");
        write_file(repo, "value.txt", b"head two");
        let head_two = commit_all(repo, "head two");
        let registry = ProjectOverlayRegistry::default();
        let limits = ObjectReadLimits::default();

        let first = registry
            .files("project", repo, &base, &head_one, "", &limits)
            .unwrap();
        assert_eq!(first[0].1, b"head one");

        let second = registry
            .files("project", repo, &base, &head_two, "", &limits)
            .unwrap();
        assert_eq!(second[0].1, b"head two");
        let state = registry.state("project", repo);
        assert!(state
            .corrections
            .lock()
            .unwrap()
            .entries
            .contains_key(&(base, head_two)));
    }

    #[test]
    fn production_overlay_path_has_no_execution_or_materialization_edge() {
        let sources = [
            include_str!("../branch.rs"),
            include_str!("overlay.rs")
                .split("#[cfg(test)]")
                .next()
                .unwrap(),
        ]
        .join("\n");
        for forbidden in [
            "acquire_store_lock(",
            "Command::new(",
            "std::process::Command",
            "materialize_",
            "CallAdmission",
            "WorktreeLease",
            "scheduler::",
        ] {
            assert!(
                !sources.contains(forbidden),
                "store-native overlay path acquired forbidden edge {forbidden}"
            );
        }
        assert!(sources.contains("cairn_vcs::resolve_coordinate"));
        assert!(sources.contains("ObjectStore::new"));
        assert!(sources.contains(".name_status("));
    }
}
