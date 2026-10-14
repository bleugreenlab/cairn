//! In-memory, layered git object access for event reconstruction.
//!
//! An [`ObjectStore`] resolves objects against an optional in-memory execution
//! pack first, then the project repository's on-disk object database. This
//! per-object layering is what makes coordinate reads cheap: a read of an
//! unmodified file at a worktree commit resolves the commit and trees from the
//! range pack but the unchanged blob from the canonical repo, so unmodified
//! content costs no archived bytes. Reconstruction never shells out to git and
//! never materializes a `.git` directory — the range pack lives entirely in
//! memory as a pair of blobs.

use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use gix_hash::{oid, Kind as HashKind, ObjectId};
use gix_object::{CommitRefIter, Find, Kind as ObjectKind, TreeRefIter};
use gix_pack::cache;
use gix_pack::data::decode::entry::ResolvedBase;
use gix_pack::{data, index};

/// All archived repositories use SHA-1 object names.
const HASH_KIND: HashKind = HashKind::Sha1;

/// Kind of an entry in a Git tree. Symlinks are blobs; gitlinks are rejected
/// because their target is a commit rather than content in this object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeEntryKind {
    Tree,
    Blob,
}

/// One immediate tree entry.
///
/// Git tree names are arbitrary bytes. Cairn's read surface is UTF-8, so names
/// that are not UTF-8 are rejected rather than lossily changing their identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub kind: TreeEntryKind,
    pub mode: u16,
    pub oid: String,
}

/// One recursively walked entry. Results are ordered by raw Git tree-name bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeItem {
    pub path: String,
    pub kind: TreeEntryKind,
    pub mode: u16,
    pub oid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameStatusKind {
    Added,
    Deleted,
    Modified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameStatus {
    pub path: String,
    pub kind: NameStatusKind,
    pub old_oid: Option<String>,
    pub new_oid: Option<String>,
    pub old_mode: Option<u16>,
    pub new_mode: Option<u16>,
}

/// Cooperative bounds shared by all object-read operations.
#[derive(Debug, Clone)]
pub struct ObjectReadLimits {
    pub max_path_depth: usize,
    pub max_entries: usize,
    pub max_blob_bytes: usize,
    pub deadline: Instant,
    pub cancellation: Arc<AtomicBool>,
}

impl ObjectReadLimits {
    pub fn new(
        max_path_depth: usize,
        max_entries: usize,
        max_blob_bytes: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            max_path_depth,
            max_entries,
            max_blob_bytes,
            deadline: Instant::now() + timeout,
            cancellation: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Default for ObjectReadLimits {
    fn default() -> Self {
        Self::new(128, 100_000, 16 * 1024 * 1024, Duration::from_secs(30))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectReadError {
    InvalidObjectId(String),
    MissingObject(String),
    WrongKind { oid: String, expected: &'static str },
    MalformedObject { oid: String, detail: String },
    InvalidPath(String),
    PathNotFound(String),
    NonUtf8Path,
    PathDepthExceeded { limit: usize },
    EntryLimitExceeded { limit: usize },
    BlobLimitExceeded { size: usize, limit: usize },
    AggregateBlobLimitExceeded { size: usize, limit: usize },
    DeadlineExceeded,
    Cancelled,
}

impl std::fmt::Display for ObjectReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidObjectId(id) => write!(f, "invalid object id {id:?}"),
            Self::MissingObject(id) => write!(f, "missing object {id}"),
            Self::WrongKind { oid, expected } => write!(f, "object {oid} is not a {expected}"),
            Self::MalformedObject { oid, detail } => write!(f, "malformed object {oid}: {detail}"),
            Self::InvalidPath(path) => write!(f, "invalid repository path {path:?}"),
            Self::PathNotFound(path) => write!(f, "repository path {path:?} not found"),
            Self::NonUtf8Path => write!(f, "repository path is not valid UTF-8"),
            Self::PathDepthExceeded { limit } => {
                write!(f, "object read path depth exceeds {limit}")
            }
            Self::EntryLimitExceeded { limit } => {
                write!(f, "object read entry limit exceeds {limit}")
            }
            Self::BlobLimitExceeded { size, limit } => {
                write!(f, "blob is {size} bytes, exceeding the {limit}-byte limit")
            }
            Self::AggregateBlobLimitExceeded { size, limit } => write!(
                f,
                "query supplied {size} blob bytes, exceeding the {limit}-byte aggregate limit"
            ),
            Self::DeadlineExceeded => write!(f, "object read deadline exceeded"),
            Self::Cancelled => write!(f, "object read cancelled"),
        }
    }
}

impl std::error::Error for ObjectReadError {}

struct ReadBudget<'a> {
    limits: &'a ObjectReadLimits,
    entries: usize,
}

impl ReadBudget<'_> {
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

fn parse_oid(value: &str) -> Result<ObjectId, ObjectReadError> {
    ObjectId::from_hex(value.as_bytes())
        .map_err(|_| ObjectReadError::InvalidObjectId(value.to_string()))
}

fn limits_check(limits: &ObjectReadLimits) -> Result<(), ObjectReadError> {
    ReadBudget { limits, entries: 0 }.check()
}

fn path_components<'a>(
    path: &'a str,
    limits: &ObjectReadLimits,
) -> Result<Vec<&'a str>, ObjectReadError> {
    if path.starts_with('/') || path.split('/').any(|component| component == "..") {
        return Err(ObjectReadError::InvalidPath(path.to_string()));
    }
    let components: Vec<_> = path
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    if components.len() > limits.max_path_depth {
        return Err(ObjectReadError::PathDepthExceeded {
            limit: limits.max_path_depth,
        });
    }
    limits_check(limits)?;
    Ok(components)
}

/// Why a `resolve_path_at_commit` walk did not reach a blob.
///
/// Reconstruction degrades every variant to a coordinate stub, but the variant
/// lets it tell a *legitimately absent* coordinate (the object was packed away
/// with a dropped execution, or the pack is missing) apart from *drift or
/// corruption* (an object exists but has the wrong kind, or a path component is
/// gone) when deciding what to log. Distinguishing these was the refinement the
/// 1544 ObjectStore builder asked reconstruction to own, rather than wrapping
/// guesswork around an opaque `None`.
#[derive(Debug, Clone)]
pub enum ResolvePathError {
    /// The path had no non-empty components.
    EmptyPath,
    /// The commit coordinate was not a valid 40-hex SHA-1.
    InvalidCommitHex(String),
    /// The commit object is in neither the pack nor the repo ODB.
    MissingCommit(ObjectId),
    /// The addressed object exists but is not a commit (or its header is
    /// undecodable as one).
    NotACommit(ObjectId),
    /// A tree object on the walk is in neither layer — the usual "content no
    /// longer resolvable" case once an execution's pack is dropped.
    MissingObject(ObjectId),
    /// An intermediate path component resolved to a non-tree object (drift).
    NotATree(ObjectId),
    /// A path component was not present in its parent tree (drift).
    NotFound { component: String },
    /// The final component resolved to a non-blob object (drift).
    NotABlob(ObjectId),
}

impl std::fmt::Display for ResolvePathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolvePathError::EmptyPath => write!(f, "empty path"),
            ResolvePathError::InvalidCommitHex(hex) => write!(f, "invalid commit sha {hex:?}"),
            ResolvePathError::MissingCommit(id) => write!(f, "missing commit {id}"),
            ResolvePathError::NotACommit(id) => write!(f, "object {id} is not a commit"),
            ResolvePathError::MissingObject(id) => write!(f, "missing object {id}"),
            ResolvePathError::NotATree(id) => write!(f, "object {id} is not a tree"),
            ResolvePathError::NotFound { component } => {
                write!(f, "path component {component:?} not found")
            }
            ResolvePathError::NotABlob(id) => write!(f, "object {id} is not a blob"),
        }
    }
}

/// An execution range pack held entirely in memory.
struct RangePack {
    data: data::File<Vec<u8>>,
    index: index::File<Vec<u8>>,
}

impl RangePack {
    fn load(pack: Vec<u8>, idx: Vec<u8>) -> Result<Self, String> {
        let data = data::File::from_data(pack, PathBuf::from("<memory>.pack"), HASH_KIND)
            .map_err(|e| format!("loading range pack data: {e}"))?;
        let index = index::File::from_data(idx, PathBuf::from("<memory>.idx"), HASH_KIND)
            .map_err(|e| format!("loading range pack index: {e}"))?;
        Ok(Self { data, index })
    }
}

/// Layered object resolver: execution pack first, then the project repo ODB.
pub struct ObjectStore {
    pack: Option<RangePack>,
    repo: gix_odb::Handle,
}

impl ObjectStore {
    /// Build a store over the canonical repository at `repo_path`, optionally
    /// layered over an in-memory execution pack `(pack, idx)`. Pass `None` when
    /// the execution's range was empty; resolution then falls entirely to the
    /// repo's object database.
    pub fn new(repo_path: &Path, pack: Option<(Vec<u8>, Vec<u8>)>) -> Result<Self, String> {
        let objects_dir = objects_dir(repo_path)?;
        let repo =
            gix_odb::at(objects_dir).map_err(|e| format!("opening repo object database: {e}"))?;
        let pack = match pack {
            Some((pack, idx)) => Some(RangePack::load(pack, idx)?),
            None => None,
        };
        Ok(Self { pack, repo })
    }

    /// Resolve an object's kind and canonical (decompressed, undeltified) bytes,
    /// trying the execution pack first and then the repo ODB. Returns `None` when
    /// neither layer holds the object.
    pub fn resolve_object(&self, id: &oid) -> Option<(ObjectKind, Vec<u8>)> {
        if let Some(pack) = &self.pack {
            if let Some(found) = self.resolve_from_pack(pack, id) {
                return Some(found);
            }
        }
        self.resolve_from_repo(id)
    }

    pub fn commit_root_tree(
        &self,
        commit_hex: &str,
        limits: &ObjectReadLimits,
    ) -> Result<String, ObjectReadError> {
        let mut budget = ReadBudget { limits, entries: 0 };
        Ok(self
            .commit_tree_id(commit_hex, &mut budget)?
            .to_hex()
            .to_string())
    }

    pub fn tree_entries(
        &self,
        tree_hex: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<TreeEntry>, ObjectReadError> {
        let tree = parse_oid(tree_hex)?;
        let mut budget = ReadBudget { limits, entries: 0 };
        self.read_tree(&tree, &mut budget)
    }

    pub fn entries_at_commit(
        &self,
        commit_hex: &str,
        prefix: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<TreeEntry>, ObjectReadError> {
        let mut budget = ReadBudget { limits, entries: 0 };
        let tree = self.tree_at_prefix(commit_hex, prefix, &mut budget)?;
        self.read_tree(&tree, &mut budget)
    }

    pub fn walk_commit(
        &self,
        commit_hex: &str,
        prefix: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<TreeItem>, ObjectReadError> {
        let mut budget = ReadBudget { limits, entries: 0 };
        self.walk_commit_with_budget(commit_hex, prefix, &mut budget)
    }

    fn walk_commit_with_budget(
        &self,
        commit_hex: &str,
        prefix: &str,
        budget: &mut ReadBudget<'_>,
    ) -> Result<Vec<TreeItem>, ObjectReadError> {
        let components = path_components(prefix, budget.limits)?;
        let tree = self.tree_at_prefix(commit_hex, prefix, budget)?;
        let mut out = Vec::new();
        self.walk_tree(
            &tree,
            prefix.trim_matches('/'),
            components.len(),
            budget,
            &mut out,
        )?;
        Ok(out)
    }

    pub fn blob(
        &self,
        oid_hex: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<u8>, ObjectReadError> {
        let oid = parse_oid(oid_hex)?;
        let budget = ReadBudget { limits, entries: 0 };
        budget.check()?;
        let (kind, bytes) = self
            .resolve_object(&oid)
            .ok_or_else(|| ObjectReadError::MissingObject(oid.to_string()))?;
        if kind != ObjectKind::Blob {
            return Err(ObjectReadError::WrongKind {
                oid: oid.to_string(),
                expected: "blob",
            });
        }
        if bytes.len() > limits.max_blob_bytes {
            return Err(ObjectReadError::BlobLimitExceeded {
                size: bytes.len(),
                limit: limits.max_blob_bytes,
            });
        }
        budget.check()?;
        Ok(bytes)
    }

    pub fn read_path_at_commit(
        &self,
        commit_hex: &str,
        path: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<u8>, ObjectReadError> {
        let mut budget = ReadBudget { limits, entries: 0 };
        let components = path_components(path, limits)?;
        if components.is_empty() {
            return Err(ObjectReadError::InvalidPath(path.to_string()));
        }
        let mut tree = self.commit_tree_id(commit_hex, &mut budget)?;
        for (index, component) in components.iter().enumerate() {
            let entries = self.read_tree(&tree, &mut budget)?;
            let entry = entries
                .into_iter()
                .find(|entry| entry.name == *component)
                .ok_or_else(|| ObjectReadError::PathNotFound(path.to_string()))?;
            if index + 1 == components.len() {
                if entry.kind != TreeEntryKind::Blob {
                    return Err(ObjectReadError::WrongKind {
                        oid: entry.oid,
                        expected: "blob",
                    });
                }
                return self.blob(&entry.oid, limits);
            }
            if entry.kind != TreeEntryKind::Tree {
                return Err(ObjectReadError::WrongKind {
                    oid: entry.oid,
                    expected: "tree",
                });
            }
            tree = parse_oid(&entry.oid)?;
        }
        Err(ObjectReadError::InvalidPath(path.to_string()))
    }

    /// Return path-level changes between arbitrary commits. Similarity and rename
    /// detection are intentionally absent: a move is Deleted plus Added, even
    /// when both paths name the same blob OID.
    pub fn name_status(
        &self,
        old_commit: &str,
        new_commit: &str,
        limits: &ObjectReadLimits,
    ) -> Result<Vec<NameStatus>, ObjectReadError> {
        let mut budget = ReadBudget { limits, entries: 0 };
        let old_tree = self.commit_tree_id(old_commit, &mut budget)?;
        let new_tree = self.commit_tree_id(new_commit, &mut budget)?;
        let mut result = Vec::new();
        self.diff_tree_ids(&old_tree, &new_tree, "", 0, &mut budget, &mut result)?;
        result.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        Ok(result)
    }

    fn diff_tree_ids(
        &self,
        old_tree: &oid,
        new_tree: &oid,
        prefix: &str,
        depth: usize,
        budget: &mut ReadBudget<'_>,
        out: &mut Vec<NameStatus>,
    ) -> Result<(), ObjectReadError> {
        budget.check()?;
        if old_tree == new_tree {
            return Ok(());
        }
        if depth > budget.limits.max_path_depth {
            return Err(ObjectReadError::PathDepthExceeded {
                limit: budget.limits.max_path_depth,
            });
        }

        let old = self.read_tree(old_tree, budget)?;
        let new = self.read_tree(new_tree, budget)?;
        let old: std::collections::BTreeMap<_, _> = old
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        let new: std::collections::BTreeMap<_, _> = new
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        let names: std::collections::BTreeSet<_> =
            old.keys().chain(new.keys()).map(String::as_str).collect();

        for name in names {
            budget.check()?;
            let path = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}/{name}")
            };
            match (old.get(name), new.get(name)) {
                (Some(before), Some(after))
                    if before.oid == after.oid && before.mode == after.mode => {}
                (Some(before), Some(after))
                    if before.kind == TreeEntryKind::Tree && after.kind == TreeEntryKind::Tree =>
                {
                    let old_oid = parse_oid(&before.oid)?;
                    let new_oid = parse_oid(&after.oid)?;
                    self.diff_tree_ids(&old_oid, &new_oid, &path, depth + 1, budget, out)?;
                }
                (Some(before), Some(after)) => {
                    if before.kind == TreeEntryKind::Tree {
                        let oid = parse_oid(&before.oid)?;
                        self.collect_tree_statuses(
                            &oid,
                            &path,
                            depth + 1,
                            NameStatusKind::Deleted,
                            budget,
                            out,
                        )?;
                    } else if after.kind == TreeEntryKind::Tree {
                        self.emit_status(
                            &path,
                            NameStatusKind::Deleted,
                            Some(before),
                            None,
                            budget,
                            out,
                        )?;
                    }
                    if after.kind == TreeEntryKind::Tree {
                        let oid = parse_oid(&after.oid)?;
                        self.collect_tree_statuses(
                            &oid,
                            &path,
                            depth + 1,
                            NameStatusKind::Added,
                            budget,
                            out,
                        )?;
                    } else if before.kind == TreeEntryKind::Tree {
                        self.emit_status(
                            &path,
                            NameStatusKind::Added,
                            None,
                            Some(after),
                            budget,
                            out,
                        )?;
                    } else {
                        self.emit_status(
                            &path,
                            NameStatusKind::Modified,
                            Some(before),
                            Some(after),
                            budget,
                            out,
                        )?;
                    }
                }
                (Some(before), None) => {
                    if before.kind == TreeEntryKind::Tree {
                        let oid = parse_oid(&before.oid)?;
                        self.collect_tree_statuses(
                            &oid,
                            &path,
                            depth + 1,
                            NameStatusKind::Deleted,
                            budget,
                            out,
                        )?;
                    } else {
                        self.emit_status(
                            &path,
                            NameStatusKind::Deleted,
                            Some(before),
                            None,
                            budget,
                            out,
                        )?;
                    }
                }
                (None, Some(after)) => {
                    if after.kind == TreeEntryKind::Tree {
                        let oid = parse_oid(&after.oid)?;
                        self.collect_tree_statuses(
                            &oid,
                            &path,
                            depth + 1,
                            NameStatusKind::Added,
                            budget,
                            out,
                        )?;
                    } else {
                        self.emit_status(
                            &path,
                            NameStatusKind::Added,
                            None,
                            Some(after),
                            budget,
                            out,
                        )?;
                    }
                }
                (None, None) => {}
            }
        }
        Ok(())
    }

    fn collect_tree_statuses(
        &self,
        tree: &oid,
        prefix: &str,
        depth: usize,
        kind: NameStatusKind,
        budget: &mut ReadBudget<'_>,
        out: &mut Vec<NameStatus>,
    ) -> Result<(), ObjectReadError> {
        if depth > budget.limits.max_path_depth {
            return Err(ObjectReadError::PathDepthExceeded {
                limit: budget.limits.max_path_depth,
            });
        }
        for entry in self.read_tree(tree, budget)? {
            let path = format!("{prefix}/{}", entry.name);
            if entry.kind == TreeEntryKind::Tree {
                let oid = parse_oid(&entry.oid)?;
                self.collect_tree_statuses(&oid, &path, depth + 1, kind, budget, out)?;
            } else {
                self.emit_status(
                    &path,
                    kind,
                    (kind == NameStatusKind::Deleted).then_some(&entry),
                    (kind == NameStatusKind::Added).then_some(&entry),
                    budget,
                    out,
                )?;
            }
        }
        Ok(())
    }

    fn emit_status(
        &self,
        path: &str,
        kind: NameStatusKind,
        before: Option<&TreeEntry>,
        after: Option<&TreeEntry>,
        budget: &mut ReadBudget<'_>,
        out: &mut Vec<NameStatus>,
    ) -> Result<(), ObjectReadError> {
        budget.entry()?;
        out.push(NameStatus {
            path: path.to_string(),
            kind,
            old_oid: before.map(|entry| entry.oid.clone()),
            new_oid: after.map(|entry| entry.oid.clone()),
            old_mode: before.map(|entry| entry.mode),
            new_mode: after.map(|entry| entry.mode),
        });
        Ok(())
    }

    fn commit_tree_id(
        &self,
        commit_hex: &str,
        budget: &mut ReadBudget<'_>,
    ) -> Result<ObjectId, ObjectReadError> {
        budget.check()?;
        let commit = parse_oid(commit_hex)?;
        let (kind, bytes) = self
            .resolve_object(&commit)
            .ok_or_else(|| ObjectReadError::MissingObject(commit.to_string()))?;
        if kind != ObjectKind::Commit {
            return Err(ObjectReadError::WrongKind {
                oid: commit.to_string(),
                expected: "commit",
            });
        }
        CommitRefIter::from_bytes(&bytes, HASH_KIND)
            .tree_id()
            .map_err(|error| ObjectReadError::MalformedObject {
                oid: commit.to_string(),
                detail: error.to_string(),
            })
    }

    fn tree_at_prefix(
        &self,
        commit_hex: &str,
        prefix: &str,
        budget: &mut ReadBudget<'_>,
    ) -> Result<ObjectId, ObjectReadError> {
        let components = path_components(prefix, budget.limits)?;
        let mut tree = self.commit_tree_id(commit_hex, budget)?;
        for component in components {
            let entry = self
                .read_tree(&tree, budget)?
                .into_iter()
                .find(|entry| entry.name == component)
                .ok_or_else(|| ObjectReadError::PathNotFound(prefix.to_string()))?;
            if entry.kind != TreeEntryKind::Tree {
                return Err(ObjectReadError::WrongKind {
                    oid: entry.oid,
                    expected: "tree",
                });
            }
            tree = parse_oid(&entry.oid)?;
        }
        Ok(tree)
    }

    fn read_tree(
        &self,
        tree: &oid,
        budget: &mut ReadBudget<'_>,
    ) -> Result<Vec<TreeEntry>, ObjectReadError> {
        budget.check()?;
        let (kind, bytes) = self
            .resolve_object(tree)
            .ok_or_else(|| ObjectReadError::MissingObject(tree.to_string()))?;
        if kind != ObjectKind::Tree {
            return Err(ObjectReadError::WrongKind {
                oid: tree.to_string(),
                expected: "tree",
            });
        }
        let mut result = Vec::new();
        for parsed in TreeRefIter::from_bytes(&bytes, HASH_KIND) {
            budget.entry()?;
            let entry = parsed.map_err(|error| ObjectReadError::MalformedObject {
                oid: tree.to_string(),
                detail: error.to_string(),
            })?;
            let name = std::str::from_utf8(entry.filename)
                .map_err(|_| ObjectReadError::NonUtf8Path)?
                .to_string();
            let kind = if entry.mode.is_tree() {
                TreeEntryKind::Tree
            } else {
                TreeEntryKind::Blob
            };
            result.push(TreeEntry {
                name,
                kind,
                mode: entry.mode.value(),
                oid: entry.oid.to_hex().to_string(),
            });
        }
        result.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        Ok(result)
    }

    fn walk_tree(
        &self,
        tree: &oid,
        prefix: &str,
        depth: usize,
        budget: &mut ReadBudget<'_>,
        out: &mut Vec<TreeItem>,
    ) -> Result<(), ObjectReadError> {
        budget.check()?;
        if depth > budget.limits.max_path_depth {
            return Err(ObjectReadError::PathDepthExceeded {
                limit: budget.limits.max_path_depth,
            });
        }
        for entry in self.read_tree(tree, budget)? {
            budget.check()?;
            let entry_depth = depth + 1;
            if entry_depth > budget.limits.max_path_depth {
                return Err(ObjectReadError::PathDepthExceeded {
                    limit: budget.limits.max_path_depth,
                });
            }
            let path = if prefix.is_empty() {
                entry.name.clone()
            } else {
                format!("{prefix}/{}", entry.name)
            };
            let oid = entry.oid.clone();
            out.push(TreeItem {
                path: path.clone(),
                kind: entry.kind,
                mode: entry.mode,
                oid: oid.clone(),
            });
            if entry.kind == TreeEntryKind::Tree {
                let child = parse_oid(&oid)?;
                self.walk_tree(&child, &path, entry_depth, budget, out)?;
            }
        }
        Ok(())
    }
    /// Resolve the blob bytes at `path` as of `commit_oid`, walking commit → root
    /// tree → nested trees → blob, each hop through the layered lookup. The
    /// typed error tells legitimate absence apart from drift/corruption (see
    /// [`ResolvePathError`]); reconstruction degrades both to a coordinate stub.
    pub fn resolve_path_at_commit(
        &self,
        commit_hex: &str,
        path: &str,
    ) -> Result<Vec<u8>, ResolvePathError> {
        let commit_oid = ObjectId::from_hex(commit_hex.as_bytes())
            .map_err(|_| ResolvePathError::InvalidCommitHex(commit_hex.to_string()))?;
        let (kind, commit) = self
            .resolve_object(&commit_oid)
            .ok_or(ResolvePathError::MissingCommit(commit_oid))?;
        if kind != ObjectKind::Commit {
            return Err(ResolvePathError::NotACommit(commit_oid));
        }
        let mut tree_id = CommitRefIter::from_bytes(&commit, HASH_KIND)
            .tree_id()
            .map_err(|_| ResolvePathError::NotACommit(commit_oid))?;

        let components: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
        if components.is_empty() {
            return Err(ResolvePathError::EmptyPath);
        }

        for (depth, component) in components.iter().enumerate() {
            let (tree_kind, tree) = self
                .resolve_object(&tree_id)
                .ok_or(ResolvePathError::MissingObject(tree_id))?;
            if tree_kind != ObjectKind::Tree {
                return Err(ResolvePathError::NotATree(tree_id));
            }
            let want = component.as_bytes();
            let entry = TreeRefIter::from_bytes(&tree, HASH_KIND)
                .filter_map(Result::ok)
                .find(|entry| {
                    let name: &[u8] = entry.filename;
                    name == want
                })
                .ok_or_else(|| ResolvePathError::NotFound {
                    component: (*component).to_string(),
                })?;

            let entry_oid = entry.oid.to_owned();
            if depth + 1 == components.len() {
                let (blob_kind, blob) = self
                    .resolve_object(&entry_oid)
                    .ok_or(ResolvePathError::MissingObject(entry_oid))?;
                if blob_kind != ObjectKind::Blob {
                    return Err(ResolvePathError::NotABlob(entry_oid));
                }
                return Ok(blob);
            }
            tree_id = entry_oid;
        }
        // The final-component branch returns inside the loop; a non-empty
        // component list always reaches it.
        Err(ResolvePathError::EmptyPath)
    }

    fn resolve_from_repo(&self, id: &oid) -> Option<(ObjectKind, Vec<u8>)> {
        let mut buf = Vec::new();
        let data = self.repo.try_find(id, &mut buf).ok()??;
        Some((data.kind, data.data.to_vec()))
    }

    fn resolve_from_pack(&self, pack: &RangePack, id: &oid) -> Option<(ObjectKind, Vec<u8>)> {
        let entry_index = pack.index.lookup(id)?;
        let offset = pack.index.pack_offset_at_index(entry_index);
        let entry = pack.data.entry(offset).ok()?;

        // A non-thin pack from `git pack-objects` resolves delta bases by offset
        // within the pack. The ref-delta path is exercised only by unusual input;
        // resolve such bases from the pack, then from the repo ODB, so the layered
        // contract holds even there.
        let resolve = |base: &oid, out: &mut Vec<u8>| -> Option<ResolvedBase> {
            if let Some(base_index) = pack.index.lookup(base) {
                let base_offset = pack.index.pack_offset_at_index(base_index);
                return Some(ResolvedBase::InPack(pack.data.entry(base_offset).ok()?));
            }
            let mut buf = Vec::new();
            let found = self.repo.try_find(base, &mut buf).ok()??;
            out.clear();
            out.extend_from_slice(found.data);
            Some(ResolvedBase::OutOfPack {
                kind: found.kind,
                end: out.len(),
            })
        };

        let mut out = Vec::new();
        let mut inflate = gix_features::zlib::Inflate::default();
        let mut delta_cache = cache::Never;
        let outcome = pack
            .data
            .decode_entry(entry, &mut out, &mut inflate, &resolve, &mut delta_cache)
            .ok()?;
        out.truncate(outcome.object_size as usize);
        Some((outcome.kind, out))
    }
}

/// Resolve the object-database directory for a canonical checkout or bare repo.
fn objects_dir(repo_path: &Path) -> Result<PathBuf, String> {
    let with_git = repo_path.join(".git").join("objects");
    if with_git.is_dir() {
        return Ok(with_git);
    }
    let bare = repo_path.join("objects");
    if bare.is_dir() {
        return Ok(bare);
    }
    Err(format!(
        "no git object database found under {}",
        repo_path.display()
    ))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::packfile::build_execution_pack;
    use crate::testutil::{commit_all, git, init_repo, write_file};

    /// origin (the canonical repo) holds main at the anchor; a separate clone
    /// makes branch commits whose new objects never reach origin's ODB, so they
    /// can only be resolved from the range pack.
    struct Fixture {
        _origin_dir: tempfile::TempDir,
        _clone_dir: tempfile::TempDir,
        origin: PathBuf,
        anchor: String,
        tip: String,
        pack: Vec<u8>,
        idx: Vec<u8>,
    }

    fn fixture() -> Fixture {
        let origin_dir = tempfile::tempdir().unwrap();
        let origin = origin_dir.path().to_path_buf();
        init_repo(&origin);
        write_file(&origin, "a.txt", b"A");
        write_file(&origin, "dir/b.txt", b"B-unchanged");
        let anchor = commit_all(&origin, "base");

        let clone_dir = tempfile::tempdir().unwrap();
        let clone = clone_dir.path().to_path_buf();
        git(
            &origin,
            &[
                "clone",
                "-q",
                origin.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );
        // Work on a branch so the durable default branch ("main") stays at anchor.
        git(&clone, &["checkout", "-q", "-b", "work"]);
        write_file(&clone, "a.txt", b"A-modified");
        let tip = commit_all(&clone, "modify a");

        let (pack, idx) = build_execution_pack(&clone, &tip, &anchor, "main")
            .unwrap()
            .unwrap();

        Fixture {
            _origin_dir: origin_dir,
            _clone_dir: clone_dir,
            origin,
            anchor,
            tip,
            pack,
            idx,
        }
    }

    #[test]
    fn modified_path_resolves_from_pack_only() {
        let fx = fixture();
        let store = ObjectStore::new(&fx.origin, Some((fx.pack.clone(), fx.idx.clone()))).unwrap();

        // Commit, root tree and the modified blob all live only in the pack.
        let modified = store.resolve_path_at_commit(&fx.tip, "a.txt").unwrap();
        assert_eq!(modified, b"A-modified");

        // Without the pack, origin's ODB lacks the branch commit entirely.
        let repo_only = ObjectStore::new(&fx.origin, None).unwrap();
        assert!(matches!(
            repo_only.resolve_path_at_commit(&fx.tip, "a.txt"),
            Err(ResolvePathError::MissingCommit(_))
        ));
    }

    #[test]
    fn unmodified_path_resolves_blob_from_repo_layer() {
        let fx = fixture();
        let store = ObjectStore::new(&fx.origin, Some((fx.pack, fx.idx))).unwrap();

        // The commit and trees come from the pack, but dir/b.txt was untouched in
        // the range, so its blob is absent from the pack and resolves from the
        // origin ODB layer.
        let unchanged = store.resolve_path_at_commit(&fx.tip, "dir/b.txt").unwrap();
        assert_eq!(unchanged, b"B-unchanged");
    }

    #[test]
    fn anchor_commit_resolves_entirely_via_repo_layer() {
        let fx = fixture();
        let store = ObjectStore::new(&fx.origin, Some((fx.pack, fx.idx))).unwrap();

        // The anchor commit is on main, not in the range pack; commit, tree and
        // blob all resolve from the repo ODB.
        let at_anchor = store.resolve_path_at_commit(&fx.anchor, "a.txt").unwrap();
        assert_eq!(at_anchor, b"A");
    }

    #[test]
    fn empty_range_store_resolves_via_repo_layer() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"A");
        let anchor = commit_all(repo, "base");
        // tip == anchor: the range is empty.
        assert!(build_execution_pack(repo, &anchor, &anchor, "main")
            .unwrap()
            .is_none());

        let store = ObjectStore::new(repo, None).unwrap();
        let bytes = store.resolve_path_at_commit(&anchor, "a.txt").unwrap();
        assert_eq!(bytes, b"A");
    }

    #[test]
    fn bounded_tree_walk_and_blob_reads_are_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "z.txt", b"Z");
        write_file(repo, "dir/b.txt", b"B");
        write_file(repo, "dir/a.txt", b"A");
        let commit = commit_all(repo, "tree");
        let store = ObjectStore::new(repo, None).unwrap();
        let limits = ObjectReadLimits::default();

        let root = store.commit_root_tree(&commit, &limits).unwrap();
        assert_eq!(root.len(), 40);
        let immediate = store.entries_at_commit(&commit, "dir", &limits).unwrap();
        assert_eq!(
            immediate
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            ["a.txt", "b.txt"]
        );
        let walked = store.walk_commit(&commit, "", &limits).unwrap();
        assert_eq!(
            walked
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            ["dir", "dir/a.txt", "dir/b.txt", "z.txt"]
        );
        assert_eq!(
            store
                .read_path_at_commit(&commit, "dir/a.txt", &limits)
                .unwrap(),
            b"A"
        );
    }

    #[test]
    fn object_read_limits_fail_without_partial_success() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "dir/a.txt", b"oversized");
        write_file(repo, "dir/b.txt", b"B");
        let commit = commit_all(repo, "limits");
        let store = ObjectStore::new(repo, None).unwrap();

        let mut limits = ObjectReadLimits {
            max_blob_bytes: 2,
            ..ObjectReadLimits::default()
        };
        assert!(matches!(
            store.read_path_at_commit(&commit, "dir/a.txt", &limits),
            Err(ObjectReadError::BlobLimitExceeded { .. })
        ));
        limits.max_blob_bytes = usize::MAX;
        limits.max_entries = 1;
        assert!(matches!(
            store.walk_commit(&commit, "", &limits),
            Err(ObjectReadError::EntryLimitExceeded { .. })
        ));
        limits.max_entries = usize::MAX;
        limits.max_path_depth = 1;
        assert!(matches!(
            store.read_path_at_commit(&commit, "dir/a.txt", &limits),
            Err(ObjectReadError::PathDepthExceeded { .. })
        ));
        assert!(matches!(
            store.walk_commit(&commit, "dir", &limits),
            Err(ObjectReadError::PathDepthExceeded { .. })
        ));
        limits.max_path_depth = 2;
        assert!(store.walk_commit(&commit, "dir", &limits).is_ok());
        limits.max_path_depth = usize::MAX;
        limits.deadline = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            store.walk_commit(&commit, "", &limits).unwrap_err(),
            ObjectReadError::DeadlineExceeded
        );
        limits.deadline = Instant::now() + Duration::from_secs(1);
        limits.cancellation.store(true, Ordering::Relaxed);
        assert_eq!(
            store.walk_commit(&commit, "", &limits).unwrap_err(),
            ObjectReadError::Cancelled
        );
    }

    #[test]
    fn name_status_skips_equal_subtrees_and_honors_shared_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        for index in 0..20 {
            write_file(
                repo,
                &format!("unchanged/{index:02}.txt"),
                format!("{index}").as_bytes(),
            );
        }
        write_file(repo, "changed.txt", b"old");
        let old = commit_all(repo, "old");
        write_file(repo, "changed.txt", b"new");
        let new = commit_all(repo, "new");
        let store = ObjectStore::new(repo, None).unwrap();

        // Two entries in each root tree plus one emitted change fit exactly. A
        // dual full-tree walk would also visit all 20 entries below unchanged.
        let mut limits = ObjectReadLimits {
            max_entries: 5,
            ..ObjectReadLimits::default()
        };
        let statuses = store.name_status(&old, &new, &limits).unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].path, "changed.txt");
        assert_eq!(statuses[0].kind, NameStatusKind::Modified);

        limits.max_entries = 4;
        assert!(matches!(
            store.name_status(&old, &new, &limits),
            Err(ObjectReadError::EntryLimitExceeded { limit: 4 })
        ));
        limits.max_entries = usize::MAX;
        limits.deadline = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            store.name_status(&old, &new, &limits).unwrap_err(),
            ObjectReadError::DeadlineExceeded
        );
        limits.deadline = Instant::now() + Duration::from_secs(1);
        limits.cancellation.store(true, Ordering::Relaxed);
        assert_eq!(
            store.name_status(&old, &new, &limits).unwrap_err(),
            ObjectReadError::Cancelled
        );
    }

    #[test]
    fn name_status_reports_modes_and_rename_as_delete_add() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "delete.txt", b"delete");
        write_file(repo, "modify.txt", b"old");
        write_file(repo, "rename.txt", b"same oid");
        write_file(repo, "mode.txt", b"mode");
        let old = commit_all(repo, "old");

        std::fs::remove_file(repo.join("delete.txt")).unwrap();
        write_file(repo, "modify.txt", b"new");
        std::fs::rename(repo.join("rename.txt"), repo.join("renamed.txt")).unwrap();
        write_file(repo, "add.txt", b"add");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                repo.join("mode.txt"),
                std::fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
        let new = commit_all(repo, "new");

        let store = ObjectStore::new(repo, None).unwrap();
        let statuses = store
            .name_status(&old, &new, &ObjectReadLimits::default())
            .unwrap();
        let summary: Vec<_> = statuses
            .iter()
            .map(|status| (status.path.as_str(), status.kind))
            .collect();
        assert_eq!(
            summary,
            [
                ("add.txt", NameStatusKind::Added),
                ("delete.txt", NameStatusKind::Deleted),
                ("mode.txt", NameStatusKind::Modified),
                ("modify.txt", NameStatusKind::Modified),
                ("rename.txt", NameStatusKind::Deleted),
                ("renamed.txt", NameStatusKind::Added),
            ]
        );
        let mode = statuses
            .iter()
            .find(|status| status.path == "mode.txt")
            .unwrap();
        assert_ne!(mode.old_mode, mode.new_mode);
        let deleted = statuses
            .iter()
            .find(|status| status.path == "rename.txt")
            .unwrap();
        let added = statuses
            .iter()
            .find(|status| status.path == "renamed.txt")
            .unwrap();
        assert_eq!(deleted.old_oid, added.new_oid);
    }
}
