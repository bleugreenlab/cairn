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

use gix_hash::{oid, Kind as HashKind, ObjectId};
use gix_object::{CommitRefIter, Find, Kind as ObjectKind, TreeRefIter};
use gix_pack::cache;
use gix_pack::data::decode::entry::ResolvedBase;
use gix_pack::{data, index};

/// All archived repositories use SHA-1 object names.
const HASH_KIND: HashKind = HashKind::Sha1;

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

    /// Resolve the blob bytes at `path` as of `commit_oid`, walking commit → root
    /// tree → nested trees → blob, each hop through the layered lookup. The
    /// typed error tells legitimate absence apart from drift/corruption (see
    /// [`ResolvePathError`]); reconstruction degrades both to a coordinate stub.
    pub fn resolve_path_at_commit(
        &self,
        commit_oid: &oid,
        path: &str,
    ) -> Result<Vec<u8>, ResolvePathError> {
        let (kind, commit) = self
            .resolve_object(commit_oid)
            .ok_or_else(|| ResolvePathError::MissingCommit(commit_oid.to_owned()))?;
        if kind != ObjectKind::Commit {
            return Err(ResolvePathError::NotACommit(commit_oid.to_owned()));
        }
        let mut tree_id = CommitRefIter::from_bytes(&commit, HASH_KIND)
            .tree_id()
            .map_err(|_| ResolvePathError::NotACommit(commit_oid.to_owned()))?;

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

    use gix_hash::ObjectId;

    use super::*;
    use crate::archival::build_execution_pack;
    use crate::archival::testutil::{commit_all, git, init_repo, write_file};

    fn object_id(sha: &str) -> ObjectId {
        ObjectId::from_hex(sha.as_bytes()).unwrap()
    }

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

        let (pack, idx, built_tip) = build_execution_pack(&clone, &anchor, "main")
            .unwrap()
            .unwrap();
        assert_eq!(built_tip, tip);

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
        let modified = store
            .resolve_path_at_commit(&object_id(&fx.tip), "a.txt")
            .unwrap();
        assert_eq!(modified, b"A-modified");

        // Without the pack, origin's ODB lacks the branch commit entirely.
        let repo_only = ObjectStore::new(&fx.origin, None).unwrap();
        assert!(matches!(
            repo_only.resolve_path_at_commit(&object_id(&fx.tip), "a.txt"),
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
        let unchanged = store
            .resolve_path_at_commit(&object_id(&fx.tip), "dir/b.txt")
            .unwrap();
        assert_eq!(unchanged, b"B-unchanged");
    }

    #[test]
    fn anchor_commit_resolves_entirely_via_repo_layer() {
        let fx = fixture();
        let store = ObjectStore::new(&fx.origin, Some((fx.pack, fx.idx))).unwrap();

        // The anchor commit is on main, not in the range pack; commit, tree and
        // blob all resolve from the repo ODB.
        let at_anchor = store
            .resolve_path_at_commit(&object_id(&fx.anchor), "a.txt")
            .unwrap();
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
        assert!(build_execution_pack(repo, &anchor, "main")
            .unwrap()
            .is_none());

        let store = ObjectStore::new(repo, None).unwrap();
        let bytes = store
            .resolve_path_at_commit(&object_id(&anchor), "a.txt")
            .unwrap();
        assert_eq!(bytes, b"A");
    }
}
