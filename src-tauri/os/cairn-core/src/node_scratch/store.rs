//! The runner's copy of a node scratch: an isolated bare Git store, one per
//! scratch, at `<cairn home>/node-scratch-store/<project_id>/<scratch_id>.git`.
//!
//! This is content storage, not lifecycle state. The scratch's state is the
//! `node_scratches` row; this store holds the objects that row names, and its
//! single ref (`refs/heads/head`) exists only so gc keeps the head's objects.
//! The ref is rewritten from the row before every gc. The store shares no lock
//! with the project's repository, jj never sees it, and closing the node
//! deletes the directory.

use std::path::{Path, PathBuf};

use cairn_codec::scratch_tree::{ScratchRepo, TreeFile};
use cairn_common::executor_protocol::ScratchBlob;

use super::merge::Tree;

pub const STORE_DIR: &str = "node-scratch-store";

/// Garbage a store may carry beyond twice its head before it is collected.
const GC_SLACK_BYTES: u64 = 16 * 1024 * 1024;
/// Every write-back installs a pack; past this many, repack.
const GC_PACK_LIMIT: u64 = 32;

pub fn store_root(config_dir: &Path) -> PathBuf {
    config_dir.join(STORE_DIR)
}

pub fn store_path(config_dir: &Path, project_id: &str, scratch_id: &str) -> PathBuf {
    store_root(config_dir)
        .join(project_id)
        .join(format!("{scratch_id}.git"))
}

pub fn open(config_dir: &Path, project_id: &str, scratch_id: &str) -> Result<ScratchRepo, String> {
    let repo = ScratchRepo::bare(store_path(config_dir, project_id, scratch_id));
    repo.ensure()?;
    Ok(repo)
}

/// A commit's files as the merge reads them.
pub fn tree(repo: &ScratchRepo, commit: Option<&str>) -> Result<Tree, String> {
    Ok(repo
        .files(commit)?
        .into_iter()
        .map(|file| {
            (
                file.path,
                ScratchBlob {
                    oid: file.oid,
                    mode: file.mode,
                    size: file.size,
                },
            )
        })
        .collect())
}

/// Write `tree` as a scratch commit. Fails if any blob is absent.
pub fn commit(repo: &ScratchRepo, tree: &Tree) -> Result<String, String> {
    let files: Vec<TreeFile> = tree
        .iter()
        .map(|(path, blob)| TreeFile {
            path: path.clone(),
            mode: blob.mode,
            oid: blob.oid.clone(),
            size: blob.size,
        })
        .collect();
    let tree = repo.write_tree(&files)?;
    repo.commit(&tree)
}

/// Derive the ref from the authority, then collect garbage once it exceeds
/// the head or the packs pile up. Returns whether gc ran.
///
/// Every state is a parentless commit, so a superseded head is garbage the
/// moment the ref moves: history never accumulates. Callers hold the
/// scratch's write lock, so no write-back's objects are in flight.
pub fn maintain(repo: &ScratchRepo, head: Option<&str>, head_bytes: u64) -> Result<bool, String> {
    repo.set_head_ref(head)?;
    let (bytes, packs) = repo.footprint()?;
    if packs <= GC_PACK_LIMIT
        && bytes <= head_bytes.saturating_mul(2).saturating_add(GC_SLACK_BYTES)
    {
        return Ok(false);
    }
    repo.run(["gc", "--prune=now", "--quiet"], None)?;
    Ok(true)
}

/// Remove a store for good. Renamed aside first, so a reader that races the
/// removal sees a store that is gone rather than one half-deleted.
pub fn delete(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let doomed = path.with_extension(format!("deleting-{}", uuid::Uuid::new_v4().simple()));
    std::fs::rename(path, &doomed)
        .map_err(|error| format!("stage {} for deletion: {error}", path.display()))?;
    std::fs::remove_dir_all(&doomed)
        .map_err(|error| format!("delete {}: {error}", doomed.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_scratch::merge;
    use cairn_common::executor_protocol::{ScratchChange, ScratchPath};

    fn hash(repo: &ScratchRepo, bytes: &[u8]) -> String {
        String::from_utf8(
            repo.run(["hash-object", "-w", "--stdin"], Some(bytes))
                .unwrap(),
        )
        .unwrap()
        .trim()
        .to_owned()
    }

    #[test]
    fn the_ref_is_derived_from_the_authority_and_gc_never_loses_the_head() {
        let temp = tempfile::tempdir().unwrap();
        let repo = open(temp.path(), "project", "scratch").unwrap();
        let mut head: Option<String> = None;
        let mut head_tree = Tree::new();
        // Enough superseded states to cross the pack limit.
        for round in 0..(GC_PACK_LIMIT + 3) {
            let oid = hash(&repo, format!("round {round}").as_bytes());
            let pack = repo.pack_of(std::slice::from_ref(&oid)).unwrap();
            repo.install_pack(&pack).unwrap();
            let plan = merge::plan(
                &head_tree,
                &[ScratchChange {
                    path: ScratchPath::from("state"),
                    base: head_tree.get(b"state".as_slice()).cloned(),
                    new: Some(ScratchBlob {
                        oid,
                        mode: 0o100644,
                        size: 8,
                    }),
                }],
                "w",
            );
            let next = commit(&repo, &plan.result).unwrap();
            head = Some(next);
            head_tree = plan.result;
        }
        // The ref lags, as it would after a crash between the row's swap and
        // the ref update: maintenance must rewrite it from the authority.
        repo.set_head_ref(None).unwrap();
        assert!(
            maintain(&repo, head.as_deref(), 8).unwrap(),
            "gc should run"
        );
        let (_, packs) = repo.footprint().unwrap();
        assert!(packs <= 1, "gc repacks: {packs}");
        assert_eq!(tree(&repo, head.as_deref()).unwrap(), head_tree);
        assert_eq!(
            String::from_utf8(repo.run(["rev-parse", "refs/heads/head"], None).unwrap())
                .unwrap()
                .trim(),
            head.unwrap()
        );
    }

    #[test]
    fn delete_removes_the_store() {
        let temp = tempfile::tempdir().unwrap();
        open(temp.path(), "project", "scratch").unwrap();
        let path = store_path(temp.path(), "project", "scratch");
        assert!(path.is_dir());
        delete(&path).unwrap();
        assert!(!path.exists());
        assert!(std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .next()
            .is_none());
    }
}
