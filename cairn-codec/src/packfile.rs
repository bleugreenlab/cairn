//! Range packfile construction for session archival.
//!
//! At worktree teardown, [`build_execution_pack`] captures the git objects that
//! exist only on the execution's branch — those reachable from the worktree tip
//! but not from the durable pack anchor or the default branch — into a single
//! packfile plus index. These are exactly the objects at risk of garbage
//! collection once the worktree is removed; everything else stays resolvable
//! from the project repository's object database.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// A built execution range pack: packfile bytes and pack index bytes.
pub type ExecutionPack = (Vec<u8>, Vec<u8>);

/// Build the execution range pack for the range `pack_anchor..tip`.
///
/// `git_dir` is the git repository the range is computed and packed in, and
/// `tip` is the worktree tip the range is built against — both resolved by the
/// caller so this never assumes the worktree itself is a git repository. Under
/// jj a production workspace is `.jj`-only (no `.git`), so the caller routes
/// `git_dir` to the project repo, where `jj git export` has landed the
/// execution's objects, and resolves `tip` from jj's `@-`; under plain git both
/// are the worktree.
///
/// Returns `Ok(Some((pack, idx)))` with the packfile bytes and its index bytes.
/// Returns `Ok(None)` when the range is empty — everything reachable from the tip
/// is already durable (a fast-forward or true merge into the default branch, or
/// tip == anchor) — so reconstruction resolves entirely from the project repo's
/// object database.
///
/// `pack_anchor` is the durable anchor the range is built against; both it and
/// `default_branch_ref` are excluded so a default branch merged into the branch
/// mid-flight never over-packs already-durable objects. Shelling out to git here
/// is intentional: this runs at teardown where the objects still exist.
pub fn build_execution_pack(
    git_dir: &Path,
    tip: &str,
    pack_anchor: &str,
    default_branch_ref: &str,
) -> Result<Option<ExecutionPack>, String> {
    let rev_list = run_git(
        git_dir,
        &[
            "rev-list",
            "--objects",
            tip,
            "--not",
            pack_anchor,
            default_branch_ref,
        ],
    )?;

    // `rev-list --objects` emits "<oid> [path]" per line; pack-objects only wants
    // the object names.
    let mut oids = String::new();
    for line in rev_list.lines() {
        if let Some(oid) = line.split_whitespace().next() {
            oids.push_str(oid);
            oids.push('\n');
        }
    }

    if oids.is_empty() {
        return Ok(None);
    }

    let scratch = tempfile::tempdir().map_err(|e| format!("creating pack scratch dir: {e}"))?;
    let base = scratch.path().join("range");
    let hash = pack_objects(git_dir, &base, &oids)?;

    let pack_path = scratch.path().join(format!("range-{hash}.pack"));
    let idx_path = scratch.path().join(format!("range-{hash}.idx"));
    let pack = std::fs::read(&pack_path)
        .map_err(|e| format!("reading packfile {}: {e}", pack_path.display()))?;
    let idx = std::fs::read(&idx_path)
        .map_err(|e| format!("reading pack index {}: {e}", idx_path.display()))?;

    Ok(Some((pack, idx)))
}

/// Pipe `oids` into `git pack-objects <base>`, which writes `<base>-<hash>.pack`
/// and `<base>-<hash>.idx` and prints `<hash>` to stdout.
fn pack_objects(git_dir: &Path, base: &Path, oids: &str) -> Result<String, String> {
    let mut child = Command::new("git")
        .current_dir(git_dir)
        .arg("pack-objects")
        .arg("--quiet")
        .arg(base)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning git pack-objects: {e}"))?;

    // Drop the stdin handle after writing so git sees EOF. Only the short pack
    // hash flows back over stdout (the pack itself is written to files), so there
    // is no risk of a stdout-fills-before-we-finish-writing deadlock.
    child
        .stdin
        .take()
        .ok_or_else(|| "git pack-objects stdin unavailable".to_string())?
        .write_all(oids.as_bytes())
        .map_err(|e| format!("writing oids to git pack-objects: {e}"))?;

    let output = child
        .wait_with_output()
        .map_err(|e| format!("waiting for git pack-objects: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "git pack-objects failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn run_git(git_dir: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .current_dir(git_dir)
        .args(args)
        .output()
        .map_err(|e| format!("spawning git {args:?}: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The set of object ids (40-hex) contained in a pack index. Exposed for archival
/// tests that assert exactly which range objects a pack captured.
#[cfg(any(test, feature = "test-utils"))]
pub fn pack_index_oids(idx: &[u8]) -> std::collections::BTreeSet<String> {
    let index = gix_pack::index::File::from_data(
        idx.to_vec(),
        std::path::PathBuf::from("<memory>.idx"),
        gix_hash::Kind::Sha1,
    )
    .expect("valid pack index");
    index.iter().map(|e| e.oid.to_hex().to_string()).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    use super::*;
    use crate::testutil::{commit_all, git, init_repo, write_file};

    fn range_oids(repo: &Path, tip: &str, anchor: &str, default_ref: &str) -> BTreeSet<String> {
        run_git(
            repo,
            &["rev-list", "--objects", tip, "--not", anchor, default_ref],
        )
        .unwrap()
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn captures_exactly_the_range_objects() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"A");
        write_file(repo, "b.txt", b"B");
        let anchor = commit_all(repo, "base");

        git(repo, &["checkout", "-q", "-b", "work"]);
        write_file(repo, "a.txt", b"A2");
        commit_all(repo, "work 1");
        write_file(repo, "c.txt", b"C");
        let tip = commit_all(repo, "work 2");

        let (pack, idx) = build_execution_pack(repo, &tip, &anchor, "main")
            .unwrap()
            .unwrap();
        assert!(!pack.is_empty());

        let expected = range_oids(repo, &tip, &anchor, "main");
        assert!(!expected.is_empty());
        assert_eq!(pack_index_oids(&idx), expected);
    }

    #[test]
    fn captures_integration_and_child_commits_in_a_chain() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"A");
        let anchor = commit_all(repo, "base");

        git(repo, &["checkout", "-q", "-b", "integration"]);
        write_file(repo, "i.txt", b"I");
        let integration = commit_all(repo, "integration 1");

        git(repo, &["checkout", "-q", "-b", "child"]);
        write_file(repo, "ch.txt", b"CH");
        let tip = commit_all(repo, "child 1");

        let (_pack, idx) = build_execution_pack(repo, &tip, &anchor, "main")
            .unwrap()
            .unwrap();
        let oids = pack_index_oids(&idx);
        // The anchor is on main; the intermediate integration commit and the child
        // commit on top are both captured by `tip --not anchor main`.
        assert!(oids.contains(&integration));
        assert!(oids.contains(&tip));
    }

    #[test]
    fn empty_range_returns_none_when_tip_is_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"A");
        let anchor = commit_all(repo, "base");
        // tip == anchor == main: nothing is at risk.
        assert!(build_execution_pack(repo, &anchor, &anchor, "main")
            .unwrap()
            .is_none());
    }

    #[test]
    fn empty_range_returns_none_when_merged_into_default() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"A");
        let anchor = commit_all(repo, "base");

        git(repo, &["checkout", "-q", "-b", "work"]);
        write_file(repo, "a.txt", b"A2");
        let tip = commit_all(repo, "work");
        // Fast-forward the default branch to the work tip: everything reachable
        // from tip is now durable on main, so the range is empty.
        git(repo, &["branch", "-f", "main", &tip]);
        assert!(build_execution_pack(repo, &tip, &anchor, "main")
            .unwrap()
            .is_none());
    }
}
