//! Working-copy and sealed-tree reads: dirty paths, tracked files, tree
//! hashes and entries via the git backend.
use super::*;
use std::path::Path;

/// The repo-relative paths currently visible in `@` (the working-copy diff vs
/// its parent), parsed from `jj diff --summary`. Each summary line is
/// `<status> <path>` (e.g. `A src/new.rs`); the status letter is dropped. Used
/// by populate's security backstop to enumerate any populated path that leaked
/// into the snapshot.
pub fn working_copy_dirty_paths(jj: &JjEnv, ws: &Path) -> Result<Vec<String>, String> {
    let out = jj.run(ws, &["diff", "--summary"], "jj diff --summary")?;
    Ok(out
        .lines()
        .filter_map(|line| {
            line.split_once(' ')
                .map(|(_, path)| path.trim().to_string())
        })
        .filter(|path| !path.is_empty())
        .collect())
}

/// Capture the working copy's diff vs its parent as a git-format unified patch
/// (`jj diff --git`). The write-path stale-recovery captures this BEFORE any
/// `update-stale`/`discard` so a give-up can persist the agent's would-be-lost
/// edits to scratch — making "recoverable" true from the agent's seat, not just
/// the jj operation log. Best-effort by contract: the caller treats any error as
/// "nothing to preserve". Empty string when `@` is clean.
pub fn working_copy_diff(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(ws, &["diff", "--git"], "jj diff --git")
}

/// Stop tracking `paths` in the working copy without deleting them from disk
/// (`jj file untrack`). Used by populate's backstop to un-track a path a
/// conservative glob translation failed to keep out of the snapshot, after the
/// path has been added to `snapshot.auto-track`. No-op for an empty slice.
pub fn untrack_paths(jj: &JjEnv, ws: &Path, paths: &[String]) -> Result<(), String> {
    if paths.is_empty() {
        return Ok(());
    }
    // `jj file untrack` takes fileset args too, so quote each path literally
    // (a bare quoted string is the default "files" pattern, matching the path).
    let quoted: Vec<String> = paths.iter().map(|p| quote_fileset(p)).collect();
    let mut args: Vec<&str> = vec!["file", "untrack"];
    args.extend(quoted.iter().map(|s| s.as_str()));
    jj.run(ws, &args, "jj file untrack").map(|_| ())
}

/// List the files tracked in the workspace's working-copy commit
/// (`jj file list`), workspace-relative, one per line, sorted. This is jj's own
/// notion of the tracked-file set — exactly what the agent edits, commits, and
/// sees in a diff — so it naturally excludes the `.jj` metadata dir and
/// populate-excluded gitignored content (`.env`, `node_modules/`) while keeping
/// tracked dotfiles (`.gitignore`, `.github/`). It is the substrate for the
/// File-tab browser over a non-colocated jj workspace, which has no `.git` for
/// `git ls-files` to read.
///
/// `--ignore-working-copy` reads the last-recorded `@` without taking the
/// working-copy lock or snapshotting, so a read-only UI browse never contends
/// with the agent's own jj operations on the same workspace. The trade-off is
/// that a brand-new file not yet snapshotted into `@` won't appear until the
/// next jj operation — acceptable for a viewer, and the agent snapshots on
/// nearly every operation.
pub fn list_files(jj: &JjEnv, ws: &Path) -> Result<Vec<String>, String> {
    let out = jj.run(
        ws,
        &["file", "list", "--ignore-working-copy"],
        "jj file list",
    )?;
    let mut files: Vec<String> = out
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    files.sort();
    Ok(files)
}

/// The full commit id of `@-` (the latest sealed commit) — the jj analogue of
/// `git rev-parse HEAD`. `@` is the empty working-copy commit; `@-` is the base
/// at job creation and the latest sealed commit thereafter, so this matches git
/// HEAD semantics for `base_commit` capture and for inherited/child worktrees.
pub fn head_commit(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(
        ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
        "jj log -r @-",
    )
}

pub fn working_copy_commit(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(
        ws,
        &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
        "jj log -r @",
    )
}

/// Graph proof used only after database/path/marker ownership coordinates agree.
/// It proves the physical workspace's sealed head descends from the job's
/// recorded base; it never establishes lineage on its own.
pub fn revision_descends_from(jj: &JjEnv, store: &Path, revision: &str, ancestor: &str) -> bool {
    let revset = format!("{ancestor}::{revision} & {revision}");
    jj.run(
        store,
        &[
            "log",
            "-r",
            &revset,
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
        "jj ancestry proof",
    )
    .map(|out| !out.trim().is_empty())
    .unwrap_or(false)
}

/// The git directory backing the shared jj store. `ensure_project_store` points
/// the store's git backend at the project's existing `.git` via
/// `jj git init --git-repo`, and `jj git root` reports that path from any
/// workspace off the store. This is the bridge that lets Cairn read genuine git
/// objects (e.g. a sealed commit's tree) for content jj's template layer cannot
/// expose.
pub fn git_backend_root(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(ws, &["git", "root"], "jj git root")
}

/// Stable identity for the sealed tree content at `@-`.
///
/// Cairn's check-result cache keys verdicts by tree content so a clean
/// rebase/squash that preserves file content carries the result forward, and the
/// merge-gate baseline survives a squash that rewrites the commit id but not the
/// tree. jj's git backend makes this reachable: a sealed `commit_id` *is* a git
/// commit sha in the project's object database, so the commit's git tree object
/// is the genuine content hash — identical tree content yields an identical hash
/// regardless of message, author, parents, or timestamp. We resolve the backend
/// git dir via [`git_backend_root`] and read the commit's tree with
/// `git rev-parse <commit>^{tree}`.
///
/// jj 0.42.0 exposes no tree-id template keyword (`tree_id`, `root_tree`, and
/// `commit.tree()` all fail to parse), so the git object is the only stable
/// surface for this. If that resolution fails for any reason we fall back to the
/// sealed commit id: correctness is preserved (a stable per-commit key) at the
/// cost of cross-equivalent-tree reuse, and write-checks still run rather than
/// being skipped on a transient git hiccup.
pub fn sealed_tree_hash(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    let commit = head_commit(jj, ws)?;
    match sealed_tree_hash_via_git(jj, ws, &commit) {
        Ok(tree) => Ok(tree),
        Err(e) => {
            log::warn!(
                "sealed_tree_hash: git tree resolution failed ({e}); falling back to \
                 the sealed commit id (cross-equivalent-tree cache reuse disabled)"
            );
            Ok(commit)
        }
    }
}

/// Resolve the git tree sha of a sealed commit through the store's git backend.
/// Reads the object directly by sha (`<commit>^{tree}`), so it needs no git ref
/// — the jj git backend writes commit objects into the project's object database
/// as they are created, independent of bookmark export.
pub(crate) fn sealed_tree_hash_via_git(
    jj: &JjEnv,
    ws: &Path,
    commit: &str,
) -> Result<String, String> {
    let git_dir = git_backend_root(jj, ws)?;
    let out = bounded_command_output(
        crate::env::git().args([
            "--git-dir",
            &git_dir,
            "rev-parse",
            &format!("{commit}^{{tree}}"),
        ]),
        JJ_DEFAULT_TIMEOUT,
        "git rev-parse tree",
    )?;
    if !out.status.success() {
        return Err(format!(
            "git rev-parse tree failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let tree = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if tree.is_empty() {
        return Err("git rev-parse tree returned empty output".into());
    }
    Ok(tree)
}

/// The sealed commit's tree as flat `(path, blob_id)` entries, read through the
/// git backend. This is the substrate for per-check INPUT hashing: filtering
/// these entries by a check's impact globs and hashing the matching
/// `(path, blob_id)` pairs yields a content identity that changes iff a matching
/// file's content (or the matched path set) changes — so a check's cached verdict
/// can be keyed by just its own inputs rather than the whole tree. Entries are
/// sorted by path. Errs (so callers fall back to whole-tree keying) when the git
/// backend can't be resolved or `git ls-tree` fails.
pub fn sealed_tree_entries(jj: &JjEnv, ws: &Path) -> Result<Vec<(String, String)>, String> {
    let commit = head_commit(jj, ws)?;
    tree_entries(jj, ws, &commit)
}

/// Flat `(path, blob_id)` entries for an arbitrary commit or tree object in the
/// jj workspace's git backend. This is intentionally treeish-based so check-cache
/// consumers can compare the current sealed tree with a previously cached baseline
/// tree even when that baseline was re-stamped by another branch or node.
pub fn tree_entries(jj: &JjEnv, ws: &Path, treeish: &str) -> Result<Vec<(String, String)>, String> {
    let git_dir = git_backend_root(jj, ws)?;
    let out = bounded_command_output(
        crate::env::git().args(["--git-dir", &git_dir, "ls-tree", "-r", "-z", treeish]),
        JJ_DEFAULT_TIMEOUT,
        "git ls-tree",
    )?;
    if !out.status.success() {
        return Err(format!(
            "git ls-tree failed for {treeish}: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(parse_ls_tree(&String::from_utf8_lossy(&out.stdout)))
}

/// Parse `git ls-tree -r -z` output into sorted `(path, blob_id)` pairs. Each
/// NUL-terminated record is `<mode> SP <type> SP <object>\t<path>`; `-z` leaves
/// paths unquoted (no C-escaping), so the tab split is unambiguous. Records that
/// don't parse are skipped rather than failing the whole read. Pure, so it is
/// unit-tested.
pub(crate) fn parse_ls_tree(output: &str) -> Vec<(String, String)> {
    let mut entries: Vec<(String, String)> = output
        .split('\0')
        .filter(|record| !record.is_empty())
        .filter_map(|record| {
            let (meta, path) = record.split_once('\t')?;
            let object = meta.split_whitespace().nth(2)?;
            Some((path.to_string(), object.to_string()))
        })
        .collect();
    entries.sort();
    entries
}
