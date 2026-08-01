//! Working-copy and sealed-tree reads: dirty paths, tracked files, tree
//! hashes and entries via the git backend.
use super::*;
use std::path::Path;
/// Capture the working copy's diff vs its parent as a git-format unified patch
/// (`jj diff --git`). The write-path stale-recovery captures this BEFORE any
/// `update-stale`/`discard` so a give-up can persist the agent's would-be-lost
/// edits to scratch — making "recoverable" true from the agent's seat, not just
/// the jj operation log. Best-effort by contract: the caller treats any error as
/// "nothing to preserve". Empty string when `@` is clean.
pub(crate) fn working_copy_diff(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(ws, &["diff", "--git"], "jj diff --git")
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
/// The git directory backing the shared jj store. `ensure_project_store` points
/// the store's git backend at the project's existing `.git` via
/// `jj git init --git-repo`, and `jj git root` reports that path from any
/// workspace off the store. This is the bridge that lets Cairn read genuine git
/// objects (e.g. a sealed commit's tree) for content jj's template layer cannot
/// expose.
fn git_backend_root(jj: &JjEnv, ws: &Path) -> Result<String, String> {
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
    logical_tree_hash(jj, ws, &commit)
}

/// Stable tree identity for an explicit logical commit.
pub(crate) fn logical_tree_hash(
    jj: &JjEnv,
    repository: &Path,
    commit: &str,
) -> Result<String, String> {
    match sealed_tree_hash_via_git(jj, repository, commit) {
        Ok(tree) => Ok(tree),
        Err(e) => {
            log::warn!(
                "sealed_tree_hash: git tree resolution failed ({e}); falling back to \
                 the sealed commit id (cross-equivalent-tree cache reuse disabled)"
            );
            Ok(commit.to_string())
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
/// Flat `(path, blob_id)` entries for an arbitrary commit or tree object in the
/// jj workspace's git backend. This is intentionally treeish-based so check-cache
/// consumers can compare the current sealed tree with a previously cached baseline
/// tree even when that baseline was re-stamped by another branch or node.
pub(crate) fn tree_entries(
    jj: &JjEnv,
    ws: &Path,
    treeish: &str,
) -> Result<Vec<(String, String)>, String> {
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

/// Blob CONTENT for a set of object ids, in ONE `git cat-file --batch`.
///
/// [`tree_entries`] gives paths and object ids; check-input derivation needs the
/// bytes of a handful of them (the manifests) to build the project's dependency
/// graph without materializing a checkout. Ids are streamed on stdin and the
/// batch records are parsed back out, so reading twenty manifests costs one
/// subprocess rather than twenty. Ids the object database does not know are
/// simply absent from the result.
///
/// The child terminates on its own: `cat-file --batch` exits when stdin closes,
/// which happens as soon as the writer thread finishes the (small, fixed) id
/// list. There is no unbounded input to stall on.
pub(crate) fn read_blobs(
    jj: &JjEnv,
    ws: &Path,
    ids: &[&str],
) -> Result<std::collections::HashMap<String, Vec<u8>>, String> {
    use std::io::Write;
    use std::process::Stdio;

    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let git_dir = git_backend_root(jj, ws)?;
    let mut command = crate::env::git();
    command
        .args(["--git-dir", &git_dir, "cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|e| format!("git cat-file --batch: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("git cat-file --batch: no stdin")?;
    let request: Vec<u8> = ids
        .iter()
        .flat_map(|id| id.bytes().chain(std::iter::once(b'\n')))
        .collect();
    let writer = std::thread::spawn(move || stdin.write_all(&request));
    let out = child
        .wait_with_output()
        .map_err(|e| format!("git cat-file --batch: {e}"))?;
    let _ = writer.join();
    if !out.status.success() {
        return Err(format!(
            "git cat-file --batch failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(parse_cat_file_batch(&out.stdout))
}

/// Parse `git cat-file --batch` output. Each record is
/// `<oid> SP <type> SP <size> LF <content> LF`, or `<name> SP missing LF` for an
/// id the object database does not hold. Content is binary and length-delimited
/// by the declared size, so it is never scanned for line structure. A record
/// that does not parse ends the scan rather than desynchronizing the rest.
pub(crate) fn parse_cat_file_batch(output: &[u8]) -> std::collections::HashMap<String, Vec<u8>> {
    let mut blobs = std::collections::HashMap::new();
    let mut cursor = 0usize;
    while cursor < output.len() {
        let Some(newline) = output[cursor..].iter().position(|byte| *byte == b'\n') else {
            break;
        };
        let header = String::from_utf8_lossy(&output[cursor..cursor + newline]).to_string();
        cursor += newline + 1;
        let mut fields = header.split_whitespace();
        let (Some(oid), Some(kind), Some(size)) = (fields.next(), fields.next(), fields.next())
        else {
            // `<name> missing` and anything else header-shaped carries no body.
            continue;
        };
        let Ok(size) = size.parse::<usize>() else {
            break;
        };
        if cursor + size > output.len() {
            break;
        }
        if kind == "blob" {
            blobs.insert(oid.to_string(), output[cursor..cursor + size].to_vec());
        }
        // The batch record ends with a newline after the body.
        cursor += size + 1;
    }
    blobs
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
