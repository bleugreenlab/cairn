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
pub fn working_copy_diff(jj: &JjEnv, ws: &Path) -> Result<String, String> {
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
/// The Git object store reachable from a repository coordinate.
///
/// Durable project coordinates can name either the runner's ordinary Git
/// checkout or one of its jj stores. A jj coordinate is authoritative about its
/// backing object database: the store may live below an unrelated Git checkout,
/// in which case `git rev-parse` would successfully discover the ancestor's
/// object database while silently choosing the wrong store. Plain-Git
/// coordinates remain independent of jj.
///
/// The jj case reads the backend from disk ([`backend_git_dir`]) rather than
/// asking `jj git root`: that command loads the repository at its head, so it
/// would snapshot a working copy and merge a split operation log on what every
/// caller here treats as a read.
pub(crate) fn git_backend_root(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    if is_jj_dir(ws) {
        return backend_git_dir(ws)
            .map(|dir| dir.to_string_lossy().into_owned())
            .ok_or_else(|| {
                format!(
                    "{} is a jj workspace, but its Git backend could not be read from \
                     .jj/repo/store",
                    ws.display()
                )
            });
    }
    let out = jj.command_output(
        cairn_platform::env::git()
            .args(["rev-parse", "--absolute-git-dir"])
            .current_dir(ws),
        JJ_DEFAULT_TIMEOUT,
        "git rev-parse --absolute-git-dir",
    )?;
    if out.status.success() {
        let git_dir = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !git_dir.is_empty() {
            return Ok(git_dir);
        }
    }
    Err(format!(
        "git rev-parse --absolute-git-dir failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    ))
}

/// The jj repository directory `ws/.jj/repo` names: that directory itself in a
/// store's own workspace, or the target of the pointer file a secondary
/// workspace carries there (a relative target is relative to `.jj`).
pub(crate) fn jj_repo_dir(ws: &Path) -> Option<std::path::PathBuf> {
    let pointer = ws.join(".jj").join("repo");
    if pointer.is_dir() {
        return Some(pointer);
    }
    let target = std::path::PathBuf::from(std::fs::read_to_string(&pointer).ok()?.trim());
    Some(if target.is_absolute() {
        target
    } else {
        pointer.parent()?.join(target)
    })
}

/// The Git directory backing the jj repository at `ws`, read from its on-disk
/// layout: `store/git_target` for an external backend (a relative target is
/// relative to `store`), else the `store/git` jj keeps internally. This is the
/// directory `jj git root` reports, found without loading the repository.
pub(crate) fn backend_git_dir(ws: &Path) -> Option<std::path::PathBuf> {
    let store = jj_repo_dir(ws)?.join("store");
    let git_dir = match std::fs::read_to_string(store.join("git_target")) {
        Ok(raw) => {
            let target = std::path::PathBuf::from(raw.trim());
            if target.is_absolute() {
                target
            } else {
                store.join(target)
            }
        }
        Err(_) => store.join("git"),
    };
    git_dir.is_dir().then_some(git_dir)
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
/// surface for this. Failure is fatal: a commit id is not a tree id, and recording
/// it as one would make the observation's content claim false.
pub fn sealed_tree_hash(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    let commit = head_commit(jj, ws)?;
    logical_tree_hash(jj, ws, &commit)
}

/// Stable tree identity for an explicit logical commit.
pub fn logical_tree_hash(jj: &JjEnv, repository: &Path, commit: &str) -> Result<String, String> {
    sealed_tree_hash_via_git(jj, repository, commit).map_err(|error| {
        format!(
            "cannot verify the content tree for requested revision {commit}; refusing to record check evidence: {error}"
        )
    })
}

/// Resolve the git tree sha of a sealed commit through the store's git backend.
/// Reads the object directly by sha (`<commit>^{tree}`), so it needs no git ref
/// — the jj git backend writes commit objects into the project's object database
/// as they are created, independent of bookmark export.
pub fn sealed_tree_hash_via_git(jj: &JjEnv, ws: &Path, commit: &str) -> Result<String, String> {
    let git_dir = git_backend_root(jj, ws)?;
    let out = jj.command_output(
        cairn_platform::env::git().args([
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
pub fn tree_entries(jj: &JjEnv, ws: &Path, treeish: &str) -> Result<Vec<(String, String)>, String> {
    let git_dir = git_backend_root(jj, ws)?;
    let out = jj.command_output(
        cairn_platform::env::git().args(["--git-dir", &git_dir, "ls-tree", "-r", "-z", treeish]),
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

/// A scoped, read-only Git object reader for named paths at immutable commits.
/// Opens the authoritative jj backend or ordinary Git repository once, sharing
/// object-database initialization across reads without retaining a global cache.
pub struct ObjectReader {
    repo: gix::Repository,
}

impl ObjectReader {
    pub fn open(repository: &Path) -> Result<Self, String> {
        let mut repo = if is_jj_dir(repository) {
            let git_dir = backend_git_dir(repository).ok_or_else(|| {
                format!("{} has an unreadable jj Git backend", repository.display())
            })?;
            gix::open(git_dir).map_err(|error| format!("open jj object store: {error}"))?
        } else {
            gix::discover(repository).map_err(|error| format!("discover object store: {error}"))?
        };
        // Immutable identities must not be redirected by user replacement refs.
        repo.objects.ignore_replacements = true;
        Ok(Self { repo })
    }

    /// Resolve only named repository-relative paths at a full immutable commit
    /// ID. Missing paths are `None`; invalid commits, unreadable trees, and
    /// non-blob entries are errors. Does not list the full tree.
    pub fn named_path_blob_ids(
        &self,
        commit: &str,
        paths: &[&str],
    ) -> Result<Vec<Option<String>>, String> {
        let tree = self.commit_tree(commit)?;
        paths
            .iter()
            .map(|path| {
                named_blob_entry(&tree, path).map(|entry| entry.map(|entry| entry.id().to_string()))
            })
            .collect()
    }

    /// Read one file in process. Only an absent path returns `None`; invalid
    /// commits and unreadable trees or blobs remain errors.
    pub fn read_commit_file(&self, commit: &str, path: &str) -> Result<Option<Vec<u8>>, String> {
        let tree = self.commit_tree(commit)?;
        named_blob_entry(&tree, path)?
            .map(|entry| {
                let object = entry
                    .object()
                    .map_err(|error| format!("read blob for {path}: {error}"))?;
                let blob = object
                    .try_into_blob()
                    .map_err(|error| format!("read blob for {path}: {error}"))?;
                Ok(blob.data.to_vec())
            })
            .transpose()
    }

    fn commit_tree(&self, commit: &str) -> Result<gix::Tree<'_>, String> {
        let id = gix::ObjectId::from_hex(commit.as_bytes())
            .map_err(|error| format!("invalid full commit ID {commit:?}: {error}"))?;
        let object = self
            .repo
            .find_object(id)
            .map_err(|error| format!("read commit {commit}: {error}"))?;
        let commit_object = object
            .try_into_commit()
            .map_err(|error| format!("read commit {commit}: {error}"))?;
        commit_object
            .tree()
            .map_err(|error| format!("read tree of {commit}: {error}"))
    }
}

fn named_blob_entry<'repo>(
    tree: &gix::Tree<'repo>,
    path: &str,
) -> Result<Option<gix::object::tree::Entry<'repo>>, String> {
    if path.is_empty()
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!("invalid repository-relative path {path:?}"));
    }
    let mut subtree = None;
    let mut components = path.split('/').peekable();
    let entry = loop {
        let component = components.next().expect("nonempty path");
        let current = subtree.as_ref().unwrap_or(tree);
        let mut found = None;
        for entry in current.iter() {
            let entry = entry.map_err(|error| format!("decode tree path {path}: {error}"))?;
            if entry.filename() == component.as_bytes() {
                found = Some(entry.to_owned());
                break;
            }
        }
        let Some(entry) = found else {
            return Ok(None);
        };
        if components.peek().is_none() {
            break Some(entry);
        }
        if !entry.mode().is_tree() {
            return Ok(None);
        }
        subtree = Some(
            entry
                .object()
                .map_err(|error| format!("read tree path {path}: {error}"))?
                .try_into_tree()
                .map_err(|error| format!("read tree path {path}: {error}"))?,
        );
    };
    if entry
        .as_ref()
        .is_some_and(|entry| !entry.mode().is_blob() && !entry.mode().is_link())
    {
        return Err(format!("tree path {path} is not a blob"));
    }
    Ok(entry)
}

/// The change a branch carries on top of an integration target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnPatch {
    /// The branch's fork point: the merge base of its head with the target.
    pub base: String,
    /// The branch's change from `base` to its head, as [`normalize_own_patch`]
    /// renders it.
    pub patch: String,
}

/// The change `head` carries on top of `target`, measured from their merge base.
///
/// Two heads with equal [`OwnPatch::patch`] carry the same change, whatever
/// their bases: a replay onto an advanced base that applied cleanly and moved
/// nothing the branch touched compares equal to the tip it replaced.
///
/// Read through the store's git object database rather than jj, because the
/// heads this compares include a branch's pre-replay tip, which jj hides once a
/// base advance rewrites it while the object database still holds it. Every
/// option that shapes the diff text is pinned on the command line, so two
/// readings never disagree because a user's git config chose a different diff
/// algorithm, context width, or path prefix.
pub fn own_patch_against(
    jj: &JjEnv,
    repository: &Path,
    head: &str,
    target: &str,
) -> Result<OwnPatch, String> {
    let git_dir = git_backend_root(jj, repository)?;
    let out = jj.command_output(
        cairn_platform::env::git().args(["--git-dir", &git_dir, "merge-base", head, target]),
        JJ_DEFAULT_TIMEOUT,
        "git merge-base",
    )?;
    let base = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !out.status.success() || base.is_empty() {
        return Err(format!(
            "git merge-base {head} {target} found no fork point: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let out = jj.command_output(
        cairn_platform::env::git().args([
            "--git-dir",
            &git_dir,
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--binary",
            "--diff-algorithm=myers",
            "--unified=3",
            "--inter-hunk-context=0",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            &base,
            head,
        ]),
        JJ_DEFAULT_TIMEOUT,
        "git diff (own patch)",
    )?;
    if !out.status.success() {
        return Err(format!(
            "git diff {base} {head} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(OwnPatch {
        base,
        patch: normalize_own_patch(&String::from_utf8_lossy(&out.stdout)),
    })
}

/// A git patch with everything that depends on the base rather than on the
/// change removed: `index` lines (whose pre-image blob ids move whenever the
/// base edits the same file anywhere) and hunk-header positions (which move
/// whenever the base adds or removes lines above a hunk). Every added, removed,
/// and context line stays, so a base edit close enough to show in a hunk's
/// context still reads as a different patch.
///
/// Neither shape can be confused with content: inside a hunk every line begins
/// with ` `, `+`, `-`, or `\`, and a `--binary` literal line begins with its
/// length character and holds no space.
pub fn normalize_own_patch(patch: &str) -> String {
    let mut out = String::with_capacity(patch.len());
    for line in patch.lines() {
        if line.starts_with("index ") {
            continue;
        }
        out.push_str(if line.starts_with("@@") { "@@" } else { line });
        out.push('\n');
    }
    out
}

/// One line of a commit's tree that a [`grep_fixed_at`] needle matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepHit {
    pub path: String,
    /// One-based.
    pub line: u32,
    pub text: String,
}

/// The lines of `commit`'s tree, limited to `pathspecs`, that contain any of
/// `needles` as a fixed string — one `git grep` over the backing object
/// database, so no checkout of `commit` is needed. Binary files are skipped.
pub fn grep_fixed_at(
    jj: &JjEnv,
    ws: &Path,
    commit: &str,
    needles: &[String],
    pathspecs: &[String],
) -> Result<Vec<GrepHit>, String> {
    if needles.is_empty() {
        return Ok(Vec::new());
    }
    let git_dir = git_backend_root(jj, ws)?;
    let mut command = cairn_platform::env::git();
    command.args([
        "--git-dir",
        &git_dir,
        "grep",
        "-n",
        "-z",
        "-I",
        "-F",
        "--no-color",
    ]);
    for needle in needles {
        command.args(["-e", needle]);
    }
    command.arg(commit).arg("--").args(pathspecs);
    let out = jj.command_output(&mut command, JJ_DEFAULT_TIMEOUT, "git grep at revision")?;
    match out.status.code() {
        Some(0) => Ok(parse_grep_z(&String::from_utf8_lossy(&out.stdout), commit)),
        // git grep's "nothing matched".
        Some(1) => Ok(Vec::new()),
        _ => Err(format!(
            "git grep at {commit} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )),
    }
}

/// Parse `git grep -n -z <commit>` output. Each record is
/// `<commit>:<path> NUL <line> NUL <text> LF`; `-z` leaves the path unquoted, and
/// a matched line cannot itself hold a newline, so the record split is exact.
pub(crate) fn parse_grep_z(output: &str, commit: &str) -> Vec<GrepHit> {
    let prefix = format!("{commit}:");
    output
        .split('\n')
        .filter_map(|record| {
            let mut fields = record.splitn(3, '\0');
            let (path, line, text) = (fields.next()?, fields.next()?, fields.next()?);
            Some(GrepHit {
                path: path.strip_prefix(&prefix).unwrap_or(path).to_string(),
                line: line.parse().ok()?,
                text: text.to_string(),
            })
        })
        .collect()
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
/// The fixed request is supplied through a temporary file, so the bounded
/// subprocess driver owns the whole child lifecycle without a blocked stdin
/// writer surviving cancellation.
pub fn read_blobs(
    jj: &JjEnv,
    ws: &Path,
    ids: &[&str],
) -> Result<std::collections::HashMap<String, Vec<u8>>, String> {
    use std::io::{Seek, Write};
    use std::process::Stdio;

    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let git_dir = git_backend_root(jj, ws)?;
    let mut request =
        tempfile::tempfile().map_err(|error| format!("git cat-file --batch request: {error}"))?;
    for id in ids {
        writeln!(request, "{id}")
            .map_err(|error| format!("git cat-file --batch request: {error}"))?;
    }
    request
        .rewind()
        .map_err(|error| format!("git cat-file --batch request: {error}"))?;
    let out = jj.command_output(
        cairn_platform::env::git()
            .args([
                "--no-replace-objects",
                "--git-dir",
                &git_dir,
                "cat-file",
                "--batch",
            ])
            .stdin(Stdio::from(request)),
        JJ_DEFAULT_TIMEOUT,
        "git cat-file --batch",
    )?;
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

#[cfg(test)]
mod named_path_tests {
    use super::*;

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = cairn_platform::env::git()
            .current_dir(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    #[test]
    fn named_paths_distinguish_absence_and_invalid_commit_and_honor_jj_backend() {
        let ancestor = tempfile::tempdir().unwrap();
        git(ancestor.path(), &["init", "-b", "main"]);
        let backend = tempfile::tempdir().unwrap();
        git(backend.path(), &["init", "-b", "main"]);
        std::fs::create_dir(backend.path().join("nested")).unwrap();
        std::fs::write(backend.path().join("nested/policy"), b"committed").unwrap();
        git(backend.path(), &["add", "."]);
        git(
            backend.path(),
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "fixture",
            ],
        );
        let commit = git(backend.path(), &["rev-parse", "HEAD"]);
        let blob = git(backend.path(), &["rev-parse", "HEAD:nested/policy"]);
        let workspace = ancestor.path().join("store");
        let store = workspace.join(".jj/repo/store");
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(
            store.join("git_target"),
            backend.path().join(".git").to_str().unwrap(),
        )
        .unwrap();

        std::fs::write(backend.path().join("replacement"), b"replacement").unwrap();
        let replacement = git(backend.path(), &["hash-object", "-w", "replacement"]);
        git(backend.path(), &["replace", &blob, &replacement]);
        assert_eq!(
            git(backend.path(), &["cat-file", "blob", &blob]),
            "replacement"
        );
        let jj = JjEnv::resolve("", ancestor.path());
        for repo in [backend.path(), workspace.as_path()] {
            assert_eq!(
                read_blobs(&jj, repo, &[&blob]).unwrap()[&blob],
                b"committed"
            );
            let objects = ObjectReader::open(repo).unwrap();
            let corrupt = gix::Tree::from_data(
                gix::ObjectId::from_hex(blob.as_bytes()).unwrap(),
                b"invalid tree".to_vec(),
                &objects.repo,
            );
            assert!(named_blob_entry(&corrupt, "absent").is_err());
            assert_eq!(
                objects
                    .named_path_blob_ids(&commit, &["nested/policy", "absent"])
                    .unwrap(),
                vec![Some(blob.clone()), None]
            );
            assert_eq!(
                objects.read_commit_file(&commit, "nested/policy").unwrap(),
                Some(b"committed".to_vec())
            );
            assert_eq!(objects.read_commit_file(&commit, "absent").unwrap(), None);
            assert!(objects
                .named_path_blob_ids(&"f".repeat(40), &["absent"])
                .is_err());
            assert!(objects.read_commit_file("HEAD", "absent").is_err());
            assert!(objects.named_path_blob_ids(&blob, &["absent"]).is_err());
            assert!(objects.read_commit_file(&commit, "nested").is_err());
        }
        std::fs::write(store.join("git_target"), "missing-backend").unwrap();
        assert!(ObjectReader::open(&workspace).is_err());
    }
}
