//! Jujutsu (jj) driver: the all-jj VCS substrate for agent worktrees.
//!
//! Cairn provisions **one shared jj store per jj-managed project** (a single
//! commit graph and operation log), backed by the project's existing `.git` so
//! commits stay in the project's object database (pushable, readable by git
//! tooling against the project) and the user's working checkout is never
//! touched. Each job's working directory is a `jj workspace` off that one store:
//! physically isolated files over one shared graph, which is what gives
//! cross-sibling auto-rebase, the entire reason to move off git.
//!
//! Workspaces created by `jj workspace add` are non-colocated: a workspace dir
//! carries a `.jj` and **no `.git`**. Branch-keyed tooling cannot read the git
//! branch inside such a dir, so Cairn records the real branch in a marker that is
//! invisible to the working-copy commit (`<workspace>/.jj/cairn-branch` — jj
//! never snapshots its own metadata dir) and `resolveBranch` reads it. See
//! `docs/jj-migration.md`.
//!
//! jj opens `$EDITOR` for `describe`/`commit`/`squash` and writes user config
//! under `~/.config/jj` unless redirected; every command here forces
//! `EDITOR=true`/`JJ_EDITOR=true` and points `JJ_CONFIG` at a Cairn-managed file.
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use crate::mcp::git::{CommitResult, GitAuthor};

/// Filename of the non-snapshotted branch marker inside a workspace's `.jj` dir.
const BRANCH_MARKER: &str = "cairn-branch";

/// Filename of the non-snapshotted base marker inside a workspace's `.jj` dir.
/// Records the integration base (branch name + resolved SHA) so in-fence check
/// tooling can diff the agent's own commits against the base it branched from —
/// the worktree otherwise has no on-disk record of its base (jj ancestry cannot
/// tell the base apart from siblings that coincide at the branch point). See
/// `scripts/lib/check-base.ts` and `docs/check-harness.md`.
const BASE_MARKER: &str = "cairn-base";

/// Fallback identity used when no per-call author is supplied. Per-commit author
/// is injected via `--config user.{name,email}=…` on each seal.
const JJ_DEFAULT_USER_NAME: &str = "Cairn Agent";
const JJ_DEFAULT_USER_EMAIL: &str = "agent@cairn.local";

/// Drives a bundled, non-interactive `jj` binary.
pub struct JjEnv {
    bin: String,
    config_path: PathBuf,
}

#[cfg(test)]
fn jj_subprocess_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl JjEnv {
    /// Resolve the jj binary and the managed config path. Binary precedence:
    /// `CAIRN_JJ_BIN` (test/override) → the bundled sidecar path → PATH `jj`.
    pub fn resolve(bundled_bin: &str, config_dir: &Path) -> Self {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| Self::resolve_bundled_or_path(bundled_bin));
        Self {
            bin,
            config_path: config_dir.join("jj").join("config.toml"),
        }
    }

    fn resolve_bundled_or_path(bundled_bin: &str) -> String {
        let bundled_bin = bundled_bin.trim();
        if bundled_bin.is_empty() {
            return "jj".to_string();
        }

        match crate::env::command(bundled_bin).arg("--version").output() {
            Ok(output) if output.status.success() => bundled_bin.to_string(),
            Ok(output) => {
                log::warn!(
                    "Bundled jj at `{bundled_bin}` failed --version with status {}; falling back to PATH jj",
                    output.status
                );
                "jj".to_string()
            }
            Err(error) => {
                log::warn!(
                    "Bundled jj at `{bundled_bin}` could not be spawned ({error}); falling back to PATH jj"
                );
                "jj".to_string()
            }
        }
    }

    /// Write the managed jj config once if absent (never clobbers user edits).
    fn ensure_config(&self) {
        if self.config_path.exists() {
            return;
        }
        if let Some(parent) = self.config_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                log::warn!("Failed to create jj config dir {:?}: {e}", parent);
                return;
            }
        }
        let body = format!(
            "ui.paginate = \"never\"\n[user]\nname = \"{JJ_DEFAULT_USER_NAME}\"\nemail = \"{JJ_DEFAULT_USER_EMAIL}\"\n"
        );
        if let Err(e) = std::fs::write(&self.config_path, body) {
            log::warn!("Failed to write jj config {:?}: {e}", self.config_path);
        }
    }

    /// A `jj` command rooted at `cwd`, wired for non-interactive use.
    fn cmd(&self, cwd: &Path) -> Command {
        self.ensure_config();
        let mut c = crate::env::command(&self.bin);
        c.current_dir(cwd)
            .env("JJ_CONFIG", &self.config_path)
            .env("EDITOR", "true")
            .env("JJ_EDITOR", "true");
        c
    }

    /// The env a bare `jj` shell command needs to behave like a managed
    /// [`JjEnv::cmd`] invocation: the Cairn-managed config path and a
    /// non-interactive editor. Exactly the env `cmd` injects, so a bare `jj` run
    /// through the run tool is byte-identical to managed jj (same managed
    /// fallback identity, same non-interactive editor) instead of writing
    /// unpushable empty-committer commits. Ensures the managed config file exists
    /// first, mirroring `cmd`, so `JJ_CONFIG` never points at a missing file.
    pub fn shell_env(&self) -> Vec<(String, String)> {
        self.ensure_config();
        vec![
            (
                "JJ_CONFIG".into(),
                self.config_path.to_string_lossy().into_owned(),
            ),
            ("EDITOR".into(), "true".into()),
            ("JJ_EDITOR".into(), "true".into()),
        ]
    }

    /// Per-call author override as repeated global `--config user.{name,email}=…`
    /// args (placed before the subcommand). jj fixes a commit's author when its
    /// working-copy commit is created, so passing this on every seal keeps a
    /// workspace's sealed commits authored consistently.
    fn author_args(author: Option<&GitAuthor>) -> Vec<String> {
        match author {
            Some(a) => vec![
                "--config".into(),
                format!("user.name={}", a.name),
                "--config".into(),
                format!("user.email={}", a.email),
            ],
            None => Vec::new(),
        }
    }

    /// Run a jj command, returning raw stdout bytes or a contextual error.
    fn run_bytes(&self, cwd: &Path, args: &[&str], ctx: &str) -> Result<Vec<u8>, String> {
        #[cfg(test)]
        let _guard = jj_subprocess_lock()
            .lock()
            .expect("jj subprocess test lock poisoned");

        let out = self
            .cmd(cwd)
            .args(args)
            .output()
            .map_err(|e| format!("{ctx}: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "{ctx} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(out.stdout)
    }

    /// Run a jj command, returning trimmed stdout or a contextual error.
    fn run(&self, cwd: &Path, args: &[&str], ctx: &str) -> Result<String, String> {
        let out = self.run_bytes(cwd, args, ctx)?;
        Ok(String::from_utf8_lossy(&out).trim().to_string())
    }
}

/// Whether `dir` is a jj repo/workspace root (carries a `.jj`). The ground-truth
/// signal the commit barrier dispatches on.
pub fn is_jj_dir(dir: &Path) -> bool {
    dir.join(".jj").is_dir()
}

/// Read a file's bytes from `rev` without consulting or snapshotting the working
/// copy. `path` is a repo-relative path (or fileset expression understood by jj).
pub fn file_show(jj: &JjEnv, cwd: &Path, rev: &str, path: &str) -> Result<Vec<u8>, String> {
    jj.run_bytes(
        cwd,
        &["file", "show", "-r", rev, "--ignore-working-copy", path],
        "jj file show",
    )
}

/// List repo-relative files visible at `rev`, optionally scoped to `path`.
pub fn file_list(jj: &JjEnv, cwd: &Path, rev: &str, path: &str) -> Result<Vec<String>, String> {
    let mut args = vec!["file", "list", "-r", rev, "--ignore-working-copy"];
    if !path.is_empty() {
        args.push(path);
    }
    Ok(jj
        .run(cwd, &args, "jj file list")?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// The shared jj store directory for a project, under the Cairn home. One store
/// per project repo, named from the repo basename plus a short hash of its
/// absolute path so distinct repos that share a basename never collide.
pub fn project_store_dir(config_dir: &Path, repo_path: &Path) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let base = repo_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("project");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    repo_path.to_string_lossy().hash(&mut hasher);
    config_dir
        .join("jj-stores")
        .join(format!("{base}-{:016x}", hasher.finish()))
}

/// Create the shared per-project jj store if absent: a Cairn-managed jj repo
/// whose git backend is the project's existing `.git`. The user's checkout is
/// never touched and sealed commits land in the project's object database.
pub fn ensure_project_store(
    jj: &JjEnv,
    store_dir: &Path,
    project_repo: &Path,
) -> Result<(), String> {
    if !is_jj_dir(store_dir) {
        if let Some(parent) = store_dir.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create jj store parent dir: {e}"))?;
        }
        let cwd = store_dir.parent().unwrap_or(store_dir);
        jj.run(
            cwd,
            &[
                "git",
                "init",
                "--git-repo",
                &project_repo.to_string_lossy(),
                &store_dir.to_string_lossy(),
            ],
            "jj git init --git-repo",
        )?;
    }
    // Always sync the backing git repo into the store. `jj git init` imports on
    // creation, but an already-existing store is otherwise frozen at the refs it
    // last saw: a base ref that advanced since then would not resolve when adding
    // a new workspace (`Revision <sha> doesn't exist`), so every later job on a
    // jj-managed project would fail to provision once the project git moved.
    import_git(jj, store_dir)?;
    Ok(())
}

/// Import the backing git repo's refs and commits into the shared store, so a
/// base ref that advanced since the store was created resolves.
pub fn import_git(jj: &JjEnv, store_dir: &Path) -> Result<(), String> {
    jj.run(store_dir, &["git", "import"], "jj git import")
        .map(|_| ())
}

/// Fetch a remote into the shared store, advancing its remote-tracking bookmarks
/// (`<branch>@<remote>`) to the remote's current tips. Used to bring an
/// externally-advanced default branch into the store independent of the project
/// checkout's branch, so a sibling can rebase onto `<default>@origin`. Mirrors
/// `import_git`: a one-liner over the store's backing git.
pub fn fetch_remote(jj: &JjEnv, store_dir: &Path, remote: &str) -> Result<(), String> {
    jj.run(
        store_dir,
        &["git", "fetch", "--remote", remote],
        "jj git fetch",
    )
    .map(|_| ())
}

/// Wrap a repo-relative path as a jj fileset string literal so paths containing
/// fileset metacharacters — `(` `)` `|` `&` `~` `:`, whitespace, etc. (e.g. a
/// Next.js `(app)` route-group directory) — are matched literally instead of
/// being parsed as a fileset expression. jj positional path arguments to
/// `commit`/`squash`/`file untrack` are fileset expressions, not literal paths,
/// so an unquoted `(app)` is read as a grouping operator and the parse fails.
/// jj double-quoted strings use backslash escaping, so `\` and `"` are escaped.
fn quote_fileset(path: &str) -> String {
    let escaped = path.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Translate one populate glob pattern into jj `snapshot.auto-track` exclude
/// filesets. Populate matches with `literal_separator(false)` (so `*` crosses
/// `/`) against repo-relative paths; the jj exclusion must be at least as broad,
/// so each pattern is anchored with a leading `**/` to match at any depth
/// (over-exclusion is safe — it only keeps more new files untracked). A trailing
/// slash marks a directory: exclude both its subtree (`**/<dir>/**`) and the
/// entry itself (`**/<dir>`, e.g. a symlinked dir, which appears as one path).
fn populate_pattern_filesets(pattern: &str) -> Vec<String> {
    let is_dir = pattern.ends_with('/');
    let body = pattern.trim_end_matches('/');
    if body.is_empty() {
        return Vec::new();
    }
    if is_dir {
        vec![
            format!("glob:\"**/{body}/**\""),
            format!("glob:\"**/{body}\""),
        ]
    } else {
        vec![format!("glob:\"**/{body}\"")]
    }
}

/// Build the `snapshot.auto-track` revset that tracks everything EXCEPT the
/// populate-matched paths (plus any `extra_paths` exact paths fed back by the
/// security backstop after a glob-translation miss). Returns `None` when there
/// is nothing to exclude, so the caller leaves jj's `all()` default untouched.
pub(crate) fn populate_auto_track_expr(
    config: &crate::config::project_settings::PopulateConfig,
    extra_paths: &[String],
) -> Option<String> {
    let mut filesets: Vec<String> = Vec::new();
    for pattern in config.copy.iter().chain(config.symlink.iter()) {
        filesets.extend(populate_pattern_filesets(pattern));
    }
    for path in extra_paths {
        let trimmed = path.trim_matches('/');
        if !trimmed.is_empty() {
            filesets.push(quote_fileset(trimmed));
        }
    }
    if filesets.is_empty() {
        return None;
    }
    Some(format!("all() ~ ({})", filesets.join(" | ")))
}

/// Set the shared store's `snapshot.auto-track` so jj's working-copy snapshot
/// never auto-tracks explicitly-populated gitignored content. Repo-scoped
/// (`--repo`), so it applies to every workspace over the store and is idempotent
/// under concurrent job provisioning. MUST run before populate copies files in:
/// jj auto-tracks a new file on the first snapshot after it appears, and a later
/// rule cannot un-track it. `extra_paths` lets the backstop extend the exclusion
/// with exact leaked paths. No-op when there is nothing to exclude.
pub fn set_populate_auto_track(
    jj: &JjEnv,
    store_dir: &Path,
    config: &crate::config::project_settings::PopulateConfig,
    extra_paths: &[String],
) -> Result<(), String> {
    let Some(expr) = populate_auto_track_expr(config, extra_paths) else {
        return Ok(());
    };
    jj.run(
        store_dir,
        &["config", "set", "--repo", "snapshot.auto-track", &expr],
        "jj config set snapshot.auto-track",
    )
    .map(|_| ())
}

/// jj workspace names cannot contain `/`; map a git branch to a stable name.
pub fn workspace_name_for_branch(branch: &str) -> String {
    branch.replace('/', "-")
}

/// Add a job workspace off the shared store at `ws_path`, basing its working
/// copy on `base_rev`, and record the real branch in the marker.
pub fn add_workspace(
    jj: &JjEnv,
    store_dir: &Path,
    ws_path: &Path,
    branch: &str,
    base_rev: &str,
    author: Option<&GitAuthor>,
) -> Result<(), String> {
    let name = workspace_name_for_branch(branch);

    // Idempotency for a retried job. A failed `jj workspace add` registers the
    // workspace name in the store and writes a `.jj` dir *before* it resolves
    // `-r`, so a naive retry hits `Workspace named X already exists` /
    // `Destination path exists`. Forget any stale registration (a no-op when
    // absent) and clear a stale workspace dir so the add below starts clean.
    let _ = forget_workspace(jj, store_dir, branch);
    if ws_path.join(".jj").exists() {
        std::fs::remove_dir_all(ws_path).map_err(|e| format!("clear stale workspace dir: {e}"))?;
    }

    let mut args: Vec<String> = JjEnv::author_args(author);
    args.extend([
        "workspace".into(),
        "add".into(),
        "--name".into(),
        name,
        "-r".into(),
        base_rev.into(),
        ws_path.to_string_lossy().to_string(),
    ]);
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    jj.run(store_dir, &argref, "jj workspace add")?;
    write_branch_marker(ws_path, branch)?;

    // Ensure the workspace's branch is a resolvable, pushable bookmark from
    // creation — git parity, where a worktree's branch ref exists immediately.
    // A Coordinator never seals (seal is the only other place a bookmark is
    // created), so without this its integration bookmark would never exist and a
    // child's `jj workspace add -r <integration-branch>` could not resolve the
    // revision (it also leaves `ensure_bookmark_on_origin` nothing to publish).
    // Create only if absent: `bookmark create` errors when the name already
    // exists and a retried job must not fail on that, while `bookmark set` is
    // wrong here because it refuses backwards/sideways moves.
    if bookmark_commit(jj, store_dir, branch).is_none() {
        jj.run(
            store_dir,
            &["bookmark", "create", branch, "-r", base_rev],
            "jj bookmark create",
        )?;
    }
    Ok(())
}

/// Whether `rev` resolves to a commit in the shared store (any revset: a
/// bookmark, commit id, or `root()`). Lets a base ref that is not a project git
/// ref (an unsealed coordinator bookmark, which lives only in the shared store)
/// still be handed to `jj workspace add`.
pub fn revset_resolves(jj: &JjEnv, store: &Path, rev: &str) -> bool {
    jj.run(
        store,
        &["log", "-r", rev, "--no-graph", "-T", "commit_id"],
        "jj log resolve",
    )
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// Resolve a base ref to a revision `jj workspace add -r` / `bookmark create -r`
/// can always resolve in the shared store, so provisioning never fails with
/// `Revision <x> doesn't exist`. The ladder, in order:
///
/// 1. `git_rev_parse(base_ref)` -> commit SHA (the common path; the store's git
///    backend is the project `.git`, so the SHA resolves directly in the store).
/// 2. Else, if `base_ref` already resolves in the store as a revset (an unsealed
///    coordinator bookmark is a store bookmark, not a project git ref) -> keep
///    it literal. This probe MUST come before the HEAD fallback, or a
///    coordinator branch would be silently re-based onto the default tip.
/// 3. Else, `git_rev_parse("HEAD")` -> the repo's current tip (a local-only repo
///    whose configured default branch name has no matching ref, but which has
///    commits, bases off its real tip — git parity).
/// 4. Else (unborn / empty repo, no `HEAD`) -> `root()`, jj's always-present
///    root commit.
///
/// `git_rev_parse` returns the trimmed SHA for a ref the project git resolves,
/// or `None`. Kept as a closure so the orchestration layer owns the git service
/// and this stays unit-testable with the jj test harness.
pub fn resolve_base_rev<F>(jj: &JjEnv, store: &Path, base_ref: &str, git_rev_parse: F) -> String
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(sha) = git_rev_parse(base_ref).filter(|s| !s.trim().is_empty()) {
        return sha.trim().to_string();
    }
    if revset_resolves(jj, store, base_ref) {
        return base_ref.to_string();
    }
    if let Some(sha) = git_rev_parse("HEAD").filter(|s| !s.trim().is_empty()) {
        return sha.trim().to_string();
    }
    "root()".to_string()
}

/// Forget a job workspace from the shared store (teardown). The directory itself
/// is removed by the caller.
pub fn forget_workspace(jj: &JjEnv, store_dir: &Path, branch: &str) -> Result<(), String> {
    let name = workspace_name_for_branch(branch);
    jj.run(
        store_dir,
        &["workspace", "forget", &name],
        "jj workspace forget",
    )
    .map(|_| ())
}

/// Record the real git branch in the workspace's non-snapshotted marker.
pub fn write_branch_marker(ws_path: &Path, branch: &str) -> Result<(), String> {
    let p = ws_path.join(".jj").join(BRANCH_MARKER);
    std::fs::write(&p, format!("{branch}\n")).map_err(|e| format!("write branch marker: {e}"))
}

/// Read the workspace's branch marker, if present.
pub fn read_branch_marker(ws_path: &Path) -> Option<String> {
    std::fs::read_to_string(ws_path.join(".jj").join(BRANCH_MARKER))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Record the integration base in the workspace's non-snapshotted marker: the
/// base branch name on line 1 (it auto-advances with the integration tip, so a
/// branch-keyed changed-file diff stays correct as the base moves) and the
/// resolved base SHA on line 2 (a stable cache key for a future baseline). The
/// `.jj` dir is never snapshotted, so the marker is invisible to the working
/// copy commit — like [`write_branch_marker`].
pub fn write_base_marker(ws_path: &Path, base_branch: &str, base_rev: &str) -> Result<(), String> {
    let p = ws_path.join(".jj").join(BASE_MARKER);
    std::fs::write(&p, format!("{base_branch}\n{base_rev}\n"))
        .map_err(|e| format!("write base marker: {e}"))
}

/// Read the workspace's base marker as `(branch, rev)`, if present. Returns
/// `None` when the marker is absent or its branch line is empty.
pub fn read_base_marker(ws_path: &Path) -> Option<(String, String)> {
    let content = std::fs::read_to_string(ws_path.join(".jj").join(BASE_MARKER)).ok()?;
    let mut lines = content.lines();
    let branch = lines.next().map(str::trim).filter(|s| !s.is_empty())?;
    let rev = lines.next().map(str::trim).unwrap_or("");
    Some((branch.to_string(), rev.to_string()))
}

/// Whether the working copy (`@`) carries changes versus its parent. Never
/// consults `git status` (non-empty mid-work under jj because the change lives
/// in `@`, not git's HEAD).
pub fn is_working_copy_dirty(jj: &JjEnv, ws: &Path) -> Result<bool, String> {
    Ok(!jj
        .run(ws, &["diff", "--summary"], "jj diff --summary")?
        .is_empty())
}

/// The change id of `@` (stable across the working copy's content amendments).
pub fn snapshot_change_id(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(
        ws,
        &["log", "-r", "@", "--no-graph", "-T", "change_id.short()"],
        "jj log -r @",
    )
}

/// Whether the seal's scoped paths carry uncommitted changes in `@`. A whole-`@`
/// seal (empty `paths`) reuses [`is_working_copy_dirty`]; a path-scoped seal
/// diffs only those filesets, because [`seal_paths`] deliberately leaves
/// unrelated un-sealed dirt in `@`, so the empty-seal expectation must be measured
/// against the scoped paths only — otherwise a legitimately no-op scoped write
/// (whose unrelated dirt makes the whole `@` look dirty) would false-positive.
pub(crate) fn scoped_dirty(jj: &JjEnv, ws: &Path, paths: &[&str]) -> Result<bool, String> {
    if paths.is_empty() {
        return is_working_copy_dirty(jj, ws);
    }
    let mut args: Vec<String> = vec!["diff".into(), "-r".into(), "@".into(), "--summary".into()];
    for path in paths {
        args.push(quote_fileset(path));
    }
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    Ok(!jj
        .run(ws, &argref, "jj diff -r @ --summary (scoped)")?
        .is_empty())
}

/// Whether the just-sealed `@-` commit is the empty/divergent data-loss shape: a
/// `jj commit` that returned a real sha but silently captured nothing because a
/// concurrent op reset `@` out from under it. `pre_dirty` is the seal's measured
/// pre-commit dirt over the same scoped paths. Returns `true` when either:
///
/// - `pre_dirty && empty`: the working copy had scoped changes to seal, but `@-`
///   has no diff vs its parent — the dirt was reset away before the commit
///   captured it (jj's `empty` keyword, correct for both seal modes since only
///   the scoped paths were committed into `@-`); or
/// - divergent: the sealed change resolves to more than one visible commit
///   (`<id>/0../n`), the shape a concurrent-op merge leaves when both forked
///   rewrites are kept.
///
/// Two cheap `jj log` reads on the just-sealed commit; runs only on the seal path.
fn sealed_commit_is_lost(jj: &JjEnv, ws: &Path, pre_dirty: bool) -> Result<bool, String> {
    let empty = jj
        .run(
            ws,
            &["log", "-r", "@-", "--no-graph", "-T", "empty"],
            "jj seal empty check",
        )?
        .contains("true");
    if pre_dirty && empty {
        return Ok(true);
    }
    let cid = jj.run(
        ws,
        &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
        "jj seal change id",
    )?;
    let cid = cid.trim();
    if cid.is_empty() {
        return Ok(false);
    }
    let twins = jj.run(
        ws,
        &[
            "log",
            "-r",
            &format!("change_id({cid})"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
        ],
        "jj seal divergence check",
    )?;
    Ok(twins.lines().filter(|l| !l.trim().is_empty()).count() > 1)
}

/// Seal the whole `@` into one addressable commit (the run-path seal: seals the
/// entire working copy). See [`seal_paths`].
pub fn seal(
    jj: &JjEnv,
    ws: &Path,
    msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    seal_paths(jj, ws, msg, author, &[])
}

/// Seal `@` into one addressable commit and open a fresh empty `@`. When `paths`
/// is non-empty the seal is **path-scoped**: only those paths leave `@`, so
/// unrelated un-sealed dirt (e.g. a prior failed or full-sandbox run's side
/// effects) stays in the working copy and is NOT folded into this commit: a
/// file-scoped seal touches only those paths. An empty slice seals the whole `@`.
/// `^` folds the scoped paths into the prior sealed commit (git `--amend`
/// equivalent). Advances the workspace's git bookmark to the sealed commit and
/// exports it to the project's git (best-effort). Returns the sealed commit id.
pub fn seal_paths(
    jj: &JjEnv,
    ws: &Path,
    msg: &str,
    author: Option<&GitAuthor>,
    paths: &[&str],
) -> Result<CommitResult, String> {
    let mut args: Vec<String> = JjEnv::author_args(author);
    if msg == "^" {
        args.extend(["squash".into(), "--use-destination-message".into()]);
    } else {
        args.extend(["commit".into(), "-m".into(), msg.into()]);
    }
    // Path-scope so only these paths leave `@`; empty = whole working copy.
    // jj parses positional path args as fileset expressions, so each path is
    // wrapped as a quoted string literal to match a path with fileset
    // metacharacters (e.g. a Next.js `(app)` route group) literally.
    for path in paths {
        args.push(quote_fileset(path));
    }
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    // Pre-commit backstop: refuse a stale-workspace seal BEFORE creating the
    // commit, so no orphan is ever produced. If the branch bookmark has advanced
    // PAST this workspace's head `@-` (a Coordinator whose integration bookmark a
    // child fold moved out from under its stale `@`), the commit would descend
    // from the stale `@-` and land OFF the branch; the bookmark advance would then
    // be refused as non-fast-forward, leaving an orphaned commit the generic
    // discard (`jj restore`, which only resets `@` to its parent) cannot recover.
    // Checking here — before `jj commit` — keeps `@` clean and on the stale line so
    // a follow-up advance can fix it. The healthy case (bookmark == `@-`) and an
    // amend (the bookmark follows the rewrite) both fast-forward. With the
    // post-fold workspace advance in place this is unreachable on the happy path.
    let branch = read_branch_marker(ws);
    if let Some(branch) = branch.as_deref() {
        if !seal_is_fast_forward(jj, ws, branch)? {
            // The fast-forward guard refused: `@` does not descend from the branch
            // bookmark. Two structurally different causes need OPPOSITE handling,
            // and ancestry alone cannot separate them (in both, `@-` is an ancestor
            // of the bookmark). The distinguisher is whether the bookmark tip
            // carries a recorded CONFLICT:
            //
            // - Conflicted tip → a deliberate resolve-at-base FLATTEN. `@` is a
            //   fresh resolved tree on the current base while the bookmark still
            //   points at the conflicted intermediate stack tip the agent is
            //   escaping. Discarding `@` would destroy the resolved work and
            //   advancing would land back on the conflict, so this returns a
            //   DISTINCT error routed to a non-destructive preserve-and-instruct
            //   path (see [`is_conflicted_branch_seal_error`]).
            // - Clean tip → a genuine STALE / coordinator-advance: the bookmark
            //   advanced onto a clean tip and `@` is a stale shell. The existing
            //   "behind its branch tip" message and its stale-family recovery
            //   (discard, self-healing via update-stale) stay unchanged.
            if branch_has_conflict(jj, ws, branch).unwrap_or(false) {
                return Err(CONFLICTED_BRANCH_SEAL_MSG.to_string());
            }
            return Err(format!(
                "seal refused: workspace `{branch}` is behind its branch tip — the branch \
                 advanced past this workspace's head, so sealing would create a commit off \
                 `{branch}`. The workspace must be advanced onto the branch tip before sealing."
            ));
        }
    }

    // Measure the scoped dirt BEFORE committing so an EMPTY seal (the working copy
    // reset out from under the commit) can be told apart from a legitimately no-op
    // scoped write. Best-effort: if the probe can't run we conservatively skip the
    // empty-anomaly arm (divergence is still checked) rather than fail a good seal.
    // Skipped for an amend (`^`): its emptiness semantics differ and it is not the
    // observed failure mode.
    let pre_dirty = if msg == "^" {
        false
    } else {
        scoped_dirty(jj, ws, paths).unwrap_or(false)
    };

    jj.run(ws, &argref, "jj commit")?;
    let sha = jj.run(
        ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
        "jj log -r @-",
    )?;

    // Detection backstop: a concurrent store advance can reset `@` out from under
    // the commit so `jj commit` succeeds but seals an EMPTY or DIVERGENT commit —
    // silent data loss otherwise reported as a real sha. Check only on a real
    // commit (the amend path is excluded above via `pre_dirty`/`msg`). On the
    // anomaly, back the bad commit out so `@` returns to its pre-seal parent and a
    // retry lands cleanly, then return the typed, recoverable lost-seal error. The
    // bookmark has NOT moved yet (that runs only on the clean path below), so
    // `jj abandon @-` reparents `@` onto the original parent and drops the commit
    // without stranding the bookmark on a twin.
    if msg != "^" && sealed_commit_is_lost(jj, ws, pre_dirty).unwrap_or(false) {
        if let Err(e) = jj.run(ws, &["abandon", "@-"], "jj abandon lost seal") {
            log::warn!("failed to back out lost-seal commit (still reporting the loss): {e}");
        }
        return Err(LOST_SEAL_MSG.to_string());
    }
    // Advance the project's git branch ref to the sealed commit so push and
    // git-side reads stay current. The pre-commit fast-forward check above
    // guarantees this is a forward move, so it stays best-effort: a transient ref
    // failure never fails an otherwise-good seal (a stale ref self-heals next
    // seal).
    if let Some(branch) = branch.as_deref() {
        if let Err(e) = jj.run(
            ws,
            &["bookmark", "set", branch, "-r", "@-"],
            "jj bookmark set",
        ) {
            log::warn!("jj bookmark set after seal (best-effort, continuing): {e}");
        }
        let _ = jj.run(ws, &["git", "export"], "jj git export");
    }
    Ok(CommitResult {
        sha,
        pr_number: None,
    })
}

/// Whether sealing this workspace would FAST-FORWARD its branch bookmark: the
/// bookmark must be an ancestor of (or equal to) the workspace head `@-`, so a new
/// commit descending from `@-` advances the bookmark forward. `false` means the
/// branch advanced PAST this workspace (a Coordinator whose integration bookmark a
/// child fold moved out from under its stale `@`); sealing then would create an
/// off-branch commit whose bookmark advance jj refuses as non-fast-forward.
/// [`seal_paths`] checks this BEFORE `jj commit` so a stale seal is refused
/// without ever creating the orphan. A bookmark that does not resolve yet (never
/// created) is treated as fast-forwardable — the post-commit `bookmark set` will
/// create it. The revset `(<bookmark>) & ::@` is non-empty iff the bookmark
/// commit is an ancestor-or-self of `@` (the working copy) — i.e. `@` descends
/// from the bookmark, so sealing fast-forwards it.
///
/// `::@` (not `::@-`) is deliberate: it also accepts the bookmark sitting ON `@`
/// itself — the legitimate state when the worktree's working-copy commit IS the
/// branch tip (e.g. an agent's last commit is the working copy, or any worktree
/// where the bookmark was set to `@`). Sealing there is a clean fast-forward (the
/// edit commits into `@` and the bookmark advances), so it must not be refused.
/// A genuinely-ahead bookmark on a divergent line (the Coordinator-fold case) is
/// still rejected, because it is not an ancestor of `@`.
fn seal_is_fast_forward(jj: &JjEnv, ws: &Path, branch: &str) -> Result<bool, String> {
    let Some(bookmark) = bookmark_commit(jj, ws, branch) else {
        return Ok(true);
    };
    let hit = jj.run(
        ws,
        &[
            "log",
            "-r",
            &format!("({bookmark}) & ::@"),
            "--no-graph",
            "-T",
            "commit_id",
        ],
        "jj seal fast-forward precheck",
    )?;
    Ok(!hit.is_empty())
}

/// Outcome of folding a `when:write` check's tracked changes into the sealed
/// commit: the repo-relative paths the check modified (also the inline summary's
/// content). `fold_worktree_into_seal` returns `None` instead of an empty list
/// when there was nothing to fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldOutcome {
    pub folded_files: Vec<String>,
}

/// Fold a `when:write` check's tracked working-copy changes into the just-sealed
/// commit (`jj squash` of `@` into `@-`), leaving `@` clean == the amended sealed
/// tip.
///
/// A check is an observer of the sealed commit, but its command may legitimately
/// rewrite tracked files (a formatter, `lint --fix`, regenerated snapshots).
/// Folding does two jobs at once: it delivers those edits into the commit AND
/// restores the seal-clean invariant, so a concurrent base-advance / reconcile in
/// the lock-free check window can never snapshot or rebase a dirty `@` into the
/// stale / divergent / behind-tip tangle that wedges the next seal (CAIRN-2260).
///
/// Only TRACKED changes fold: gitignored writes (vitest/tsc caches) are excluded
/// from the working-copy snapshot (gitignore + `snapshot.auto-track`), so they
/// never enter `@` and are never committed — they stay as ignored files on disk.
/// `jj squash` keeps the sealed commit's message and author, so the folded edits
/// ride the agent's original commit. The bookmark follows the rewrite onto the
/// amended commit; the git ref and origin are re-published (best-effort) so they
/// reflect the new tree (an amend-push jj tracks via the remote bookmark).
///
/// Returns `Ok(None)` when `@` carried no tracked change (a pure verify check) —
/// the amend is then a no-op and `@` was already clean.
pub fn fold_worktree_into_seal(jj: &JjEnv, ws: &Path) -> Result<Option<FoldOutcome>, String> {
    // The tracked files the check changed. Empty => pure verify check: nothing to
    // fold, `@` already clean. (A stale `@` makes this error and propagate, so the
    // caller falls back to the next seal's stale recovery rather than amending
    // blindly.)
    let changed = jj.run(ws, &["diff", "-r", "@", "--name-only"], "jj fold diff")?;
    let folded_files: Vec<String> = changed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    if folded_files.is_empty() {
        return Ok(None);
    }
    // Fold `@`'s tracked changes into the sealed parent and open a fresh empty `@`.
    jj.run(ws, &["squash"], "jj squash (fold check changes)")?;
    // Re-establish bookmark / git ref / origin at the amended commit, mirroring a
    // seal. The bookmark auto-follows the rewrite; `bookmark set` is idempotent
    // belt-and-braces, and the export/push propagate the amended tree.
    if let Some(branch) = read_branch_marker(ws) {
        if let Err(e) = jj.run(
            ws,
            &["bookmark", "set", &branch, "-r", "@-"],
            "jj bookmark set (fold)",
        ) {
            log::warn!("fold: bookmark set after squash (best-effort): {e}");
        }
        let _ = jj.run(ws, &["git", "export"], "jj git export (fold)");
        push_to_origin(jj, ws, &branch);
    }
    Ok(Some(FoldOutcome { folded_files }))
}

/// Discard working-copy changes by resetting `@` to its parent. Reversible via
/// the operation log — replacing git's destructive `reset --hard`.
///
/// Self-heals a STALE working copy. `jj restore` is itself blocked on a stale
/// `@` (a sibling workspace rewrote it over the shared store) — the same refusal
/// that blocks the seal — so a naive `restore` would dead-end and strand the
/// loose edits uncommitted, exactly the data-loss path the commit barrier must
/// not have. `update-stale` is the one op staleness does not block: it refreshes
/// `@` onto the rewritten/advanced commit and overwrites the loose
/// (unsnapshotted) batch edits, leaving the worktree == fresh `@`. So when
/// `restore` reports staleness, recover through `update-stale` instead of
/// failing, and the rollback no longer shares the seal's single point of
/// failure. See [`is_stale_error`].
pub fn discard(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    match jj.run(ws, &["restore"], "jj restore") {
        Ok(_) => Ok(()),
        Err(e) if is_stale_error(&e) => {
            // update-stale advances `@` and discards the loose edits → clean.
            update_stale(jj, ws)?;
            // Belt-and-braces: a now-unblocked restore guarantees `@` == parent.
            let _ = jj.run(ws, &["restore"], "jj restore (post update-stale)");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

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
fn sealed_tree_hash_via_git(jj: &JjEnv, ws: &Path, commit: &str) -> Result<String, String> {
    let git_dir = git_backend_root(jj, ws)?;
    let out = crate::env::git()
        .args([
            "--git-dir",
            &git_dir,
            "rev-parse",
            &format!("{commit}^{{tree}}"),
        ])
        .output()
        .map_err(|e| format!("git rev-parse tree: {e}"))?;
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
    let out = crate::env::git()
        .args(["--git-dir", &git_dir, "ls-tree", "-r", "-z", treeish])
        .output()
        .map_err(|e| format!("git ls-tree: {e}"))?;
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
fn parse_ls_tree(output: &str) -> Vec<(String, String)> {
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

/// One changed file derived from the live sealed jj graph: its repo-relative
/// path, status, and `+`/`-` line counts, plus the previous path for a rename.
/// The substrate for the node `/changed` projection, which derives the changed
/// set from the graph ([`node_changed_files`]) rather than the best-effort
/// `file_changes` cache, so a just-sealed commit's file is never omitted the way
/// the decoupled async cache insert could lag or drop it (CAIRN-2101).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFileChange {
    pub path: String,
    pub previous_path: Option<String>,
    /// `added` | `modified` | `deleted` | `renamed` — the same vocabulary the
    /// `file_changes` cache records, so the rendered table reads identically
    /// whichever source produced it.
    pub status: String,
    pub additions: i32,
    pub deletions: i32,
}

/// Cumulative changed files of a workspace against its recorded base, read from
/// the live sealed jj graph rather than the side-channel `file_changes` cache.
///
/// Runs `jj diff --git -r '<base>..@'` from `ws`. The range revset is what makes
/// this both correct and base-advance-resilient:
///
/// - It spans every sealed commit on the node's branch AND the loose edits in
///   `@` (jj snapshots the working copy into `@`), so a just-sealed file can
///   never lag the way the async cache insert could — the bug this fixes.
/// - `base..@` is the node's OWN commits even when the base advanced and `@` has
///   not yet rebased onto the new tip; a `--from base --to @` tree diff would
///   instead pollute the result with the base-advance deltas (verified against
///   jj 0.42).
///
/// `--ignore-working-copy` reads the last-recorded `@` without taking the
/// working-copy lock, so this read-only projection never contends with the live
/// agent's own jj operations (the same trade-off as [`list_files`]: an edit made
/// since the last jj op won't show until the next snapshot, which the agent
/// takes on nearly every operation).
///
/// Returns `None` when `ws` is not a jj workspace or neither base anchor
/// resolves, so the caller falls back to the recorded cache (e.g. a torn-down
/// workspace whose only surviving record is the DB).
pub fn node_changed_files(
    jj: &JjEnv,
    ws: &Path,
    base_branch: Option<&str>,
    base_commit: Option<&str>,
) -> Option<Vec<GraphFileChange>> {
    if !is_jj_dir(ws) {
        return None;
    }
    let fork = resolve_node_fork_point(jj, ws, base_branch, base_commit)?;
    let revset = format!("{fork}..@");
    let out = jj
        .run(
            ws,
            &["diff", "--ignore-working-copy", "--git", "-r", &revset],
            "jj diff --git (node changed)",
        )
        .ok()?;
    Some(parse_git_diff(&out))
}

/// Resolve the node's current effective fork point from the live jj graph.
///
/// The recorded `base_commit`/`pack_anchor` is the original fork point. That is
/// not necessarily where the workspace is currently based: default-branch
/// reconciliation can rebase the node onto `<base>@origin`, while local/manual
/// advancement can move the local bookmark first. Rather than trusting one stale
/// reference, resolve every base form that exists and choose the newest commit
/// common to `@` and any of those bases. That keeps `/changed` and live PR diffs
/// measuring only the node's own commits whether the node was rebased or the base
/// advanced without the node.
///
/// Returns `None` when no base candidate resolves, so callers keep their existing
/// cache or anchor fallback rather than diffing against an empty revset, which
/// would dump the workspace's entire history. Lock-free via
/// `--ignore-working-copy`.
pub fn resolve_node_fork_point(
    jj: &JjEnv,
    ws: &Path,
    base_branch: Option<&str>,
    base_commit: Option<&str>,
) -> Option<String> {
    let mut candidates: Vec<String> = Vec::new();
    if let Some(branch) = base_branch.filter(|s| !s.is_empty()) {
        candidates.push(format!("{branch}@origin"));
        candidates.push(format!("bookmarks(exact:{branch:?})"));
    }
    if let Some(commit) = base_commit.filter(|s| !s.is_empty()) {
        candidates.push(commit.to_string());
    }

    let resolved: Vec<String> = candidates
        .into_iter()
        .filter(|rev| changed_base_resolves(jj, ws, rev))
        .collect();
    if resolved.is_empty() {
        return None;
    }

    let union = resolved.join(" | ");
    let revset = format!("heads(::@ & ::({union}))");
    // A criss-cross graph can produce multiple heads here. Taking the first is
    // intentionally git-like: callers need a stable base, and any merge-base is
    // a valid common ancestor for this defensive diff.
    jj.run(
        ws,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            &revset,
            "--no-graph",
            "-T",
            "commit_id ++ \"\n\"",
        ],
        "jj resolve node fork point",
    )
    .ok()
    .and_then(|s| {
        s.lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(str::to_string)
    })
}

/// Whether `rev` resolves to a commit in the store, read lock-free. An exact
/// bookmark that does not exist resolves to the empty set (empty stdout, exit
/// 0), which this reports as unresolved.
fn changed_base_resolves(jj: &JjEnv, ws: &Path, rev: &str) -> bool {
    jj.run(
        ws,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            rev,
            "--no-graph",
            "-T",
            "commit_id",
        ],
        "jj resolve (node changed base)",
    )
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// Parse `jj diff --git` (standard git unified-diff) output into structured
/// per-file changes. Status comes from the rename markers and the `/dev/null`
/// side of the `---`/`+++` headers; `+`/`-` lines inside hunks are counted for
/// the line totals. Pure (no jj invocation), so the risky bit carries its own
/// unit tests.
fn parse_git_diff(diff: &str) -> Vec<GraphFileChange> {
    let mut files: Vec<GraphFileChange> = Vec::new();
    let mut block: Option<DiffBlock> = None;
    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            if let Some(done) = block.take() {
                files.push(done.finish());
            }
            block = Some(DiffBlock::new(rest));
            continue;
        }
        let Some(b) = block.as_mut() else { continue };
        if line.starts_with("@@") {
            // First hunk header: everything after is content, where a leading
            // `+`/`-` is an added/removed line rather than a file header.
            b.in_hunk = true;
            continue;
        }
        if b.in_hunk {
            if line.starts_with('+') {
                b.additions += 1;
            } else if line.starts_with('-') {
                b.deletions += 1;
            }
            continue;
        }
        // Header region (before the first hunk): file-level metadata only.
        if let Some(p) = line.strip_prefix("rename from ") {
            b.renamed = true;
            b.old_path = Some(unquote_diff_path(p));
        } else if let Some(p) = line.strip_prefix("rename to ") {
            b.renamed = true;
            b.new_path = Some(unquote_diff_path(p));
        } else if line.starts_with("new file mode") {
            b.added = true;
        } else if line.starts_with("deleted file mode") {
            b.deleted = true;
        } else if let Some(p) = line.strip_prefix("--- ") {
            if p == "/dev/null" {
                b.added = true;
            } else {
                b.old_path = Some(strip_diff_prefix(p));
            }
        } else if let Some(p) = line.strip_prefix("+++ ") {
            if p == "/dev/null" {
                b.deleted = true;
            } else {
                b.new_path = Some(strip_diff_prefix(p));
            }
        }
    }
    if let Some(done) = block.take() {
        files.push(done.finish());
    }
    files
}

/// Public wrapper over [`parse_git_diff`]: turn a captured `git`/`jj diff --git`
/// patch into structured [`GraphFileChange`] rows. Lets callers outside `jj`
/// (the run-path commit barrier) record a just-sealed commit's file changes from
/// the working-copy patch captured before the seal, feeding the same
/// `file_changes` cache the write path records into.
pub fn parse_git_patch(diff: &str) -> Vec<GraphFileChange> {
    parse_git_diff(diff)
}

/// Accumulator for one `diff --git` file block while [`parse_git_diff`] scans.
struct DiffBlock {
    header_old: Option<String>,
    header_new: Option<String>,
    old_path: Option<String>,
    new_path: Option<String>,
    renamed: bool,
    added: bool,
    deleted: bool,
    in_hunk: bool,
    additions: i32,
    deletions: i32,
}

impl DiffBlock {
    fn new(header: &str) -> Self {
        let (header_old, header_new) = parse_diff_header_paths(header);
        DiffBlock {
            header_old,
            header_new,
            old_path: None,
            new_path: None,
            renamed: false,
            added: false,
            deleted: false,
            in_hunk: false,
            additions: 0,
            deletions: 0,
        }
    }

    fn finish(self) -> GraphFileChange {
        let new_path = self.new_path.or(self.header_new);
        let old_path = self.old_path.or(self.header_old);
        let (status, path, previous_path) = if self.renamed {
            (
                "renamed",
                new_path.or_else(|| old_path.clone()).unwrap_or_default(),
                old_path,
            )
        } else if self.added {
            ("added", new_path.or(old_path).unwrap_or_default(), None)
        } else if self.deleted {
            ("deleted", old_path.or(new_path).unwrap_or_default(), None)
        } else {
            ("modified", new_path.or(old_path).unwrap_or_default(), None)
        };
        GraphFileChange {
            path,
            previous_path,
            status: status.to_string(),
            additions: self.additions,
            deletions: self.deletions,
        }
    }
}

/// Split a `diff --git a/X b/Y` header tail into (old, new) paths with the
/// `a/`/`b/` prefixes stripped. Whitespace-split is unambiguous for the common
/// no-space case; quoted/spaced paths fall back on the more reliable
/// `---`/`+++`/`rename` lines, so this is only a backstop for hunkless entries
/// (binary or pure mode changes).
fn parse_diff_header_paths(header: &str) -> (Option<String>, Option<String>) {
    let tokens: Vec<&str> = header.split_whitespace().collect();
    if tokens.len() == 2 {
        (
            Some(strip_diff_prefix(tokens[0])),
            Some(strip_diff_prefix(tokens[1])),
        )
    } else {
        (None, None)
    }
}

/// Strip a leading `a/`/`b/` diff prefix, then any surrounding quotes git adds
/// for paths with special characters.
fn strip_diff_prefix(path: &str) -> String {
    let path = path
        .strip_prefix("a/")
        .or_else(|| path.strip_prefix("b/"))
        .unwrap_or(path);
    unquote_diff_path(path)
}

/// Drop surrounding double quotes git adds around a path with special
/// characters. C-escapes inside are left as-is (rare; the path still renders
/// recognizably).
fn unquote_diff_path(path: &str) -> String {
    let trimmed = path.trim();
    trimmed
        .strip_prefix('"')
        .and_then(|p| p.strip_suffix('"'))
        .unwrap_or(trimmed)
        .to_string()
}

/// Export jj's state to the workspace's backing git refs (`jj git export`), so a
/// git-level read of HEAD/refs reflects jj's current (post-rebase) state. Used by
/// archival before it packs the worktree history: an out-of-workspace auto-rebase
/// (an orchestration merge) may not have exported to this workspace's git yet, so
/// without the refresh the pack could be built from stale refs. Best-effort.
pub fn export_git(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    jj.run(ws, &["git", "export"], "jj git export").map(|_| ())
}

/// Forward-map a possibly-rewritten commit to its current commit-id and stable
/// change-id. jj's headline auto-rebase rewrites a commit's commit-id while its
/// change-id stays stable, so a coordinate recorded before a rebase points at a
/// now-hidden commit; this resolves it forward to the commit that actually lands
/// in the archival pack, with the durable change-id as provenance.
///
/// Returns `None` when `ws` is not a jj workspace or jj cannot resolve the commit.
/// Both yield identity/skip semantics at the call site: plain-git worktrees and
/// unresolvable ids keep today's behavior.
pub fn forward_resolve_commit(
    jj: &JjEnv,
    ws: &Path,
    commit: &str,
) -> Option<(
    String, /* change_id */
    String, /* current_commit */
)> {
    if !is_jj_dir(ws) {
        return None;
    }
    // Step 1: resolve the (possibly hidden/rewritten) commit to its stable
    // change-id. `jj log -r <commit_id>` resolves even a commit jj has since
    // rewritten, as long as its object still exists — which it does at teardown,
    // before any git gc.
    let change_id = jj
        .run(
            ws,
            &["log", "-r", commit, "--no-graph", "-T", "change_id"],
            "jj forward-resolve change_id",
        )
        .ok()
        .filter(|s| !s.is_empty())?;

    // Step 2: resolve the change-id forward to its current visible commit-id. A
    // divergent change resolves to several visible commits; prefer the one
    // reachable from the worktree tip (`@`), which is the commit the archival pack
    // (built over `pack_anchor..tip`) actually contains. The `change_id(...)`
    // function form is divergence-safe (a bare change-id symbol errors when the
    // change is divergent), and `latest(...)` collapses an empty or multi-commit
    // result to a single line. Fall back to the change-id's latest visible commit
    // when nothing is tip-reachable.
    let tip_reachable = format!("latest(change_id({change_id}) & ::@)");
    let current = jj
        .run(
            ws,
            &["log", "-r", &tip_reachable, "--no-graph", "-T", "commit_id"],
            "jj forward-resolve commit_id (tip-reachable)",
        )
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            jj.run(
                ws,
                &[
                    "log",
                    "-r",
                    &format!("latest(change_id({change_id}))"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ],
                "jj forward-resolve commit_id",
            )
            .ok()
            .filter(|s| !s.is_empty())
        })?;

    Some((change_id, current))
}

/// Push the workspace's bookmark to origin. Best-effort: logs and never fails,
/// mirroring `mcp::git::push_to_origin`'s contract so a local/remoteless jj
/// project never fails a seal. Skips empty/`main`/`master` branches (the same
/// guard the git path uses). jj 0.42 auto-tracks a new bookmark on push, so the
/// removed `--allow-new` flag is not passed; seals only advance the bookmark, so
/// the push is a fast-forward and needs no force.
///
/// `--ignore-working-copy`: a publish must never SNAPSHOT the live `@`. The
/// bookmark already points at the sealed `@-`, so pushing needs no fresh
/// snapshot — and snapshotting here would fold whatever transient dirt sits in
/// `@` (e.g. a `when:write` check's caches, since the post-seal push runs from
/// the workspace) into the working-copy commit, exactly the kind of working-copy
/// mutation a concurrent store op can then wedge a later seal on. Matches
/// `advance_workspace_onto` / `node_changed_files`, which pass it deliberately.
pub fn push_to_origin(jj: &JjEnv, ws: &Path, branch: &str) {
    if branch.is_empty() || branch == "main" || branch == "master" {
        log::debug!("Skipping jj push for branch: {branch}");
        return;
    }
    match jj.run(
        ws,
        &[
            "git",
            "push",
            "--remote",
            "origin",
            "--bookmark",
            branch,
            "--ignore-working-copy",
        ],
        "jj git push",
    ) {
        Ok(_) => log::info!("Pushed bookmark {branch} to origin (jj)"),
        Err(e) => log::warn!("jj push failed (seal succeeded locally): {e}"),
    }
}

/// Resolve a bookmark name to a commit id over the shared store, or `None` when
/// the bookmark does not exist. `bookmarks(exact:"…")` matches the literal name
/// (bookmark names carry `/`, which a bare revset symbol also accepts but the
/// exact form is unambiguous), and an empty revset exits 0 with empty output.
pub fn bookmark_commit(jj: &JjEnv, store: &Path, branch: &str) -> Option<String> {
    let revset = format!("bookmarks(exact:{:?})", branch);
    revset_commit(jj, store, &revset)
}

/// Whether the `src` bookmark's tip has already landed in `dst` — its commit is
/// an ancestor of (or equal to) the `dst` bookmark's tip in the shared store.
///
/// `bookmarks(exact:SRC) & ::bookmarks(exact:DST)` intersects SRC's target commit
/// with DST's ancestor set (inclusive); a non-empty result means SRC's tip lies
/// on DST's history, i.e. a fold already carried SRC into DST. Returns `false`
/// when either bookmark is missing or the revset is empty — a landed check fails
/// closed ("cannot prove landed" is treated as "not landed"), so a caller that
/// deletes only landed branches preserves anything it cannot verify.
///
/// Note this is a *lineage* test: a squash landing rewrites SRC onto DST before
/// the fold, so the rewritten SRC bookmark is an ancestor of DST and this holds;
/// but an out-of-band squash that discards SRC's commits (e.g. GitHub's own
/// squash-merge) leaves SRC off DST's history and returns `false`. Use it only
/// where the store owns the fold (the local jj merge path and its teardown).
pub fn bookmark_landed_in(jj: &JjEnv, store: &Path, src: &str, dst: &str) -> bool {
    if src.is_empty() || dst.is_empty() {
        return false;
    }
    let revset = format!("bookmarks(exact:{src:?}) & ::bookmarks(exact:{dst:?})");
    revset_commit(jj, store, &revset).is_some()
}

/// Resolve a single revset to a commit id over the shared store, or `None` when
/// it does not resolve. Used for both exact local bookmarks and remote-tracking
/// bookmarks such as `main@origin`.
pub fn revset_commit(jj: &JjEnv, store: &Path, revset: &str) -> Option<String> {
    jj.run(
        store,
        &["log", "-r", revset, "--no-graph", "-T", "commit_id"],
        "jj log revset commit",
    )
    .ok()
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
}

/// The commit id of an active workspace's working-copy commit (`<name>@`),
/// resolved over the shared store. Used to detect whether
/// [`advance_workspace_onto`] actually moved the `@` (a real advance) versus an
/// idempotent no-op, so the on-branch advance only notifies on a genuine move.
pub fn workspace_head_commit(jj: &JjEnv, store: &Path, ws_branch: &str) -> Option<String> {
    let source = format!("{}@", workspace_name_for_branch(ws_branch));
    jj.run(
        store,
        &["log", "-r", &source, "--no-graph", "-T", "commit_id"],
        "jj log workspace head commit",
    )
    .ok()
    .filter(|s| !s.is_empty())
}

/// Publish a bookmark that already lives in the shared store to origin. Used to
/// put a Coordinator integration-branch base on origin from the store, where it
/// exists as a bookmark even though the project checkout carries no local ref
/// for it (so the git `push origin <base>` the git path uses cannot find it).
///
/// No-op when the bookmark does not resolve in the store (base not sealed yet)
/// or already matches origin (`jj git push` reports "Nothing changed"). jj 0.42
/// auto-tracks a new bookmark on push, so no `--allow-new` is passed.
pub fn ensure_bookmark_on_origin(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    if branch.is_empty() {
        return Ok(());
    }
    if bookmark_commit(jj, store, branch).is_none() {
        log::debug!("jj base bookmark {branch} absent from store; nothing to publish");
        return Ok(());
    }
    jj.run(
        store,
        &["git", "push", "--remote", "origin", "--bookmark", branch],
        "jj git push base bookmark",
    )
    .map(|_| ())
}

// ── Sibling reconcile (auto-rebase onto an advanced integration tip) ─────────

/// Outcome of reconciling in-flight siblings onto an advanced integration tip:
/// which sibling bookmarks rebased cleanly, which recorded a conflict, and which
/// were held back untouched. A recorded conflict is STOP-THE-LINE, not a
/// convenience item: jj refuses to push or merge a conflicted commit, so a
/// conflicted branch destined for GitHub is wedged until the agent resolves the
/// markers and re-seals. The reconcile also never hands a conflicted base down to
/// clean siblings — when the rebase dest itself carries a conflict, every sibling
/// is `held` on its prior clean commit rather than rebased onto the conflict.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Sibling bookmarks that rebased with no conflict.
    pub rebased_clean: Vec<String>,
    /// Sibling bookmarks whose rebase recorded a conflict.
    pub conflicted: Vec<String>,
    /// Sibling bookmarks held UNrebased because the rebase dest itself carries
    /// a recorded conflict — never handed a conflicted base. Cleared on the next
    /// reconcile once the base re-seals conflict-free.
    pub held: Vec<String>,
}

/// Fold a child's real commit into the integration bookmark over the shared
/// store — the local "merge" of a child PR. `jj bookmark set` is forward-only (it
/// refuses a backwards/sideways move), so the child must already sit on the
/// current integration tip; callers establish that by rebasing the source onto
/// the current tip before folding (`store_merge_child`, `rebase_then_fold_into`).
/// A refusal here means that rebase did not run or did not take — surface it
/// loudly rather than silently regressing the tip.
/// `--ignore-working-copy` because the fold is driven from the store, not a
/// workspace (Gotcha A: the store's default `@` may be stale after a prior
/// `--ignore-working-copy` rebase).
///
/// A backwards/sideways refusal is mapped to a safe, actionable message: jj's
/// raw stderr hints `--allow-backwards`, which would move the bookmark BACKWARD
/// and clobber the commits that advanced it. That hint must never reach an
/// agent, so it is never echoed. For a fold whose target advances out of band
/// (the project default branch), callers use `rebase_then_fold_into`, which
/// rebases first so this path is never reached.
pub fn merge_into_bookmark(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    child_branch: &str,
) -> Result<(), String> {
    let child_rev = format!("bookmarks(exact:{child_branch:?})");
    if let Err(e) = jj.run(
        store,
        &[
            "bookmark",
            "set",
            integration_branch,
            "-r",
            &child_rev,
            "--ignore-working-copy",
        ],
        "jj bookmark set (merge fold)",
    ) {
        // Sanitize jj's raw backwards/sideways refusal: its stderr hints
        // `--allow-backwards`, which would move the bookmark BACKWARD and clobber
        // the commits that advanced it. Map it to a message that names the real
        // cause (the source is not a descendant of the target) and the safe
        // remedy (rebase first), and NEVER echo the dangerous hint.
        let lowered = e.to_lowercase();
        if lowered.contains("backwards") || lowered.contains("sideways") {
            return Err(format!(
                "Refusing to fold `{child_branch}` into `{integration_branch}`: the source is not a descendant of the target (the target advanced past the source's fork point). Rebase the source onto the current target tip and let it re-seal, then merge again."
            ));
        }
        return Err(e);
    }
    // Export the advanced bookmark to the backing git repo so the project's
    // `refs/heads/<integration>` tracks the fold (as `seal` does after a sealed
    // commit). Without this the store bookmark is advanced but the project git
    // ref lags, and a later child provisioned off the integration branch
    // resolves its base via that stale ref (`execution/jobs/worktrees.rs`) and
    // would start from the pre-merge tip — breaking the store-owns-merge
    // invariant. Load-bearing, so it fails the fold rather than silently leaving
    // a stale ref.
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (merge fold)",
    )
    .map(|_| ())
}

/// Merge a source bookmark into a target whose tip may have advanced out of band
/// (the project default branch). Unlike `merge_into_bookmark`'s forward-only fold
/// — which assumes Cairn's reconcile keeps the source on an integration tip — the
/// default branch advances OUTSIDE the fold chain (another PR merged, or an
/// external push), so the source is first rebased onto the current target tip,
/// exactly as `reconcile_siblings` rebases siblings, then the target FFs to it.
/// A recorded conflict returns a safe, actionable error and NEVER the
/// `--allow-backwards` hint (which would move the default branch backward and
/// clobber it). `dest` is the resolved live target tip (`<target>@origin` for a
/// remote project after a fetch, else the local bookmark). Idempotent when the
/// source already sits on `dest` (the rebase is a `jj rebase` no-op).
pub fn rebase_then_fold_into(
    jj: &JjEnv,
    store: &Path,
    target_branch: &str,
    source_branch: &str,
    dest: &str,
) -> Result<(), String> {
    rebase_branch_onto(jj, store, source_branch, dest)?;
    if branch_has_conflict(jj, store, source_branch)? {
        return Err(format!(
            "Refusing to merge: rebasing `{source_branch}` onto the advanced default branch `{target_branch}` recorded a conflict. Resolve the conflict markers in the workspace and let it re-seal, then merge again."
        ));
    }
    // The source is now a descendant of `dest` (and thus of the local target
    // bookmark, which `dest` advanced from), so this FF can never go backwards.
    merge_into_bookmark(jj, store, target_branch, source_branch)
}

/// Collapse a (possibly multi-commit) branch into a single commit on top of
/// `base_rev`, preserving its current tree. This restores the squash *shape* at
/// a default-branch landing: after the source is rebased onto the live default
/// tip, this rewrites the source bookmark to one commit whose parent is that tip
/// and whose tree equals the rebased source tree, so the FF fold lands exactly
/// one commit on the default branch instead of every per-change commit the agent
/// sealed. `message` becomes that commit's description (the PR title).
///
/// Operates entirely over the shared store with `--ignore-working-copy`
/// discipline (the store's `@` is a scratch working copy that must never be
/// snapshotted — Gotcha A, matching `merge_into_bookmark`/`rebase_branch_onto`).
/// Crucially the store's `@` is also never *moved*: `jj new --no-edit` creates
/// the squashed commit WITHOUT checking it out, so the working copy stays on its
/// scratch commit and a later plain (non-`--ignore-working-copy`) read — e.g.
/// `bookmark_commit` at the end of the fold — does not trip jj's stale-working-
/// copy guard.
///
/// Steps: capture the rebased tip (it carries the full source tree); create an
/// empty commit as a child of `base_rev`, addressing it by the set difference of
/// `base_rev`'s children before and after (`jj new` prints no machine-readable
/// id); repoint the bookmark to that empty commit; then `restore` the captured
/// tree INTO the bookmark. The restore mints a fresh commit id, so the bookmark
/// is moved FIRST and the restore targets the bookmark revset so it follows the
/// rewrite. The repoint is a deliberate sideways move — the squashed commit is
/// NOT a descendant of the old branch tip — so it passes `--allow-backwards`;
/// that hint is legitimate here (we are replacing the branch's own history with
/// an equivalent-tree single commit), unlike `merge_into_bookmark`, where the
/// same hint would clobber commits that advanced a shared target.
pub fn squash_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    base_rev: &str,
    message: &str,
) -> Result<(), String> {
    // The rebased tip still carries the complete source tree; capture it before
    // the bookmark is moved off it.
    let source_tree_rev = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("squash: branch `{branch}` did not resolve"))?;

    // Create an empty commit as a child of the live default tip, WITHOUT moving
    // `@`. `jj new` emits no machine-readable id, so address the new commit by
    // the set difference of `base_rev`'s children before and after.
    let before = base_children(jj, store, base_rev)?;
    jj.run(
        store,
        &[
            "new",
            "--no-edit",
            "-r",
            base_rev,
            "-m",
            message,
            "--ignore-working-copy",
        ],
        "jj new (squash base)",
    )?;
    let after = base_children(jj, store, base_rev)?;
    let mut added: Vec<String> = after.difference(&before).cloned().collect();
    let squashed = match added.len() {
        1 => added.remove(0),
        n => {
            return Err(format!(
                "squash: expected exactly one new commit on `{base_rev}`, found {n}"
            ))
        }
    };

    // Repoint the branch at the empty commit FIRST, then restore the source tree
    // INTO the bookmark so it follows the rewrite (`restore` mints a new id).
    // The repoint is a deliberate sideways move, so `--allow-backwards` is
    // correct here.
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &squashed,
            "--ignore-working-copy",
            "--allow-backwards",
        ],
        "jj bookmark set (squash)",
    )?;
    let branch_rev = format!("bookmarks(exact:{branch:?})");
    jj.run(
        store,
        &[
            "restore",
            "--from",
            &source_tree_rev,
            "--into",
            &branch_rev,
            "--ignore-working-copy",
        ],
        "jj restore (squash tree)",
    )?;
    // Export the rewritten bookmark to the backing git, as the fold path does,
    // so the project's `refs/heads/<branch>` tracks the squashed commit.
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (squash)",
    )
    .map(|_| ())
}

/// Commit ids of the direct children of `rev` in the shared store. Used to
/// address a freshly-created `jj new --no-edit` commit by set difference, since
/// `jj new` emits no machine-readable id.
fn base_children(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
) -> Result<std::collections::HashSet<String>, String> {
    let revset = format!("children({rev})");
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            &revset,
            "--no-graph",
            "--ignore-working-copy",
            "-T",
            "commit_id ++ \"\\n\"",
        ],
        "jj log (base children)",
    )?;
    Ok(out
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

/// Idempotently mark a remote bookmark as jj-tracked so a local push of it is
/// accepted. jj refuses to push a local bookmark whose `@origin` counterpart is
/// untracked ("Non-tracking remote bookmark … exists"), which happens when
/// origin's ref was created outside this store's jj. A no-op when already
/// tracked; errors (best-effort for the caller) when there is no such remote
/// bookmark, e.g. a no-remote project.
pub fn track_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    let remote_ref = format!("{branch}@origin");
    jj.run(
        store,
        &["bookmark", "track", &remote_ref],
        "jj bookmark track",
    )
    .map(|_| ())
}

/// Push an already-advanced store bookmark to origin with `--ignore-working-copy`
/// (Gotcha A: the store's default `@` may be stale after a fold/rebase). Used to
/// advance both the integration tip after a fold and a cleanly-rebased sibling's
/// PR head; jj's remote-tracking model accepts a rewritten bookmark without a
/// force-push.
pub fn push_store_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    jj.run(
        store,
        &[
            "git",
            "push",
            "--ignore-working-copy",
            "--remote",
            "origin",
            "--bookmark",
            branch,
        ],
        "jj git push store bookmark",
    )
    .map(|_| ())
}

/// Rebase a whole branch onto a destination over the shared store, non-blocking.
/// `--ignore-working-copy` because this is driven from the store, not the
/// sibling's workspace. A resulting conflict is recorded in the rebased commit
/// (the command still succeeds); the sibling's descendant `@` auto-rebases.
///
/// After the rebase, export the store's bookmarks back to git immediately. jj
/// moves the local bookmark during the rebase, and leaving the backing git ref at
/// the old commit produces a local-vs-`@git` conflicted bookmark; once conflicted,
/// idempotent descendant checks stop being reliable and later reconciles can keep
/// rewriting the branch. Exporting here keeps the two ref views in lockstep.
pub fn rebase_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &["rebase", "-b", branch, "-o", dest, "--ignore-working-copy"],
        "jj rebase",
    )?;
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (rebase)",
    )
    .map(|_| ())
}

/// Classify a jj error as the STALE-working-copy refusal family.
///
/// jj refuses every working-copy-touching command on a stale workspace (one
/// whose `@` a sibling workspace rewrote over the shared store) with the stable,
/// documented `working copy is stale` message. Both the seal (`jj commit`) and
/// the discard (`jj restore`) hit it, so the commit barrier's rollback must
/// classify and self-heal it rather than dead-end. Also classify the `seal_paths`
/// pre-commit "behind its branch tip" refusal: it is the same family (the
/// bookmark advanced past a rewritten `@`), and the write path recovers from it
/// the same way. The DISTINCT conflicted-branch refusal (a divergent `@` over a
/// bookmark whose tip carries a recorded conflict) is split off into
/// [`is_conflicted_branch_seal_error`] and deliberately NOT matched here: it must
/// preserve the working copy, not discard it.
///
/// Detection is by error-string because jj 0.42 exposes no non-snapshotting
/// staleness probe (`jj debug workingcopy` is gone; `--ignore-working-copy`
/// skips the check entirely). Centralized here with the jj phrasing cited so a
/// future jj rewording is a one-line change.
pub fn is_stale_error(msg: &str) -> bool {
    msg.contains("working copy is stale") || msg.contains("behind its branch tip")
}

/// Stable marker phrase for a seal that captured no change because the working
/// copy was reset under a concurrent store advance — the empty/divergent-seal
/// data-loss mode. Carried in the `Err` [`seal_paths`] returns when its
/// post-commit anomaly check fires, so the routing sites can recognize it.
const LOST_SEAL_MSG: &str =
    "seal captured no change (the working copy was reset under a concurrent store advance)";

/// Classify a jj error as the LOST-SEAL family: a `jj commit` that returned a sha
/// but sealed an empty or divergent commit because a concurrent op reset `@` out
/// from under it (silent data loss reported as a real commit). Kept distinct from
/// [`is_stale_error`] — the cause and jj phrasing differ — and OR'd with it at the
/// routing sites, because both are recoverable the same way: re-apply the batch
/// against the current base and re-seal.
pub fn is_lost_seal_error(msg: &str) -> bool {
    msg.contains(LOST_SEAL_MSG)
}

/// Stable marker phrase for a seal refused because the workspace head diverged
/// from a branch bookmark whose tip carries a recorded CONFLICT — the deliberate
/// resolve-at-base FLATTEN case. The agent moved `@` onto a fresh resolved line
/// off the current base while the bookmark still points at the conflicted
/// intermediate stack tip it is escaping; jj will not fold that conflicted
/// history, so sealing forward is refused. Unlike the clean "behind its branch
/// tip" refusal (genuine stale / coordinator-advance, recovered by discard +
/// update-stale), this MUST NOT discard: `@` holds real resolved work the discard
/// would destroy. Deliberately omits the "behind its branch tip" phrase so
/// [`is_stale_error`] does not match it. `pub(crate)` so the cross-module barrier
/// tests can reference the exact string without drift.
pub(crate) const CONFLICTED_BRANCH_SEAL_MSG: &str =
    "seal refused: branch tip carries a recorded conflict; sealing forward would advance onto the conflict";

/// Classify a jj error as the CONFLICTED-BRANCH seal refusal: the fast-forward
/// guard refused a seal because the branch bookmark tip carries a recorded
/// conflict and `@` has diverged from it (a deliberate resolve-at-base flatten).
/// Kept DISTINCT from [`is_stale_error`] and [`is_lost_seal_error`] — those
/// recover by discarding / re-sealing, but this one must PRESERVE the working
/// copy, because discarding destroys the resolved flatten and advancing lands
/// back on the conflict. The routing sites give it its own non-destructive arm
/// that preserves `@` and points at the git-workflow resolve-at-base flatten.
pub fn is_conflicted_branch_seal_error(msg: &str) -> bool {
    msg.contains(CONFLICTED_BRANCH_SEAL_MSG)
}

/// Refresh a workspace whose `@` was rebased out from under it. A rebased live
/// workspace goes stale; `update-stale` updates the on-disk files and
/// materializes any conflict markers for the agent to resolve.
pub fn update_stale(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    jj.run(
        ws,
        &["workspace", "update-stale"],
        "jj workspace update-stale",
    )
    .map(|_| ())
}

/// Whether any commit in `revset` carries a recorded conflict. `revset` is any
/// revset string — a bare bookmark name (`integration`), a remote ref
/// (`main@origin`), or a `bookmarks(exact:...)` expression. Used to vet a rebase
/// DEST before handing it to clean siblings: a conflicted dest must never
/// propagate down to children.
pub fn revset_has_conflict(jj: &JjEnv, store: &Path, revset: &str) -> Result<bool, String> {
    let out = jj.run(
        store,
        &["log", "-r", revset, "--no-graph", "-T", "self.conflict()"],
        "jj dest conflict check",
    )?;
    Ok(out.contains("true"))
}

/// Whether a bookmark's commit carries a recorded conflict. GitHub reports a
/// jj-conflicted commit as mergeable (and renders it as garbage), so the merge
/// gate trusts this over the GitHub mergeable bit for jj projects.
pub fn branch_has_conflict(jj: &JjEnv, store: &Path, branch: &str) -> Result<bool, String> {
    revset_has_conflict(jj, store, &format!("bookmarks(exact:{:?})", branch))
}

/// Enumerate the conflicting file paths in a workspace whose conflict markers are
/// already materialized on disk (callers run [`update_stale`] first). Runs IN the
/// workspace, not the bare store: `jj resolve --list` is working-copy-scoped, so
/// it must see the materialized `@`. Each output line is `<path>  <N-sided
/// conflict>`; the leading whitespace-delimited token is the path. Returns empty
/// on no conflicts (jj exits non-zero with "No conflicts found", mapped to an Err
/// and swallowed) or any other error — the file list is advisory detail on a note,
/// never load-bearing.
pub fn conflicted_files(jj: &JjEnv, ws_path: &Path) -> Vec<String> {
    let Ok(out) = jj.run(ws_path, &["resolve", "--list"], "jj resolve --list") else {
        return Vec::new();
    };
    out.lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(|token| token.to_string())
        .collect()
}

/// A commit carrying a recorded conflict, with the paths whose merge did not
/// resolve. Store-side detail for *reporting* which commits and files block a
/// merge — advisory enumeration only; the boolean [`branch_has_conflict`] stays
/// the load-bearing gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictedCommit {
    /// Short commit id.
    pub commit_id: String,
    /// Short change id.
    pub change_id: String,
    /// First line of the commit description.
    pub description: String,
    /// Conflicted file paths recorded in this commit.
    pub files: Vec<String>,
}

/// Enumerate the conflicted commits in `range_revset` (e.g. `"a..b"`, or a bare
/// `bookmarks(exact:...)`) with their conflicted file paths — store-side, no
/// workspace needed. Returns an empty Vec on no conflicts or any jj error: this
/// is advisory detail for a diagnostic, never load-bearing (mirrors
/// [`conflicted_files`]).
///
/// In jj a conflict in an ancestor propagates to every descendant until
/// resolved, so `<dest>..<branch> & conflicts()` names precisely the offending
/// commits. Fields are emitted unit-separated (`\x1f`) and the file list
/// record-separated (`\x1e`) so the parse stays robust against arbitrary
/// descriptions and paths.
pub fn conflicted_commits(jj: &JjEnv, store: &Path, range_revset: &str) -> Vec<ConflictedCommit> {
    // jj template, verified against jj 0.42: `\x1f`/`\x1e` are jj string escapes
    // that emit the literal control bytes we split on; `conflicts()` is the
    // revset and `self.conflicted_files()` yields store-side paths (no workspace).
    const TEMPLATE: &str = "commit_id.short() ++ \"\\x1f\" ++ change_id.short() ++ \"\\x1f\" ++ description.first_line() ++ \"\\x1f\" ++ self.conflicted_files().map(|f| f.path()).join(\"\\x1e\") ++ \"\\n\"";
    let revset = format!("({range_revset}) & conflicts()");
    let Ok(out) = jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", TEMPLATE],
        "jj conflicted commits",
    ) else {
        return Vec::new();
    };
    out.lines()
        .filter_map(|line| {
            let mut parts = line.split('\u{1f}');
            let commit_id = parts.next()?.trim().to_string();
            if commit_id.is_empty() {
                return None;
            }
            let change_id = parts.next().unwrap_or_default().trim().to_string();
            let description = parts.next().unwrap_or_default().trim().to_string();
            let files = parts
                .next()
                .map(|field| {
                    field
                        .split('\u{1e}')
                        .map(str::trim)
                        .filter(|p| !p.is_empty())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            Some(ConflictedCommit {
                commit_id,
                change_id,
                description,
                files,
            })
        })
        .collect()
}

/// Classification of a branch (already rebased onto its dest) for flatten
/// recovery. A base advance that re-applies a whole feature lineage onto a new
/// base can record conflicts in INTERMEDIATE commits while a later commit
/// resolves them, leaving the NET tip clean. jj still refuses to push or fold a
/// branch whose history contains ANY conflicted commit, so a mechanically-clean
/// tip is blocked by its own conflicted ancestors. This three-way split drives
/// the guarded flatten that clears them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlattenState {
    /// No recorded conflict anywhere in `dest..branch` — nothing to recover.
    Clean,
    /// The branch TIP itself carries a recorded conflict. A flatten preserves the
    /// tip tree, so it would NOT clear this — the agent must resolve the markers
    /// and re-seal. Kept distinct so callers escalate rather than flatten.
    TipConflicted,
    /// The tip is clean but an INTERMEDIATE commit in `dest..branch` carries a
    /// recorded conflict — flatten-recoverable: collapsing to one commit whose
    /// tree equals the clean tip discards the conflicted history without changing
    /// the net tree.
    IntermediateOnly,
}

/// Classify a branch for flatten recovery against `dest_revset` (any revset that
/// resolves to the dest tip — a bookmark name, `<default>@origin`, or a concrete
/// commit id). The tip is checked FIRST so a conflicted tip is never misread as a
/// recoverable intermediate: `conflicted_commits(dest..branch)` would include the
/// tip too, but a conflicted tip needs manual resolution, not a flatten.
pub fn flatten_state(
    jj: &JjEnv,
    store: &Path,
    dest_revset: &str,
    branch: &str,
) -> Result<FlattenState, String> {
    if branch_has_conflict(jj, store, branch)? {
        return Ok(FlattenState::TipConflicted);
    }
    let range = format!("({dest_revset})..bookmarks(exact:{branch:?})");
    if conflicted_commits(jj, store, &range).is_empty() {
        Ok(FlattenState::Clean)
    } else {
        Ok(FlattenState::IntermediateOnly)
    }
}

/// True when `tip_revset` resolves to a commit that already descends from (or
/// equals) `dest_commit`. `tip_revset` is any single-revision revset — a
/// `bookmarks(...)` expression, a `<name>@` workspace ref, etc. Implemented with
/// the DAG-range operator `dest::(tip)`: that range is non-empty exactly when
/// `tip` is reachable forward from `dest`, i.e. `tip` descends from or equals
/// `dest`. A resolve failure (a ref that does not exist) falls to `false` so the
/// caller rebases rather than wrongly skipping.
fn revset_descends_from(jj: &JjEnv, store: &Path, tip_revset: &str, dest_commit: &str) -> bool {
    let revset = format!("{dest_commit}::({tip_revset})");
    jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", "commit_id"],
        "jj descends check",
    )
    .ok()
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// True when `branch`'s tip already descends from (or equals) `dest_commit`.
/// Lets a reconcile skip re-rebasing an already-rebased branch so repeated or
/// serialized passes never re-rewrite an already-rebased commit (clean or
/// conflicted) — the structural idempotence that, paired with single-writer
/// serialization, stops new divergent conflicted copies from accumulating and
/// keeps a resolved clean bookmark from being dragged back onto a conflicted
/// twin.
pub fn branch_descends_from(jj: &JjEnv, store: &Path, branch: &str, dest_commit: &str) -> bool {
    revset_descends_from(
        jj,
        store,
        &format!("bookmarks(exact:{branch:?})"),
        dest_commit,
    )
}

/// The change-id of a bookmark's tip over the store, or empty when the bookmark
/// does not resolve. A jj *divergent* change's twins all share ONE change-id,
/// and the bookmark points at exactly one of them, so this returns that shared
/// id even mid-divergence. `--ignore-working-copy` keeps it store-driven.
pub fn change_id_of(jj: &JjEnv, store: &Path, branch: &str) -> String {
    jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("bookmarks(exact:{branch:?})"),
            "--no-graph",
            "-T",
            "change_id",
            "--ignore-working-copy",
        ],
        "change id of bookmark",
    )
    .map(|s| s.trim().to_string())
    .unwrap_or_default()
}

/// Every visible commit id sharing one change-id over the store. A healthy change
/// resolves to exactly one commit; a jj *divergent* change (the `<id>/0 /1 ...`
/// accumulation) resolves to several. The `change_id(...)` revset function is
/// used (not the bare id) because jj refuses a bare divergent change-id symbol.
pub fn visible_commit_ids_for_change(jj: &JjEnv, store: &Path, change_id: &str) -> Vec<String> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("change_id({change_id})"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "visible commits for change",
    )
    .unwrap_or_default()
    .lines()
    .map(|l| l.trim().to_string())
    .filter(|l| !l.is_empty())
    .collect()
}

/// Count the visible commits sharing one change-id over the store. Thin wrapper
/// over [`visible_commit_ids_for_change`].
pub fn visible_commits_for_change(jj: &JjEnv, store: &Path, change_id: &str) -> usize {
    visible_commit_ids_for_change(jj, store, change_id).len()
}

/// One twin of a (possibly) divergent change: its commit id and whether it
/// carries a recorded conflict.
struct Twin {
    commit: String,
    conflicted: bool,
}

/// The result of collapsing a (possibly) divergent change on a bookmark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollapseOutcome {
    /// The change resolves to 0 or 1 visible commit — nothing to collapse.
    NotDivergent,
    /// Exactly one non-conflicted twin survived: the bookmark now points at it
    /// and every other twin was abandoned.
    Collapsed {
        kept: String,
        abandoned: Vec<String>,
    },
    /// Canonicalization is ambiguous — every twin conflicts, or more than one
    /// carries edits. The store is left UNTOUCHED for a human to resolve.
    Ambiguous {
        change_id: String,
        twins: Vec<String>,
    },
}

/// Collapse a *pre-existing* divergent change on `branch` over the shared store.
/// New divergence is already prevented upstream (the per-store mutex plus the
/// idempotent [`reconcile_siblings`] skip); this heals a twin that forked BEFORE
/// that serialization landed, or via an external `jj` op outside Cairn's lock —
/// the cleanup `reconcile_siblings` never did, because it operates on each
/// bookmark's single tip and never touches the orphaned twin sharing the
/// change-id, so [`sealed_commit_is_lost`] keeps firing on every re-seal.
///
/// Resolves the bookmark tip's change-id, lists every visible commit sharing it,
/// and:
/// - 0 or 1 visible commit -> [`CollapseOutcome::NotDivergent`] (no-op);
/// - exactly one NON-conflicted twin -> keep it, `bookmark set` the bookmark to
///   it, abandon the conflicted twin(s) -> [`CollapseOutcome::Collapsed`];
/// - otherwise (every twin conflicts, or more than one clean twin) ->
///   [`CollapseOutcome::Ambiguous`] with NO mutation.
///
/// The single-clean-twin rule is the deliberate hybrid: the lone resolved commit
/// the agent produced is the unambiguous keep, but choosing among several clean
/// twins (or among only-conflicted ones) would be a guess on the shared store
/// that could lose work, so those tangles surface to a human instead. There is
/// deliberately NO descendancy tiebreak among multiple clean twins.
///
/// Mutations use `--ignore-working-copy` (store-driven, like `reconcile_siblings`).
/// The caller MUST hold the per-store lock so the collapse cannot itself fork.
pub fn collapse_divergent_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<CollapseOutcome, String> {
    // A bookmark that does not resolve to a local tip (missing, or a
    // remote-tracking dest like `main@origin`) has nothing to collapse here.
    if bookmark_commit(jj, store, branch).is_none() {
        return Ok(CollapseOutcome::NotDivergent);
    }
    let change_id = change_id_of(jj, store, branch);
    if change_id.is_empty() {
        return Ok(CollapseOutcome::NotDivergent);
    }

    let commit_ids = visible_commit_ids_for_change(jj, store, &change_id);
    if commit_ids.len() <= 1 {
        return Ok(CollapseOutcome::NotDivergent);
    }

    // Classify each twin by whether it carries a recorded conflict. A transient
    // check error falls to `false` (treat as clean) — the same
    // liveness-over-strictness convention as the reconcile guards; the
    // single-clean-twin rule below still surfaces anything genuinely ambiguous.
    let twins: Vec<Twin> = commit_ids
        .into_iter()
        .map(|commit| {
            let conflicted = revset_has_conflict(jj, store, &commit).unwrap_or(false);
            Twin { commit, conflicted }
        })
        .collect();

    let clean: Vec<&Twin> = twins.iter().filter(|twin| !twin.conflicted).collect();
    if clean.len() != 1 {
        return Ok(CollapseOutcome::Ambiguous {
            change_id,
            twins: twins.into_iter().map(|twin| twin.commit).collect(),
        });
    }

    let kept = clean[0].commit.clone();
    let abandoned: Vec<String> = twins
        .iter()
        .filter(|twin| twin.commit != kept)
        .map(|twin| twin.commit.clone())
        .collect();

    // Point the bookmark at the surviving clean twin FIRST (so it never strands
    // on a commit we are about to abandon), then drop every other twin.
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &kept,
            "--ignore-working-copy",
        ],
        "collapse: point bookmark at the clean twin",
    )?;
    for commit in &abandoned {
        jj.run(
            store,
            &["abandon", commit, "--ignore-working-copy"],
            "collapse: abandon divergent twin",
        )?;
    }
    Ok(CollapseOutcome::Collapsed { kept, abandoned })
}

/// The set of paths a branch changes relative to a dest, plus which of those are
/// deletions. Captured before a flatten and re-checked after: [`squash_branch_onto`]
/// restores the exact clean-tip tree, so the post-flatten footprint MUST match
/// the pre-flatten one. A footprint mismatch — above all a NEW deletion of a dest
/// file — means the flatten re-parented a stale tree (wrong base/tip) and would
/// silently revert base files, so the guard rejects it.
#[derive(Debug, Default, PartialEq, Eq)]
struct BranchFootprint {
    changed: std::collections::BTreeSet<String>,
    deletions: std::collections::BTreeSet<String>,
}

/// Compute the tree diff footprint of `dest_commit..tip_commit` over the store.
/// Uses the same `jj diff --git` → [`parse_git_diff`] path as `node_changed_files`,
/// so a rename is decomposed into an added new path plus a deleted old path.
fn branch_footprint(
    jj: &JjEnv,
    store: &Path,
    dest_commit: &str,
    tip_commit: &str,
) -> Result<BranchFootprint, String> {
    let out = jj.run(
        store,
        &[
            "diff",
            "--ignore-working-copy",
            "--git",
            "--from",
            dest_commit,
            "--to",
            tip_commit,
        ],
        "jj diff (flatten footprint)",
    )?;
    let mut footprint = BranchFootprint::default();
    for change in parse_git_diff(&out) {
        footprint.changed.insert(change.path.clone());
        if change.status == "deleted" {
            footprint.deletions.insert(change.path.clone());
        }
        if let Some(prev) = change.previous_path {
            // A rename removes the old path from the tree.
            footprint.changed.insert(prev.clone());
            footprint.deletions.insert(prev);
        }
    }
    Ok(footprint)
}

/// The full description of a bookmark's tip over the store, trimmed, or empty when
/// the bookmark does not resolve. A reconcile-time flatten preserves the branch's
/// own seal message (not a PR title) by passing this as the squash description.
pub fn branch_description(jj: &JjEnv, store: &Path, branch: &str) -> String {
    jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("bookmarks(exact:{branch:?})"),
            "--no-graph",
            "-T",
            "description",
            "--ignore-working-copy",
        ],
        "branch description",
    )
    .map(|s| s.trim().to_string())
    .unwrap_or_default()
}

/// Reset `branch` back to `tip` (a deliberate sideways/backwards move) and
/// re-export the ref to git. Used to UNDO a [`squash_branch_onto`] when a
/// post-squash guard in [`flatten_branch_recovery`] refuses the result: the squash
/// has already moved the bookmark, so without this a rejected flatten would leave
/// the visible branch rewritten. The git export is best-effort (a stale ref
/// self-heals on the next seal); the bookmark move is load-bearing and propagated.
fn restore_bookmark(jj: &JjEnv, store: &Path, branch: &str, tip: &str) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            tip,
            "--allow-backwards",
            "--ignore-working-copy",
        ],
        "flatten: restore bookmark after guard failure",
    )?;
    let _ = jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "flatten: git export after restore",
    );
    Ok(())
}

/// The store's current operation id (the newest entry in the op log). Paired with
/// [`restore_operation`] to snapshot store state before a multi-step mutation and
/// rewind to it if a later step fails.
///
/// EXACT ONLY UNDER THE PER-STORE LOCK: the id is only a faithful "pre-mutation"
/// marker if no other writer interleaves an op before the matching restore. The
/// caller MUST hold the per-store lock (as the merge fold and every Cairn jj
/// writer do via `resolve_store_lock`), under which every op between snapshot and
/// restore is the caller's own.
pub fn operation_id(jj: &JjEnv, store: &Path) -> Result<String, String> {
    jj.run(
        store,
        &[
            "op",
            "log",
            "--no-graph",
            "-n",
            "1",
            "-T",
            "id",
            "--ignore-working-copy",
        ],
        "jj op id",
    )
    .map(|s| s.trim().to_string())
}

/// Rewind the whole store to a prior operation `op_id` (an exact undo of every
/// bookmark move and commit rewrite since it), then re-export the backing git
/// refs so `refs/heads/*` realign with the restored bookmarks. Used to roll a
/// partially-applied merge back to its pre-merge snapshot so a push failure never
/// leaves local bookmarks diverged from origin.
///
/// EXACT ONLY UNDER THE PER-STORE LOCK: `jj op restore` restores whole-store
/// state, so any op another writer interleaved between the [`operation_id`]
/// snapshot and this restore would also be undone. The caller MUST hold the
/// per-store lock; under it every op since the snapshot is the caller's own and
/// the restore is precise.
pub fn restore_operation(jj: &JjEnv, store: &Path, op_id: &str) -> Result<(), String> {
    jj.run(
        store,
        &["op", "restore", op_id, "--ignore-working-copy"],
        "jj op restore",
    )?;
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (after op restore)",
    )
    .map(|_| ())
}

/// Local bookmarks that ride a commit inside `range_revset`, excluding `exclude`.
/// A base advance that bakes conflicts into a branch's intermediate commits can
/// leave a SIBLING bookmark pointing at one of those intermediates (its own tip
/// is one of this branch's lineage commits, so it has no seals of its own). When
/// this branch is flattened, that rider is orphaned onto an abandoned lineage;
/// [`flatten_branch_recovery`] re-points each such rider onto the flattened
/// commit. `local_bookmarks` is the jj 0.42 template keyword for a commit's local
/// bookmarks; `--ignore-working-copy` keeps the enumeration store-driven.
fn local_bookmarks_in_range(
    jj: &JjEnv,
    store: &Path,
    range_revset: &str,
    exclude: &str,
) -> Vec<String> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            range_revset,
            "--no-graph",
            "-T",
            "local_bookmarks.map(|b| b.name()).join(\"\\n\") ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (rider bookmarks in range)",
    )
    .unwrap_or_default()
    .lines()
    .map(str::trim)
    .filter(|name| !name.is_empty() && *name != exclude)
    .map(ToOwned::to_owned)
    .collect::<std::collections::BTreeSet<_>>()
    .into_iter()
    .collect()
}

/// The outcome of a guarded flatten recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenReport {
    /// The single commit the branch now points at: its tree equals the clean
    /// rebased tip and its parent is `dest_commit`.
    pub flattened_commit: String,
    /// How many conflicted intermediate commits the flatten collapsed away
    /// (advisory count).
    pub collapsed_conflicted_commits: usize,
    /// Orphaned twins of the PRE-flatten change-id that were abandoned in the
    /// twin-cleanup pass.
    pub abandoned_twins: Vec<String>,
    /// Sibling bookmarks that rode a flattened-away intermediate commit and were
    /// re-pointed onto the flattened commit (so a later `reconcile_siblings` does
    /// not resurrect the orphaned lineage).
    pub repointed_bookmarks: Vec<String>,
}

/// Guarded flatten recovery for a clean-tip / conflicted-intermediate branch.
///
/// A base advance that re-applies a whole lineage onto a new base can bake
/// conflicts into intermediate commits while a later commit resolves them, so the
/// NET tip is clean but jj still refuses to push/fold the branch (its history
/// contains conflicted commits). This collapses the branch to ONE commit on
/// `dest_commit` whose tree equals the clean rebased tip — clearing the conflicted
/// history while preserving the exact net tree — behind two guards:
///
/// - **Pre-guard:** the branch tip must genuinely descend from `dest_commit`.
///   Otherwise the squash would re-parent a stale tree onto dest and revert dest's
///   own files (the wrong-base reversion). Fails with a typed guard error.
/// - **Post-guard (footprint):** after the squash, the flattened tree must equal
///   the pre-flatten tip tree AND the flatten must delete no dest path that was
///   not already a deletion in the pre-flatten footprint. A violation is a
///   wrong-base/wrong-tip bug; the caller escalates rather than landing a
///   footprint-unsafe flatten.
///
/// Then a **twin-cleanup** pass: the squash mints a fresh change-id, so any commit
/// still sharing the PRE-flatten change-id is an orphaned twin (including the
/// "every twin conflicted" divergence that [`collapse_divergent_bookmark`] cannot
/// resolve). Now that a clean flattened commit exists they are safe to abandon.
///
/// The caller MUST hold the per-store lock (like [`collapse_divergent_bookmark`])
/// so the flatten cannot itself fork the op log. `message` becomes the flattened
/// commit's description (a PR title at a default landing, the branch's own seal
/// message at a reconcile).
pub fn flatten_branch_recovery(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: &str,
    message: &str,
) -> Result<FlattenReport, String> {
    let branch_revset = format!("bookmarks(exact:{branch:?})");

    // Pre-guard: the branch must genuinely descend from the dest it is about to be
    // flattened onto, or the squash re-parents a stale tree and reverts base files.
    if !revset_descends_from(jj, store, &branch_revset, dest_commit) {
        return Err(format!(
            "flatten guard: branch `{branch}` does not descend from dest `{dest_commit}`; refusing to flatten (would re-parent a stale tree and revert base files)"
        ));
    }

    let pre_tip = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("flatten: branch `{branch}` did not resolve"))?;
    let pre_change_id = change_id_of(jj, store, branch);
    let pre_footprint = branch_footprint(jj, store, dest_commit, &pre_tip)?;
    let pre_tree = sealed_tree_hash_via_git(jj, store, &pre_tip).ok();
    let collapsed_conflicted_commits =
        conflicted_commits(jj, store, &format!("{dest_commit}..{branch_revset}")).len();

    // Enumerate sibling bookmarks riding the about-to-be-flattened lineage BEFORE
    // the squash (afterwards the range collapses and their commits leave it). Any
    // bookmark in `dest..branch` other than `branch` itself sits on one of this
    // branch's own lineage commits and would be orphaned onto an abandoned line by
    // the flatten; it is re-pointed onto the flattened commit once the guards pass.
    let riders = local_bookmarks_in_range(
        jj,
        store,
        &format!("{dest_commit}..{branch_revset}"),
        branch,
    );

    // Collapse the branch to one commit on dest whose tree = the clean rebased tip.
    squash_branch_onto(jj, store, branch, dest_commit, message)?;

    // From here on the bookmark has ALREADY been rewritten by the squash, so every
    // post-squash failure path — a footprint mismatch, a transient tree-hash read,
    // or an unresolved post tip — must first restore the bookmark to `pre_tip`, or a
    // refused flatten would leave the visible branch mutated despite reporting a
    // refusal (the load-bearing safety guarantee). `fail` performs that restore and
    // returns the reason to hand back as the `Err`.
    let fail = |reason: String| -> String {
        if let Err(e) = restore_bookmark(jj, store, branch, &pre_tip) {
            log::warn!(
                "flatten: failed to restore bookmark {branch} to {pre_tip} after a post-squash guard failure: {e}"
            );
        }
        reason
    };

    let post_tip = match bookmark_commit(jj, store, branch) {
        Some(commit) => commit,
        None => {
            return Err(fail(format!(
                "flatten: branch `{branch}` did not resolve after squash"
            )))
        }
    };

    // Post-guard (footprint): a wrong-base/wrong-tip squash would delete dest files
    // the branch never touched. Reject any NEW deletion, naming the offending paths.
    let post_footprint = match branch_footprint(jj, store, dest_commit, &post_tip) {
        Ok(footprint) => footprint,
        Err(e) => return Err(fail(e)),
    };
    let new_deletions: Vec<String> = post_footprint
        .deletions
        .difference(&pre_footprint.deletions)
        .cloned()
        .collect();
    if !new_deletions.is_empty() {
        return Err(fail(format!(
            "flatten footprint guard: flattening `{branch}` onto `{dest_commit}` would delete base file(s) the branch did not delete: {}. Refusing (wrong-base/wrong-tip flatten).",
            new_deletions.join(", ")
        )));
    }
    // The restore should have copied the tip tree exactly; verify byte-for-byte via
    // the git tree object when it resolves (advisory fallback: skip on git hiccup).
    if let Some(pre_tree) = pre_tree {
        let post_tree = match sealed_tree_hash_via_git(jj, store, &post_tip) {
            Ok(tree) => tree,
            Err(e) => return Err(fail(e)),
        };
        if post_tree != pre_tree {
            return Err(fail(format!(
                "flatten footprint guard: flattened `{branch}` tree {post_tree} does not match the pre-flatten tip tree {pre_tree}. Refusing (wrong-base/wrong-tip flatten)."
            )));
        }
    }

    // Re-point every rider sibling onto the flattened commit BEFORE the twin
    // cleanup, so a rider that happened to sit on the pre-flatten tip is moved off
    // it before that lineage is abandoned. Best-effort per bookmark with logging: a
    // failed re-point leaves the rider on the orphaned lineage (the same state as
    // before this recovery existed), so the good flatten still stands.
    let mut repointed_bookmarks = Vec::new();
    for rider in riders {
        match jj.run(
            store,
            &[
                "bookmark",
                "set",
                &rider,
                "-r",
                &post_tip,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "flatten: re-point rider bookmark",
        ) {
            Ok(_) => {
                let _ = jj.run(
                    store,
                    &["git", "export", "--ignore-working-copy"],
                    "flatten: git export after rider re-point",
                );
                repointed_bookmarks.push(rider);
            }
            Err(e) => log::warn!("flatten: re-pointing rider bookmark {rider} failed: {e}"),
        }
    }

    // Twin cleanup: the squash minted a fresh change-id, so any commit still
    // sharing the PRE-flatten change-id is an orphaned twin (the old lineage tip,
    // or every twin of an ambiguous conflicted divergence). Drop them now that a
    // clean flattened commit exists.
    let flattened_change_id = change_id_of(jj, store, branch);
    let mut abandoned_twins = Vec::new();
    if !pre_change_id.is_empty() && pre_change_id != flattened_change_id {
        for commit in visible_commit_ids_for_change(jj, store, &pre_change_id) {
            if commit == post_tip {
                continue;
            }
            match jj.run(
                store,
                &["abandon", &commit, "--ignore-working-copy"],
                "flatten: abandon orphaned twin",
            ) {
                Ok(_) => abandoned_twins.push(commit),
                Err(e) => log::warn!("flatten: abandoning orphaned twin {commit} failed: {e}"),
            }
        }
    }

    Ok(FlattenReport {
        flattened_commit: post_tip,
        collapsed_conflicted_commits,
        abandoned_twins,
        repointed_bookmarks,
    })
}

/// Classify one sibling that is already positioned on `dest_commit` (either it was
/// just rebased there, or it already descended from it) into the reconcile report,
/// applying proactive flatten recovery for the clean-tip / conflicted-intermediate
/// case. `dest_commit` is `None` only when the reconcile dest was unresolvable, in
/// which case this falls back to the bare tip-conflict check (liveness over
/// strictness). `push_clean` advances a cleanly-rebased sibling's PR head on
/// origin; a FLATTENED sibling is always pushed regardless, because the flatten
/// rewrote its commit id and origin's PR head must follow. The caller holds the
/// per-store lock, so the flatten cannot fork.
fn classify_reconciled_sibling(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    ws_path: &Path,
    dest_commit: Option<&str>,
    push_clean: bool,
    report: &mut ReconcileReport,
) {
    let state = dest_commit.and_then(|dest| flatten_state(jj, store, dest, branch).ok());
    match state {
        // No concrete dest to classify against: fall back to the tip-conflict check.
        None => match branch_has_conflict(jj, store, branch) {
            Ok(true) => report.conflicted.push(branch.to_string()),
            Ok(false) => {
                if push_clean {
                    if let Err(e) = push_store_bookmark(jj, store, branch) {
                        log::warn!("jj reconcile: push rebased sibling {branch} failed: {e}");
                    }
                }
                report.rebased_clean.push(branch.to_string());
            }
            Err(e) => {
                log::warn!("jj reconcile: conflict check for {branch} failed: {e}");
                report.rebased_clean.push(branch.to_string());
            }
        },
        Some(FlattenState::Clean) => {
            if push_clean {
                if let Err(e) = push_store_bookmark(jj, store, branch) {
                    log::warn!("jj reconcile: push rebased sibling {branch} failed: {e}");
                }
            }
            report.rebased_clean.push(branch.to_string());
        }
        // A genuinely-conflicted tip needs the agent to resolve markers; a
        // conflicted commit can never push, so it is stop-the-line.
        Some(FlattenState::TipConflicted) => report.conflicted.push(branch.to_string()),
        // Clean tip, conflicted intermediates: heal it in place so the branch stays
        // pushable/mergeable at all times (no silent seal-push failure, no
        // misleading "clean" note while the merge is actually blocked).
        Some(FlattenState::IntermediateOnly) => {
            let dest = dest_commit.expect("IntermediateOnly implies a concrete dest");
            let desc = branch_description(jj, store, branch);
            let message = if desc.is_empty() {
                format!("Flatten {branch} onto base (auto-recovery)")
            } else {
                desc
            };
            match flatten_branch_recovery(jj, store, branch, dest, &message) {
                Ok(recovered) => {
                    // Re-parent the sibling's live workspace onto the flattened
                    // commit so its `@` no longer sits on the abandoned conflicted
                    // line; this refreshes the on-disk files via update-stale.
                    if let Err(e) = advance_workspace_onto(
                        jj,
                        store,
                        ws_path,
                        branch,
                        &recovered.flattened_commit,
                    ) {
                        log::warn!(
                            "jj reconcile: re-parent workspace {branch} onto flattened tip failed: {e}"
                        );
                    }
                    // The flatten rewrote the commit id, so the PR head must move
                    // even when a plain clean rebase would have been skipped.
                    if let Err(e) = push_store_bookmark(jj, store, branch) {
                        log::warn!("jj reconcile: push flattened sibling {branch} failed: {e}");
                    }
                    log::info!(
                        "jj reconcile: flattened {branch} ({} conflicted intermediate(s) collapsed, {} twin(s) abandoned)",
                        recovered.collapsed_conflicted_commits,
                        recovered.abandoned_twins.len()
                    );
                    report.rebased_clean.push(branch.to_string());
                }
                Err(e) => {
                    // Guard failure: a footprint-unsafe flatten. Fall back to a
                    // conflicted classification so the agent is interrupted rather
                    // than the branch silently wedged.
                    log::warn!(
                        "jj reconcile: flatten of {branch} refused by guard ({e}); classifying conflicted"
                    );
                    report.conflicted.push(branch.to_string());
                }
            }
        }
    }
}

/// Reconcile in-flight siblings onto the locally-advanced integration tip: the
/// store already owns the merge (the child's commit was folded into the
/// integration bookmark by `merge_into_bookmark`), so there is no fetch or origin
/// round-trip — each sibling bookmark rebases non-blockingly onto the local
/// integration bookmark, its workspace refreshes, and a cleanly-rebased sibling
/// is pushed so its PR head advances on origin. This replaces the git "notify
/// each sibling to rebase + force-push" tax: conflicts are recorded (not
/// blocking), change-IDs are preserved, and no force-push is needed.
///
/// `siblings` pairs each in-flight sibling's bookmark with its workspace dir.
/// Every step is best-effort per sibling: a failure on one is logged and the
/// rest proceed. A conflicted sibling is not pushed (and `jj git push` would
/// refuse it anyway — the self-enforcing backstop). Returns which siblings landed
/// clean versus conflicted.
pub fn reconcile_siblings(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[(String, PathBuf)],
) -> Result<ReconcileReport, String> {
    let mut report = ReconcileReport::default();
    // Resolve the rebase dest to a concrete commit id ONCE up front: it may be a
    // bookmark name or a `<default>@origin` remote ref, and pinning it keeps the
    // dest from moving mid-loop and lets each sibling check whether it already
    // descends from this exact commit. None (unresolvable dest) disables the
    // skip and falls back to the unconditional rebase below.
    let dest_commit = jj
        .run(
            store,
            &[
                "log",
                "-r",
                integration_branch,
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "jj resolve reconcile dest commit",
        )
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Stop-the-line guard: never hand a conflicted base to clean siblings. If the
    // rebase dest itself carries a recorded conflict, hold every sibling on its
    // prior clean commit (no rewrite, nothing pushed) and classify them `held` —
    // neither rebased_clean nor conflicted, so the notify layer fires nothing for
    // them. The conflict must be resolved AT THE BASE first; the next reconcile
    // sees a clean dest, the guard does not fire, and the per-sibling descends
    // skip handles the rest (self-clearing). A transient check error falls to
    // `false` (proceed) — the same liveness-over-strictness convention as
    // `revset_descends_from`; only a confirmed conflicted dest holds the line, so
    // a flaky check never wedges every reconcile.
    if revset_has_conflict(jj, store, integration_branch).unwrap_or(false) {
        log::warn!(
            "jj reconcile: rebase dest {integration_branch} carries a conflict; holding {} sibling(s) off the conflicted base",
            siblings.len()
        );
        report.held = siblings.iter().map(|(branch, _)| branch.clone()).collect();
        return Ok(report);
    }

    for (branch, ws_path) in siblings {
        // Idempotent skip: when the sibling already descends from the exact dest
        // commit, a re-rebase would re-rewrite its (clean or conflicted) commit
        // and, under concurrency, mint a divergent copy — and it would drag a
        // resolved clean bookmark back. Skip the rebase and the stale refresh
        // entirely (no rewrite), but still classify the branch so the report
        // stays accurate. A skipped clean sibling is not re-pushed: its PR head
        // was already advanced by the reconcile that first put it on this dest.
        let already_on_dest = dest_commit
            .as_deref()
            .map(|dest| branch_descends_from(jj, store, branch, dest))
            .unwrap_or(false);
        if already_on_dest {
            // No rebase/stale-refresh needed and (per the comment above) a plain
            // clean sibling was already pushed by the reconcile that first put it
            // here — but flatten recovery still runs, because a sibling that
            // arrived here with conflicted intermediates (a prior reconcile that
            // rebased but predates this healing) is otherwise wedged: clean tip,
            // unpushable history. `classify_reconciled_sibling` no-ops on a truly
            // clean branch and flattens+pushes only the intermediate-only case.
            classify_reconciled_sibling(
                jj,
                store,
                branch,
                ws_path,
                dest_commit.as_deref(),
                false,
                &mut report,
            );
            continue;
        }
        if let Err(e) = rebase_branch_onto(jj, store, branch, integration_branch) {
            log::warn!("jj reconcile: rebase {branch} onto {integration_branch} failed: {e}");
            continue;
        }
        if let Err(e) = update_stale(jj, ws_path) {
            log::warn!(
                "jj reconcile: update-stale {} failed: {e}",
                ws_path.display()
            );
        }
        classify_reconciled_sibling(
            jj,
            store,
            branch,
            ws_path,
            dest_commit.as_deref(),
            true,
            &mut report,
        );
    }
    Ok(report)
}

/// Advance an active workspace that sits ON `dest`'s branch (a Coordinator on its
/// integration bookmark) after that bookmark was folded forward out from under
/// the workspace's working copy. The sibling auto-rebase ([`reconcile_siblings`])
/// only touches workspaces branched *from* the branch; the workspace *on* the
/// branch has its `@` re-parented onto the new tip here, then its on-disk files
/// refreshed — the jj-native form of the old git "post-merge fast-forward of
/// active worktrees".
///
/// Store-driven for consistency with [`reconcile_siblings`]: `jj rebase -s
/// <name>@ -o <dest>` over the store re-parents the workspace's working-copy
/// commit (`<name>` is [`workspace_name_for_branch`]), then [`update_stale`]
/// materializes the new files (and any conflict markers) on disk.
/// `--ignore-working-copy` because the rebase is driven from the store, not the
/// workspace. Idempotent: when `@` already sits on `dest`, jj reports "Nothing
/// changed" and this is a no-op, so it is safe under the merge/webhook
/// double-fire.
///
/// Guaranteed-safe by the forward-only fold: a successful `merge_into_bookmark`
/// means the new tip is a descendant of the prior integration bookmark, hence of
/// every coordinator seal, so re-parenting the coordinator's empty/idle `@` onto
/// it never drops coordinator work.
///
/// Returns whether the re-parent recorded a conflict (non-blocking: jj
/// materializes it for the agent rather than failing). A Coordinator's `@` is
/// empty/idle, so re-parenting it never conflicts in practice; the signal exists
/// so a caller can wake a workspace that somehow lands on a conflicted `@` rather
/// than leaving it idle there.
pub fn advance_workspace_onto(
    jj: &JjEnv,
    store: &Path,
    ws_path: &Path,
    ws_branch: &str,
    dest: &str,
) -> Result<bool, String> {
    let source = format!("{}@", workspace_name_for_branch(ws_branch));
    // Idempotent skip: when `@` already descends from `dest`, a re-rebase would
    // re-rewrite the working-copy commit (and could mint a divergent copy under
    // the merge/webhook double-fire). `dest` here is a concrete commit id
    // (resolved by the caller via `bookmark_commit`). Nothing to move, no
    // conflict to surface — report a clean no-op.
    if revset_descends_from(jj, store, &source, dest) {
        return Ok(false);
    }
    jj.run(
        store,
        &["rebase", "-s", &source, "-o", dest, "--ignore-working-copy"],
        "jj rebase (advance workspace onto branch tip)",
    )?;
    // Refresh the live workspace: the rebase rewrote its `@` out from under it.
    update_stale(jj, ws_path)?;
    // A conflict from the re-parent lands in the rewritten working-copy commit.
    let out = jj.run(
        ws_path,
        &["log", "-r", "@", "--no-graph", "-T", "self.conflict()"],
        "jj advance conflict check",
    )?;
    Ok(out.contains("true"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn jj_bin() -> Option<String> {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "jj".to_string());
        crate::env::command(&bin)
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
            .then_some(bin)
    }

    #[test]
    fn base_marker_round_trips() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join(".jj")).unwrap();

        // Absent marker reads as None.
        assert_eq!(read_base_marker(dir.path()), None);

        // Branch + rev round-trip through the two-line format, landing beside
        // the branch marker in the non-snapshotted `.jj` dir.
        write_base_marker(dir.path(), "agent/CAIRN-2091-coordinator-0", "e4555f70").unwrap();
        assert_eq!(
            read_base_marker(dir.path()),
            Some((
                "agent/CAIRN-2091-coordinator-0".to_string(),
                "e4555f70".to_string()
            ))
        );
        assert!(dir.path().join(".jj").join("cairn-base").exists());

        // A branch-only marker yields an empty rev rather than failing.
        write_base_marker(dir.path(), "main", "").unwrap();
        assert_eq!(
            read_base_marker(dir.path()),
            Some(("main".to_string(), String::new()))
        );
    }

    /// Provision a real non-colocated workspace, record the base marker as
    /// production does (after `add_workspace`), and assert it persists across a
    /// seal — the `.jj` dir is never snapshotted, so the marker is invisible to
    /// the working-copy commit, exactly like the branch marker.
    #[test]
    #[serial_test::serial(jj)]
    fn base_marker_provisions_and_survives_a_seal() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping base_marker_provisions_and_survives_a_seal: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();
        write_base_marker(&ws, "main", "deadbeef").unwrap();
        assert_eq!(
            read_base_marker(&ws),
            Some(("main".to_string(), "deadbeef".to_string()))
        );

        // Seal real work; the marker still reads (non-snapshotted).
        std::fs::write(ws.join("f.rs"), "code\n").unwrap();
        seal(&jj, &ws, "work", None).unwrap();
        assert_eq!(
            read_base_marker(&ws),
            Some(("main".to_string(), "deadbeef".to_string()))
        );
    }

    /// CAIRN-2260 (b)+(c): a `when:write` check that rewrites a TRACKED file folds
    /// that edit into the just-sealed commit and leaves `@` clean; a GITIGNORED
    /// write the same check makes is neither folded into the commit nor counted as
    /// working-copy dirt.
    #[test]
    #[serial_test::serial(jj)]
    fn fold_worktree_into_seal_amends_tracked_and_skips_gitignored() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping fold_worktree_into_seal_amends_tracked_and_skips_gitignored: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let branch = "agent/CAIRN-1-builder-0";
        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

        // write1: a normal path-scoped seal of source + a .gitignore that ignores caches.
        std::fs::create_dir_all(ws.join("src")).unwrap();
        std::fs::write(ws.join("src/foo.ts"), "const x=1\n").unwrap();
        std::fs::write(ws.join(".gitignore"), "*.cache\n").unwrap();
        seal_paths(&jj, &ws, "edit1", None, &["src/foo.ts", ".gitignore"]).unwrap();
        let sealed_before = head_commit(&jj, &ws).unwrap();

        // The check reformats the tracked source AND writes a gitignored cache.
        std::fs::write(ws.join("src/foo.ts"), "const x = 1;\n").unwrap();
        std::fs::write(ws.join("vitest.cache"), "junk\n").unwrap();

        let outcome = fold_worktree_into_seal(&jj, &ws)
            .unwrap()
            .expect("a tracked reformat folds into the seal");
        assert_eq!(outcome.folded_files, vec!["src/foo.ts".to_string()]);

        // `@` is clean == the amended tip; the seal's commit id changed (amended).
        assert!(
            !is_working_copy_dirty(&jj, &ws).unwrap(),
            "@ is clean after the fold"
        );
        assert_ne!(
            sealed_before,
            head_commit(&jj, &ws).unwrap(),
            "the seal was amended in place"
        );

        // The reformat is in the commit; the gitignored cache is not.
        let foo_in_commit = jj
            .run(&ws, &["file", "show", "-r", "@-", "src/foo.ts"], "show foo")
            .unwrap();
        assert!(
            foo_in_commit.contains("const x = 1;"),
            "the reformatted source is folded into the commit: {foo_in_commit}"
        );
        let committed = jj
            .run(&ws, &["diff", "-r", "@-", "--name-only"], "committed files")
            .unwrap();
        assert!(
            committed.contains("src/foo.ts"),
            "the source file is in the amended commit: {committed}"
        );
        assert!(
            !committed.contains("vitest.cache"),
            "the gitignored cache must NOT be committed: {committed}"
        );
    }

    /// CAIRN-2260 (a): with the check's changes folded into the seal, a concurrent
    /// base advance (a sibling merge that rebases this workspace) in the lock-free
    /// check window leaves the NEXT write's seal clean — no stale / divergent /
    /// behind-tip wedge, and no divergent `@` twin (the bug's signature).
    #[test]
    #[serial_test::serial(jj)]
    fn folded_check_keeps_next_seal_clean_under_a_concurrent_advance() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping folded_check_keeps_next_seal_clean_under_a_concurrent_advance: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Coordinator integration bookmark + one sibling builder branched off it.
        let int = "agent/CAIRN-1-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let branch = "agent/CAIRN-2-builder-0";
        let ws = wts.path().join("builder");
        add_workspace(&jj, &store, &ws, branch, int, None).unwrap();

        // write1: a path-scoped seal on the builder.
        std::fs::write(ws.join("a.rs"), "fn a() {}\n").unwrap();
        seal_paths(&jj, &ws, "edit1", None, &["a.rs"]).unwrap();

        // The check reformats a.rs (tracked dirt in @); the fix folds it into the seal.
        std::fs::write(ws.join("a.rs"), "fn a() {} // fmt\n").unwrap();
        fold_worktree_into_seal(&jj, &ws)
            .unwrap()
            .expect("the tracked reformat folds into the seal");
        assert!(
            !is_working_copy_dirty(&jj, &ws).unwrap(),
            "@ is clean after the fold"
        );

        // A child merges into the integration branch: advance its tip with a
        // different file, then reconcile rebases the sibling onto the new tip
        // (the concurrent advance that, on a check-dirtied @, used to wedge).
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("z.rs"), "fn z() {}\n").unwrap();
        jj.run(&store, &["describe", "-m", "child merged"], "describe")
            .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();
        let report =
            reconcile_siblings(&jj, &store, int, &[(branch.to_string(), ws.clone())]).unwrap();
        assert_eq!(report.rebased_clean, vec![branch.to_string()]);

        // write2: the next seal must SUCCEED and leave `@` clean == tip.
        std::fs::write(ws.join("b.rs"), "fn b() {}\n").unwrap();
        seal_paths(&jj, &ws, "edit2", None, &["b.rs"])
            .expect("the second seal succeeds after a concurrent advance");
        assert!(
            !is_working_copy_dirty(&jj, &ws).unwrap(),
            "@ is clean after the second seal"
        );

        // No divergent twin of `@` (the bug's signature): change_id(@) resolves to one.
        let cid = snapshot_change_id(&jj, &ws).unwrap();
        let twins = jj
            .run(
                &ws,
                &[
                    "log",
                    "-r",
                    &format!("change_id({})", cid.trim()),
                    "--no-graph",
                    "-T",
                    "commit_id ++ \"\\n\"",
                ],
                "divergence check",
            )
            .unwrap();
        assert_eq!(
            twins.lines().filter(|l| !l.trim().is_empty()).count(),
            1,
            "no divergent @ twin after the fold + advance + reseal"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn resolve_falls_back_to_path_when_bundled_jj_is_unspawnable() {
        let original = std::env::var("CAIRN_JJ_BIN").ok();
        std::env::remove_var("CAIRN_JJ_BIN");

        let home = TempDir::new().unwrap();
        let jj = JjEnv::resolve("/definitely/not/a/spawnable/jj", home.path());

        if let Some(value) = original {
            std::env::set_var("CAIRN_JJ_BIN", value);
        }

        assert_eq!(jj.bin, "jj");
    }

    #[test]
    #[serial_test::serial(jj)]
    fn resolve_keeps_explicit_env_override() {
        let original = std::env::var("CAIRN_JJ_BIN").ok();
        std::env::set_var("CAIRN_JJ_BIN", "/explicit/jj");

        let home = TempDir::new().unwrap();
        let jj = JjEnv::resolve("/definitely/not/a/spawnable/jj", home.path());

        match original {
            Some(value) => std::env::set_var("CAIRN_JJ_BIN", value),
            None => std::env::remove_var("CAIRN_JJ_BIN"),
        }

        assert_eq!(jj.bin, "/explicit/jj");
    }

    fn git(repo: &Path, args: &[&str]) {
        assert!(
            crate::env::git()
                .args(args)
                .current_dir(repo)
                .status()
                .unwrap()
                .success(),
            "git {args:?} failed"
        );
    }

    fn init_project(repo: &Path) {
        git(repo, &["init", "-q", "-b", "main"]);
        git(repo, &["config", "user.email", "p@e.com"]);
        git(repo, &["config", "user.name", "P"]);
        std::fs::write(repo.join("shared.rs"), "base\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "base"]);
    }

    /// Capture trimmed stdout of a git command (test helper).
    fn git_stdout(repo: &Path, args: &[&str]) -> String {
        let out = crate::env::git()
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?} failed");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn advance_project(repo: &Path) -> String {
        std::fs::write(repo.join("more.rs"), "more\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "advance"]);
        let out = crate::env::git()
            .args(["rev-parse", "HEAD"])
            .current_dir(repo)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A store created earlier must re-import the backing git when the project
    /// advances, or a later job based on the new head fails to provision with
    /// `Revision <sha> doesn't exist`.
    #[test]
    #[serial_test::serial(jj)]
    fn add_workspace_after_project_git_advances() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping add_workspace_after_project_git_advances: jj not resolvable via CAIRN_JJ_BIN/PATH"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");

        ensure_project_store(&jj, &store, proj.path()).unwrap();
        add_workspace(
            &jj,
            &store,
            &wts.path().join("a"),
            "agent/CAIRN-1-x-0",
            "main",
            None,
        )
        .unwrap();

        // The project's git advances after the store was first created.
        let new_sha = advance_project(proj.path());

        // ensure_project_store is a no-op for the existing store dir, but must
        // re-import so the advanced base resolves for the next job.
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        add_workspace(
            &jj,
            &store,
            &wts.path().join("b"),
            "agent/CAIRN-2-x-0",
            &new_sha,
            None,
        )
        .unwrap();
        assert!(
            is_jj_dir(&wts.path().join("b")),
            "a later job on the advanced base must provision"
        );
    }

    /// The Coordinator topology, WITHOUT any manual bookmark creation: a
    /// coordinator workspace based on `main`, then a child workspace based on the
    /// coordinator's integration branch. Before the fix the child add failed with
    /// `Revision <branch> doesn't exist`, because a coordinator never seals and so
    /// its integration bookmark was never created. `add_workspace` now creates
    /// the branch bookmark at base, so the integration branch is a resolvable,
    /// pushable bookmark from creation and a child bases off it.
    #[test]
    #[serial_test::serial(jj)]
    fn child_workspace_bases_off_unsealed_coordinator_branch() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping child_workspace_bases_off_unsealed_coordinator_branch: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let coordinator = "agent/CAIRN-1940-coordinator-0";
        let child = "agent/CAIRN-1959-builder-0";

        // The coordinator workspace bases on main and never seals.
        add_workspace(
            &jj,
            &store,
            &wts.path().join("coord"),
            coordinator,
            "main",
            None,
        )
        .unwrap();

        // Its integration branch resolves as a bookmark immediately (no seal).
        assert!(
            bookmark_commit(&jj, &store, coordinator).is_some(),
            "add_workspace must create the workspace's branch bookmark at base"
        );

        // The child bases off the coordinator's integration branch — this is the
        // add that failed with `Revision ... doesn't exist` before the fix.
        add_workspace(
            &jj,
            &store,
            &wts.path().join("child"),
            child,
            coordinator,
            None,
        )
        .unwrap();
        assert!(
            is_jj_dir(&wts.path().join("child")),
            "child workspace based on the unsealed coordinator branch must provision"
        );
    }

    /// A failed `jj workspace add` registers the workspace name (and writes a
    /// half-created `.jj` dir) before it resolves `-r`, so a retried job would hit
    /// `Workspace named X already exists` / `Destination path exists`.
    /// `add_workspace` forgets the stale registration and clears the dir, so the
    /// retry recovers and provisions cleanly.
    #[test]
    #[serial_test::serial(jj)]
    fn add_workspace_recovers_from_half_created_workspace() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping add_workspace_recovers_from_half_created_workspace: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let branch = "agent/CAIRN-1-builder-0";
        let ws = wts.path().join("job");
        let name = workspace_name_for_branch(branch);

        // Simulate the live failure: an add against an unresolvable revision still
        // registers the workspace name and writes a `.jj` dir, then errors.
        let _ = jj.run(
            &store,
            &[
                "workspace",
                "add",
                "--name",
                &name,
                "-r",
                "does-not-exist",
                &ws.to_string_lossy(),
            ],
            "seed half-created workspace",
        );
        assert!(
            ws.join(".jj").exists(),
            "the failed add still wrote a stale .jj dir"
        );

        // The retry recovers rather than failing on the stale registration/dir.
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        assert!(is_jj_dir(&ws), "the retried add provisions the workspace");
        assert!(
            bookmark_commit(&jj, &store, branch).is_some(),
            "the retried add creates the branch bookmark"
        );
    }

    /// The whole topology, proven in-tree: one shared store backed by the
    /// project `.git`, two sibling workspaces on one graph, a `.jj`-only
    /// workspace whose branch resolves via the marker, a seal that lands one
    /// addressable commit reachable in the project's object db, and a discard.
    #[test]
    #[serial_test::serial(jj)]
    fn shared_store_workspaces_seal_and_discard() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping shared_store_workspaces_seal_and_discard: jj not resolvable via CAIRN_JJ_BIN/PATH"
            );
            return;
        };
        let home = TempDir::new().unwrap(); // cairn home: JJ_CONFIG + the store live here
        let proj = TempDir::new().unwrap(); // the user's project checkout
        let wts = TempDir::new().unwrap(); // worktrees root
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        let author = GitAuthor::new("Alice", "alice@example.com");

        // Shared store backed by the project's .git; user checkout stays clean.
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        assert!(is_jj_dir(&store));
        assert!(
            !proj.path().join(".jj").exists(),
            "the user's checkout must stay pristine (no .jj)"
        );
        // Idempotent.
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Two sibling job workspaces off the one store.
        let a = wts.path().join("jobA");
        let b = wts.path().join("jobB");
        add_workspace(
            &jj,
            &store,
            &a,
            "agent/CAIRN-1-builder-0",
            "main",
            Some(&author),
        )
        .unwrap();
        add_workspace(
            &jj,
            &store,
            &b,
            "agent/CAIRN-2-builder-0",
            "main",
            Some(&author),
        )
        .unwrap();

        // Branch resolves inside the .jj-only workspace via the marker.
        assert!(!a.join(".git").exists(), "workspace is .jj-only (no .git)");
        assert_eq!(
            read_branch_marker(&a).as_deref(),
            Some("agent/CAIRN-1-builder-0")
        );

        // Shared graph: one op log / one repo, both workspaces listed.
        let list = jj
            .run(&store, &["workspace", "list"], "workspace list")
            .unwrap();
        assert!(
            list.contains("agent-CAIRN-1-builder-0") && list.contains("agent-CAIRN-2-builder-0"),
            "both workspaces share one store: {list}"
        );

        // Seal in jobA: clean @, edit, dirty, seal -> one addressable commit.
        assert!(!is_working_copy_dirty(&jj, &a).unwrap());
        std::fs::write(a.join("mod.rs"), "code\n").unwrap();
        assert!(is_working_copy_dirty(&jj, &a).unwrap());
        let res = seal(&jj, &a, "agent work", Some(&author)).unwrap();
        assert!(!res.sha.is_empty(), "seal returns the sealed commit id");
        assert!(
            !is_working_copy_dirty(&jj, &a).unwrap(),
            "@ is empty again after seal"
        );

        // The sealed commit is reachable in the PROJECT's object db (shared backend).
        let full = jj
            .run(
                &a,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "id",
            )
            .unwrap();
        assert!(
            crate::env::git()
                .args(["cat-file", "-t", &full])
                .current_dir(proj.path())
                .output()
                .unwrap()
                .status
                .success(),
            "sealed commit {full} must be reachable in the project .git"
        );

        // Discard in jobB returns @ to clean and removes the dirt.
        std::fs::write(b.join("scratch.rs"), "junk\n").unwrap();
        assert!(is_working_copy_dirty(&jj, &b).unwrap());
        discard(&jj, &b).unwrap();
        assert!(!is_working_copy_dirty(&jj, &b).unwrap());
        assert!(!b.join("scratch.rs").exists(), "discard removes the dirt");
    }

    /// The `--git` parser classifies modify/add/delete and counts `+`/`-` lines
    /// per file. Input is verbatim `jj diff --git` output (jj 0.42).
    #[test]
    fn parse_git_diff_classifies_modify_add_delete_with_counts() {
        let diff = "\
diff --git a/a.txt b/a.txt
index df967b96a5..f6474b4ea7 100644
--- a/a.txt
+++ b/a.txt
@@ -1,1 +1,3 @@
 base
+more
+loose
diff --git a/b.txt b/b.txt
deleted file mode 100644
index 3367afdbbf..0000000000
--- a/b.txt
+++ /dev/null
@@ -1,1 +0,0 @@
-old
diff --git a/c.txt b/c.txt
new file mode 100644
index 0000000000..fa49b07797
--- /dev/null
+++ b/c.txt
@@ -0,0 +1,1 @@
+new file
";
        let changes = parse_git_diff(diff);
        assert_eq!(changes.len(), 3, "{changes:?}");

        let a = &changes[0];
        assert_eq!(a.path, "a.txt");
        assert_eq!(a.status, "modified");
        assert_eq!((a.additions, a.deletions), (2, 0));
        assert_eq!(a.previous_path, None);

        let b = &changes[1];
        assert_eq!(b.path, "b.txt");
        assert_eq!(b.status, "deleted");
        assert_eq!((b.additions, b.deletions), (0, 1));

        let c = &changes[2];
        assert_eq!(c.path, "c.txt");
        assert_eq!(c.status, "added");
        assert_eq!((c.additions, c.deletions), (1, 0));
    }

    /// A rename carries the previous path and counts only real content lines
    /// (the `rename from/to` headers are not edits).
    #[test]
    fn parse_git_diff_reports_rename_with_previous_path() {
        let diff = "\
diff --git a/orig.txt b/renamed.txt
rename from orig.txt
rename to renamed.txt
index 83db48f84e..788e1a6204 100644
--- a/orig.txt
+++ b/renamed.txt
@@ -1,3 +1,4 @@
 line1
 line2
 line3
+added
";
        let changes = parse_git_diff(diff);
        assert_eq!(changes.len(), 1, "{changes:?}");
        let r = &changes[0];
        assert_eq!(r.status, "renamed");
        assert_eq!(r.path, "renamed.txt");
        assert_eq!(r.previous_path.as_deref(), Some("orig.txt"));
        assert_eq!((r.additions, r.deletions), (1, 0));
    }

    /// A removed line whose content begins with `-` (e.g. a markdown rule) must
    /// count as a deletion, not be mistaken for a `--- ` file header. The header
    /// `---`/`+++` lines only precede the first `@@`.
    #[test]
    fn parse_git_diff_counts_dashy_content_lines_inside_hunks() {
        let diff = "\
diff --git a/doc.md b/doc.md
index 1111111111..2222222222 100644
--- a/doc.md
+++ b/doc.md
@@ -1,2 +1,2 @@
 title
---- old rule
+++ new rule
";
        let changes = parse_git_diff(diff);
        assert_eq!(changes.len(), 1, "{changes:?}");
        let d = &changes[0];
        assert_eq!(d.path, "doc.md");
        assert_eq!(d.status, "modified");
        assert_eq!((d.additions, d.deletions), (1, 1));
    }

    #[test]
    fn parse_git_diff_empty_is_empty() {
        assert!(parse_git_diff("").is_empty());
    }

    /// A non-jj directory yields `None` so the projection falls back to the
    /// recorded cache. No jj binary is invoked (the `.jj` probe short-circuits).
    #[test]
    fn node_changed_files_returns_none_for_non_jj_dir() {
        let jj = JjEnv::resolve("jj", Path::new("/tmp"));
        let dir = TempDir::new().unwrap();
        assert!(node_changed_files(&jj, dir.path(), Some("main"), None).is_none());
    }

    fn setup_base_advance_with_node_change(
        bin: &str,
        rebase_workspace: bool,
    ) -> (
        TempDir,
        TempDir,
        TempDir,
        TempDir,
        JjEnv,
        std::path::PathBuf,
        String,
    ) {
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();

        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        std::fs::write(proj.path().join("base-only.rs"), "base-only\n").unwrap();
        git(proj.path(), &["add", "-A"]);
        git(proj.path(), &["commit", "-q", "-m", "add base-only"]);
        let base = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);

        let jj = JjEnv::resolve(bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("job");
        let branch = "agent/CAIRN-1-builder-0";
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

        std::fs::write(ws.join("node.rs"), "node\n").unwrap();
        seal(&jj, &ws, "node work", None).unwrap();

        std::fs::remove_file(proj.path().join("base-only.rs")).unwrap();
        git(proj.path(), &["add", "-A"]);
        git(
            proj.path(),
            &["commit", "-q", "-m", "external base deletion"],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        fetch_remote(&jj, &store, "origin").unwrap();

        if rebase_workspace {
            reconcile_siblings(
                &jj,
                &store,
                "main@origin",
                &[(branch.to_string(), ws.clone())],
            )
            .unwrap();
        }

        (home, origin, proj, wts, jj, ws, base)
    }

    /// The graph-derived change set lists a SEALED file (the omission this fixes)
    /// AND a loose, un-sealed `@` edit, while leaving the unchanged base file
    /// out. Drives the real shared-store seal path, mirroring the seal/discard
    /// fixture.
    #[test]
    #[serial_test::serial(jj)]
    fn node_changed_files_derives_sealed_and_loose_against_base() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping node_changed_files_derives_sealed_and_loose_against_base: jj not resolvable via CAIRN_JJ_BIN/PATH"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        let author = GitAuthor::new("Alice", "alice@example.com");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let a = wts.path().join("jobA");
        add_workspace(
            &jj,
            &store,
            &a,
            "agent/CAIRN-1-builder-0",
            "main",
            Some(&author),
        )
        .unwrap();

        // Seal a new file: it must appear in the graph-derived change set.
        std::fs::write(a.join("mod.rs"), "code\n").unwrap();
        let res = seal(&jj, &a, "agent work", Some(&author)).unwrap();
        assert!(!res.sha.is_empty());

        // A loose, un-sealed edit, snapshotted into `@` by a jj op (as the
        // agent's own operations do before a reviewer reads `/changed`).
        std::fs::write(a.join("loose.rs"), "wip\n").unwrap();
        assert!(is_working_copy_dirty(&jj, &a).unwrap());

        let changes = node_changed_files(&jj, &a, Some("main"), None)
            .expect("jj workspace resolves the base bookmark");
        let by_path: std::collections::HashMap<&str, &GraphFileChange> =
            changes.iter().map(|c| (c.path.as_str(), c)).collect();

        let sealed = by_path.get("mod.rs").expect("sealed file present in graph");
        assert_eq!(sealed.status, "added");
        assert_eq!(sealed.additions, 1);

        let loose = by_path
            .get("loose.rs")
            .expect("loose @ edit present in graph");
        assert_eq!(loose.status, "added");

        assert!(
            !by_path.contains_key("shared.rs"),
            "the unchanged base file must not appear: {changes:?}"
        );
    }

    /// When origin/main advances externally and deletes a base file, but the node
    /// has not yet been rebased, the effective fork point remains the original
    /// base. `/changed` must report only the node's own file, not the unrelated
    /// base deletion.
    #[test]
    #[serial_test::serial(jj)]
    fn node_changed_files_excludes_unrebased_external_base_deletion() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping node_changed_files_excludes_unrebased_external_base_deletion: jj not resolvable");
            return;
        };
        let (_home, _origin, _proj, _wts, jj, ws, base) =
            setup_base_advance_with_node_change(&bin, false);

        let changes = node_changed_files(&jj, &ws, Some("main"), Some(&base))
            .expect("jj workspace resolves an effective fork point");
        let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["node.rs"],
            "base deletion must be absent: {changes:?}"
        );
    }

    /// After the same external advance, reconcile rebases the node onto
    /// `main@origin`. The effective fork point must move to that remote-tracking
    /// tip, so the base branch's deletion still does not appear as node work.
    #[test]
    #[serial_test::serial(jj)]
    fn node_changed_files_excludes_rebased_external_base_deletion() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping node_changed_files_excludes_rebased_external_base_deletion: jj not resolvable");
            return;
        };
        let (_home, _origin, _proj, _wts, jj, ws, base) =
            setup_base_advance_with_node_change(&bin, true);

        let changes = node_changed_files(&jj, &ws, Some("main"), Some(&base))
            .expect("jj workspace resolves an effective fork point");
        let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["node.rs"],
            "base deletion must be absent: {changes:?}"
        );
    }

    /// `list_files` enumerates a non-colocated workspace's tracked files — the
    /// exact `.jj`-only shape (no `.git`) where the File tab's old `git ls-files`
    /// returned nothing and rendered "Path not found" for everything. Asserts the
    /// newly added, workspace-relative paths appear and that no `.jj/…` metadata
    /// entry leaks into the listing.
    #[test]
    #[serial_test::serial(jj)]
    fn list_files_enumerates_jj_workspace_tracked_files() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping list_files_enumerates_jj_workspace_tracked_files: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

        // A non-colocated workspace: `.jj` only, no `.git` — the shape that broke
        // git-in-worktree listing.
        assert!(
            !ws.join(".git").exists() && ws.join(".jj").is_dir(),
            "workspace is non-colocated (.jj only, no .git)"
        );

        // Write files in a subdir, then seal so they are snapshotted into the
        // working-copy commit `list_files` reads with --ignore-working-copy.
        std::fs::create_dir_all(ws.join("src")).unwrap();
        std::fs::write(ws.join("src").join("feature.rs"), "code\n").unwrap();
        std::fs::write(ws.join("notes.md"), "notes\n").unwrap();
        seal(&jj, &ws, "add files", None).unwrap();

        let files = list_files(&jj, &ws).unwrap();
        assert!(
            files.iter().any(|f| f == "src/feature.rs"),
            "workspace-relative subdir path is listed: {files:?}"
        );
        assert!(
            files.iter().any(|f| f == "notes.md"),
            "top-level file is listed: {files:?}"
        );
        assert!(
            files.iter().any(|f| f == "shared.rs"),
            "the base commit's tracked files are listed too: {files:?}"
        );
        assert!(
            !files.iter().any(|f| f.starts_with(".jj")),
            "the .jj metadata dir never leaks into the listing: {files:?}"
        );
        assert!(
            files.windows(2).all(|w| w[0] <= w[1]),
            "listing is sorted: {files:?}"
        );
    }

    /// `head_commit` is the jj analogue of `git rev-parse HEAD`: it returns the
    /// base sha for a fresh workspace and the latest sealed commit after a seal.
    #[test]
    #[serial_test::serial(jj)]
    fn head_commit_returns_base_then_sealed() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping head_commit_returns_base_then_sealed: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let base_sha = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

        // Fresh workspace: @- is the base commit.
        assert_eq!(
            head_commit(&jj, &ws).unwrap(),
            base_sha,
            "head_commit of a fresh workspace is the base sha"
        );

        // After a seal, @- is the newly sealed commit.
        std::fs::write(ws.join("mod.rs"), "code\n").unwrap();
        let sealed = seal(&jj, &ws, "agent work", None).unwrap();
        let head = head_commit(&jj, &ws).unwrap();
        assert_ne!(head, base_sha, "head advanced past base after seal");
        assert!(
            head.starts_with(&sealed.sha),
            "head_commit ({head}) is the sealed commit ({})",
            sealed.sha
        );
    }

    /// `sealed_tree_hash` returns the sealed commit's git **tree** object, so it
    /// is content-addressed: two genuinely distinct commits with identical tree
    /// content (different branches, messages, and authors) hash identically,
    /// which is what lets the check cache and the merge-gate baseline carry
    /// forward across an equivalent-tree squash/rebase. Different content hashes
    /// differently, and the hash is distinct from the commit id itself.
    #[test]
    #[serial_test::serial(jj)]
    fn sealed_tree_hash_is_content_addressed() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping sealed_tree_hash_is_content_addressed: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Two sibling workspaces off `main` seal IDENTICAL file content under
        // different branches, messages, and authors — distinct commit ids over
        // one tree.
        let a = wts.path().join("a");
        let b = wts.path().join("b");
        add_workspace(&jj, &store, &a, "agent/CAIRN-1-builder-0", "main", None).unwrap();
        add_workspace(&jj, &store, &b, "agent/CAIRN-2-builder-0", "main", None).unwrap();
        std::fs::write(a.join("mod.rs"), "code\n").unwrap();
        std::fs::write(b.join("mod.rs"), "code\n").unwrap();
        let author_a = GitAuthor::new("Alice", "alice@example.com");
        let author_b = GitAuthor::new("Bob", "bob@example.com");
        seal(&jj, &a, "message one", Some(&author_a)).unwrap();
        seal(&jj, &b, "a totally different message", Some(&author_b)).unwrap();

        let hash_a = sealed_tree_hash(&jj, &a).unwrap();
        let hash_b = sealed_tree_hash(&jj, &b).unwrap();

        // Stable for repeated reads of the same sealed revision.
        assert_eq!(
            hash_a,
            sealed_tree_hash(&jj, &a).unwrap(),
            "helper is stable for repeated reads"
        );

        // The two sealed commits are genuinely distinct ids …
        assert_ne!(
            head_commit(&jj, &a).unwrap(),
            head_commit(&jj, &b).unwrap(),
            "the two sealed commits are distinct commit ids"
        );
        // … yet identical tree content yields an identical content hash.
        assert_eq!(
            hash_a, hash_b,
            "identical tree content hashes identically across distinct commits"
        );
        // The hash is the git tree object, NOT the commit id — true content
        // addressing, which is exactly what the old commit-id fallback lacked.
        assert_ne!(
            hash_a,
            head_commit(&jj, &a).unwrap(),
            "sealed_tree_hash is the content tree, distinct from the sealed commit id"
        );

        // Different tree content hashes differently.
        let c = wts.path().join("c");
        add_workspace(&jj, &store, &c, "agent/CAIRN-3-builder-0", "main", None).unwrap();
        std::fs::write(c.join("mod.rs"), "different content\n").unwrap();
        seal(&jj, &c, "message one", Some(&author_a)).unwrap();
        assert_ne!(
            hash_a,
            sealed_tree_hash(&jj, &c).unwrap(),
            "different tree content yields a different hash"
        );
    }

    /// `parse_ls_tree` extracts `(path, blob)` from `-z` records, ignores
    /// mode/type, tolerates a trailing NUL, and sorts by path. Pure — no jj
    /// binary needed.
    #[test]
    fn parse_ls_tree_extracts_sorted_path_blob_pairs() {
        let out = "100644 blob aaa\tsrc/b.rs\x00100644 blob bbb\tsrc/a.rs\x00";
        assert_eq!(
            super::parse_ls_tree(out),
            vec![
                ("src/a.rs".to_string(), "bbb".to_string()),
                ("src/b.rs".to_string(), "aaa".to_string()),
            ]
        );
    }

    /// A seal followed by `push_to_origin` lands the workspace's bookmark on a
    /// bare `origin` — the in-tree form of the bare-origin spike.
    #[test]
    #[serial_test::serial(jj)]
    fn push_to_origin_lands_bookmark_in_bare_origin() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping push_to_origin_lands_bookmark_in_bare_origin: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();

        // A bare origin, with the project checkout wired to push main there.
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let branch = "agent/CAIRN-9-builder-0";
        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        std::fs::write(ws.join("f.rs"), "x\n").unwrap();
        seal(&jj, &ws, "agent work", None).unwrap();

        push_to_origin(&jj, &ws, branch);

        let refs = git_stdout(
            origin.path(),
            &["for-each-ref", "--format=%(refname)", "refs/heads/"],
        );
        assert!(
            refs.contains(branch),
            "pushed bookmark {branch} must appear on origin: {refs}"
        );

        // main/master are skipped (the same guard git uses); no panic, no push.
        push_to_origin(&jj, &ws, "main");
    }

    /// `ensure_bookmark_on_origin` publishes a Coordinator integration-branch
    /// base that lives only as a bookmark in the shared store (the project
    /// checkout has no local ref for it), and no-ops cleanly when the bookmark
    /// does not exist.
    #[test]
    #[serial_test::serial(jj)]
    fn ensure_bookmark_on_origin_publishes_store_bookmark() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping ensure_bookmark_on_origin_publishes_store_bookmark: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();

        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let base = "agent/CAIRN-1940-coordinator-0";
        // A nonexistent bookmark is a clean no-op (base not sealed yet).
        ensure_bookmark_on_origin(&jj, &store, base).unwrap();
        let before = git_stdout(
            origin.path(),
            &["for-each-ref", "--format=%(refname)", "refs/heads/"],
        );
        assert!(
            !before.contains(base),
            "absent bookmark must not be created on origin: {before}"
        );

        // Seal an integration bookmark in the store, then publish it.
        jj.run(
            &store,
            &["bookmark", "create", "-r", "main", base],
            "bookmark create",
        )
        .unwrap();
        ensure_bookmark_on_origin(&jj, &store, base).unwrap();
        let after = git_stdout(
            origin.path(),
            &["for-each-ref", "--format=%(refname)", "refs/heads/"],
        );
        assert!(
            after.contains(base),
            "published integration bookmark {base} must appear on origin: {after}"
        );

        // Idempotent: a second call is a no-op (already matches origin).
        ensure_bookmark_on_origin(&jj, &store, base).unwrap();
    }

    /// The headline: the coordinator topology reconciled in-tree. Two sibling
    /// workspaces off a shared integration bookmark; the integration tip advances
    /// (a child PR merged into it); `reconcile_siblings` non-blockingly rebases
    /// both. The overlapping sibling records a conflict with its change-id
    /// preserved (only the commit-id churns), its workspace goes stale and
    /// `update-stale` materializes conflict markers, and jj refuses to push that
    /// conflicted bookmark while the cleanly-rebased sibling pushes fine.
    #[test]
    #[serial_test::serial(jj)]
    fn reconcile_siblings_auto_rebases_with_recorded_conflict() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping reconcile_siblings_auto_rebases_with_recorded_conflict: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();

        // Project wired to a bare origin; shared store over its .git.
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // The Coordinator's integration bookmark, created by the coordinator's
        // own `add_workspace` (the real flow — a coordinator never seals, so its
        // bookmark must exist from creation) and published to origin.
        let int = "agent/CAIRN-1940-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        ensure_bookmark_on_origin(&jj, &store, int).unwrap();

        // Two sibling jobs off the integration tip: one edits the shared file
        // (conflict-bound), one edits a different file (clean).
        let overlap = "agent/CAIRN-1-builder-0";
        let clean = "agent/CAIRN-2-builder-0";
        let ws_overlap = wts.path().join("overlap");
        let ws_clean = wts.path().join("clean");
        add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
        add_workspace(&jj, &store, &ws_clean, clean, int, None).unwrap();
        std::fs::write(ws_overlap.join("shared.rs"), "sibling-A-change\n").unwrap();
        seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();
        std::fs::write(ws_clean.join("other.rs"), "b-only\n").unwrap();
        seal(&jj, &ws_clean, "clean edits other", None).unwrap();
        // Establish each sibling's PR head on origin (both clean so far).
        push_to_origin(&jj, &ws_overlap, overlap);
        push_to_origin(&jj, &ws_clean, clean);

        let change_overlap_before = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{overlap:?})"),
                    "--no-graph",
                    "-T",
                    "change_id.short()",
                ],
                "change before",
            )
            .unwrap();
        let commit_overlap_before = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{overlap:?})"),
                    "--no-graph",
                    "-T",
                    "commit_id.short()",
                ],
                "commit before",
            )
            .unwrap();

        // A child PR merges into the integration branch: advance its tip with a
        // conflicting change to the shared file, and publish it to origin.
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "child merged: shared advanced"],
            "describe",
        )
        .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();
        jj.run(
            &store,
            &["git", "push", "--remote", "origin", "--bookmark", int],
            "push advanced int",
        )
        .unwrap();

        // The cleanly-rebased sibling's PR head on origin before reconcile.
        let clean_origin_before = git_stdout(origin.path(), &["rev-parse", clean]);

        // The reconcile: both siblings rebase onto the advanced tip.
        let report = reconcile_siblings(
            &jj,
            &store,
            int,
            &[
                (overlap.to_string(), ws_overlap.clone()),
                (clean.to_string(), ws_clean.clone()),
            ],
        )
        .unwrap();
        assert_eq!(report.conflicted, vec![overlap.to_string()]);
        assert_eq!(report.rebased_clean, vec![clean.to_string()]);

        // reconcile pushed the cleanly-rebased sibling, advancing its PR head on
        // origin (no force-push needed); the conflicted one was not pushed.
        let clean_origin_after = git_stdout(origin.path(), &["rev-parse", clean]);
        assert_ne!(
            clean_origin_before, clean_origin_after,
            "reconcile pushes the cleanly-rebased sibling's advanced tip to origin"
        );

        // The overlapping sibling kept its change-id; only the commit churned.
        let change_overlap_after = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{overlap:?})"),
                    "--no-graph",
                    "-T",
                    "change_id.short()",
                ],
                "change after",
            )
            .unwrap();
        let commit_overlap_after = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{overlap:?})"),
                    "--no-graph",
                    "-T",
                    "commit_id.short()",
                ],
                "commit after",
            )
            .unwrap();
        assert_eq!(
            change_overlap_before, change_overlap_after,
            "auto-rebase preserves the sibling's change-id"
        );
        assert_ne!(
            commit_overlap_before, commit_overlap_after,
            "the rebased commit-id churns"
        );

        // jj records the conflict on the overlapping sibling, not the clean one.
        assert!(branch_has_conflict(&jj, &store, overlap).unwrap());
        assert!(!branch_has_conflict(&jj, &store, clean).unwrap());

        // update-stale materialized the conflict markers in the workspace file.
        let conflicted_file = std::fs::read_to_string(ws_overlap.join("shared.rs")).unwrap();
        assert!(
            conflicted_file.contains("<<<<<<<") && conflicted_file.contains(">>>>>>>"),
            "the agent sees materialized conflict markers: {conflicted_file}"
        );

        // jj refuses to push the conflicted bookmark (so a conflicted sibling
        // cannot advance its PR head on origin); the clean one pushes fine.
        assert!(
            jj.run(
                &store,
                &["git", "push", "--remote", "origin", "--bookmark", overlap],
                "push conflicted",
            )
            .is_err(),
            "jj must refuse to push a conflicted bookmark"
        );
        assert!(
            jj.run(
                &store,
                &["git", "push", "--remote", "origin", "--bookmark", clean],
                "push clean",
            )
            .is_ok(),
            "the cleanly-rebased sibling pushes its advanced tip"
        );
    }

    /// External default-branch advance: origin/main moves OUT OF BAND (a non-Cairn
    /// merge or direct push, not folded through the store). `fetch_remote` brings
    /// the new tip into the store as `main@origin`, which resolves as the rebase
    /// dest; siblings based on `main` auto-rebase onto it exactly as the
    /// Cairn-merge path does. Also proves the double-fire guard's premise: a
    /// second reconcile at the same tip leaves the conflicted commit id unchanged
    /// (a `jj rebase` no-op), so the before/after wake guard suppresses a
    /// redundant wake.
    #[test]
    #[serial_test::serial(jj)]
    fn reconcile_external_advance_via_origin_fetch_is_idempotent() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping reconcile_external_advance_via_origin_fetch_is_idempotent: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();

        // Project wired to a bare origin; shared store over its .git.
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Two sibling jobs based directly on the default branch `main`: one edits
        // the shared file (conflict-bound vs the external advance), one edits a
        // different file (clean).
        let overlap = "agent/CAIRN-1-builder-0";
        let clean = "agent/CAIRN-2-builder-0";
        let ws_overlap = wts.path().join("overlap");
        let ws_clean = wts.path().join("clean");
        add_workspace(&jj, &store, &ws_overlap, overlap, "main", None).unwrap();
        add_workspace(&jj, &store, &ws_clean, clean, "main", None).unwrap();
        std::fs::write(ws_overlap.join("shared.rs"), "sibling-A-change\n").unwrap();
        seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();
        std::fs::write(ws_clean.join("other.rs"), "b-only\n").unwrap();
        seal(&jj, &ws_clean, "clean edits other", None).unwrap();
        // Establish each sibling's PR head on origin (both clean so far).
        push_to_origin(&jj, &ws_overlap, overlap);
        push_to_origin(&jj, &ws_clean, clean);

        // The default branch advances OUTSIDE Cairn: edit + commit + push to
        // origin/main directly from the project checkout, with a change that
        // conflicts with the overlapping sibling. This never folds through the
        // store, so the store's view of main is stale until we fetch.
        std::fs::write(proj.path().join("shared.rs"), "external-advance\n").unwrap();
        git(proj.path(), &["add", "-A"]);
        git(
            proj.path(),
            &["commit", "-q", "-m", "external merge advances main"],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);

        // Store-sync: fetch origin so `main@origin` resolves to the new tip.
        fetch_remote(&jj, &store, "origin").unwrap();
        let dest = "main@origin";
        assert!(
            bookmark_commit(&jj, &store, dest).is_some()
                || jj
                    .run(
                        &store,
                        &["log", "-r", dest, "--no-graph", "-T", "commit_id"],
                        "resolve dest",
                    )
                    .is_ok(),
            "the externally-advanced tip must resolve as the rebase dest after fetch"
        );

        let clean_origin_before = git_stdout(origin.path(), &["rev-parse", clean]);

        // First reconcile: both siblings rebase onto the externally-advanced tip.
        let report = reconcile_siblings(
            &jj,
            &store,
            dest,
            &[
                (overlap.to_string(), ws_overlap.clone()),
                (clean.to_string(), ws_clean.clone()),
            ],
        )
        .unwrap();
        assert_eq!(report.conflicted, vec![overlap.to_string()]);
        assert_eq!(report.rebased_clean, vec![clean.to_string()]);

        // The cleanly-rebased sibling's PR head advanced on origin.
        let clean_origin_after = git_stdout(origin.path(), &["rev-parse", clean]);
        assert_ne!(
            clean_origin_before, clean_origin_after,
            "reconcile pushes the cleanly-rebased sibling's advanced tip to origin"
        );
        assert!(branch_has_conflict(&jj, &store, overlap).unwrap());
        assert!(!branch_has_conflict(&jj, &store, clean).unwrap());

        // The conflicted sibling's commit id after the first reconcile.
        let commit_overlap_after_first = bookmark_commit(&jj, &store, overlap).unwrap();

        // Second reconcile at the SAME tip (the double-fire): a `jj rebase` no-op,
        // so the conflicted commit id is unchanged. The before/after wake guard
        // reads exactly this equality to suppress a redundant wake.
        fetch_remote(&jj, &store, "origin").unwrap();
        let report2 = reconcile_siblings(
            &jj,
            &store,
            dest,
            &[
                (overlap.to_string(), ws_overlap.clone()),
                (clean.to_string(), ws_clean.clone()),
            ],
        )
        .unwrap();
        assert_eq!(
            report2.conflicted,
            vec![overlap.to_string()],
            "the sibling is still conflicted on the second pass"
        );
        let commit_overlap_after_second = bookmark_commit(&jj, &store, overlap).unwrap();
        assert_eq!(
            commit_overlap_after_first, commit_overlap_after_second,
            "a second reconcile at the same dest tip leaves the conflicted commit id unchanged (no redundant wake)"
        );
    }

    /// Acceptance: advancing the integration base with a conflicting change under
    /// N in-flight children, then reconciling REPEATEDLY (with the real
    /// `jj git import` default-advance round-trip between passes), must not
    /// accumulate divergent conflicted copies. The first reconcile rebases each
    /// child; every later pass finds each child already descended from the dest
    /// and SKIPS the rebase, so the conflicted child's commit id is stable and
    /// every change-id resolves to exactly one visible commit — no `<id>/0 /1`
    /// thrash. This is the structural-idempotence half of the 2041 fix (the
    /// per-store mutex is the concurrency half).
    #[test]
    #[serial_test::serial(jj)]
    fn reconcile_siblings_idempotent_no_divergence_across_import_round_trips() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping reconcile_siblings_idempotent_no_divergence_across_import_round_trips: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // A coordinator integration bookmark with three children branched FROM it:
        // one overlaps the shared file (conflict-bound vs the base advance), two
        // edit distinct files (clean).
        let int = "agent/CAIRN-2041-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let overlap = "agent/CAIRN-1-builder-0";
        let clean_a = "agent/CAIRN-2-builder-0";
        let clean_b = "agent/CAIRN-3-builder-0";
        let ws_overlap = wts.path().join("overlap");
        let ws_a = wts.path().join("a");
        let ws_b = wts.path().join("b");
        add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
        add_workspace(&jj, &store, &ws_a, clean_a, int, None).unwrap();
        add_workspace(&jj, &store, &ws_b, clean_b, int, None).unwrap();
        std::fs::write(ws_overlap.join("shared.rs"), "sibling-overlap\n").unwrap();
        seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();
        std::fs::write(ws_a.join("a.rs"), "a\n").unwrap();
        seal(&jj, &ws_a, "a edits a", None).unwrap();
        std::fs::write(ws_b.join("b.rs"), "b\n").unwrap();
        seal(&jj, &ws_b, "b edits b", None).unwrap();

        // The integration tip advances with a change that conflicts with overlap.
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "integration advances shared"],
            "describe",
        )
        .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();

        let specs = vec![
            (overlap.to_string(), ws_overlap.clone()),
            (clean_a.to_string(), ws_a.clone()),
            (clean_b.to_string(), ws_b.clone()),
        ];

        // First reconcile: overlap conflicts, the other two land clean.
        let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert_eq!(report1.conflicted, vec![overlap.to_string()]);
        assert_eq!(
            report1.rebased_clean,
            vec![clean_a.to_string(), clean_b.to_string()]
        );
        assert!(branch_has_conflict(&jj, &store, overlap).unwrap());

        // Snapshot every child's post-reconcile commit id; later passes must not
        // move any of them.
        let commit_overlap_1 = bookmark_commit(&jj, &store, overlap).unwrap();
        let commit_a_1 = bookmark_commit(&jj, &store, clean_a).unwrap();
        let commit_b_1 = bookmark_commit(&jj, &store, clean_b).unwrap();
        let cid_overlap = change_id_of(&jj, &store, overlap);
        let cid_a = change_id_of(&jj, &store, clean_a);
        let cid_b = change_id_of(&jj, &store, clean_b);

        // Repeated reconciles, each preceded by the real default-advance round-trip
        // (`jj git import` via `ensure_project_store`). Every pass is a no-op.
        for pass in 0..3 {
            ensure_project_store(&jj, &store, proj.path()).unwrap();
            let report = reconcile_siblings(&jj, &store, int, &specs).unwrap();
            assert_eq!(
                report.conflicted,
                vec![overlap.to_string()],
                "pass {pass}: overlap stays classified conflicted"
            );

            // The conflicted child's commit id is UNCHANGED — the rebase was
            // skipped (no re-rewrite), which is what stops divergent twins.
            assert_eq!(
                bookmark_commit(&jj, &store, overlap).unwrap(),
                commit_overlap_1,
                "pass {pass}: conflicted commit id is stable (rebase skipped)"
            );
            assert_eq!(bookmark_commit(&jj, &store, clean_a).unwrap(), commit_a_1);
            assert_eq!(bookmark_commit(&jj, &store, clean_b).unwrap(), commit_b_1);

            // Exactly one visible commit per change-id: no `<id>/0 /1` divergence.
            assert_eq!(
                visible_commits_for_change(&jj, &store, &cid_overlap),
                1,
                "pass {pass}: overlap change-id resolves to exactly one commit (no divergence)"
            );
            assert_eq!(visible_commits_for_change(&jj, &store, &cid_a), 1);
            assert_eq!(visible_commits_for_change(&jj, &store, &cid_b), 1);
        }
    }

    /// A manually-resolved bookmark (clean tip over a conflicted intermediate) is
    /// FLATTENED by the next reconcile, never dragged back onto a conflicted copy.
    /// After the base advance conflicts the overlapping child, the agent resolves
    /// the markers and re-seals — but that leaves the conflicted rebase commit as a
    /// conflicted INTERMEDIATE in the history, so the branch is still unmergeable
    /// (jj refuses a conflicted history). The reconcile-time flatten collapses it
    /// to ONE clean commit on the dest, preserving the agent's resolved TREE and
    /// clearing the conflicted intermediate, so the branch becomes genuinely
    /// mergeable. The resolution is preserved (never regenerated as a conflict) and
    /// the pre-flatten change-id is cleaned up (no divergent twin). This is the
    /// automation of the old hand-run resolve-at-base flatten.
    #[test]
    #[serial_test::serial(jj)]
    fn reconcile_siblings_preserves_resolved_bookmark() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping reconcile_siblings_preserves_resolved_bookmark: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2041-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let overlap = "agent/CAIRN-1-builder-0";
        let ws_overlap = wts.path().join("overlap");
        add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
        std::fs::write(ws_overlap.join("shared.rs"), "sibling-overlap\n").unwrap();
        seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();

        // The integration tip advances with a conflicting change.
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "integration advances shared"],
            "describe",
        )
        .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();

        let specs = vec![(overlap.to_string(), ws_overlap.clone())];

        // First reconcile records the conflict and materializes markers on disk.
        let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert_eq!(report1.conflicted, vec![overlap.to_string()]);
        assert!(branch_has_conflict(&jj, &store, overlap).unwrap());

        // The agent resolves the markers in its workspace and re-seals: the
        // bookmark advances to a CLEAN commit on top of the conflicted rebase.
        update_stale(&jj, &ws_overlap).unwrap();
        std::fs::write(ws_overlap.join("shared.rs"), "resolved-by-agent\n").unwrap();
        seal(&jj, &ws_overlap, "resolve base conflict", None).unwrap();
        assert!(
            !branch_has_conflict(&jj, &store, overlap).unwrap(),
            "the re-seal resolves the conflict; the bookmark is clean"
        );
        let resolved_commit = bookmark_commit(&jj, &store, overlap).unwrap();
        let resolved_cid = change_id_of(&jj, &store, overlap);

        // The next reconcile FLATTENS the resolved-but-conflicted-intermediate
        // branch: it already descends from the dest, but its history still carries
        // the conflicted rebase commit (unmergeable), so the reconcile collapses it
        // to one clean commit — preserving the resolved tree, never regenerating a
        // conflict.
        let _ = resolved_commit; // the flatten deliberately rewrites this commit id
        let report2 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert_eq!(
            report2.rebased_clean,
            vec![overlap.to_string()],
            "the resolved child is classified clean, not conflicted"
        );
        assert!(report2.conflicted.is_empty());
        assert!(
            !branch_has_conflict(&jj, &store, overlap).unwrap(),
            "the resolution is preserved — no regenerated conflict"
        );
        // The branch is now genuinely mergeable: the flatten cleared the conflicted
        // intermediate from its history.
        let dest = bookmark_commit(&jj, &store, int).unwrap();
        assert!(
            conflicted_commits(
                &jj,
                &store,
                &format!("{dest}..bookmarks(exact:{overlap:?})")
            )
            .is_empty(),
            "the flatten cleared the conflicted intermediate — the branch is mergeable"
        );
        assert_eq!(
            count_commits(
                &jj,
                &store,
                &format!("{dest}..bookmarks(exact:{overlap:?})")
            ),
            1,
            "the branch is collapsed to a single clean commit on the dest"
        );
        // The agent's resolved TREE is preserved though the commit was rewritten.
        assert_eq!(
            String::from_utf8_lossy(&file_show(&jj, &store, overlap, "shared.rs").unwrap()),
            "resolved-by-agent\n",
            "the flatten preserves the agent's resolved content"
        );
        // The pre-flatten change-id is cleaned up: no lingering (divergent) twin.
        assert_eq!(
            visible_commits_for_change(&jj, &store, &resolved_cid),
            0,
            "the pre-flatten change-id is abandoned by twin cleanup — no divergent twin"
        );
    }

    /// The no-propagate guard: when the rebase dest itself carries a recorded
    /// conflict, every sibling is HELD on its prior clean commit rather than
    /// rebased onto the conflicted base — the load-bearing fix for the live bug
    /// where a conflicted integration tip was handed to all in-flight children.
    /// The hold is self-clearing: once the base re-seals clean, the next reconcile
    /// rebases the child normally.
    #[test]
    #[serial_test::serial(jj)]
    fn reconcile_siblings_holds_children_off_conflicted_base() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping reconcile_siblings_holds_children_off_conflicted_base: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2042-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        // The clean integration tip the child branches from.
        let int_base = bookmark_commit(&jj, &store, int).unwrap();

        // The child branches from the clean int tip and edits a NON-overlapping
        // file, so on a clean base it would rebase cleanly.
        let child = "agent/CAIRN-1-builder-0";
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
        std::fs::write(ws_child.join("other.rs"), "child-edit\n").unwrap();
        seal(&jj, &ws_child, "child edits other", None).unwrap();
        let child_commit_before = bookmark_commit(&jj, &store, child).unwrap();

        // Drive the integration bookmark to a CONFLICTED tip without rewriting the
        // child's ancestor: two changes from the same base edit shared.rs
        // conflictingly, and rebasing one onto the other records a conflict in its
        // commit; int is pointed at that conflicted commit.
        jj.run(&store, &["new", &int_base, "-m", "left"], "new left")
            .unwrap();
        std::fs::write(store.join("shared.rs"), "left-side\n").unwrap();
        jj.run(
            &store,
            &["bookmark", "create", "tmp-left", "-r", "@"],
            "create tmp-left",
        )
        .unwrap();
        jj.run(&store, &["new", &int_base, "-m", "right"], "new right")
            .unwrap();
        std::fs::write(store.join("shared.rs"), "right-side\n").unwrap();
        jj.run(
            &store,
            &["bookmark", "create", "tmp-right", "-r", "@"],
            "create tmp-right",
        )
        .unwrap();
        jj.run(
            &store,
            &[
                "rebase",
                "-r",
                "tmp-left",
                "-d",
                "tmp-right",
                "--ignore-working-copy",
            ],
            "rebase tmp-left onto tmp-right to record a conflict",
        )
        .unwrap();
        let conflicted_tip = bookmark_commit(&jj, &store, "tmp-left").unwrap();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                int,
                "-r",
                &conflicted_tip,
                "--ignore-working-copy",
            ],
            "point int at the conflicted commit",
        )
        .unwrap();
        assert!(
            branch_has_conflict(&jj, &store, int).unwrap(),
            "the integration tip is conflicted"
        );

        let specs = vec![(child.to_string(), ws_child.clone())];

        // First reconcile: the dest (int) is conflicted, so the child is HELD on
        // its prior clean commit — never rebased onto the conflicted base.
        let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert_eq!(
            report1.held,
            vec![child.to_string()],
            "the child is held off the conflicted base"
        );
        assert!(
            report1.conflicted.is_empty(),
            "a held child is not classified conflicted"
        );
        assert!(
            report1.rebased_clean.is_empty(),
            "a held child is not classified clean"
        );
        assert_eq!(
            bookmark_commit(&jj, &store, child).unwrap(),
            child_commit_before,
            "the held child's commit is unchanged — never rebased onto the conflicted base"
        );
        assert!(
            !branch_has_conflict(&jj, &store, child).unwrap(),
            "the held child stayed clean"
        );

        // The base is resolved and re-sealed: a fresh commit on int fully rewrites
        // the conflicted file, advancing int to a clean tip.
        jj.run(&store, &["new", int, "-m", "resolve"], "new on int")
            .unwrap();
        std::fs::write(store.join("shared.rs"), "resolved\n").unwrap();
        jj.run(
            &store,
            &["bookmark", "set", int, "-r", "@"],
            "advance int clean",
        )
        .unwrap();
        assert!(
            !branch_has_conflict(&jj, &store, int).unwrap(),
            "the base re-sealed clean"
        );

        // Second reconcile: the guard no longer fires, the child rebases normally
        // onto the clean tip (the hold clears), and it now descends from int.
        let report2 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert!(report2.held.is_empty(), "with a clean base nothing is held");
        assert_eq!(
            report2.rebased_clean,
            vec![child.to_string()],
            "the child rebases cleanly onto the resolved base"
        );
        assert!(report2.conflicted.is_empty());
        let int_clean = bookmark_commit(&jj, &store, int).unwrap();
        assert!(
            branch_descends_from(&jj, &store, child, &int_clean),
            "the child now descends from the resolved int tip"
        );
    }

    /// `conflicted_files` enumerates the conflicting file paths in a workspace
    /// whose markers are materialized — the detail threaded into the stop-the-line
    /// note so the agent knows exactly where to look.
    #[test]
    #[serial_test::serial(jj)]
    fn conflicted_files_lists_conflicting_paths() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping conflicted_files_lists_conflicting_paths: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2042-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let child = "agent/CAIRN-1-builder-0";
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
        std::fs::write(ws_child.join("shared.rs"), "child-side\n").unwrap();
        seal(&jj, &ws_child, "child edits shared", None).unwrap();

        // The integration tip advances with a conflicting change to the same file.
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "integration advances shared"],
            "describe",
        )
        .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();

        // The reconcile rebases the child onto the advanced tip, recording a
        // conflict and materializing the markers in the child workspace.
        let report =
            reconcile_siblings(&jj, &store, int, &[(child.to_string(), ws_child.clone())]).unwrap();
        assert_eq!(report.conflicted, vec![child.to_string()]);

        update_stale(&jj, &ws_child).unwrap();
        let files = conflicted_files(&jj, &ws_child);
        assert_eq!(
            files,
            vec!["shared.rs".to_string()],
            "the conflicting path is listed"
        );
    }

    /// `conflicted_commits` enumerates each conflicted commit in a range with its
    /// conflicted file paths — store-side, no workspace — and reports nothing for
    /// a clean range. This is the detail the pre-flight diagnostic surfaces.
    #[test]
    #[serial_test::serial(jj)]
    fn conflicted_commits_enumerates_conflicting_commits_and_files() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping conflicted_commits_enumerates_conflicting_commits_and_files: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2042-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let child = "agent/CAIRN-1-builder-0";
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
        std::fs::write(ws_child.join("shared.rs"), "child-side\n").unwrap();
        seal(&jj, &ws_child, "child edits shared", None).unwrap();

        // A clean range reports nothing before any conflict is recorded.
        assert!(
            conflicted_commits(&jj, &store, &format!("bookmarks(exact:{child:?})")).is_empty(),
            "a clean source has no conflicted commits"
        );

        // The integration tip advances with a conflicting change to the same file,
        // then the reconcile rebases the child onto it, recording a conflict.
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "integration advances shared"],
            "describe",
        )
        .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();
        let report =
            reconcile_siblings(&jj, &store, int, &[(child.to_string(), ws_child.clone())]).unwrap();
        assert_eq!(report.conflicted, vec![child.to_string()]);

        // The conflicted child commit is enumerated with its conflicted path.
        let conflicts = conflicted_commits(&jj, &store, &format!("bookmarks(exact:{child:?})"));
        assert_eq!(
            conflicts.len(),
            1,
            "the conflicted child commit is reported"
        );
        assert_eq!(conflicts[0].files, vec!["shared.rs".to_string()]);
        assert!(
            !conflicts[0].commit_id.is_empty() && !conflicts[0].change_id.is_empty(),
            "commit and change ids are populated"
        );

        // The cleanly-advanced integration tip itself carries no conflict.
        assert!(
            conflicted_commits(&jj, &store, &format!("bookmarks(exact:{int:?})")).is_empty(),
            "the clean integration tip reports no conflicted commits"
        );
    }

    /// The current operation id over the store.
    fn current_op_id(jj: &JjEnv, store: &Path) -> String {
        jj.run(
            store,
            &["op", "log", "--no-graph", "-n", "1", "-T", "id"],
            "current op id",
        )
        .unwrap()
        .trim()
        .to_string()
    }

    /// Deterministic reproduction of the divergence MECHANISM, plus proof the fix
    /// avoids it. Two rebases of the same child from the SAME base operation
    /// (`--at-op`) fork the operation log; the next command merges the divergent
    /// op heads, and jj keeps BOTH rewritten commits as a divergent change
    /// (`<id>/0 /1`) — exactly the `spnmzyvp/0../5` accumulation observed live.
    /// This is what concurrent, unserialized reconciles did on the shared store.
    /// The fix's single-writer discipline (the per-store mutex) plus the
    /// resolve-dest-once + descends skip in `reconcile_siblings` make a serialized
    /// re-reconcile a structural no-op, so it converges to ONE commit.
    #[test]
    #[serial_test::serial(jj)]
    fn forked_op_rebase_diverges_but_reconcile_converges() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping forked_op_rebase_diverges_but_reconcile_converges: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2041-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let overlap = "agent/CAIRN-1-builder-0";
        let ws_overlap = wts.path().join("overlap");
        add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
        std::fs::write(ws_overlap.join("shared.rs"), "sibling-overlap\n").unwrap();
        seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();

        // overlap is sealed on the original integration base P.
        let p = bookmark_commit(&jj, &store, int).unwrap();

        // Two DISTINCT advances of the integration base off P, each conflicting
        // with overlap differently. A moving dest is what made the live
        // reconciles rewrite the same change to different commits.
        let commit_of_at = |jj: &JjEnv| {
            jj.run(
                &store,
                &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
                "commit of @",
            )
            .unwrap()
            .trim()
            .to_string()
        };
        jj.run(&store, &["new", &p], "new D1 off base").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced-1\n").unwrap();
        jj.run(&store, &["describe", "-m", "advance 1"], "describe D1")
            .unwrap();
        let d1 = commit_of_at(&jj);
        jj.run(&store, &["new", &p], "new D2 off base").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced-2\n").unwrap();
        jj.run(&store, &["describe", "-m", "advance 2"], "describe D2")
            .unwrap();
        let d2 = commit_of_at(&jj);
        // The integration bookmark tracks the canonical advanced tip D1.
        jj.run(
            &store,
            &["bookmark", "set", int, "-r", &d1, "--ignore-working-copy"],
            "set int = D1",
        )
        .unwrap();

        let cid_overlap = change_id_of(&jj, &store, overlap);

        // MECHANISM: fork the op log. Rebase overlap onto D1 in one forked op and
        // onto D2 in another, both from the SAME base operation. The two ops
        // rewrite overlap to DIFFERENT commits (distinct parents); merging the
        // divergent op heads keeps both as a divergent change `<id>/0 /1`.
        let base_op = current_op_id(&jj, &store);
        jj.run(
            &store,
            &[
                "rebase",
                "-b",
                overlap,
                "-o",
                &d1,
                "--ignore-working-copy",
                "--at-op",
                &base_op,
            ],
            "forked rebase onto D1",
        )
        .unwrap();
        jj.run(
            &store,
            &[
                "rebase",
                "-b",
                overlap,
                "-o",
                &d2,
                "--ignore-working-copy",
                "--at-op",
                &base_op,
            ],
            "forked rebase onto D2",
        )
        .unwrap();
        // Trigger the concurrent-op merge (any normal command does it).
        let _ = jj.run(
            &store,
            &["log", "-r", "root()", "--no-graph", "-T", "commit_id"],
            "trigger op merge",
        );
        assert_eq!(
            visible_commits_for_change(&jj, &store, &cid_overlap),
            2,
            "two forked rebases onto distinct tips accumulate a divergent copy (the bug)"
        );

        // Converge the corrupted store the way a live one is hand-repaired: point
        // the bookmark at the twin that descends from the canonical tip D1
        // (= int) and abandon the orphaned D2 twin.
        let twins = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("change_id({cid_overlap})"),
                    "--no-graph",
                    "-T",
                    "commit_id ++ \"\\n\"",
                    "--ignore-working-copy",
                ],
                "list divergent twins",
            )
            .unwrap();
        let twin_ids: Vec<String> = twins
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(twin_ids.len(), 2);
        let keep = twin_ids
            .iter()
            .find(|c| revset_descends_from(&jj, &store, c, &d1))
            .cloned()
            .expect("one twin descends from the canonical tip D1");
        let drop = twin_ids
            .iter()
            .find(|c| **c != keep)
            .cloned()
            .expect("the other twin");
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                overlap,
                "-r",
                &keep,
                "--ignore-working-copy",
            ],
            "point bookmark at kept twin",
        )
        .unwrap();
        jj.run(
            &store,
            &["abandon", &drop, "--ignore-working-copy"],
            "abandon divergent twin",
        )
        .unwrap();
        assert_eq!(
            visible_commits_for_change(&jj, &store, &cid_overlap),
            1,
            "after convergence the change resolves to a single commit"
        );

        // FIX: a serialized re-reconcile at the same dest is now a structural
        // no-op (the child already descends from `int`), so it never re-mints a
        // divergent twin. This is the single-writer + skip behavior the mutex
        // guarantees in production.
        let specs = vec![(overlap.to_string(), ws_overlap.clone())];
        let before = bookmark_commit(&jj, &store, overlap).unwrap();
        reconcile_siblings(&jj, &store, int, &specs).unwrap();
        reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert_eq!(
            bookmark_commit(&jj, &store, overlap).unwrap(),
            before,
            "the skip-guarded reconcile leaves the commit id unchanged"
        );
        assert_eq!(
            visible_commits_for_change(&jj, &store, &cid_overlap),
            1,
            "the skip-guarded reconcile does not re-mint a divergent twin"
        );
    }

    const DIV_INT: &str = "agent/CAIRN-2100-coordinator-0";
    const DIV_SIBLING: &str = "agent/CAIRN-1-builder-0";

    /// A project store with an integration bookmark and one `agent/...` sibling
    /// branched from it, the sibling sealed editing `shared.rs`. The TempDirs are
    /// kept alive by the returned struct.
    struct DivergenceFixture {
        _home: TempDir,
        _proj: TempDir,
        _wts: TempDir,
        jj: JjEnv,
        store: PathBuf,
        ws_sibling: PathBuf,
    }

    fn setup_divergence_fixture(bin: &str) -> DivergenceFixture {
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        add_workspace(
            &jj,
            &store,
            &wts.path().join("coord"),
            DIV_INT,
            "main",
            None,
        )
        .unwrap();
        let ws_sibling = wts.path().join("sibling");
        add_workspace(&jj, &store, &ws_sibling, DIV_SIBLING, DIV_INT, None).unwrap();
        std::fs::write(ws_sibling.join("shared.rs"), "sibling-edit\n").unwrap();
        seal(&jj, &ws_sibling, "sibling edits shared", None).unwrap();
        DivergenceFixture {
            _home: home,
            _proj: proj,
            _wts: wts,
            jj,
            store,
            ws_sibling,
        }
    }

    /// Advance the integration tip with a `shared.rs` edit that conflicts with the
    /// sibling's edit; returns the new tip commit id (a conflicting rebase dest).
    fn advance_int_conflicting(jj: &JjEnv, store: &Path, content: &str) -> String {
        jj.run(store, &["new", DIV_INT], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), content).unwrap();
        jj.run(
            store,
            &["describe", "-m", "int advances shared"],
            "describe int",
        )
        .unwrap();
        let tip = jj
            .run(
                store,
                &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
                "int tip",
            )
            .unwrap()
            .trim()
            .to_string();
        jj.run(
            store,
            &[
                "bookmark",
                "set",
                DIV_INT,
                "-r",
                "@",
                "--ignore-working-copy",
            ],
            "advance int bookmark",
        )
        .unwrap();
        tip
    }

    /// Mint a divergent change on the sibling carrying one CONFLICTED twin (the
    /// base-advance copy rebased onto a conflicting dest) and one CLEAN twin (the
    /// original commit re-described, standing in for the agent's resolved
    /// re-seal), via two forked ops from the same base operation. Returns
    /// (shared change-id, conflicted twin id, clean twin id). The change-id is
    /// captured BEFORE the fork (a single pre-fork commit) because the forked
    /// bookmark itself goes divergent.
    fn fork_conflicted_and_clean(
        jj: &JjEnv,
        store: &Path,
        conflicting_dest: &str,
    ) -> (String, String, String) {
        let cid = change_id_of(jj, store, DIV_SIBLING);
        let base_op = current_op_id(jj, store);
        jj.run(
            store,
            &[
                "rebase",
                "-b",
                DIV_SIBLING,
                "-o",
                conflicting_dest,
                "--ignore-working-copy",
                "--at-op",
                &base_op,
            ],
            "fork conflicted twin",
        )
        .unwrap();
        jj.run(
            store,
            &[
                "describe",
                DIV_SIBLING,
                "-m",
                "agent resolved re-seal",
                "--ignore-working-copy",
                "--at-op",
                &base_op,
            ],
            "fork clean twin",
        )
        .unwrap();
        // Any normal command merges the divergent op heads.
        let _ = jj.run(
            store,
            &[
                "log",
                "-r",
                "root()",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "trigger op merge",
        );
        let ids = visible_commit_ids_for_change(jj, store, &cid);
        assert_eq!(ids.len(), 2, "fork mints exactly two twins");
        let conflicted = ids
            .iter()
            .find(|c| revset_has_conflict(jj, store, c).unwrap())
            .cloned()
            .expect("one conflicted twin");
        let clean = ids
            .iter()
            .find(|c| !revset_has_conflict(jj, store, c).unwrap())
            .cloned()
            .expect("one clean twin");
        (cid, conflicted, clean)
    }

    /// (1) Self-heal: a divergent change with one conflicted twin (the
    /// base-advance copy) and one clean twin (the agent's resolved re-seal)
    /// collapses to the clean twin — the bookmark repoints, the conflicted twin is
    /// abandoned, and the change resolves to a single visible commit.
    #[test]
    #[serial_test::serial(jj)]
    fn collapse_self_heals_one_conflicted_one_clean_twin() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping collapse_self_heals_one_conflicted_one_clean_twin: jj not resolvable"
            );
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
        let (cid, conflicted, clean) = fork_conflicted_and_clean(&fx.jj, &fx.store, &dest);

        // Production: the agent's re-seal leaves the bookmark on the CLEAN twin
        // while the base-advance conflicted twin orphans. Pin it there.
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "set",
                    DIV_SIBLING,
                    "-r",
                    &clean,
                    "--ignore-working-copy",
                ],
                "pin bookmark to clean twin",
            )
            .unwrap();

        // Precondition: divergent, exactly one conflicted + one clean twin.
        assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 2);
        assert!(revset_has_conflict(&fx.jj, &fx.store, &conflicted).unwrap());
        assert!(!revset_has_conflict(&fx.jj, &fx.store, &clean).unwrap());

        let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert_eq!(
            outcome,
            CollapseOutcome::Collapsed {
                kept: clean.clone(),
                abandoned: vec![conflicted.clone()],
            }
        );
        assert_eq!(
            visible_commits_for_change(&fx.jj, &fx.store, &cid),
            1,
            "the change resolves to a single visible commit after collapse"
        );
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            clean,
            "the bookmark points at the surviving clean twin"
        );
    }

    /// (2) THE #162 closure: after collapsing the divergence, an agent re-seal on
    /// the sibling workspace returns `Ok` — `sealed_commit_is_lost` no longer fires
    /// (a single visible commit) and the re-sealed commit carries no resurfaced
    /// conflict. This is the invariant the thrash violated.
    #[test]
    #[serial_test::serial(jj)]
    fn reseal_after_collapse_returns_ok_without_resurfaced_conflict() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping reseal_after_collapse_returns_ok_without_resurfaced_conflict: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        // Park the workspace `@` off the sibling's sealed change before forking, so
        // ONLY the sealed change diverges. In production `@` is a fresh working
        // copy on the sibling tip, never itself divergent; without this, the
        // `--at-op` fork rewrites the whole subtree (including `@`) and corrupts
        // the working-copy commit — a test artifact, not the real failure mode.
        fx.jj
            .run(
                &fx.ws_sibling,
                &["new", "main"],
                "park workspace @ off the sibling",
            )
            .unwrap();
        let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
        let (cid, _conflicted, clean) = fork_conflicted_and_clean(&fx.jj, &fx.store, &dest);
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "set",
                    DIV_SIBLING,
                    "-r",
                    &clean,
                    "--ignore-working-copy",
                ],
                "pin bookmark to clean twin",
            )
            .unwrap();

        let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert!(matches!(outcome, CollapseOutcome::Collapsed { .. }));
        assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 1);

        // Bring the workspace current onto the collapsed tip (the production
        // re-parent a reconcile performs), then drive an agent re-seal. Before the
        // collapse this seal tripped the lost-seal backstop (still-divergent
        // change) and returned `LOST_SEAL_MSG`; now it lands cleanly.
        let conflicted =
            advance_workspace_onto(&fx.jj, &fx.store, &fx.ws_sibling, DIV_SIBLING, &clean).unwrap();
        assert!(
            !conflicted,
            "advancing onto the clean collapsed tip is conflict-free"
        );
        std::fs::write(fx.ws_sibling.join("resolved.rs"), "resolved\n").unwrap();
        let result = seal(&fx.jj, &fx.ws_sibling, "re-seal after collapse", None);
        assert!(
            result.is_ok(),
            "the re-seal returns Ok (no lost-seal thrash): {result:?}"
        );
        assert!(
            !branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            "the re-sealed sibling carries no resurfaced conflict"
        );
    }

    /// (3) Ambiguous — both twins conflicted: every twin still conflicts, so there
    /// is no single clean keep. The helper holds and surfaces, mutating nothing.
    #[test]
    #[serial_test::serial(jj)]
    fn collapse_holds_when_all_twins_conflicted() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping collapse_holds_when_all_twins_conflicted: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let d1 = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced-1\n");
        // A second distinct conflicting tip off the same base, so the two forked
        // rebases land the sibling on different parents (both conflicted).
        let cid = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
        fx.jj
            .run(&fx.store, &["new", "main"], "new D2 off base")
            .unwrap();
        std::fs::write(fx.store.join("shared.rs"), "int-advanced-2\n").unwrap();
        fx.jj
            .run(&fx.store, &["describe", "-m", "advance 2"], "describe D2")
            .unwrap();
        let d2 = fx
            .jj
            .run(
                &fx.store,
                &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
                "D2 tip",
            )
            .unwrap()
            .trim()
            .to_string();
        let base_op = current_op_id(&fx.jj, &fx.store);
        for (dest, label) in [(&d1, "rebase onto D1"), (&d2, "rebase onto D2")] {
            fx.jj
                .run(
                    &fx.store,
                    &[
                        "rebase",
                        "-b",
                        DIV_SIBLING,
                        "-o",
                        dest,
                        "--ignore-working-copy",
                        "--at-op",
                        &base_op,
                    ],
                    label,
                )
                .unwrap();
        }
        let _ = fx.jj.run(
            &fx.store,
            &[
                "log",
                "-r",
                "root()",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "trigger op merge",
        );
        let ids = visible_commit_ids_for_change(&fx.jj, &fx.store, &cid);
        assert_eq!(ids.len(), 2);
        assert!(ids
            .iter()
            .all(|c| revset_has_conflict(&fx.jj, &fx.store, c).unwrap()));
        // Pin the bookmark to one twin so it resolves to a single tip.
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "set",
                    DIV_SIBLING,
                    "-r",
                    &ids[0],
                    "--ignore-working-copy",
                ],
                "pin bookmark",
            )
            .unwrap();
        let pinned = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();

        let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        match outcome {
            CollapseOutcome::Ambiguous { change_id, twins } => {
                assert_eq!(change_id, cid);
                assert_eq!(twins.len(), 2);
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
        assert_eq!(
            visible_commits_for_change(&fx.jj, &fx.store, &cid),
            2,
            "an ambiguous tangle leaves the store untouched"
        );
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            pinned,
            "the bookmark is not moved"
        );
    }

    /// (4) Ambiguous — both twins clean (both carry edits): more than one clean
    /// keep means picking one would guess. Hold and surface, mutating nothing.
    #[test]
    #[serial_test::serial(jj)]
    fn collapse_holds_when_multiple_clean_twins() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping collapse_holds_when_multiple_clean_twins: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let cid = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
        let base_op = current_op_id(&fx.jj, &fx.store);
        // Two re-describes of the same change from one base op: two clean twins.
        for (msg, label) in [("twin a", "fork clean a"), ("twin b", "fork clean b")] {
            fx.jj
                .run(
                    &fx.store,
                    &[
                        "describe",
                        DIV_SIBLING,
                        "-m",
                        msg,
                        "--ignore-working-copy",
                        "--at-op",
                        &base_op,
                    ],
                    label,
                )
                .unwrap();
        }
        let _ = fx.jj.run(
            &fx.store,
            &[
                "log",
                "-r",
                "root()",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "trigger op merge",
        );
        let ids = visible_commit_ids_for_change(&fx.jj, &fx.store, &cid);
        assert_eq!(ids.len(), 2);
        assert!(ids
            .iter()
            .all(|c| !revset_has_conflict(&fx.jj, &fx.store, c).unwrap()));
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "set",
                    DIV_SIBLING,
                    "-r",
                    &ids[0],
                    "--ignore-working-copy",
                ],
                "pin bookmark",
            )
            .unwrap();
        let pinned = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();

        let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert!(
            matches!(outcome, CollapseOutcome::Ambiguous { .. }),
            "two clean twins are ambiguous: {outcome:?}"
        );
        assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 2);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            pinned
        );
    }

    /// (5) A healthy single-commit bookmark is NotDivergent and mutates nothing.
    #[test]
    #[serial_test::serial(jj)]
    fn collapse_noops_on_healthy_bookmark() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping collapse_noops_on_healthy_bookmark: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let cid = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
        let before = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 1);

        let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert_eq!(outcome, CollapseOutcome::NotDivergent);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            before
        );
    }

    /// (6) Idempotence: collapsing an already-collapsed bookmark is a no-op — the
    /// second pass sees a single visible commit and returns NotDivergent.
    #[test]
    #[serial_test::serial(jj)]
    fn collapse_is_idempotent() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping collapse_is_idempotent: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
        let (cid, _conflicted, clean) = fork_conflicted_and_clean(&fx.jj, &fx.store, &dest);
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "set",
                    DIV_SIBLING,
                    "-r",
                    &clean,
                    "--ignore-working-copy",
                ],
                "pin bookmark to clean twin",
            )
            .unwrap();

        assert!(matches!(
            collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            CollapseOutcome::Collapsed { .. }
        ));
        let after_first = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();

        let second = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert_eq!(second, CollapseOutcome::NotDivergent);
        assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 1);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            after_first
        );
    }

    /// The store-owns-merge fold: `merge_into_bookmark` fast-forwards the
    /// integration bookmark to the child's *real* commit (not a squash), and
    /// refuses a backwards move once integration has advanced past the child.
    #[test]
    #[serial_test::serial(jj)]
    fn merge_into_bookmark_folds_child_and_refuses_backwards() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping merge_into_bookmark_folds_child_and_refuses_backwards: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-1940-coordinator-0";
        let child = "agent/CAIRN-1-builder-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();

        // The child seals a real commit on top of the integration tip.
        std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
        seal(&jj, &ws_child, "child work", None).unwrap();
        let child_tip = bookmark_commit(&jj, &store, child).unwrap();

        // Fold the child's real commit into integration (forward-only).
        merge_into_bookmark(&jj, &store, int, child).unwrap();
        assert_eq!(
            bookmark_commit(&jj, &store, int).unwrap(),
            child_tip,
            "the fold advances integration to the child's real commit, not a squash"
        );

        // Advance integration beyond the child, then attempt to fold the
        // now-older child: a backwards move must be refused.
        jj.run(&store, &["new", int, "--ignore-working-copy"], "new on int")
            .unwrap();
        jj.run(
            &store,
            &[
                "describe",
                "-m",
                "integration advances",
                "--ignore-working-copy",
            ],
            "describe",
        )
        .unwrap();
        jj.run(
            &store,
            &["bookmark", "set", int, "-r", "@", "--ignore-working-copy"],
            "advance int",
        )
        .unwrap();
        assert!(
            merge_into_bookmark(&jj, &store, int, child).is_err(),
            "folding an older child into an advanced integration is refused (forward-only)"
        );

        // The backwards refusal must never leak jj's raw `--allow-backwards`
        // hint (which would clobber the commits that advanced integration); the
        // error is mapped to safe rebase-first guidance.
        let err = merge_into_bookmark(&jj, &store, int, child).unwrap_err();
        assert!(
            !err.to_lowercase().contains("allow-backwards"),
            "the backwards refusal must not surface the dangerous --allow-backwards hint: {err}"
        );
        assert!(
            err.contains("not a descendant"),
            "the sanitized error names the real cause: {err}"
        );
    }

    /// `bookmark_landed_in` is the ancestor test the merge postcondition and the
    /// merged-teardown guard both rely on: a child sealed ON TOP of integration is
    /// NOT landed until the fold, and IS landed once `merge_into_bookmark`
    /// fast-forwards integration onto it. Empty/missing bookmarks fail closed.
    #[test]
    #[serial_test::serial(jj)]
    fn bookmark_landed_in_tracks_the_fold() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping bookmark_landed_in_tracks_the_fold: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2287-coordinator-0";
        let child = "agent/CAIRN-1-builder-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();

        // The child seals a commit ON TOP of integration: its tip descends from
        // int, so it has NOT yet landed in int.
        std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
        seal(&jj, &ws_child, "child work", None).unwrap();
        assert!(
            !bookmark_landed_in(&jj, &store, child, int),
            "an un-folded child is not landed in integration"
        );

        // Fold it, and now the child's tip IS an ancestor of (equal to) int.
        merge_into_bookmark(&jj, &store, int, child).unwrap();
        assert!(
            bookmark_landed_in(&jj, &store, child, int),
            "once folded, the child has landed in integration"
        );

        // Fail-closed on empty or unknown bookmarks.
        assert!(!bookmark_landed_in(&jj, &store, "", int));
        assert!(!bookmark_landed_in(&jj, &store, child, ""));
        assert!(!bookmark_landed_in(&jj, &store, "agent/nonexistent", int));
    }

    /// Drive `DIV_SIBLING` into the clean-tip / conflicted-intermediate shape: the
    /// integration tip advances with a conflicting `shared.rs` edit, the sibling's
    /// original sealed commit is rebased onto it (recording a conflict on that
    /// INTERMEDIATE commit), then a resolving seal on top leaves the TIP clean.
    /// Returns the advanced integration tip commit id (the flatten dest).
    fn make_intermediate_only(fx: &DivergenceFixture) -> String {
        let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
        rebase_branch_onto(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT).unwrap();
        assert!(
            branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            "the rebase records a conflict on the sibling's sealed commit"
        );
        update_stale(&fx.jj, &fx.ws_sibling).unwrap();
        std::fs::write(fx.ws_sibling.join("shared.rs"), "resolved\n").unwrap();
        seal(&fx.jj, &fx.ws_sibling, "resolve conflict", None).unwrap();
        assert!(
            !branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
            "the resolving seal leaves the tip clean"
        );
        dest
    }

    /// A wedged coordinator hub for the CAIRN-2288 merge-time repro, built the way
    /// the incident arose. The hub (`int`) seals an edit to `shared.rs`; a child
    /// (`child`) branches from it and seals a DISTINCT file; `main` then advances
    /// with a CONFLICTING edit to `shared.rs`. The hub auto-rebases onto the
    /// advanced main (baking the conflict into the shared `hub-edit` intermediate,
    /// which the child also descends from) and the coordinator resolves at its tip
    /// and re-seals; the child, dragged onto the same conflicted intermediate,
    /// resolves at ITS OWN tip and re-seals. Both branches end with a CLEAN tip
    /// over the conflicted intermediate, and — crucially, mirroring the live
    /// topology — the child carries its own resolution rather than depending on
    /// the hub's. `main_tip` is the advanced base (the flatten dest); origin holds
    /// the hub at its pre-conflict tip so a post-merge push is a real advance.
    struct WedgedHub {
        _home: TempDir,
        _proj: TempDir,
        _wts: TempDir,
        _origin: TempDir,
        origin_path: PathBuf,
        jj: JjEnv,
        store: PathBuf,
        ws_coord: PathBuf,
        main_tip: String,
        int: &'static str,
        child: &'static str,
    }

    fn setup_wedged_hub(bin: &str) -> WedgedHub {
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let jj = JjEnv::resolve(bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Coordinator integration branch on main, published to origin, with its own
        // edit to shared.rs.
        let int = "agent/CAIRN-2241-coordinator-0";
        let ws_coord = wts.path().join("coord");
        add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
        ensure_bookmark_on_origin(&jj, &store, int).unwrap();
        std::fs::write(ws_coord.join("shared.rs"), "hub-edit\n").unwrap();
        seal(&jj, &ws_coord, "hub edits shared", None).unwrap();

        // A child branches from the hub's tip and seals a DISTINCT file, so it
        // shares the `hub-edit` commit as an ancestor.
        let child = "agent/CAIRN-2284-builder-0";
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
        std::fs::write(ws_child.join("child.rs"), "child-work\n").unwrap();
        seal(&jj, &ws_child, "child work", None).unwrap();

        // `main` advances out of band with a CONFLICTING edit to shared.rs.
        jj.run(&store, &["new", "main"], "new on main").unwrap();
        std::fs::write(store.join("shared.rs"), "main-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "main advances shared"],
            "describe main",
        )
        .unwrap();
        let main_tip = jj
            .run(
                &store,
                &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
                "main tip",
            )
            .unwrap()
            .trim()
            .to_string();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                "@",
                "--ignore-working-copy",
            ],
            "advance main",
        )
        .unwrap();

        // The hub auto-rebases onto the advanced main, baking the conflict into the
        // shared `hub-edit` commit; the child (which descends from it) is dragged
        // onto the same conflicted commit. Resolve each branch at ITS OWN tip and
        // re-seal, leaving both with a CLEAN tip over the conflicted INTERMEDIATE.
        rebase_branch_onto(&jj, &store, int, "main").unwrap();
        assert!(branch_has_conflict(&jj, &store, int).unwrap());
        update_stale(&jj, &ws_coord).unwrap();
        std::fs::write(ws_coord.join("shared.rs"), "hub-resolved\n").unwrap();
        seal(&jj, &ws_coord, "resolve hub conflict", None).unwrap();
        assert!(!branch_has_conflict(&jj, &store, int).unwrap());

        // The child was dragged onto the rewritten conflicted `hub-edit`; resolve
        // it independently (its own resolution commit, not the hub's).
        assert!(branch_has_conflict(&jj, &store, child).unwrap());
        update_stale(&jj, &ws_child).unwrap();
        std::fs::write(ws_child.join("shared.rs"), "hub-resolved\n").unwrap();
        seal(&jj, &ws_child, "resolve child conflict", None).unwrap();
        assert!(!branch_has_conflict(&jj, &store, child).unwrap());

        assert_eq!(
            flatten_state(&jj, &store, &main_tip, int).unwrap(),
            FlattenState::IntermediateOnly,
            "hub: clean tip over a conflicted intermediate"
        );

        let origin_path = origin.path().to_path_buf();
        WedgedHub {
            _home: home,
            _proj: proj,
            _wts: wts,
            _origin: origin,
            origin_path,
            jj,
            store,
            ws_coord,
            main_tip,
            int,
            child,
        }
    }

    /// Count the commits a range revset resolves to over the store.
    fn count_commits(jj: &JjEnv, store: &Path, range: &str) -> usize {
        jj.run(
            store,
            &[
                "log",
                "-r",
                range,
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "count commits",
        )
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count()
    }

    /// The core recovery: a branch with a conflicted INTERMEDIATE commit and a
    /// clean tip is classified `IntermediateOnly`, and `flatten_branch_recovery`
    /// collapses it to ONE clean commit on the dest whose tree equals the clean
    /// tip — no conflict anywhere, exact tree preserved, parented on the dest.
    #[test]
    #[serial_test::serial(jj)]
    fn flatten_recovers_clean_tip_conflicted_intermediate() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping flatten_recovers_clean_tip_conflicted_intermediate: jj not resolvable"
            );
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let dest = make_intermediate_only(&fx);

        assert_eq!(
            flatten_state(&fx.jj, &fx.store, &dest, DIV_SIBLING).unwrap(),
            FlattenState::IntermediateOnly,
            "clean tip over a conflicted intermediate is flatten-recoverable"
        );
        let pre_tree = file_show(&fx.jj, &fx.store, DIV_SIBLING, "shared.rs").unwrap();

        let report =
            flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened recovery")
                .unwrap();
        assert!(
            report.collapsed_conflicted_commits >= 1,
            "the flatten collapsed at least the conflicted intermediate"
        );

        // Exactly one commit remains in dest..branch, and it carries no conflict.
        let range = format!("{dest}..bookmarks(exact:{DIV_SIBLING:?})");
        assert_eq!(
            count_commits(&fx.jj, &fx.store, &range),
            1,
            "the branch is collapsed to a single commit on the dest"
        );
        assert!(
            conflicted_commits(&fx.jj, &fx.store, &range).is_empty(),
            "no conflicted commit survives the flatten"
        );
        assert!(!branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap());

        // The net tree is preserved exactly.
        let post_tree = file_show(&fx.jj, &fx.store, DIV_SIBLING, "shared.rs").unwrap();
        assert_eq!(
            post_tree, pre_tree,
            "the flattened tree equals the clean tip tree"
        );
        assert_eq!(String::from_utf8_lossy(&post_tree), "resolved\n");

        // The single commit's only parent is the dest.
        let parents = fx
            .jj
            .run(
                &fx.store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{DIV_SIBLING:?})"),
                    "--no-graph",
                    "--ignore-working-copy",
                    "-T",
                    "parents.map(|c| c.commit_id()).join(\",\")",
                ],
                "flattened parents",
            )
            .unwrap();
        assert_eq!(
            parents, dest,
            "the flattened commit is parented on the dest"
        );
    }

    /// The footprint pre-guard: flattening onto a base the branch does NOT descend
    /// from returns the typed guard error and leaves the bookmark UNMUTATED (the
    /// squash never runs), so a wrong-base flatten can never revert base files.
    #[test]
    #[serial_test::serial(jj)]
    fn flatten_footprint_guard_rejects_wrong_base() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping flatten_footprint_guard_rejects_wrong_base: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        // A second sibling off the integration branch on a divergent line: the
        // first sibling does not descend from it, so it is a wrong flatten base.
        let wts2 = TempDir::new().unwrap();
        let other = "agent/CAIRN-9-builder-0";
        let ws_other = wts2.path().join("other");
        add_workspace(&fx.jj, &fx.store, &ws_other, other, DIV_INT, None).unwrap();
        std::fs::write(ws_other.join("other2.rs"), "x\n").unwrap();
        seal(&fx.jj, &ws_other, "other edits other2", None).unwrap();
        let wrong_dest = bookmark_commit(&fx.jj, &fx.store, other).unwrap();

        let before = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        let err = flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &wrong_dest, "wrong")
            .unwrap_err();
        assert!(
            err.contains("does not descend"),
            "the pre-guard names the wrong-base cause: {err}"
        );
        let after = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        assert_eq!(
            before, after,
            "a rejected flatten does not mutate the bookmark"
        );
    }

    /// Twin/orphan cleanup: the squash mints a fresh change-id, so every commit
    /// sharing the PRE-flatten change-id (the orphaned old lineage tip, and any
    /// conflicted divergent twin) is abandoned — no commit retains the old id.
    #[test]
    #[serial_test::serial(jj)]
    fn flatten_abandons_orphaned_change_id_commits() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping flatten_abandons_orphaned_change_id_commits: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        let dest = make_intermediate_only(&fx);
        let pre_change = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
        assert!(
            visible_commits_for_change(&fx.jj, &fx.store, &pre_change) >= 1,
            "precondition: the pre-flatten change-id is visible"
        );

        let report =
            flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened").unwrap();
        assert!(
            !report.abandoned_twins.is_empty(),
            "the orphaned old lineage tip is abandoned"
        );
        assert_eq!(
            visible_commits_for_change(&fx.jj, &fx.store, &pre_change),
            0,
            "no commit retains the pre-flatten change-id after cleanup"
        );
        assert_ne!(
            pre_change,
            change_id_of(&fx.jj, &fx.store, DIV_SIBLING),
            "the flattened commit carries a fresh change-id"
        );
    }

    /// End-to-end reconcile: a sibling in the clean-tip / conflicted-intermediate
    /// shape is FLATTENED by `reconcile_siblings` (not left wedged), classified
    /// `rebased_clean`, and pushed so its PR head advances on origin — the branch is
    /// pushable/mergeable with no hand-run jj.
    #[test]
    #[serial_test::serial(jj)]
    fn reconcile_siblings_flattens_intermediate_only_sibling() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping reconcile_siblings_flattens_intermediate_only_sibling: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-1940-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        ensure_bookmark_on_origin(&jj, &store, int).unwrap();

        let sibling = "agent/CAIRN-1-builder-0";
        let ws = wts.path().join("sib");
        add_workspace(&jj, &store, &ws, sibling, int, None).unwrap();
        std::fs::write(ws.join("shared.rs"), "sibling-edit\n").unwrap();
        seal(&jj, &ws, "sibling edits shared", None).unwrap();
        push_to_origin(&jj, &ws, sibling);
        let origin_before = git_stdout(origin.path(), &["rev-parse", sibling]);

        // Advance the integration tip conflictingly and publish it.
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "int advances shared"],
            "describe",
        )
        .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();
        jj.run(
            &store,
            &["git", "push", "--remote", "origin", "--bookmark", int],
            "push int",
        )
        .unwrap();

        // Drive the sibling into the clean-tip / conflicted-intermediate shape:
        // rebase onto the conflicting tip, then resolve on top.
        rebase_branch_onto(&jj, &store, sibling, int).unwrap();
        assert!(branch_has_conflict(&jj, &store, sibling).unwrap());
        update_stale(&jj, &ws).unwrap();
        std::fs::write(ws.join("shared.rs"), "resolved\n").unwrap();
        seal(&jj, &ws, "resolve conflict", None).unwrap();
        assert!(!branch_has_conflict(&jj, &store, sibling).unwrap());
        let dest = bookmark_commit(&jj, &store, int).unwrap();
        assert_eq!(
            flatten_state(&jj, &store, &dest, sibling).unwrap(),
            FlattenState::IntermediateOnly
        );

        // The reconcile flattens the sibling (already-on-dest path), classifies it
        // clean, and pushes the flattened tip to origin.
        let report =
            reconcile_siblings(&jj, &store, int, &[(sibling.to_string(), ws.clone())]).unwrap();
        assert_eq!(report.rebased_clean, vec![sibling.to_string()]);
        assert!(report.conflicted.is_empty());

        let range = format!("{dest}..bookmarks(exact:{sibling:?})");
        assert_eq!(
            count_commits(&jj, &store, &range),
            1,
            "sibling collapsed to one commit"
        );
        assert!(conflicted_commits(&jj, &store, &range).is_empty());
        assert!(!branch_has_conflict(&jj, &store, sibling).unwrap());

        let origin_after = git_stdout(origin.path(), &["rev-parse", sibling]);
        assert_ne!(
            origin_before, origin_after,
            "the flattened sibling's PR head advanced on origin"
        );
        // A flattened (clean) bookmark pushes; the wedge is gone.
        assert!(
            jj.run(
                &store,
                &["git", "push", "--remote", "origin", "--bookmark", sibling],
                "re-push flattened",
            )
            .is_ok(),
            "the flattened sibling is pushable"
        );
    }

    /// Component C: a sibling bookmark riding a conflicted INTERMEDIATE commit
    /// (the live `agent/CAIRN-2285-planner-0 @ c6b16933` shape) does NOT block the
    /// flatten, is re-pointed onto the flattened commit, and is reported in
    /// `repointed_bookmarks` — so a later reconcile finds it clean on the new tip
    /// instead of resurrecting the orphaned conflicted lineage.
    #[test]
    #[serial_test::serial(jj)]
    fn flatten_repoints_rider_bookmark_on_conflicted_intermediate() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping flatten_repoints_rider_bookmark_on_conflicted_intermediate: jj not resolvable");
            return;
        };
        let fx = setup_divergence_fixture(&bin);
        // Advance the integration tip conflictingly and rebase the sibling onto it,
        // recording a conflict on the sibling's (now INTERMEDIATE) sealed commit.
        let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
        rebase_branch_onto(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT).unwrap();
        assert!(branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap());

        // A sibling planner bookmark rides the conflicted intermediate.
        let rider = "agent/CAIRN-2285-planner-0";
        let conflicted_intermediate = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "create",
                    rider,
                    "-r",
                    &conflicted_intermediate,
                    "--ignore-working-copy",
                ],
                "create rider bookmark",
            )
            .unwrap();

        // Resolve on top so the sibling's TIP is clean over the conflicted intermediate.
        update_stale(&fx.jj, &fx.ws_sibling).unwrap();
        std::fs::write(fx.ws_sibling.join("shared.rs"), "resolved\n").unwrap();
        seal(&fx.jj, &fx.ws_sibling, "resolve conflict", None).unwrap();
        assert_eq!(
            flatten_state(&fx.jj, &fx.store, &dest, DIV_SIBLING).unwrap(),
            FlattenState::IntermediateOnly
        );

        let report =
            flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened").unwrap();

        // The rider did not block the flatten and was re-pointed onto the flattened commit.
        assert!(
            report.repointed_bookmarks.contains(&rider.to_string()),
            "the rider is reported as re-pointed: {:?}",
            report.repointed_bookmarks
        );
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, rider).unwrap(),
            report.flattened_commit,
            "the rider now points at the flattened commit"
        );
        // The re-pointed rider is a clean descendant of the dest — pushable, no
        // orphaned conflicted lineage for a later reconcile to resurrect.
        assert!(!branch_has_conflict(&fx.jj, &fx.store, rider).unwrap());
        assert!(branch_descends_from(&fx.jj, &fx.store, rider, &dest));
    }

    /// Component B: `operation_id` + `restore_operation` roll a fold back to its
    /// exact pre-merge state — after a rebase+fold advances the integration
    /// bookmark, restoring the snapshot returns both bookmarks to their pre-merge
    /// commits and realigns the backing git refs, and a retry then lands cleanly
    /// with no divergent-change accumulation.
    #[test]
    #[serial_test::serial(jj)]
    fn operation_id_and_restore_operation_roll_back_a_fold() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping operation_id_and_restore_operation_roll_back_a_fold: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2288-coordinator-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let child = "agent/CAIRN-1-builder-0";
        let ws = wts.path().join("child");
        add_workspace(&jj, &store, &ws, child, int, None).unwrap();
        std::fs::write(ws.join("child.rs"), "child\n").unwrap();
        seal(&jj, &ws, "child edits child.rs", None).unwrap();

        let source_pre = bookmark_commit(&jj, &store, child).unwrap();
        let target_pre = bookmark_commit(&jj, &store, int).unwrap();

        // Snapshot, then fold the child into the integration bookmark.
        let op = operation_id(&jj, &store).unwrap();
        rebase_branch_onto(&jj, &store, child, int).unwrap();
        merge_into_bookmark(&jj, &store, int, child).unwrap();
        assert_ne!(
            bookmark_commit(&jj, &store, int).unwrap(),
            target_pre,
            "the fold advanced the integration bookmark"
        );

        // Roll back to the snapshot: both bookmarks return to their pre-merge commits.
        restore_operation(&jj, &store, &op).unwrap();
        assert_eq!(
            bookmark_commit(&jj, &store, child).unwrap(),
            source_pre,
            "the source bookmark is restored to its pre-merge commit"
        );
        assert_eq!(
            bookmark_commit(&jj, &store, int).unwrap(),
            target_pre,
            "the target bookmark is restored to its pre-merge commit"
        );
        // The exported backing git ref realigns with the restored bookmark.
        assert_eq!(
            git_stdout(proj.path(), &["rev-parse", int]),
            target_pre,
            "the git ref realigned to the restored target"
        );

        // A retry after the rollback lands cleanly — no empty-commit accumulation,
        // no divergent twin for the child change.
        rebase_branch_onto(&jj, &store, child, int).unwrap();
        merge_into_bookmark(&jj, &store, int, child).unwrap();
        assert!(
            bookmark_landed_in(&jj, &store, child, int),
            "the retried fold carries the child into the integration branch"
        );
        assert_eq!(
            visible_commits_for_change(&jj, &store, &change_id_of(&jj, &store, child)),
            1,
            "no divergent twin accumulated for the child change across the rollback+retry"
        );
    }

    /// Component A (the load-bearing fix), positive case: a hub with a CLEAN tip
    /// over a conflicted INTERMEDIATE (a `main` advance baked the conflict into the
    /// hub's own history; the coordinator resolved at the tip and re-sealed) is
    /// flattened at merge time, the child rebases + folds onto it, and the
    /// integration branch pushes to a bare origin — the wedge is cleared.
    #[test]
    #[serial_test::serial(jj)]
    fn target_flatten_unwedges_child_merge_into_conflicted_hub() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping target_flatten_unwedges_child_merge_into_conflicted_hub: jj not resolvable");
            return;
        };
        let hub = setup_wedged_hub(&bin);
        // The fixture's child: clean tip over its own conflicted intermediate,
        // carrying its own resolution (as the live child B did).
        let child = hub.child;

        // === Replicate store_merge_child's integration path ===
        // 1. Target preflight: flatten the hub onto its base (main_tip).
        let report =
            flatten_branch_recovery(&hub.jj, &hub.store, hub.int, &hub.main_tip, "hub flattened")
                .unwrap();
        assert!(
            report.collapsed_conflicted_commits >= 1,
            "the hub flatten collapsed its conflicted intermediate"
        );
        assert!(!branch_has_conflict(&hub.jj, &hub.store, hub.int).unwrap());
        let flattened_int = bookmark_commit(&hub.jj, &hub.store, hub.int).unwrap();
        advance_workspace_onto(&hub.jj, &hub.store, &hub.ws_coord, hub.int, &flattened_int)
            .unwrap();

        // 2. Rebase the source onto the flattened integration tip. The rebase
        //    re-applies the child's inherited (old) hub lineage, so it now carries a
        //    conflicted intermediate of its own — the SOURCE flatten clears it.
        rebase_branch_onto(&hub.jj, &hub.store, child, hub.int).unwrap();
        if let FlattenState::IntermediateOnly =
            flatten_state(&hub.jj, &hub.store, hub.int, child).unwrap()
        {
            let d = bookmark_commit(&hub.jj, &hub.store, hub.int).unwrap();
            flatten_branch_recovery(&hub.jj, &hub.store, child, &d, "child flattened").unwrap();
        }
        let range = format!("bookmarks(exact:{:?})..bookmarks(exact:{child:?})", hub.int);
        assert!(
            conflicted_commits(&hub.jj, &hub.store, &range).is_empty(),
            "no conflicted commit survives on the child after the flattens"
        );

        // 3. Fold + push: the integration branch advances on origin (was wedged).
        merge_into_bookmark(&hub.jj, &hub.store, hub.int, child).unwrap();
        let origin_before = git_stdout(&hub.origin_path, &["rev-parse", hub.int]);
        track_bookmark(&hub.jj, &hub.store, child).ok();
        push_store_bookmark(&hub.jj, &hub.store, child).unwrap();
        push_store_bookmark(&hub.jj, &hub.store, hub.int).unwrap();
        let origin_after = git_stdout(&hub.origin_path, &["rev-parse", hub.int]);
        assert_ne!(
            origin_before, origin_after,
            "the integration branch advanced on origin after the merge"
        );
        assert!(
            bookmark_landed_in(&hub.jj, &hub.store, child, hub.int),
            "the child's tip is an ancestor of the advanced integration branch"
        );
    }

    /// Component A, the pinned failure mode: WITHOUT the target flatten, folding a
    /// child into the conflicted hub leaves the integration branch's ancestry
    /// carrying a conflicted commit, so the push is REFUSED — exactly the live
    /// wedge (`Won't push commit ... since it has conflicts`).
    #[test]
    #[serial_test::serial(jj)]
    fn child_merge_push_refused_without_target_flatten() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping child_merge_push_refused_without_target_flatten: jj not resolvable"
            );
            return;
        };
        let hub = setup_wedged_hub(&bin);
        let child = hub.child;

        // Skip the target flatten; fold the child straight into the conflicted hub.
        rebase_branch_onto(&hub.jj, &hub.store, child, hub.int).unwrap();
        merge_into_bookmark(&hub.jj, &hub.store, hub.int, child).unwrap();

        // The integration branch's ancestry now includes the hub's conflicted
        // intermediate, so jj refuses to push it.
        let err = push_store_bookmark(&hub.jj, &hub.store, hub.int).unwrap_err();
        assert!(
            err.to_lowercase().contains("conflict"),
            "the push is refused for the conflicted ancestor: {err}"
        );
    }

    /// `restore_bookmark` undoes a `squash_branch_onto`: after the squash moves the
    /// bookmark to a new flattened commit, restoring it returns the bookmark to the
    /// exact pre-squash tip and its full multi-commit lineage. This is the recovery
    /// the post-squash flatten guards run so a refused flatten never leaves the
    /// branch rewritten. (The footprint guard itself cannot be triggered through the
    /// real jj harness — `squash_branch_onto` restores the exact tip tree, so the
    /// post/pre footprints are equal by construction — so the restore mechanism is
    /// covered directly here rather than via a forced guard failure.)
    #[test]
    #[serial_test::serial(jj)]
    fn restore_bookmark_resets_a_squashed_branch() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping restore_bookmark_resets_a_squashed_branch: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let branch = "agent/CAIRN-3001-builder-0";
        let ws = wts.path().join("src");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        for i in 1..=2 {
            std::fs::write(ws.join(format!("c{i}.rs")), format!("c{i}\n")).unwrap();
            seal(&jj, &ws, &format!("c{i}"), None).unwrap();
        }
        let pre_tip = bookmark_commit(&jj, &store, branch).unwrap();

        squash_branch_onto(&jj, &store, branch, "main", "squashed").unwrap();
        assert_ne!(
            bookmark_commit(&jj, &store, branch).unwrap(),
            pre_tip,
            "the squash moved the bookmark off the pre-squash tip"
        );

        restore_bookmark(&jj, &store, branch, &pre_tip).unwrap();
        assert_eq!(
            bookmark_commit(&jj, &store, branch).unwrap(),
            pre_tip,
            "restore returns the bookmark to the exact pre-squash tip"
        );
        assert_eq!(
            count_commits(&jj, &store, &format!("main..bookmarks(exact:{branch:?})")),
            2,
            "the original multi-commit lineage is restored (the squash is fully undone)"
        );
    }

    /// `squash_branch_onto` collapses a multi-commit branch into a single commit
    /// on top of a base, preserving the branch's tree and taking the given
    /// message — the store-side primitive that restores the squash shape at a
    /// default-branch landing.
    #[test]
    #[serial_test::serial(jj)]
    fn squash_branch_onto_collapses_chain_to_one_commit() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping squash_branch_onto_collapses_chain_to_one_commit: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // A branch cut from main with THREE sealed commits, each adding a file.
        let branch = "agent/CAIRN-2001-builder-0";
        let ws = wts.path().join("src");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        for i in 1..=3 {
            std::fs::write(ws.join(format!("change{i}.rs")), format!("change {i}\n")).unwrap();
            seal(&jj, &ws, &format!("change {i}"), None).unwrap();
        }
        let base = bookmark_commit(&jj, &store, "main").unwrap();

        squash_branch_onto(&jj, &store, branch, "main", "Squashed PR title").unwrap();

        // One commit: its only parent is the base (the main tip).
        let parents = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{branch:?})"),
                    "--no-graph",
                    "--ignore-working-copy",
                    "-T",
                    "parents.map(|c| c.commit_id()).join(\",\")",
                ],
                "squash parents",
            )
            .unwrap();
        assert_eq!(
            parents, base,
            "the squashed commit's only parent is the base"
        );

        // Tree equals the source: all three files survive in the single commit.
        let files = jj
            .run(
                &store,
                &["file", "list", "--ignore-working-copy", "-r", branch],
                "squash files",
            )
            .unwrap();
        for i in 1..=3 {
            assert!(
                files.contains(&format!("change{i}.rs")),
                "file change{i}.rs present in the squashed tree: {files}"
            );
        }

        // The single commit carries the squash message (the PR title).
        let desc = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("bookmarks(exact:{branch:?})"),
                    "--no-graph",
                    "--ignore-working-copy",
                    "-T",
                    "description",
                ],
                "squash description",
            )
            .unwrap();
        assert!(
            desc.contains("Squashed PR title"),
            "the squashed commit's description is the PR title: {desc}"
        );
    }

    /// `rebase_then_fold_into`'s clean path: the project default branch advances
    /// OUT OF BAND past the source's fork point (another PR merged into it), so a
    /// bare FF would be refused. The primitive rebases the source onto the
    /// advanced default, then FFs the default to it — landing the source's real
    /// (rebased) commit, never a squash, and moving the default strictly forward.
    #[test]
    #[serial_test::serial(jj)]
    fn rebase_then_fold_lands_source_after_out_of_band_default_advance() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping rebase_then_fold_lands_source_after_out_of_band_default_advance: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // A source branch cut from main, sealing a commit that edits a NEW file
        // (so it never conflicts with the out-of-band default advance below).
        let source = "agent/CAIRN-1987-coordinator-0";
        let ws_src = wts.path().join("src");
        add_workspace(&jj, &store, &ws_src, source, "main", None).unwrap();
        std::fs::write(ws_src.join("feature.rs"), "feature\n").unwrap();
        seal(&jj, &ws_src, "feature work", None).unwrap();

        // The default branch advances OUT OF BAND past the source's fork point,
        // via its own workspace editing a different file, then main FFs to it.
        let oob = "agent/CAIRN-9-oob-0";
        let ws_oob = wts.path().join("oob");
        add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
        std::fs::write(ws_oob.join("infra.rs"), "infra\n").unwrap();
        seal(&jj, &ws_oob, "main advances out of band", None).unwrap();
        let oob_tip = bookmark_commit(&jj, &store, oob).unwrap();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                &oob_tip,
                "--ignore-working-copy",
            ],
            "advance main out of band",
        )
        .unwrap();

        // A bare FF is refused now (source is sideways from the advanced main).
        assert!(
            merge_into_bookmark(&jj, &store, "main", source).is_err(),
            "precondition: a bare fold is refused once main advanced past the source"
        );

        // Rebase-then-fold against the LOCAL default tip (no remote needed).
        rebase_then_fold_into(&jj, &store, "main", source, "main").unwrap();

        // The source landed as its real rebased commit (not a squash): main and
        // the source bookmark resolve to the same commit.
        assert_eq!(
            bookmark_commit(&jj, &store, "main").unwrap(),
            bookmark_commit(&jj, &store, source).unwrap(),
            "the fold advances main to the source's rebased commit, not a squash"
        );
        // Forward-only: the out-of-band tip is an ancestor of the new main.
        let main_after = bookmark_commit(&jj, &store, "main").unwrap();
        let fwd = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("{oob_tip} & ::{main_after}"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ],
                "forward-only check",
            )
            .unwrap();
        assert_eq!(
            fwd, oob_tip,
            "main moved forward: the out-of-band commit is an ancestor of the folded tip"
        );
        assert!(
            !branch_has_conflict(&jj, &store, source).unwrap(),
            "the clean rebase recorded no conflict"
        );
    }

    /// `rebase_then_fold_into`'s conflict path: the source and the out-of-band
    /// default advance edit the same file conflictingly. The rebase records a
    /// conflict, so the primitive returns a SAFE error (resolve-and-retry, never
    /// `--allow-backwards`) and leaves the default bookmark UNCHANGED — it is
    /// never moved backward.
    #[test]
    #[serial_test::serial(jj)]
    fn rebase_then_fold_conflict_returns_safe_error_and_leaves_default_unmoved() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping rebase_then_fold_conflict_returns_safe_error_and_leaves_default_unmoved: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // The source edits `shared.rs` (present at base) and seals.
        let source = "agent/CAIRN-1987-coordinator-0";
        let ws_src = wts.path().join("src");
        add_workspace(&jj, &store, &ws_src, source, "main", None).unwrap();
        std::fs::write(ws_src.join("shared.rs"), "source-change\n").unwrap();
        seal(&jj, &ws_src, "source edits shared", None).unwrap();

        // The default advances out of band editing the SAME file conflictingly.
        let oob = "agent/CAIRN-9-oob-0";
        let ws_oob = wts.path().join("oob");
        add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
        std::fs::write(ws_oob.join("shared.rs"), "out-of-band-change\n").unwrap();
        seal(&jj, &ws_oob, "main advances conflictingly", None).unwrap();
        let oob_tip = bookmark_commit(&jj, &store, oob).unwrap();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                &oob_tip,
                "--ignore-working-copy",
            ],
            "advance main out of band",
        )
        .unwrap();

        let main_before = bookmark_commit(&jj, &store, "main").unwrap();

        let err = rebase_then_fold_into(&jj, &store, "main", source, "main").unwrap_err();
        assert!(
            !err.to_lowercase().contains("allow-backwards"),
            "the conflict error must never surface the dangerous --allow-backwards hint: {err}"
        );
        assert!(
            err.to_lowercase().contains("conflict"),
            "the error explains a conflict was recorded: {err}"
        );
        assert_eq!(
            bookmark_commit(&jj, &store, "main").unwrap(),
            main_before,
            "the default bookmark is left unchanged — never moved backward on a conflict"
        );

        // The conflict was recorded on the source bookmark, but its live
        // workspace `@` was rebased out from under it and is stale. Refreshing it
        // (what `store_merge_child` does via `materialize_source_conflict_in_workspaces`
        // on this conflict) materializes the markers the resolve-and-retry error
        // points the agent at — without this, the guidance would be empty.
        assert!(
            branch_has_conflict(&jj, &store, source).unwrap(),
            "the conflict is recorded on the source bookmark"
        );
        update_stale(&jj, &ws_src).unwrap();
        let on_disk = std::fs::read_to_string(ws_src.join("shared.rs")).unwrap();
        assert!(
            on_disk.contains("<<<<<<<") && on_disk.contains(">>>>>>>"),
            "refreshing the source workspace materializes the conflict markers on disk: {on_disk}"
        );
    }

    /// A store-side rebase must export the moved bookmark back to the backing git
    /// ref. Otherwise jj leaves the bookmark conflicted between the local tip and
    /// the stale `@git` tracking ref, which makes later descendant checks stop
    /// seeing the branch as already reconciled.
    #[test]
    #[serial_test::serial(jj)]
    fn rebase_branch_exports_git_ref_to_rebased_tip() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping rebase_branch_exports_git_ref_to_rebased_tip: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let branch = "agent/CAIRN-2078-builder-0";
        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        std::fs::write(ws.join("agent.rs"), "agent work\n").unwrap();
        seal(&jj, &ws, "agent work", None).unwrap();
        let git_before = git_stdout(proj.path(), &["rev-parse", branch]);
        assert_eq!(
            git_before,
            bookmark_commit(&jj, &store, branch).unwrap(),
            "seal exports the initial branch ref"
        );

        advance_project(proj.path());
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        rebase_branch_onto(&jj, &store, branch, "main").unwrap();
        let rebased_tip = bookmark_commit(&jj, &store, branch).unwrap();
        let git_after = git_stdout(proj.path(), &["rev-parse", branch]);

        assert_ne!(git_before, rebased_tip, "the rebase moved the branch tip");
        assert_eq!(
            git_after, rebased_tip,
            "rebase_branch_onto exports the moved bookmark to the backing git ref"
        );
        let bookmarks = jj
            .run(
                &store,
                &["bookmark", "list", branch],
                "jj bookmark list branch",
            )
            .unwrap();
        assert!(
            !bookmarks.contains("@git"),
            "the branch must not remain conflicted against a stale @git ref: {bookmarks}"
        );
    }

    /// After a fold, the project's backing git ref for the integration branch must
    /// track the advanced tip, so a later child — provisioned the way
    /// `execution/jobs/worktrees.rs` does (rev-parse the base ref in the project
    /// git, then `add_workspace`) — bases on the folded tip rather than a stale
    /// pre-merge ref left behind by an earlier child's `jj git export`.
    #[test]
    #[serial_test::serial(jj)]
    fn fold_exports_so_a_later_child_bases_on_the_folded_tip() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping fold_exports_so_a_later_child_bases_on_the_folded_tip: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-1940-coordinator-0";
        let child_a = "agent/CAIRN-1-builder-0";
        add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
        let ws_a = wts.path().join("a");
        add_workspace(&jj, &store, &ws_a, child_a, int, None).unwrap();
        std::fs::write(ws_a.join("a.rs"), "a work\n").unwrap();
        // Sealing exports ALL store bookmarks to the project git, creating
        // `refs/heads/<int>` at the *pre-fold* integration tip — the stale ref the
        // bug would later rev-parse.
        seal(&jj, &ws_a, "child a work", None).unwrap();
        let child_tip = bookmark_commit(&jj, &store, child_a).unwrap();
        let int_before = git_stdout(proj.path(), &["rev-parse", int]);
        assert_ne!(
            int_before, child_tip,
            "precondition: the project git int ref starts at the pre-fold tip"
        );

        // Fold child A into integration; this must export the advanced ref.
        merge_into_bookmark(&jj, &store, int, child_a).unwrap();
        let int_after = git_stdout(proj.path(), &["rev-parse", int]);
        assert_eq!(
            int_after, child_tip,
            "the fold exports the advanced integration ref to the backing git"
        );

        // Provision a later child exactly as worktrees.rs does: rev-parse the base
        // ref in the project, then add_workspace off that commit id.
        let base_rev = git_stdout(proj.path(), &["rev-parse", int]);
        let child_b = "agent/CAIRN-2-builder-0";
        let ws_b = wts.path().join("b");
        add_workspace(&jj, &store, &ws_b, child_b, &base_rev, None).unwrap();
        assert_eq!(
            bookmark_commit(&jj, &store, child_b).unwrap(),
            child_tip,
            "the later child bases off the folded integration tip, not a stale project ref"
        );
    }

    /// Shared setup for the coordinator-advance tests: a coordinator workspace on
    /// its integration bookmark plus a child workspace branched from it; the child
    /// seals a file and folds into integration. Returns the integration tip after
    /// the fold and the coordinator workspace path — whose `@` is now STALE behind
    /// the tip (the exact post-merge state CAIRN-1994 is about).
    #[cfg(test)]
    fn fold_child_leaving_coordinator_stale(
        jj: &JjEnv,
        store: &Path,
        wts: &Path,
    ) -> (
        String,  /* int_tip */
        PathBuf, /* ws_coord */
        String,  /* int branch */
    ) {
        let int = "agent/CAIRN-1987-coordinator-0";
        let child = "agent/CAIRN-1988-builder-0";
        let ws_coord = wts.join("coord");
        add_workspace(jj, store, &ws_coord, int, "main", None).unwrap();
        let ws_child = wts.join("child");
        add_workspace(jj, store, &ws_child, child, int, None).unwrap();

        std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
        seal(jj, &ws_child, "child work", None).unwrap();
        merge_into_bookmark(jj, store, int, child).unwrap();
        let int_tip = bookmark_commit(jj, store, int).unwrap();

        // Precondition: the coordinator `@` is stale — its parent is the pre-fold
        // base, not the folded tip, and the child's file is absent on disk.
        let coord_parent = jj
            .run(
                &ws_coord,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "coord @-",
            )
            .unwrap();
        assert_ne!(
            coord_parent, int_tip,
            "precondition: the coordinator @ is stale behind the folded tip"
        );
        assert!(
            !ws_coord.join("child.rs").exists(),
            "precondition: the child's file is absent from the stale coordinator workspace"
        );
        (int_tip, ws_coord, int.to_string())
    }

    /// `is_stale_error` classifies the two jj refusals the commit barrier must
    /// self-heal — the `working copy is stale` message and the `seal_paths`
    /// "behind its branch tip" precheck — and nothing else.
    #[test]
    fn is_stale_error_classifies_the_stale_family() {
        assert!(is_stale_error(
            "Error: The working copy is stale (not updated since operation abc123)."
        ));
        assert!(is_stale_error(
            "seal refused: workspace `agent/x` is behind its branch tip — the branch advanced"
        ));
        assert!(!is_stale_error("nothing to commit, working tree clean"));
        assert!(!is_stale_error("error: pre-commit hook failed"));
        // The lost-seal marker is its OWN family, not folded into the stale one:
        // the cause and remediation differ, so the predicates stay distinct.
        assert!(!is_stale_error(LOST_SEAL_MSG));
        // The conflicted-branch refusal is ALSO its own family — it must PRESERVE
        // the working copy, not discard it — so the stale classifier must not claim
        // it (it deliberately omits the "behind its branch tip" phrase).
        assert!(!is_stale_error(CONFLICTED_BRANCH_SEAL_MSG));
    }

    /// `is_conflicted_branch_seal_error` recognizes the conflicted-branch marker
    /// and rejects every other seal-failure family, so the routing sites can give
    /// it its own non-destructive arm without stealing the stale / lost-seal
    /// cases that recover by discard / re-seal.
    #[test]
    fn is_conflicted_branch_seal_error_classifies_the_conflicted_branch_message() {
        assert!(is_conflicted_branch_seal_error(CONFLICTED_BRANCH_SEAL_MSG));
        // Wrapped in the write-path's surrounding text it still classifies.
        assert!(is_conflicted_branch_seal_error(&format!(
            "Applied file changes but the seal was refused: {CONFLICTED_BRANCH_SEAL_MSG}. ..."
        )));
        // Distinct from the stale and lost-seal families it is routed alongside.
        assert!(!is_conflicted_branch_seal_error(
            "Error: The working copy is stale (not updated since operation abc123)."
        ));
        assert!(!is_conflicted_branch_seal_error(
            "seal refused: workspace `agent/x` is behind its branch tip"
        ));
        assert!(!is_conflicted_branch_seal_error(LOST_SEAL_MSG));
        // And neither sibling classifier claims the conflicted-branch message.
        assert!(!is_stale_error(CONFLICTED_BRANCH_SEAL_MSG));
        assert!(!is_lost_seal_error(CONFLICTED_BRANCH_SEAL_MSG));
    }

    /// Real-store regression guard: when the branch bookmark tip carries a
    /// recorded conflict and `@` has been moved to a fresh line off the current
    /// base (the deliberate resolve-at-base flatten), `seal_paths` refuses with
    /// the DISTINCT conflicted-branch error — NOT the stale "behind its branch
    /// tip" message that would route the seal into a destructive discard. This is
    /// the empirical confirmation that the conflicted-tip distinguisher fires and
    /// the new classifier never lets the silent-data-loss path reach the flatten.
    #[test]
    #[serial_test::serial(jj)]
    fn seal_refuses_conflicted_branch_with_distinct_error() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping seal_refuses_conflicted_branch_with_distinct_error: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path()); // main: shared.rs = "base\n"
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let branch = "agent/CAIRN-2081-builder-0";
        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

        // Feature edit on shared.rs, sealed on the agent branch.
        std::fs::write(ws.join("shared.rs"), "feature change\n").unwrap();
        seal(&jj, &ws, "feature edit", None).unwrap();

        // main advances with a CONFLICTING change to the same file, re-imported.
        std::fs::write(proj.path().join("shared.rs"), "main change\n").unwrap();
        git(proj.path(), &["commit", "-aqm", "main change"]);
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Reconcile: rebase the feature branch onto main — the bookmark tip now
        // carries a recorded conflict.
        rebase_branch_onto(&jj, &store, branch, "main").unwrap();
        assert!(
            branch_has_conflict(&jj, &store, branch).unwrap(),
            "precondition: the rebased bookmark tip carries a recorded conflict"
        );

        // Refresh the now-stale workspace, then move `@` to a fresh line off the
        // current base tip — the resolve-at-base flatten shape, where `@` no longer
        // descends from the conflicted bookmark.
        let _ = update_stale(&jj, &ws);
        let base_tip = revset_commit(&jj, &store, "main").unwrap();
        jj.run(&ws, &["new", &base_tip], "jj new off base").unwrap();
        std::fs::write(ws.join("flat.rs"), "resolved flat\n").unwrap();

        // Sealing through the commit_msg path is refused with the DISTINCT
        // conflicted-branch error, not the stale "behind its branch tip" message.
        let err = seal(&jj, &ws, "flatten", None).unwrap_err();
        assert!(
            is_conflicted_branch_seal_error(&err),
            "a divergent seal over a conflicted bookmark tip returns the conflicted-branch error: {err}"
        );
        assert!(
            !is_stale_error(&err),
            "and it is NOT misclassified as the stale family: {err}"
        );
    }

    /// `is_lost_seal_error` recognizes the lost-seal marker (even wrapped in the
    /// write-path's surrounding text) and rejects unrelated jj errors, including
    /// the stale family it is OR'd with at the routing sites.
    #[test]
    fn is_lost_seal_error_classifies_the_lost_seal_marker() {
        assert!(is_lost_seal_error(LOST_SEAL_MSG));
        assert!(is_lost_seal_error(&format!(
            "Applied file changes but commit failed: {LOST_SEAL_MSG}; the worktree was restored."
        )));
        assert!(!is_lost_seal_error("working copy is stale"));
        assert!(!is_lost_seal_error("nothing to commit"));
        assert!(!is_lost_seal_error(
            "seal refused: workspace `agent/x` is behind its branch tip"
        ));
    }

    /// Fork a committed change into a DIVERGENT twin via two `--at-op` describes
    /// from the same base operation: each rewrites the change to a distinct
    /// commit, and merging the divergent op heads keeps BOTH (`<id>/0 /1`). This
    /// is the op-fork shape a concurrent, unserialized store advance leaves —
    /// reused from the `forked_op_rebase_*` tests, scoped to a single change.
    fn fork_into_divergent(jj: &JjEnv, ws: &Path, change_id: &str) {
        let base_op = jj
            .run(
                ws,
                &["op", "log", "--no-graph", "-n", "1", "-T", "id"],
                "op id",
            )
            .unwrap()
            .trim()
            .to_string();
        for (i, msg) in ["twin a", "twin b"].iter().enumerate() {
            jj.run(
                ws,
                &[
                    "describe",
                    change_id,
                    "-m",
                    msg,
                    "--at-op",
                    &base_op,
                    "--ignore-working-copy",
                ],
                &format!("fork twin {i}"),
            )
            .unwrap();
        }
        // Any normal command merges the divergent op heads.
        let _ = jj.run(
            ws,
            &[
                "log",
                "-r",
                "root()",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "trigger op merge",
        );
    }

    /// `scoped_dirty` measures the WHOLE working copy for an empty path slice and
    /// only the named filesets when scoped. The scoped case is what keeps a
    /// legitimately no-op scoped seal (whose unrelated dirt makes the whole `@`
    /// look dirty) from false-positiving as a lost seal.
    #[test]
    #[serial_test::serial(jj)]
    fn scoped_dirty_measures_whole_and_scoped_paths() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping scoped_dirty_measures_whole_and_scoped_paths: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("w");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

        // Clean working copy: nothing dirty either way.
        assert!(!scoped_dirty(&jj, &ws, &[]).unwrap());
        assert!(!scoped_dirty(&jj, &ws, &["a.txt"]).unwrap());

        // Dirt in a.txt: whole-`@` is dirty and a check scoped to a.txt is dirty,
        // but a check scoped to an UNTOUCHED path is NOT — the no-op-scoped guard.
        std::fs::write(ws.join("a.txt"), "change\n").unwrap();
        assert!(scoped_dirty(&jj, &ws, &[]).unwrap());
        assert!(scoped_dirty(&jj, &ws, &["a.txt"]).unwrap());
        assert!(
            !scoped_dirty(&jj, &ws, &["shared.rs"]).unwrap(),
            "a scoped check on an untouched path is clean even when the whole `@` is dirty"
        );
    }

    /// `sealed_commit_is_lost` flags the empty-with-pre-dirt and divergent shapes
    /// and clears a genuine no-op (empty, no pre-dirt) and a real non-empty seal —
    /// the true/false-positive matrix the seal-path detection depends on.
    #[test]
    #[serial_test::serial(jj)]
    fn sealed_commit_is_lost_flags_empty_and_divergent_not_clean() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping sealed_commit_is_lost_flags_empty_and_divergent_not_clean: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("w");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

        // A real, non-empty seal is NOT lost even with pre-commit dirt measured.
        std::fs::write(ws.join("a.txt"), "v1\n").unwrap();
        seal(&jj, &ws, "real work", None).unwrap();
        assert!(
            !sealed_commit_is_lost(&jj, &ws, true).unwrap(),
            "a real non-empty seal is not the lost shape"
        );

        // An EMPTY `@-`: a bare `jj commit` on a clean `@` seals nothing. With
        // pre-commit dirt it is the lost shape; from a genuine no-op (no
        // pre-dirt) it is NOT flagged.
        jj.run(&ws, &["commit", "-m", "empty seal"], "empty commit")
            .unwrap();
        assert!(
            sealed_commit_is_lost(&jj, &ws, true).unwrap(),
            "an empty `@-` despite pre-commit dirt is the lost shape"
        );
        assert!(
            !sealed_commit_is_lost(&jj, &ws, false).unwrap(),
            "an empty `@-` from a genuine no-op (no pre-dirt) is not flagged"
        );

        // A DIVERGENT `@-`: fork the just-sealed change into a twin. Flagged
        // regardless of pre-dirt (a concurrent-op merge, never a clean seal).
        std::fs::write(ws.join("b.txt"), "v2\n").unwrap();
        seal(&jj, &ws, "seal to fork", None).unwrap();
        let cid = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
                "@- change id",
            )
            .unwrap()
            .trim()
            .to_string();
        fork_into_divergent(&jj, &ws, &cid);
        assert_eq!(
            visible_commits_for_change(&jj, &ws, &cid),
            2,
            "precondition: `@-` resolves to a divergent change"
        );
        assert!(
            sealed_commit_is_lost(&jj, &ws, false).unwrap(),
            "a divergent `@-` is the lost shape regardless of pre-dirt"
        );
    }

    /// End-to-end: a `seal_paths` whose commit lands on a divergent change DETECTS
    /// the anomaly, returns a typed lost-seal `Err` (not `Ok` with a phantom sha),
    /// and backs the bad commit out so `@` reparents onto its pre-seal parent —
    /// the silent-data-loss-as-success regression this fix closes. A normal seal
    /// in the same workspace shape still succeeds (no false positive).
    #[test]
    #[serial_test::serial(jj)]
    fn seal_paths_detects_and_backs_out_a_lost_seal() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping seal_paths_detects_and_backs_out_a_lost_seal: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("w");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

        // A clean seal succeeds normally (the no-false-positive baseline).
        std::fs::write(ws.join("a.txt"), "v1\n").unwrap();
        let ok = seal_paths(&jj, &ws, "seal1", None, &[]).expect("a clean seal succeeds");
        assert!(!ok.sha.is_empty());
        let parent_cid = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
                "@- change id",
            )
            .unwrap()
            .trim()
            .to_string();

        // Fork the sealed parent into a divergent twin. A subsequent seal's own
        // commit then inherits the divergence — the empty/divergent shape a
        // concurrent store advance leaves.
        fork_into_divergent(&jj, &ws, &parent_cid);
        // The fork rewrote the bookmarked commit; repoint the bookmark to the live
        // parent twin so the seal's fast-forward precheck (an orthogonal concern,
        // covered by its own test) passes and this test exercises the ANOMALY path.
        jj.run(
            &ws,
            &[
                "bookmark",
                "set",
                "agent/CAIRN-1-builder-0",
                "-r",
                "@-",
                "--ignore-working-copy",
            ],
            "repoint bookmark to live twin",
        )
        .unwrap();

        std::fs::write(ws.join("b.txt"), "v2\n").unwrap();
        let err = seal_paths(&jj, &ws, "seal2", None, &[])
            .expect_err("a lost seal must surface as Err, not Ok with a phantom sha");
        assert!(
            is_lost_seal_error(&err),
            "the seal error is classified lost-seal: {err}"
        );

        // Backout: `jj abandon @-` reparented `@` onto the original seal1 parent,
        // so the bad seal2 commit is gone rather than reported as committed.
        let after = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
                "@- after backout",
            )
            .unwrap()
            .trim()
            .to_string();
        assert_eq!(
            after, parent_cid,
            "the backed-out seal returns `@` to its pre-seal parent"
        );
    }

    /// The data-loss regression guard: `discard` on a STALE workspace carrying
    /// loose (unsnapshotted) edits self-heals via `update-stale` instead of
    /// dead-ending on the stale refusal — leaving the worktree clean and equal to
    /// the advanced `@`, with the loose batch edits discarded (not orphaned
    /// uncommitted, which is how the production 28-patch batch was later wiped).
    #[test]
    #[serial_test::serial(jj)]
    fn discard_self_heals_stale_working_copy_with_loose_edits() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping discard_self_heals_stale_working_copy_with_loose_edits: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-1-coordinator-0";
        let child = "agent/CAIRN-2-builder-0";
        let ws_coord = wts.path().join("coord");
        add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
        let ws_child = wts.path().join("child");
        add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();

        // Seal a sibling commit to rebase the coordinator onto.
        std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
        seal(&jj, &ws_child, "child work", None).unwrap();

        // Loose, UNSNAPSHOTTED edits in the coordinator: write files but run no jj
        // command there, so they never enter `@`. A new file plus a modification.
        std::fs::write(ws_coord.join("loose.txt"), "loose work\n").unwrap();
        std::fs::write(ws_coord.join("shared.rs"), "coordinator change\n").unwrap();

        // Rewrite the coordinator's OWN `@` from the store (the reconcile-rebase
        // shape: `advance_workspace_onto` minus its `update_stale`). Rewriting the
        // workspace's working-copy commit out from under it is what makes the
        // workspace OP-LOG stale — the condition that blocks `jj restore` and
        // `jj commit` alike, unlike a mere bookmark advance. (A fold via
        // `merge_into_bookmark` only advances the bookmark; `jj restore` still
        // succeeds there. This store-side rebase is the true data-loss shape.)
        let source = format!("{}@", workspace_name_for_branch(int));
        jj.run(
            &store,
            &[
                "rebase",
                "-s",
                &source,
                "-o",
                child,
                "--ignore-working-copy",
            ],
            "rebase coordinator @ onto sibling (no update-stale)",
        )
        .unwrap();

        // Precondition: the workspace is now stale, so every working-copy command
        // refuses — the snapshot-taking dirty probe and the rollback alike.
        let dirty = is_working_copy_dirty(&jj, &ws_coord);
        assert!(
            dirty.as_ref().err().is_some_and(|e| is_stale_error(e)),
            "precondition: a stale workspace blocks the snapshot/dirty probe: {dirty:?}"
        );
        // Reproduce the bug: a bare `jj restore` (the OLD discard) is ALSO blocked
        // by staleness and would dead-end, orphaning the loose edits uncommitted.
        let bare = jj.run(&ws_coord, &["restore"], "bare restore");
        let bare_err = bare.expect_err("bare restore is blocked on a stale copy");
        assert!(
            is_stale_error(&bare_err),
            "the block is the stale refusal: {bare_err}"
        );

        // The self-healing discard returns Ok, clears staleness, and discards the
        // loose edits → worktree == fresh @.
        discard(&jj, &ws_coord).unwrap();
        assert!(
            !ws_coord.join("loose.txt").exists(),
            "the loose new file is discarded by the self-heal"
        );
        assert_eq!(
            std::fs::read_to_string(ws_coord.join("shared.rs")).unwrap(),
            "base\n",
            "the loose modification is reverted to the committed base"
        );
        assert!(
            ws_coord.join("child.rs").exists(),
            "update-stale advanced @ onto the rewritten parent, materializing the sibling's file"
        );
        // No longer stale: a dirty check (which snapshots) now succeeds and is clean.
        assert_eq!(
            is_working_copy_dirty(&jj, &ws_coord),
            Ok(false),
            "the worktree is clean and equals the advanced @ after self-heal"
        );
    }

    /// The #158 recovery contract at the jj layer: a child folds into the
    /// coordinator's integration base, leaving the coordinator workspace STALE.
    /// The primitives `recover_stale_file_commit` chains — reconcile the stale
    /// workspace onto the advanced base (the `update_stale` it calls, here via the
    /// production `advance_workspace_onto`), re-apply the agent's edit, then
    /// re-seal — must land the batch ON the advanced base, preserving BOTH the
    /// merged sibling work and the agent's edit, never losing either. (The
    /// loose-edits-during-rebase shape that mints a divergent twin — correctly
    /// caught by the lost-seal guard — is a distinct failure tracked as a separate
    /// follow-up; this pins the common clean case.)
    #[test]
    #[serial_test::serial(jj)]
    fn stale_seal_recovers_batch_onto_advanced_base() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping stale_seal_recovers_batch_onto_advanced_base: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // A child folds into the coordinator's integration bookmark, advancing the
        // base and leaving the coordinator workspace STALE behind it — the
        // post-advance race that routes a write into recovery.
        let (int_tip, ws_coord, int) =
            fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

        // Recovery (a): reconcile the stale coordinator onto the advanced base.
        // `advance_workspace_onto` performs the store-side rebase plus the same
        // `update_stale` `recover_stale_file_commit` calls, materializing the
        // merged sibling work. Re-parenting the empty `@` leaves no divergent twin.
        advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();
        assert!(
            ws_coord.join("child.rs").exists(),
            "the merged sibling's work is materialized on the advanced base"
        );

        // Recovery (b): re-apply the agent's batch edit against the advanced base.
        std::fs::write(ws_coord.join("feature.rs"), "agent feature\n").unwrap();

        // Recovery (c): re-seal — lands a real commit on the advanced base.
        let result = seal(&jj, &ws_coord, "agent feature", None).unwrap();
        assert!(
            !result.sha.is_empty(),
            "the recovered batch seals a real commit"
        );
        assert_eq!(
            is_working_copy_dirty(&jj, &ws_coord),
            Ok(false),
            "the worktree equals HEAD after the recovered seal"
        );

        // The sealed commit (`@-`) carries the agent's edit...
        let sealed_names = jj
            .run(
                &ws_coord,
                &["diff", "-r", "@-", "--name-only"],
                "recovered seal contents",
            )
            .unwrap();
        assert!(
            sealed_names.contains("feature.rs"),
            "the recovered seal commits the agent's edit: {sealed_names}"
        );

        // ...and the integration bookmark advanced FORWARD over the merged sibling
        // tip, so neither the sibling's merged work nor the agent's edit was lost.
        let int_after = bookmark_commit(&jj, &store, &int).unwrap();
        let fwd = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("{int_tip} & ::{int_after}"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ],
                "forward-only check",
            )
            .unwrap();
        assert_eq!(
            fwd, int_tip,
            "the recovered seal advances the base forward over the merged sibling work, never backward"
        );
        assert!(
            ws_coord.join("feature.rs").exists() && ws_coord.join("child.rs").exists(),
            "both the agent's edit and the merged sibling work coexist after recovery"
        );
    }

    /// `advance_workspace_onto` re-parents the stale coordinator `@` onto the
    /// folded integration tip and materializes the merged child's file on disk —
    /// the jj-native restoration of §6's post-merge fast-forward. Idempotent: a
    /// second advance at the same tip is a no-op.
    #[test]
    #[serial_test::serial(jj)]
    fn advance_coordinator_workspace_after_fold() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping advance_coordinator_workspace_after_fold: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let (int_tip, ws_coord, int) =
            fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

        let conflicted = advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();
        assert!(
            !conflicted,
            "re-parenting the empty coordinator @ never conflicts"
        );

        let coord_parent_after = jj
            .run(
                &ws_coord,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "coord @- after",
            )
            .unwrap();
        assert_eq!(
            coord_parent_after, int_tip,
            "the coordinator @ is re-parented onto the folded tip"
        );
        assert!(
            ws_coord.join("child.rs").exists(),
            "update-stale materialized the merged child's file in the coordinator workspace"
        );

        // Idempotency under the merge/webhook double-fire: a second advance when
        // `@` already sits on the tip is a no-op.
        let again = advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();
        assert!(!again);
        let coord_parent_twice = jj
            .run(
                &ws_coord,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "coord @- twice",
            )
            .unwrap();
        assert_eq!(
            coord_parent_twice, int_tip,
            "re-running the advance when already on the tip leaves @ in place"
        );
    }

    /// After the coordinator is advanced onto the folded tip, an edit + `seal`
    /// moves the integration bookmark FORWARD to the new sealed commit (a
    /// descendant of the folded tip) — never backward, so no merged child is
    /// dropped.
    #[test]
    #[serial_test::serial(jj)]
    fn seal_after_fold_advances_bookmark_forward() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping seal_after_fold_advances_bookmark_forward: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let (int_tip, ws_coord, int) =
            fold_child_leaving_coordinator_stale(&jj, &store, wts.path());
        advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();

        std::fs::write(ws_coord.join("coord.rs"), "coord work\n").unwrap();
        seal(&jj, &ws_coord, "coord work", None).unwrap();

        let int_after = bookmark_commit(&jj, &store, &int).unwrap();
        assert_ne!(
            int_after, int_tip,
            "the seal advanced the integration bookmark"
        );
        // Forward-only: the folded tip is an ancestor of the new bookmark commit.
        let fwd = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    &format!("{int_tip} & ::{int_after}"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ],
                "forward-only check",
            )
            .unwrap();
        assert_eq!(
            fwd, int_tip,
            "the folded tip is an ancestor of the new bookmark (forward-only, never backward)"
        );
    }

    /// Fix (b) backstop: with the coordinator `@` left deliberately STALE (no
    /// advance), a `seal` must fail loudly and — critically — BEFORE creating any
    /// commit, so it never produces an orphaned off-branch commit that the generic
    /// discard (`jj restore`) could not recover. The working copy stays dirty on
    /// the stale line and the integration tip is preserved.
    #[test]
    #[serial_test::serial(jj)]
    fn seal_refuses_non_fast_forward_bookmark_move() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping seal_refuses_non_fast_forward_bookmark_move: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let (int_tip, ws_coord, int) =
            fold_child_leaving_coordinator_stale(&jj, &store, wts.path());
        // The coordinator head `@-` before the (refused) seal — must be unchanged
        // afterwards: a true pre-commit guard creates no commit at all.
        let head_before = jj
            .run(
                &ws_coord,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "head before",
            )
            .unwrap();
        // Deliberately skip the advance: the coordinator @ stays stale.
        std::fs::write(ws_coord.join("coord.rs"), "coord work\n").unwrap();
        let result = seal(&jj, &ws_coord, "coord work", None);

        assert!(
            result.is_err(),
            "a stale-@ seal must fail loudly, not silently orphan the commit off the branch"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("behind its branch tip"),
            "the error explains the stale-@ cause: {err}"
        );
        // No orphan: the seal was refused BEFORE `jj commit`, so the workspace
        // head is unchanged and the working copy is still dirty on the stale line.
        let head_after = jj
            .run(
                &ws_coord,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "head after",
            )
            .unwrap();
        assert_eq!(
            head_before, head_after,
            "the refused seal creates NO commit — the workspace head is unchanged (no orphan)"
        );
        assert!(
            is_working_copy_dirty(&jj, &ws_coord).unwrap(),
            "the working-copy changes are NOT sealed away — they remain for a post-advance reseal"
        );
        let int_after = bookmark_commit(&jj, &store, &int).unwrap();
        assert_eq!(
            int_after, int_tip,
            "the refused seal never moves the integration bookmark backward/sideways"
        );
    }

    /// The archival load-bearing case: jj's auto-rebase churns a descendant's
    /// commit-id while its change-id stays stable, and `forward_resolve_commit`
    /// maps the now-hidden original commit-id forward to the current one. A
    /// non-jj directory yields `None` (identity at the call site).
    #[test]
    #[serial_test::serial(jj)]
    fn forward_resolve_commit_maps_churned_commit_to_current() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping forward_resolve_commit_maps_churned_commit_to_current: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

        // Seal a parent then a child commit on top of it.
        std::fs::write(ws.join("parent.rs"), "p\n").unwrap();
        seal(&jj, &ws, "parent", None).unwrap();
        std::fs::write(ws.join("child.rs"), "c\n").unwrap();
        seal(&jj, &ws, "child", None).unwrap();

        let child_commit_before = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "child commit",
            )
            .unwrap();
        let child_change = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
                "child change",
            )
            .unwrap();
        // The SHORT form a write tool_result actually records.
        let child_short_before = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
                "child short",
            )
            .unwrap();
        // This is a non-colocated workspace: it has no .git, so a git-in-worktree
        // resolution of the recorded id is impossible — only jj can resolve it.
        assert!(
            !ws.join(".git").exists(),
            "a jj workspace is non-colocated (no .git)"
        );

        // Reword the PARENT (@--), which auto-rebases the child and churns its
        // commit-id while preserving its change-id.
        jj.run(
            &ws,
            &["describe", "-r", "@--", "-m", "parent reworded"],
            "reword parent",
        )
        .unwrap();
        let child_commit_after = jj
            .run(
                &ws,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "child commit after",
            )
            .unwrap();
        assert_ne!(
            child_commit_before, child_commit_after,
            "auto-rebase churns the child commit-id"
        );

        let (change_id, current) = forward_resolve_commit(&jj, &ws, &child_commit_before)
            .expect("forward-map resolves a churned commit");
        assert_eq!(change_id, child_change, "the stable change-id is recovered");
        assert_eq!(
            current, child_commit_after,
            "forward-maps the hidden original commit-id to the current one"
        );

        // The same holds for the SHORT, now-hidden id — the form archival actually
        // hands in — with no git in the worktree.
        let (_, from_short) = forward_resolve_commit(&jj, &ws, &child_short_before)
            .expect("forward-map resolves a churned SHORT commit-id");
        assert_eq!(from_short, child_commit_after);

        // A non-jj directory is a clean identity no-op (returns None).
        let plain = wts.path().join("plain-git");
        std::fs::create_dir_all(&plain).unwrap();
        assert!(forward_resolve_commit(&jj, &plain, &child_commit_before).is_none());
    }

    /// A divergent change resolves to several visible commits; `forward_resolve_commit`
    /// must pick the one reachable from the worktree tip (`@`) — the side that
    /// lands in the archival pack — rather than erroring on the bare change-id.
    #[test]
    #[serial_test::serial(jj)]
    fn forward_resolve_commit_prefers_tip_reachable_on_divergence() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping forward_resolve_commit_prefers_tip_reachable_on_divergence: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let dir = TempDir::new().unwrap();
        let d = dir.path();
        // A colocated jj repo over a git repo, so `--at-op` divergence is easy to
        // construct and the change-id resolves the same way archival sees it.
        git(d, &["init", "-q", "-b", "main"]);
        git(d, &["config", "user.email", "t@e.com"]);
        git(d, &["config", "user.name", "t"]);
        std::fs::write(d.join("a.txt"), "base\n").unwrap();
        git(d, &["add", "-A"]);
        git(d, &["commit", "-q", "-m", "base"]);
        let jj = JjEnv::resolve(&bin, home.path());
        jj.run(d, &["git", "init", "--colocate"], "colocate")
            .unwrap();
        jj.run(d, &["new"], "new").unwrap();
        std::fs::write(d.join("c.rs"), "c\n").unwrap();
        jj.run(d, &["commit", "-m", "C"], "commit C").unwrap();

        let c0 = jj
            .run(
                d,
                &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
                "c0",
            )
            .unwrap();
        let change = jj
            .run(
                d,
                &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
                "change",
            )
            .unwrap();
        let op0 = jj
            .run(
                d,
                &["op", "log", "--no-graph", "--limit", "1", "-T", "id"],
                "op0",
            )
            .unwrap();

        // Rewrite C one way, then concurrently rewrite the ORIGINAL C a different
        // way at the earlier operation, creating a divergent change.
        jj.run(d, &["describe", "-r", &c0, "-m", "A"], "describe A")
            .unwrap();
        jj.run(
            d,
            &["--at-op", &op0, "describe", "-r", &c0, "-m", "B"],
            "describe B",
        )
        .unwrap();
        // A normal command resolves the concurrent operations.
        let _ = jj.run(
            d,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "resolve ops",
        );

        let visible = jj
            .run(
                d,
                &[
                    "log",
                    "-r",
                    &format!("change_id({change})"),
                    "--no-graph",
                    "-T",
                    "commit_id ++ \"\\n\"",
                ],
                "visible",
            )
            .unwrap();
        let count = visible.lines().filter(|l| !l.is_empty()).count();
        assert!(
            count >= 2,
            "the change must be divergent (>=2 visible commits): {visible}"
        );

        let (_, current) =
            forward_resolve_commit(&jj, d, &c0).expect("forward-map resolves a divergent change");
        assert!(
            visible.lines().any(|l| l == current),
            "the chosen commit is one of the divergent commits: {current} in {visible}"
        );
        let reachable = jj
            .run(
                d,
                &[
                    "log",
                    "-r",
                    &format!("({current}) & ::@"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ],
                "reachable check",
            )
            .unwrap();
        assert!(
            !reachable.is_empty(),
            "forward-map picks the tip-reachable side of the divergence: {current}"
        );
    }

    /// The populate glob -> jj `snapshot.auto-track` fileset translation: file
    /// globs become depth-agnostic `glob:"**/<p>"`, a `dir/` becomes both its
    /// subtree and its own path, an empty config keeps jj's `all()` default, and
    /// backstop `extra_paths` are added as exact literal filesets.
    #[test]
    fn populate_auto_track_expr_translates_patterns() {
        use crate::config::project_settings::PopulateConfig;
        let config = PopulateConfig {
            copy: vec![".env".into(), ".env*".into(), "target/".into()],
            symlink: vec!["node_modules/".into()],
        };
        let expr = populate_auto_track_expr(&config, &[]).unwrap();
        assert!(expr.starts_with("all() ~ ("), "got: {expr}");
        assert!(expr.contains("glob:\"**/.env\""), "got: {expr}");
        assert!(expr.contains("glob:\"**/.env*\""), "got: {expr}");
        assert!(expr.contains("glob:\"**/target/**\""), "got: {expr}");
        assert!(expr.contains("glob:\"**/target\""), "got: {expr}");
        assert!(expr.contains("glob:\"**/node_modules/**\""), "got: {expr}");
        assert!(expr.contains("glob:\"**/node_modules\""), "got: {expr}");

        assert!(
            populate_auto_track_expr(&PopulateConfig::default(), &[]).is_none(),
            "empty config leaves jj's all() default untouched"
        );

        let healed =
            populate_auto_track_expr(&PopulateConfig::default(), &["secret/leaked.txt".into()])
                .unwrap();
        assert!(healed.contains("\"secret/leaked.txt\""), "got: {healed}");
    }

    /// `quote_fileset` wraps a repo-relative path as a jj string literal so paths
    /// with fileset metacharacters (a Next.js `(app)` route group) match
    /// literally instead of parsing as a fileset expression. `"` and `\` are
    /// backslash-escaped per jj's double-quoted-string rules.
    #[test]
    fn quote_fileset_wraps_and_escapes() {
        // A plain path quotes to itself wrapped in quotes (happy-path no-op).
        assert_eq!(quote_fileset("src/app/page.tsx"), "\"src/app/page.tsx\"");
        // The reported bug: parentheses are preserved verbatim inside the quotes.
        assert_eq!(
            quote_fileset("apps/quarry/src/app/(app)/drawings/page.tsx"),
            "\"apps/quarry/src/app/(app)/drawings/page.tsx\""
        );
        // Other fileset metacharacters ride through literally once quoted.
        assert_eq!(
            quote_fileset("a b/c & d|e~f:g.tsx"),
            "\"a b/c & d|e~f:g.tsx\""
        );
        // A literal double-quote is backslash-escaped.
        assert_eq!(quote_fileset("a\"b.tsx"), "\"a\\\"b.tsx\"");
        // A literal backslash is doubled (and escaped before the quote escape).
        assert_eq!(quote_fileset("a\\b.tsx"), "\"a\\\\b.tsx\"");
    }

    /// REGRESSION (the reported bug): sealing a path whose directory is a fileset
    /// metacharacter group — a Next.js `(app)` route group — must commit cleanly
    /// instead of failing with `Failed to parse fileset`. Before the
    /// `quote_fileset` fix, the bare `(app)` positional arg parsed as a grouping
    /// operator and the whole batch was restored to HEAD, losing the edit.
    #[test]
    #[serial_test::serial(jj)]
    fn seal_paths_commits_path_with_fileset_metacharacters() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping seal_paths_commits_path_with_fileset_metacharacters: jj not resolvable"
            );
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-2019-builder-0", "main", None).unwrap();

        // Edit a file under a parens route-group directory, then path-scope seal it.
        let rel = "apps/quarry/src/app/(app)/drawings/page.tsx";
        let abs = ws.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, "export default function Page() {}\n").unwrap();

        let res = seal_paths(&jj, &ws, "add drawings page", None, &[rel]).unwrap();
        assert!(
            !res.sha.is_empty(),
            "path-scoped seal of a parens path returns a commit id"
        );

        // The file landed in @- (the sealed commit), not left dangling in @.
        let listed = jj
            .run(&ws, &["file", "list", "-r", "@-"], "file list @-")
            .unwrap();
        assert!(
            listed.contains("(app)/drawings/page.tsx"),
            "the parens path is committed in @-: {listed}"
        );
        assert!(
            !is_working_copy_dirty(&jj, &ws).unwrap(),
            "@ is clean after the path-scoped seal"
        );
    }

    /// SECURITY: explicitly-populated gitignored content must stay UNCOMMITTED in
    /// a jj workspace. With `snapshot.auto-track` set BEFORE the files appear, a
    /// populated path that has NO ignore rule in the workspace (the residual
    /// leak: ignored only by an untracked source `.gitignore`) never enters `@`
    /// and cannot be sealed, while a normal new source file IS tracked and
    /// sealable. Runs in the production NON-colocated shape (store + workspace,
    /// asserted no `.git` — a colocated repo would mask the bug).
    #[test]
    #[serial_test::serial(jj)]
    fn populate_auto_track_keeps_ignored_content_out_of_snapshot_and_seals() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping populate_auto_track_keeps_ignored_content_out_of_snapshot_and_seals: jj not resolvable");
            return;
        };
        use crate::config::project_settings::PopulateConfig;
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let branch = "agent/CAIRN-7-builder-0";
        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

        // Production shape: a `.jj`-only workspace with NO `.git`.
        assert!(ws.join(".jj").is_dir());
        assert!(
            !ws.join(".git").exists(),
            "workspace must be .jj-only (non-colocated) or both bugs are masked"
        );

        let config = PopulateConfig {
            copy: vec![".env".into()],
            symlink: vec!["node_modules/".into()],
        };
        // Establish the exclude BEFORE the populated files appear.
        set_populate_auto_track(&jj, &store, &config, &[]).unwrap();

        // Simulate populate: a secret with NO ignore rule in the workspace, a
        // populated dir, plus a normal source file an agent would later write.
        std::fs::write(ws.join(".env"), "SECRET=token\n").unwrap();
        std::fs::create_dir_all(ws.join("node_modules")).unwrap();
        std::fs::write(ws.join("node_modules").join("pkg.js"), "x\n").unwrap();
        std::fs::write(ws.join("real.rs"), "fn main() {}\n").unwrap();

        let dirty = working_copy_dirty_paths(&jj, &ws).unwrap();
        assert!(
            dirty.iter().any(|p| p == "real.rs"),
            "a normal source file must be tracked: {dirty:?}"
        );
        assert!(
            !dirty.iter().any(|p| p.contains(".env")),
            "populated secret must NOT be snapshot-visible: {dirty:?}"
        );
        assert!(
            !dirty.iter().any(|p| p.contains("node_modules")),
            "populated dir must NOT be snapshot-visible: {dirty:?}"
        );

        // A seal must not fold the populated content into the commit.
        seal(&jj, &ws, "agent work", None).unwrap();
        let cfg = home.path().join("jj").join("config.toml");
        let out = crate::env::command(&bin)
            .args(["diff", "-r", "@-", "--name-only"])
            .current_dir(&ws)
            .env("JJ_CONFIG", &cfg)
            .output()
            .unwrap();
        let names = String::from_utf8_lossy(&out.stdout);
        assert!(
            names.contains("real.rs"),
            "the agent's file must be sealed: {names}"
        );
        assert!(
            !names.contains(".env"),
            "the secret must NEVER be sealed: {names}"
        );
        assert!(
            !names.contains("node_modules"),
            "populated dir must NEVER be sealed: {names}"
        );
    }

    /// Backstop self-heal: a populated path the initial glob translation missed
    /// gets tracked; feeding it back through `set_populate_auto_track(extra)` +
    /// `untrack_paths` removes it from the snapshot WITHOUT deleting it from disk
    /// — the recovery that runs before fail-loud in `prepare_worktree_for_job`.
    #[test]
    #[serial_test::serial(jj)]
    fn untrack_self_heals_a_leaked_populated_path() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping untrack_self_heals_a_leaked_populated_path: jj not resolvable");
            return;
        };
        use crate::config::project_settings::PopulateConfig;
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-7-builder-0", "main", None).unwrap();

        // A populate config whose globs do NOT cover this path — it leaks.
        let config = PopulateConfig {
            copy: vec![".env".into()],
            symlink: vec![],
        };
        set_populate_auto_track(&jj, &store, &config, &[]).unwrap();
        std::fs::write(ws.join("leaked.secret"), "token\n").unwrap();
        assert!(
            working_copy_dirty_paths(&jj, &ws)
                .unwrap()
                .iter()
                .any(|p| p == "leaked.secret"),
            "the unmatched path is tracked until self-heal"
        );

        // Self-heal: add the exact leaked path to auto-track and untrack it.
        set_populate_auto_track(&jj, &store, &config, &["leaked.secret".into()]).unwrap();
        untrack_paths(&jj, &ws, &["leaked.secret".into()]).unwrap();

        assert!(
            !working_copy_dirty_paths(&jj, &ws)
                .unwrap()
                .iter()
                .any(|p| p == "leaked.secret"),
            "self-heal removes the leaked path from the snapshot"
        );
        assert!(
            ws.join("leaked.secret").exists(),
            "untrack must NOT delete the file from disk"
        );
    }

    /// A `git rev-parse` test closure over `repo` mirroring the production
    /// `GitService::rev_parse` contract: `Some(trimmed_sha)` for a ref git
    /// resolves, `None` otherwise (non-zero exit — unborn or unmatched ref).
    fn rev_parse_closure(repo: &Path) -> impl Fn(&str) -> Option<String> + '_ {
        move |r: &str| {
            let out = crate::env::git()
                .args(["rev-parse", r])
                .current_dir(repo)
                .output()
                .ok()?;
            out.status
                .success()
                .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
                .filter(|s| !s.is_empty())
        }
    }

    /// Ladder step 1: a base ref the project git resolves yields its commit SHA,
    /// equal to `git rev-parse <ref>`.
    #[test]
    #[serial_test::serial(jj)]
    fn resolve_base_rev_prefers_project_git_sha() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping resolve_base_rev_prefers_project_git_sha: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let expected = git_stdout(proj.path(), &["rev-parse", "main"]);
        let got = resolve_base_rev(&jj, &store, "main", rev_parse_closure(proj.path()));
        assert_eq!(got, expected, "a project git ref resolves to its SHA");
    }

    /// Ladder step 2: a base ref that is NOT a project git ref but IS a store
    /// bookmark (the unsealed-coordinator case) is kept literal, and
    /// `add_workspace` provisions off it. Guards the coordinator path.
    #[test]
    #[serial_test::serial(jj)]
    fn resolve_base_rev_keeps_store_only_bookmark() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping resolve_base_rev_keeps_store_only_bookmark: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        // A bookmark that lives only in the shared store, never as a git ref in
        // the project repo — the shape of an unsealed coordinator branch.
        let bookmark = "agent/coord-0";
        jj.run(
            &store,
            &["bookmark", "create", bookmark, "-r", "main"],
            "seed store-only bookmark",
        )
        .unwrap();
        let rev_parse = rev_parse_closure(proj.path());
        assert!(
            rev_parse(bookmark).is_none(),
            "the store bookmark is not a project git ref"
        );

        let got = resolve_base_rev(&jj, &store, bookmark, &rev_parse);
        assert_eq!(got, bookmark, "a store-only bookmark is kept literal");

        // And it provisions, the way a child workspace bases off the coordinator.
        let ws = wts.path().join("child");
        add_workspace(&jj, &store, &ws, "agent/CAIRN-9-builder-0", &got, None).unwrap();
        assert!(
            is_jj_dir(&ws),
            "workspace based on the store bookmark provisions"
        );
    }

    /// Ladder step 3: a base ref matching neither a project git ref nor a store
    /// bookmark, in a repo that HAS commits, falls back to the repo's HEAD tip
    /// (git parity for a local-only repo with a mismatched default branch name).
    #[test]
    #[serial_test::serial(jj)]
    fn resolve_base_rev_falls_back_to_repo_head() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping resolve_base_rev_falls_back_to_repo_head: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let head = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
        let got = resolve_base_rev(
            &jj,
            &store,
            "does-not-exist",
            rev_parse_closure(proj.path()),
        );
        assert_eq!(
            got, head,
            "an unmatched base falls back to the repo HEAD tip"
        );
    }

    /// Ladder step 4 — the direct regression test for this bug: an unborn repo
    /// (`git init -b main`, no commit) whose default branch resolves nowhere
    /// yields `root()`, and `add_workspace(.., "main", "root()", ..)` provisions a
    /// workspace and creates the `main` bookmark at root. Before the fix this
    /// path produced `Revision "main" doesn't exist`.
    #[test]
    #[serial_test::serial(jj)]
    fn resolve_base_rev_uses_root_for_unborn_repo() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping resolve_base_rev_uses_root_for_unborn_repo: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        // Unborn repo: an initialized repo with the branch set but no commit.
        git(proj.path(), &["init", "-q", "-b", "main"]);
        git(proj.path(), &["config", "user.email", "p@e.com"]);
        git(proj.path(), &["config", "user.name", "P"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();

        let got = resolve_base_rev(&jj, &store, "main", rev_parse_closure(proj.path()));
        assert_eq!(got, "root()", "an unborn repo bases off jj's root commit");

        let ws = wts.path().join("job");
        add_workspace(&jj, &store, &ws, "main", &got, None).unwrap();
        assert!(
            is_jj_dir(&ws),
            "a workspace on an unborn repo provisions off root()"
        );
        assert!(
            bookmark_commit(&jj, &store, "main").is_some(),
            "the branch bookmark is created at root"
        );
    }
}
