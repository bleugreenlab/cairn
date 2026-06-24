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

use crate::mcp::git::{CommitResult, GitAuthor};

/// Filename of the non-snapshotted branch marker inside a workspace's `.jj` dir.
const BRANCH_MARKER: &str = "cairn-branch";

/// Fallback identity used when no per-call author is supplied. Per-commit author
/// is injected via `--config user.{name,email}=…` on each seal.
const JJ_DEFAULT_USER_NAME: &str = "Cairn Agent";
const JJ_DEFAULT_USER_EMAIL: &str = "agent@cairn.local";

/// Drives a bundled, non-interactive `jj` binary.
pub struct JjEnv {
    bin: String,
    config_path: PathBuf,
}

impl JjEnv {
    /// Resolve the jj binary and the managed config path. Binary precedence:
    /// `CAIRN_JJ_BIN` (test/override) → the bundled sidecar path → PATH `jj`.
    pub fn resolve(bundled_bin: &str, config_dir: &Path) -> Self {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| {
                if bundled_bin.trim().is_empty() {
                    "jj".to_string()
                } else {
                    bundled_bin.to_string()
                }
            });
        Self {
            bin,
            config_path: config_dir.join("jj").join("config.toml"),
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

    /// Run a jj command, returning trimmed stdout or a contextual error.
    fn run(&self, cwd: &Path, args: &[&str], ctx: &str) -> Result<String, String> {
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
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

/// Whether `dir` is a jj repo/workspace root (carries a `.jj`). The ground-truth
/// signal the commit barrier dispatches on.
pub fn is_jj_dir(dir: &Path) -> bool {
    dir.join(".jj").is_dir()
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
            filesets.push(format!("\"{trimmed}\""));
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
    for path in paths {
        args.push((*path).to_string());
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
            return Err(format!(
                "seal refused: workspace `{branch}` is behind its branch tip — the branch \
                 advanced past this workspace's head, so sealing would create a commit off \
                 `{branch}`. The workspace must be advanced onto the branch tip before sealing."
            ));
        }
    }

    jj.run(ws, &argref, "jj commit")?;
    let sha = jj.run(
        ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
        "jj log -r @-",
    )?;
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
/// create it. The revset `(<bookmark>) & ::@-` is non-empty iff the bookmark
/// commit is an ancestor-or-self of `@-`.
fn seal_is_fast_forward(jj: &JjEnv, ws: &Path, branch: &str) -> Result<bool, String> {
    let Some(bookmark) = bookmark_commit(jj, ws, branch) else {
        return Ok(true);
    };
    let hit = jj.run(
        ws,
        &[
            "log",
            "-r",
            &format!("({bookmark}) & ::@-"),
            "--no-graph",
            "-T",
            "commit_id",
        ],
        "jj seal fast-forward precheck",
    )?;
    Ok(!hit.is_empty())
}

/// Discard working-copy changes by resetting `@` to its parent. Reversible via
/// the operation log — replacing git's destructive `reset --hard`.
pub fn discard(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    jj.run(ws, &["restore"], "jj restore").map(|_| ())
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

/// Stop tracking `paths` in the working copy without deleting them from disk
/// (`jj file untrack`). Used by populate's backstop to un-track a path a
/// conservative glob translation failed to keep out of the snapshot, after the
/// path has been added to `snapshot.auto-track`. No-op for an empty slice.
pub fn untrack_paths(jj: &JjEnv, ws: &Path, paths: &[String]) -> Result<(), String> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut args: Vec<&str> = vec!["file", "untrack"];
    args.extend(paths.iter().map(|s| s.as_str()));
    jj.run(ws, &args, "jj file untrack").map(|_| ())
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
pub fn push_to_origin(jj: &JjEnv, ws: &Path, branch: &str) {
    if branch.is_empty() || branch == "main" || branch == "master" {
        log::debug!("Skipping jj push for branch: {branch}");
        return;
    }
    match jj.run(
        ws,
        &["git", "push", "--remote", "origin", "--bookmark", branch],
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
    jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", "commit_id"],
        "jj log bookmark commit",
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
/// which sibling bookmarks rebased cleanly versus recorded a conflict. A
/// recorded conflict is non-blocking — jj materializes it for the agent to
/// resolve at its convenience rather than halting the rebase.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Sibling bookmarks that rebased with no conflict.
    pub rebased_clean: Vec<String>,
    /// Sibling bookmarks whose rebase recorded a conflict.
    pub conflicted: Vec<String>,
}

/// Fold a child's real commit into the integration bookmark over the shared
/// store — the local "merge" of a child PR. `jj bookmark set` is forward-only (it
/// refuses a backwards/sideways move), so the child must already sit on the
/// current integration tip; the merge gate and the sibling-reconcile invariant
/// guarantee that. A refusal here means the child was never reconciled onto
/// integration — surface it loudly rather than silently regressing the tip.
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
    )
    .map(|_| ())
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

/// Whether a bookmark's commit carries a recorded conflict. GitHub reports a
/// jj-conflicted commit as mergeable (and renders it as garbage), so the merge
/// gate trusts this over the GitHub mergeable bit for jj projects.
pub fn branch_has_conflict(jj: &JjEnv, store: &Path, branch: &str) -> Result<bool, String> {
    let revset = format!("bookmarks(exact:{:?})", branch);
    let out = jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", "self.conflict()"],
        "jj conflict check",
    )?;
    Ok(out.contains("true"))
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
    for (branch, ws_path) in siblings {
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
        match branch_has_conflict(jj, store, branch) {
            Ok(true) => report.conflicted.push(branch.clone()),
            Ok(false) => {
                // Advance the cleanly-rebased sibling's PR head on origin.
                // Best-effort: a no-remote project has nothing to push to.
                if let Err(e) = push_store_bookmark(jj, store, branch) {
                    log::warn!("jj reconcile: push rebased sibling {branch} failed: {e}");
                }
                report.rebased_clean.push(branch.clone());
            }
            Err(e) => {
                log::warn!("jj reconcile: conflict check for {branch} failed: {e}");
                report.rebased_clean.push(branch.clone());
            }
        }
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
}
