//! jj subprocess driver (`JjEnv`), repo/file probes, per-project store
//! store initialization and bounded command execution.
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use crate::PublicationIdentity;

/// Fallback identity used when no per-call author is supplied. Per-commit author
/// is injected via `--config user.{name,email}=…` on each seal. Shared with the
/// embedded publication path so the CLI-driven and in-process jj drivers can
/// never fall back to different identities.
const JJ_DEFAULT_USER_NAME: &str = crate::MANAGED_IDENTITY_NAME;
const JJ_DEFAULT_USER_EMAIL: &str = crate::MANAGED_IDENTITY_EMAIL;
pub(crate) const JJ_DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) use cairn_platform::process::{
    bounded_command_output, NETWORK_TIMEOUT as JJ_NETWORK_TIMEOUT,
};

/// Drives a bundled, non-interactive `jj` binary.
#[derive(Clone)]
pub struct JjEnv {
    bin: String,
    config_path: PathBuf,
    pub(super) checkout_repair_policy: crate::checkout::CheckoutRepairPolicy,
}

#[cfg(test)]
fn jj_subprocess_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl JjEnv {
    /// Set the host's policy for repairing a checkout detached by an export.
    pub fn with_checkout_repair_policy(
        mut self,
        policy: crate::checkout::CheckoutRepairPolicy,
    ) -> Self {
        self.checkout_repair_policy = policy;
        self
    }

    /// Build an environment around an already-resolved binary.
    ///
    /// Callers that snapshot an executable must not consult `CAIRN_JJ_BIN` or
    /// PATH again: doing so would discard the snapshot and reopen the race this
    /// constructor is meant to close.
    pub fn with_binary(bin: impl Into<String>, config_dir: &Path) -> Self {
        Self {
            bin: bin.into(),
            config_path: config_dir.join("jj").join("config.toml"),
            checkout_repair_policy: Default::default(),
        }
    }

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
            checkout_repair_policy: Default::default(),
        }
    }

    fn resolve_bundled_or_path(bundled_bin: &str) -> String {
        let bundled_bin = bundled_bin.trim();
        if bundled_bin.is_empty() {
            return "jj".to_string();
        }

        match bounded_command_output(
            cairn_platform::env::command(bundled_bin).arg("--version"),
            JJ_DEFAULT_TIMEOUT,
            "bundled jj --version",
        ) {
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
        let mut c = cairn_platform::env::command(&self.bin);
        c.current_dir(cwd)
            .env("JJ_CONFIG", &self.config_path)
            .env("EDITOR", "true")
            .env("JJ_EDITOR", "true")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "true");
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
            ("GIT_TERMINAL_PROMPT".into(), "0".into()),
            ("GIT_ASKPASS".into(), "true".into()),
        ]
    }

    /// The resolved real jj binary path (bundled sidecar, `CAIRN_JJ_BIN`
    /// override, or PATH `jj`). Exposed so the agent-shell env can point the
    /// intercept shim's `CAIRN_JJ_BIN` at the same binary managed jj runs.
    pub fn binary_path(&self) -> &str {
        &self.bin
    }

    /// Per-call author override as repeated global `--config user.{name,email}=…`
    /// args (placed before the subcommand). jj fixes a commit's author when its
    /// working-copy commit is created, so passing this on every seal keeps a
    /// workspace's sealed commits authored consistently.
    pub(crate) fn author_args(author: Option<&PublicationIdentity>) -> Vec<String> {
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
    ///
    /// The untrimmed form, exposed for `jj file show`: file content's leading
    /// and trailing bytes are part of the file, and a merge that quietly rewrote
    /// them would be worse than no merge at all.
    pub(crate) fn run_bytes(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: &str,
    ) -> Result<Vec<u8>, String> {
        self.run_bytes_with_timeout(cwd, args, ctx, JJ_DEFAULT_TIMEOUT)
    }

    fn run_bytes_with_timeout(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: &str,
        timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        #[cfg(test)]
        let _guard = jj_subprocess_lock()
            .lock()
            .expect("jj subprocess test lock poisoned");

        let out = bounded_command_output(self.cmd(cwd).args(args), timeout, ctx)?;
        if !out.status.success() {
            return Err(format!(
                "{ctx} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(out.stdout)
    }

    /// Run a jj command, returning trimmed stdout or a contextual error.
    pub fn run(&self, cwd: &Path, args: &[&str], ctx: &str) -> Result<String, String> {
        let out = self.run_bytes(cwd, args, ctx)?;
        Ok(String::from_utf8_lossy(&out).trim().to_string())
    }

    /// Run a jj command, returning trimmed stdout AND trimmed stderr on success.
    ///
    /// Every other runner drops stderr on a zero exit, which is correct for the
    /// commands whose failures are exit codes. `jj git export` is not one of
    /// them: when a `refs/heads/*` ref moved outside jj, the export refuses that
    /// ref, reports it as `Warning: Failed to export some bookmarks: …` on
    /// stderr, and **exits 0**. The bookmark advances, the git ref does not, and
    /// nothing downstream can tell. Verified against jj 0.42. The export
    /// verifier reads that stderr so a silent freeze becomes a named,
    /// diagnosable event instead of a stale push.
    pub(crate) fn run_capturing_stderr(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: &str,
    ) -> Result<(String, String), String> {
        #[cfg(test)]
        let _guard = jj_subprocess_lock()
            .lock()
            .expect("jj subprocess test lock poisoned");

        let out = bounded_command_output(self.cmd(cwd).args(args), JJ_DEFAULT_TIMEOUT, ctx)?;
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if !out.status.success() {
            return Err(format!("{ctx} failed: {stderr}"));
        }
        Ok((
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
            stderr,
        ))
    }

    pub(crate) fn run_with_timeout(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: &str,
        timeout: Duration,
    ) -> Result<String, String> {
        let out = self.run_bytes_with_timeout(cwd, args, ctx, timeout)?;
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

/// The repository every coordinate resolves against: the Cairn-managed store,
/// created on first use. There is no second answer — the user's checkout is
/// never a jj repo, so falling back to it only produces a raw jj error.
pub fn coordinate_repository(
    jj: &JjEnv,
    config_dir: &Path,
    project_repo: &Path,
) -> Result<PathBuf, String> {
    let store_dir = project_store_dir(config_dir, project_repo);
    ensure_store_initialized(jj, &store_dir, project_repo)?;
    Ok(store_dir)
}

/// Create the shared per-project jj store if absent: a Cairn-managed jj repo
/// whose git backend is the project's existing `.git`. The user's checkout is
/// never touched and sealed commits land in the project's object database.
pub fn ensure_project_store(
    jj: &JjEnv,
    store_dir: &Path,
    project_repo: &Path,
) -> Result<(), String> {
    ensure_store_initialized(jj, store_dir, project_repo)?;
    // Always sync the backing git repo into the store. `jj git init` imports on
    // creation, but an already-existing store is otherwise frozen at the refs it
    // last saw: a base ref that advanced since then would not resolve when adding
    // a new workspace (`Revision <sha> doesn't exist`), so every later job on a
    // jj-managed project would fail to provision once the project git moved.
    import_git(jj, store_dir)?;
    Ok(())
}

/// Create the shared per-project jj store if absent, WITHOUT importing.
///
/// Split out of [`ensure_project_store`] so a caller that owns its own import —
/// [`crate::jj::reconcile_tracked_bookmark`], which must import at a precise
/// point between a fetch and a bookmark comparison — can guarantee the store
/// exists without paying for a redundant import first.
pub fn ensure_store_initialized(
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
    Ok(())
}

/// Import the backing git repo's refs and commits into the shared store, so a
/// base ref that advanced since the store was created resolves.
///
/// `--ignore-working-copy` for the same reason every other store operation
/// passes it: nothing here reads the store default workspace's `@`, and a store
/// whose default workspace went stale (the ordinary consequence of the
/// `--ignore-working-copy` writes everywhere else) would otherwise fail this
/// import outright — which is how a stale default workspace came to kill every
/// new child spawn. This was the ONLY jj invocation against the store that
/// omitted the flag.
///
/// Patient with `packed-refs.lock`, through the shared wrapper every runner-owned
/// ref transaction uses. This is job preparation, so losing the race for the
/// lock here kills the whole execution being prepared; the import itself is
/// idempotent, so repeating it costs nothing but time.
///
/// # No observation of origin walks a branch backwards
///
/// jj treats a tracked remote's move as authoritative and carries it onto the
/// local bookmark, so a fetch that received an advertisement lagging one of
/// this store's own pushes would move the branch backwards and hide the commits
/// it had moved past. That fetch may be Cairn's or the operator's: the backing
/// repository is the project checkout, and anyone's `git fetch` there writes
/// the same refs, at any moment, without the store lock.
///
/// So the import holds every tracked `<branch>@origin` to the value this store
/// last recorded for it, wherever origin's ref has moved to a strict ancestor of
/// that value. It does so twice: before importing, by restoring the git ref,
/// and after, by judging what the import actually recorded, because a fetch can
/// write the ref between the two. A regression found after the import is
/// restored and imported again, which also brings back any commit the first
/// import hid. Sideways and forward moves pass through untouched; integrating a
/// foreign rewrite is `converge_managed_branch_after_remote_rewrite`'s job.
///
/// Branches declared origin-authoritative (see
/// [`import_git_with_remote_authority`]) are exempt: origin is the sole
/// authority there, so a rewind of one is observed, not held off.
pub fn import_git(jj: &JjEnv, store_dir: &Path) -> Result<(), String> {
    super::with_packed_refs_patience(
        super::PackedRefs::JjStore(store_dir),
        "jj git import",
        || import_holding_tracking_refs(jj, store_dir, None),
    )
}

/// [`import_git`] for a branch whose authority is origin: `branch`'s tracking
/// ref is imported wherever it now points, backwards included.
///
/// It also DECLARES `branch` origin-authoritative in the store, and every later
/// import, [`import_git`] included, honours that declaration. Without it, any
/// ordinary import that ran between a fetch of a legitimate rewind and this
/// branch's reconcile (job provisioning, say) would restore the old value and
/// erase the rewind before its reconcile ever saw it.
///
/// The exemption is named by the caller, never inferred, for the same reason
/// `reconcile_tracked_bookmark` must be handed a remote-authoritative branch:
/// moving a branch backwards is only correct where origin is the sole authority.
pub fn import_git_with_remote_authority(
    jj: &JjEnv,
    store_dir: &Path,
    branch: &str,
) -> Result<(), String> {
    declare_origin_authority(store_dir, branch)?;
    super::with_packed_refs_patience(
        super::PackedRefs::JjStore(store_dir),
        "jj git import",
        || import_holding_tracking_refs(jj, store_dir, Some(branch)),
    )
}

/// Where a store records the branches origin is the sole authority for. Inside
/// `.jj`, beside jj's own state, because it is a fact about this store and
/// nothing in Cairn snapshots that directory.
fn origin_authority_file(store_dir: &Path) -> PathBuf {
    store_dir
        .join(".jj")
        .join("cairn-origin-authoritative-branches")
}

fn declared_origin_authority(store_dir: &Path) -> std::collections::BTreeSet<String> {
    std::fs::read_to_string(origin_authority_file(store_dir))
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn declare_origin_authority(store_dir: &Path, branch: &str) -> Result<(), String> {
    if branch.is_empty() || !is_jj_dir(store_dir) {
        return Ok(());
    }
    let mut declared = declared_origin_authority(store_dir);
    if !declared.insert(branch.to_string()) {
        return Ok(());
    }
    let path = origin_authority_file(store_dir);
    let staged = path.with_extension("tmp");
    let body = declared.into_iter().collect::<Vec<_>>().join("\n") + "\n";
    std::fs::write(&staged, body)
        .and_then(|()| std::fs::rename(&staged, &path))
        .map_err(|error| {
            format!(
                "declare `{branch}` origin-authoritative in {}: {error}",
                path.display()
            )
        })
}

#[cfg(test)]
static BETWEEN_GUARD_AND_IMPORT: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>> =
    std::sync::Mutex::new(None);

/// Run `write` once, in the next guarded import, after the guard has judged the
/// tracking refs and before jj reads them: the window a concurrent fetch hits.
#[cfg(test)]
pub(crate) fn between_next_guard_and_import(write: impl FnOnce() + Send + 'static) {
    *BETWEEN_GUARD_AND_IMPORT.lock().unwrap() = Some(Box::new(write));
}

fn import_holding_tracking_refs(
    jj: &JjEnv,
    store_dir: &Path,
    origin_authoritative: Option<&str>,
) -> Result<(), String> {
    let import = || {
        jj.run(
            store_dir,
            &["git", "import", "--ignore-working-copy"],
            "jj git import",
        )
        .map(|_| ())
    };
    let checkout = is_jj_dir(store_dir)
        .then(|| super::export::resolve_backing_checkout(store_dir))
        .flatten();
    let Some(checkout) = checkout else {
        log::debug!(
            "jj import guard: no backing checkout resolved for {}; importing unguarded",
            store_dir.display()
        );
        return import();
    };
    let mut exempt = declared_origin_authority(store_dir);
    exempt.extend(origin_authoritative.map(ToOwned::to_owned));
    let recorded = recorded_origin_tracking_targets(jj, store_dir)?
        .into_iter()
        .filter(|(branch, _)| !exempt.contains(branch))
        .collect::<Vec<_>>();

    // Before: restore what the git refs say now, so the common case never
    // hides anything in the first place.
    let observed = origin_tracking_refs(&checkout)?;
    for (branch, recorded) in &recorded {
        if let Some(observed) = observed.get(branch) {
            if observed != recorded && git_is_ancestor(&checkout, observed, recorded) {
                restore_tracking_ref(&checkout, branch, recorded, observed, "before importing")?;
            }
        }
    }
    #[cfg(test)]
    if let Some(write) = BETWEEN_GUARD_AND_IMPORT.lock().unwrap().take() {
        write();
    }
    import()?;

    // After: judge what jj actually recorded, since a fetch can write a ref
    // between the check above and jj reading it. Re-importing a restored ref is
    // a forward move to jj, which carries the bookmark forward again and makes
    // the commits the first import hid visible again.
    for _ in 0..3 {
        let imported = recorded_origin_tracking_targets(jj, store_dir)?
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let regressed = recorded
            .iter()
            .filter_map(|(branch, recorded)| {
                let now = imported.get(branch)?;
                (now != recorded && git_is_ancestor(&checkout, now, recorded))
                    .then(|| (branch, recorded, now.clone()))
            })
            .collect::<Vec<_>>();
        if regressed.is_empty() {
            return Ok(());
        }
        for (branch, recorded, now) in regressed {
            restore_tracking_ref(&checkout, branch, recorded, &now, "after an import took it")?;
        }
        import()?;
    }
    Err(
        "jj import guard: origin tracking refs kept regressing while being restored; the last \
         import may have recorded a stale origin observation"
            .to_string(),
    )
}

/// Put `refs/remotes/origin/<branch>` back from `regressed` to `recorded`.
///
/// The recorded value is jj's `<branch>@origin`: what this store last pushed
/// there or last imported from there. A ref that is a strict ancestor of it is
/// origin claiming to have gone backwards past something this store already saw
/// it accept. The restore is a compare-and-swap against the regressed value, so
/// a concurrent writer is never overwritten; losing that race fails the import
/// rather than importing a ref nobody has checked.
fn restore_tracking_ref(
    checkout: &Path,
    branch: &str,
    recorded: &str,
    regressed: &str,
    when: &str,
) -> Result<(), String> {
    let refname = format!("refs/remotes/origin/{branch}");
    let output = cairn_platform::env::git()
        .args(["update-ref", &refname, recorded, regressed])
        .current_dir(checkout)
        .output()
        .map_err(|error| format!("jj import guard: restore {refname}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "jj import guard: {refname} regressed from {recorded} to {regressed} and moved again \
             before it could be restored: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    log::warn!(
        "jj import guard: `{branch}`'s origin tracking ref regressed from {recorded} to its \
         ancestor {regressed} ({when}): a stale origin observation; kept the value this store \
         last pushed or fetched"
    );
    Ok(())
}

/// Every tracked `<branch>@origin` the store has recorded, with its target.
fn recorded_origin_tracking_targets(
    jj: &JjEnv,
    store_dir: &Path,
) -> Result<Vec<(String, String)>, String> {
    let out = jj.run(
        store_dir,
        &[
            "bookmark",
            "list",
            "--all-remotes",
            "--ignore-working-copy",
            "-T",
            "if(remote == \"origin\" && tracked && normal_target, name ++ \"\\t\" ++ \
             normal_target.commit_id() ++ \"\\n\", \"\")",
        ],
        "jj bookmark list (recorded origin tracking targets)",
    )?;
    Ok(out
        .lines()
        .filter_map(|line| {
            let (name, commit) = line.trim().split_once('\t')?;
            Some((name.to_string(), commit.to_string()))
        })
        .collect())
}

/// `refs/remotes/origin/*` in the backing repository, keyed by branch name.
fn origin_tracking_refs(
    checkout: &Path,
) -> Result<std::collections::HashMap<String, String>, String> {
    let output = cairn_platform::env::git()
        .args([
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/remotes/origin/",
        ])
        .current_dir(checkout)
        .output()
        .map_err(|error| format!("jj import guard: list origin tracking refs: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "jj import guard: list origin tracking refs: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let (commit, refname) = line.trim().split_once(' ')?;
            let branch = refname.strip_prefix("refs/remotes/origin/")?;
            Some((branch.to_string(), commit.to_string()))
        })
        .collect())
}

pub(crate) fn git_is_ancestor(checkout: &Path, ancestor: &str, descendant: &str) -> bool {
    cairn_platform::env::git()
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .current_dir(checkout)
        .status()
        .is_ok_and(|status| status.success())
}

/// Fetch one branch into the backing Git remote-tracking ref without opening or
/// mutating jj. Stale-publication recovery calls this outside the per-store lock,
/// then imports the fetched ref under the lock before changing the graph.
pub fn fetch_remote_branch_via_git(
    store_dir: &Path,
    remote: &str,
    branch: &str,
) -> Result<(), String> {
    let checkout = super::export::resolve_backing_checkout(store_dir).ok_or_else(|| {
        format!(
            "git fetch {remote} branch `{branch}`: no backing checkout resolved for {}",
            store_dir.display()
        )
    })?;
    let refspec = format!("+refs/heads/{branch}:refs/remotes/{remote}/{branch}");
    let output = cairn_platform::env::git()
        .args(["fetch", remote, &refspec])
        .current_dir(checkout)
        .output()
        .map_err(|error| format!("git fetch {remote} branch `{branch}`: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git fetch {remote} branch `{branch}`: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// Where a fetch of origin's copy of a store-owned branch waits until the store
/// has judged it. Under `refs/cairn/`, which compaction already treats as a pin,
/// so the observed commit's objects survive the judgement. Its lifecycle is one
/// fetch-and-judge: [`crate::jj::fast_forward_branch_from_origin`] deletes it
/// whatever it answers, and a fetch that finds no such branch on origin deletes
/// any leftover. An observation is never a cache.
pub fn origin_observation_ref(branch: &str) -> String {
    format!("refs/cairn/origin-observed/{branch}")
}

/// Fetch origin's `branch` into [`origin_observation_ref`] and answer the commit
/// it names, or `Ok(None)` when origin has no such branch.
///
/// Never into `refs/remotes/origin/<branch>`. For a branch the store owns, that
/// tracking ref is what every import reads, and an import merges any move of it
/// into the local bookmark: a copy origin diverged from would turn the branch
/// into a conflicted name on the next import anyone ran. Parked here, nothing
/// imports it until [`crate::jj::fast_forward_branch_from_origin`] has decided
/// that it is a pure fast-forward.
///
/// Network I/O with no jj involvement, so callers run it outside the store lock
/// and inside the repository's fetch lock.
pub fn fetch_origin_observation(store_dir: &Path, branch: &str) -> Result<Option<String>, String> {
    let checkout = super::export::resolve_backing_checkout(store_dir).ok_or_else(|| {
        format!(
            "git fetch origin branch `{branch}`: no backing checkout resolved for {}",
            store_dir.display()
        )
    })?;
    let observation = origin_observation_ref(branch);
    let refspec = format!("+refs/heads/{branch}:{observation}");
    // An empty `--refmap` matters as much as the refspec: without it git also
    // makes its "opportunistic" update of `refs/remotes/origin/<branch>` from
    // the configured fetch mapping, which is precisely the write this avoids.
    let output = bounded_command_output(
        cairn_platform::env::git()
            .args(["fetch", "--no-tags", "--refmap=", "origin", &refspec])
            .current_dir(&checkout),
        JJ_NETWORK_TIMEOUT,
        "git fetch origin",
    )?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("couldn't find remote ref") {
            let _ = cairn_platform::env::git()
                .args(["update-ref", "-d", &observation])
                .current_dir(&checkout)
                .output();
            return Ok(None);
        }
        return Err(format!(
            "git fetch origin branch `{branch}`: {}",
            stderr.trim()
        ));
    }
    let resolved = cairn_platform::env::git()
        .args(["rev-parse", "--verify", "--quiet", &observation])
        .current_dir(&checkout)
        .output()
        .map_err(|error| format!("read {observation}: {error}"))?;
    let commit = String::from_utf8_lossy(&resolved.stdout).trim().to_string();
    if !resolved.status.success() || commit.is_empty() {
        return Err(format!(
            "git fetch origin branch `{branch}` succeeded but {observation} does not resolve"
        ));
    }
    Ok(Some(commit))
}

/// Fetch one branch of a remote into the shared store, advancing its
/// remote-tracking bookmark (`<branch>@<remote>`) to the remote's current tip.
/// Used to bring an externally-advanced target branch into the store, so a
/// source can be rebased onto `<branch>@origin` before a fold.
///
/// Only the named branch. `jj git fetch` writes the store's view of every
/// branch it fetches directly, with no import in between to guard it, and it
/// abandons commits the remote no longer advertises. A whole-remote fetch that
/// received an advertisement lagging one of this store's own pushes would walk
/// that agent branch backwards and hide its newest commits.
///
/// For the same reason no fetch of this store passes `--prune`, and nothing
/// runs `git remote prune` against it. A prune deletes every tracking ref the
/// advertisement leaves out, so a lagging one takes the tracking ref of a branch
/// the store has just pushed, and the next import then deletes that branch's
/// local bookmark, or conflicts it if it moved. Tracking refs of finished
/// branches are retired by the ref-store maintenance instead, keyed on Cairn's
/// own records of the branch and never on the remote's advertisement
/// (`retire_remote_tracking_refs`; cairn/5235).
pub fn fetch_remote_branch(
    jj: &JjEnv,
    store_dir: &Path,
    remote: &str,
    branch: &str,
) -> Result<(), String> {
    let pattern = format!("exact:{branch:?}");
    jj.run_with_timeout(
        store_dir,
        &[
            "git",
            "fetch",
            "--remote",
            remote,
            "--branch",
            &pattern,
            "--ignore-working-copy",
        ],
        "jj git fetch",
        JJ_NETWORK_TIMEOUT,
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
pub(crate) fn quote_fileset(path: &str) -> String {
    let escaped = path.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(jj)]
    fn managed_command_times_out_and_kills_child() {
        use std::os::unix::fs::PermissionsExt;

        let home = TempDir::new().unwrap();
        let script = home.path().join("slow-jj");
        let pid_file = home.path().join("pid");
        std::fs::write(
            &script,
            format!("#!/bin/sh\necho $$ > {}\nsleep 30\n", pid_file.display()),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let jj = JjEnv {
            checkout_repair_policy: Default::default(),
            bin: script.to_string_lossy().into_owned(),
            config_path: home.path().join("config.toml"),
        };

        // The timeout has to outlast the child's own startup, not merely be
        // short. It was 500ms, which the shell wins comfortably on an idle
        // machine and loses under a loaded one: the kill lands before `echo $$`
        // runs, the pid file is never written, and the test fails reading it —
        // reporting a timeout defect where there is only CPU contention. Three
        // seconds is still far below the script's 30s sleep, so the assertion
        // that the timeout fired remains exactly as sharp.
        let started = Instant::now();
        let error = jj
            .run_bytes_with_timeout(home.path(), &[], "slow jj", Duration::from_secs(3))
            .unwrap_err();
        assert!(crate::jj::is_jj_timeout_error(&error), "{error}");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "the timeout must cut the 30s sleep short"
        );
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap_or_else(|error| {
                panic!(
                    "the timed-out child never recorded its pid at {}: {error}",
                    pid_file.display()
                )
            })
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "timed-out child is still alive"
        );
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(jj)]
    fn managed_command_bounds_pipe_reader_shutdown() {
        use std::os::unix::fs::PermissionsExt;

        let home = TempDir::new().unwrap();
        let script = home.path().join("leaky-jj");
        std::fs::write(&script, "#!/bin/sh\n(sleep 30) &\nexit 0\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let jj = JjEnv {
            checkout_repair_policy: Default::default(),
            bin: script.to_string_lossy().into_owned(),
            config_path: home.path().join("config.toml"),
        };

        // Same load-sensitivity as `managed_command_times_out_and_kills_child`,
        // and the same remedy. The bound must outlast the script's own startup,
        // not merely be short: at 500ms a loaded machine loses the race, the
        // deadline fires before `exit 0` runs, and the SIGKILL to the process
        // group reaps the backgrounded sleep as well — so the pipes close, the
        // reader joins cleanly, and the error is the TIMEOUT rather than the
        // pipe-shutdown bound this test exists to cover. Observed failing 2 of 5
        // runs. Three seconds is still far below the script's 30s sleep, so the
        // assertion is exactly as sharp; the elapsed bound covers that timeout
        // plus the 2s reader join with room for a loaded machine.
        let started = Instant::now();
        let error = jj
            .run_bytes_with_timeout(home.path(), &[], "leaky jj", Duration::from_secs(3))
            .unwrap_err();
        assert!(error.contains("pipe remained open"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(15));
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(jj)]
    fn managed_command_disables_git_prompts() {
        use std::os::unix::fs::PermissionsExt;

        let home = TempDir::new().unwrap();
        let script = home.path().join("env-jj");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s:%s' \"$GIT_TERMINAL_PROMPT\" \"$GIT_ASKPASS\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let jj = JjEnv {
            checkout_repair_policy: Default::default(),
            bin: script.to_string_lossy().into_owned(),
            config_path: home.path().join("config.toml"),
        };

        assert_eq!(jj.run(home.path(), &[], "env jj").unwrap(), "0:true");
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
}
