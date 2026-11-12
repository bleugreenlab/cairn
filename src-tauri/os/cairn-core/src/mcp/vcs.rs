//! VCS mutation seam for the commit-barrier and write-commit paths.
//!
//! Physical-checkout VCS helpers retained for user live checkouts and executor
//! projections. Agent identity and logical file mutations never resolve through
//! process cwd: the runner-owned branch store and logical-head transaction are
//! authoritative. The trait remains as a deterministic test seam for physical
//! checkout barriers used outside agent residence.
//!
//! This trait deliberately carries NO publication operation. An agent process
//! lives in scratch and owns no jj workspace, so a publication routed through a
//! worktree resolves to the read-only [`NonWorktreeVcs`] and silently publishes
//! nothing — which is exactly how commit after commit reported success while the
//! branch stayed where it was. Publication addresses the shared store instead;
//! see [`crate::jj::publish_branch_to_origin`].

use std::path::Path;
use std::time::Duration;

use super::git::CommitResult;
use cairn_vcs::PublicationIdentity;

/// How long a file-target write waits for the project store lock. It is the
/// shared constant rather than a mirror of one: the wait here and the socket
/// ceiling `cairn-cmd` sizes above it are the same fact, and a mirror is a place
/// for them to drift.
pub(crate) const STORE_LOCK_TIMEOUT: Duration =
    Duration::from_millis(cairn_common::write_contract::WRITE_STORE_LOCK_WAIT_MS);

pub(crate) async fn acquire_store_lock(
    orch: &crate::orchestrator::Orchestrator,
    store: Option<&Path>,
    class: crate::orchestrator::StoreLockClass,
    operation: &str,
    timeout: Duration,
) -> Result<Option<crate::orchestrator::JjStoreGuard>, String> {
    let Some(store) = store else { return Ok(None) };
    orch.acquire_jj_store_lock_with_timeout(store, class, operation, Some(timeout))
        .await
        .map(Some)
        .map_err(|()| store_busy_message(orch.store_lock_status(store).as_ref()))
}

/// The refusal for a store-lock wait that ran out, naming who holds the lock,
/// for how long, and how many others are queued.
fn store_busy_message(status: Option<&crate::orchestrator::StoreLockStatus>) -> String {
    match status {
        Some(status) if status.holder.is_some() => format!(
            "The project's version-control store is busy behind: {} ({} waiting); retry this \
             operation.",
            status.describe_holder(),
            status.waiters.len()
        ),
        _ => "The project's version-control store is busy behind: another version-control \
              operation; retry this operation."
            .to_string(),
    }
}

/// Opaque pre-batch working-copy snapshot, captured before a batch runs so the
/// barrier can attribute new dirt to the call that caused it. Carries the jj `@`
/// change id.
#[derive(Debug, Clone)]
pub struct VcsSnapshot(pub(crate) String);

impl VcsSnapshot {
    /// The backend-internal string this snapshot carries.
    pub fn raw(&self) -> &str {
        &self.0
    }
}

/// What the ambient commit barrier may do to a physical checkout: observe it,
/// and commit it when the backend allows. No implementation reverts a checkout.
/// None of the checkouts an ambient run can land in is Cairn's to roll back,
/// because a restore takes every uncommitted change with it, not just the
/// batch's.
pub trait WorktreeVcs: Send + Sync {
    /// Capture pre-batch working-copy state.
    fn snapshot(&self, worktree: &Path) -> Result<VcsSnapshot, String>;
    /// Did the working copy change versus `before`?
    fn changed_since(&self, worktree: &Path, before: &VcsSnapshot) -> Result<bool, String>;
    /// Is the working copy dirty right now?
    fn is_dirty(&self, worktree: &Path) -> Result<bool, String>;
    /// Seal the whole working copy into one addressable commit.
    fn seal_all(
        &self,
        worktree: &Path,
        msg: &str,
        author: Option<&PublicationIdentity>,
    ) -> Result<CommitResult, String>;
    /// The working copy's current edits as a unified patch, read before a seal
    /// so the barrier can scan the touched files for conflict markers and report
    /// the commit's line counts. `None` when there is nothing to capture or the
    /// backend cannot produce one.
    fn capture_patch(&self, worktree: &Path) -> Option<String> {
        let _ = worktree;
        None
    }
}

/// jj backend for a jj checkout Cairn does not own: a repository the user
/// colocated themselves and pointed an ambient session at. It seals `@` into one
/// addressable commit that nominates no branch, and it never reverts. A checkout
/// that belongs to Cairn's shared store resolves to [`CairnResidenceVcs`]
/// instead.
pub struct JjBackend {
    jj: crate::jj::JjEnv,
}

impl JjBackend {
    pub(crate) fn new(jj: crate::jj::JjEnv) -> Self {
        Self { jj }
    }
}

impl WorktreeVcs for JjBackend {
    fn snapshot(&self, worktree: &Path) -> Result<VcsSnapshot, String> {
        Ok(VcsSnapshot(crate::jj::snapshot_change_id(
            &self.jj, worktree,
        )?))
    }

    fn changed_since(&self, worktree: &Path, _before: &VcsSnapshot) -> Result<bool, String> {
        // Any non-empty `@` is reported. The answer only ever produces a
        // warning, so counting the user's own earlier edits costs a line of text,
        // never their work.
        crate::jj::is_working_copy_dirty(&self.jj, worktree)
    }

    fn is_dirty(&self, worktree: &Path) -> Result<bool, String> {
        crate::jj::is_working_copy_dirty(&self.jj, worktree)
    }

    fn seal_all(
        &self,
        worktree: &Path,
        msg: &str,
        author: Option<&PublicationIdentity>,
    ) -> Result<CommitResult, String> {
        // No branch: the checkout is not Cairn's, so the commit lands locally and
        // no bookmark advances.
        Ok(crate::jj::seal_paths(&self.jj, worktree, msg, author, &[], None)?.into())
    }

    fn capture_patch(&self, worktree: &Path) -> Option<String> {
        // Best-effort: a diff failure (e.g. jj refusing on a stale copy) yields
        // `None`, which only drops the marker scan's file list and the line count.
        crate::jj::working_copy_diff(&self.jj, worktree)
            .ok()
            .filter(|patch| !patch.trim().is_empty())
    }
}

/// Rejection returned when a seal is attempted in a non-worktree cwd. Changes
/// can only happen in a worktree; the project's live checkout is read-only for
/// agents.
const NON_WORKTREE_SEAL_ERROR: &str =
    "Changes can only be made in a worktree. This agent runs on the project's live \
     checkout (no worktree) and cannot commit.";

/// Read-only [`WorktreeVcs`] for a non-jj cwd: the project's live checkout used
/// by long-lived triage / read-only-analysis agents and other no-worktree runs.
/// Changes can only happen in worktrees, so there is nothing here for
/// Cairn to *manage* — but it is NOT inert. `snapshot`/`changed_since` perform a
/// read-only `git status` so the no-`commit_msg` barrier can detect when a run
/// left stray dirt in the live checkout and WARN about it. Detection is
/// read-only, and the warning never reverts. See `docs/worktree-fence.md`.
pub struct NonWorktreeVcs;

/// Read-only `git status --porcelain` line set for a checkout, best-effort.
///
/// Returns `None` when the directory is not a git repo or git is unavailable —
/// detection is advisory, so a missing signal must never fail a run. Respects
/// `.gitignore`, so build artifacts (`target/`, `node_modules/`) never appear:
/// the warning fires only on real, status-visible source dirt.
fn checkout_status(worktree: &Path) -> Option<String> {
    let output = crate::env::git()
        .arg("-C")
        .arg(worktree)
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).to_string())
}

impl WorktreeVcs for NonWorktreeVcs {
    fn snapshot(&self, worktree: &Path) -> Result<VcsSnapshot, String> {
        // Capture the checkout's pre-batch dirt set so `changed_since` can
        // attribute NEW dirt to this batch and not blame the user's own
        // pre-existing uncommitted work. Best-effort: empty on a non-repo.
        Ok(VcsSnapshot(checkout_status(worktree).unwrap_or_default()))
    }

    fn changed_since(&self, worktree: &Path, before: &VcsSnapshot) -> Result<bool, String> {
        // New dirt = any porcelain line present now but not at batch entry. A
        // line the user already had stays attributed to them. Best-effort: a
        // missing post-batch status reports "unchanged" so we never fabricate a
        // warning.
        let Some(after) = checkout_status(worktree) else {
            return Ok(false);
        };
        let before_lines: std::collections::HashSet<&str> = before.0.lines().collect();
        Ok(after.lines().any(|line| !before_lines.contains(line)))
    }

    fn is_dirty(&self, _worktree: &Path) -> Result<bool, String> {
        Ok(false)
    }

    fn seal_all(
        &self,
        _worktree: &Path,
        _msg: &str,
        _author: Option<&PublicationIdentity>,
    ) -> Result<CommitResult, String> {
        Err(NON_WORKTREE_SEAL_ERROR.to_string())
    }
}

/// Leads every refusal [`CairnResidenceVcs`] returns, so the barrier reports it
/// as the refusal it is rather than as a failed commit in somebody else's
/// checkout.
const CAIRN_RESIDENCE: &str = "This checkout is a Cairn-managed agent workspace";

/// Whether `error` is a [`CairnResidenceVcs`] refusal.
pub(crate) fn is_cairn_residence_refusal(error: &str) -> bool {
    error.starts_with(CAIRN_RESIDENCE)
}

/// A physical jj checkout that belongs to Cairn: a workspace of a Cairn project
/// store, or one carrying Cairn's branch marker. Cairn no longer provisions
/// these. The ones an ambient run can still land in are legacy agent residences
/// that boot cleanup could not prove or deregister, still attached to the shared
/// store.
///
/// An ambient run carries no authenticated run identity, and the only identity
/// such a checkout offers is a marker inside the tree the batch itself can
/// write. So nothing here publishes or rolls back: an agent branch lands only
/// through its owning run's prepared publication, under that branch's
/// reservation, and a seal here would move the bookmark under neither. The
/// backend does not run jj at all, because any jj command that reads the working
/// copy snapshots it, which records an operation in the shared store with no
/// lock held.
pub struct CairnResidenceVcs {
    /// The branch marker as preflight read it. It only names the branch in the
    /// refusal; nothing is published to it.
    branch: Option<String>,
}

impl CairnResidenceVcs {
    /// Classify `worktree` before the batch runs. `stores_root` is the directory
    /// every Cairn project store lives in.
    fn detect(worktree: &Path, stores_root: &Path) -> Option<Self> {
        let branch = crate::jj::read_branch_marker(worktree);
        (branch.is_some() || repository_is_under(worktree, stores_root)).then_some(Self { branch })
    }

    fn refusal(&self, consequence: &str) -> String {
        let branch = self
            .branch
            .as_deref()
            .map(|branch| format!(" for `{branch}`"))
            .unwrap_or_default();
        format!(
            "{CAIRN_RESIDENCE}{branch}, and this run carries no agent identity, so {consequence} \
             An agent branch lands only through the run that owns it, under that branch's \
             reservation."
        )
    }
}

/// Whether the jj repository behind `worktree` lives under `stores_root`,
/// compared canonically so a symlinked home or a relative pointer gives the
/// same answer. A secondary workspace's `.jj/repo` is a file naming the repo
/// directory relative to its `.jj`, as jj itself resolves it; a store's own
/// `.jj/repo` is that directory.
fn repository_is_under(worktree: &Path, stores_root: &Path) -> bool {
    let Ok(root) = std::fs::canonicalize(stores_root) else {
        return false;
    };
    let dot_jj = worktree.join(".jj");
    let pointer = dot_jj.join("repo");
    let repository = if pointer.is_file() {
        match std::fs::read_to_string(&pointer) {
            Ok(named) => dot_jj.join(named.trim()),
            Err(_) => return false,
        }
    } else {
        pointer
    };
    std::fs::canonicalize(repository).is_ok_and(|repository| repository.starts_with(&root))
}

impl WorktreeVcs for CairnResidenceVcs {
    fn snapshot(&self, _worktree: &Path) -> Result<VcsSnapshot, String> {
        Ok(VcsSnapshot(String::new()))
    }

    fn changed_since(&self, _worktree: &Path, _before: &VcsSnapshot) -> Result<bool, String> {
        // Unobservable without a snapshot, so every run here is told it was left
        // alone.
        Err(self.refusal(
            "Cairn neither inspected nor reverted this checkout: anything this run changed was \
             left in place.",
        ))
    }

    fn is_dirty(&self, _worktree: &Path) -> Result<bool, String> {
        // Unknown for the same reason. Reported dirty so that a `commit_msg`
        // reaches the refusal rather than a silent "nothing to commit".
        Ok(true)
    }

    fn seal_all(
        &self,
        _worktree: &Path,
        _msg: &str,
        _author: Option<&PublicationIdentity>,
    ) -> Result<CommitResult, String> {
        Err(self.refusal(
            "it cannot commit here: nothing was committed, and the working copy was left \
             exactly as it was.",
        ))
    }
}

/// Resolve the physical VCS backend for an ambient run's cwd, before the batch
/// runs. A checkout of Cairn's shared store resolves to the refusing
/// [`CairnResidenceVcs`], any other `.jj` checkout to [`JjBackend`], and
/// anything else (a plain checkout) to the read-only [`NonWorktreeVcs`], so the
/// commit barrier never shells jj in, or reverts, a checkout that is not its to
/// change.
pub(crate) fn resolve_worktree_vcs(
    orch: &crate::orchestrator::Orchestrator,
    worktree: &Path,
) -> Box<dyn WorktreeVcs> {
    if !crate::jj::is_jj_dir(worktree) {
        return Box::new(NonWorktreeVcs);
    }
    let store = crate::jj::project_store_dir(&orch.config_dir, worktree);
    let stores_root = store.parent().unwrap_or(&orch.config_dir);
    if let Some(residence) = CairnResidenceVcs::detect(worktree, stores_root) {
        return Box::new(residence);
    }
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    Box::new(JjBackend::new(jj))
}

/// Resolve the runner-owned store lock from authenticated run identity. Process
/// cwd is deliberately irrelevant: it is only a scratch residence.
pub(crate) async fn resolve_store_lock(
    orch: &crate::orchestrator::Orchestrator,
    request: &cairn_common::protocol::CallbackRequest,
) -> Option<std::path::PathBuf> {
    crate::mcp::handlers::branch::resolve_current_for_read(orch, request)
        .await
        .ok()
        .map(|resolution| resolution.repository_path)
}

/// Env that makes a bare `git`/`jj` shell command run through the run tool
/// behave correctly inside an explicitly selected jj executor projection. Empty
/// for a user live checkout, where bare git already resolves the checkout.
///
/// Two distinct fixes compose here, both scoped to an explicit `.jj` projection:
///
/// 1. **Managed jj identity** ([`crate::jj::JjEnv::shell_env`]). A non-colocated
///    jj workspace has no `.git`, so a bare `jj` shell command that never saw
///    `JJ_CONFIG` would commit with an empty/wrong committer and be unpushable.
///    This injects exactly the env managed jj already runs with, giving a bare
///    `jj` the managed fallback identity (`Cairn Agent <agent@cairn.local>`) —
///    a valid, pushable committer. The *per-project* author used on managed
///    seals is injected only as `--config user.{name,email}=…` args on each
///    seal (`JjEnv::author_args`); a bare jj command cannot carry those, and
///    that is correct — do NOT "fix" it by leaking project identity into the
///    global jj config; the managed fallback is itself a valid committer.
/// 2. **`GIT_CEILING_DIRECTORIES`**. A non-colocated workspace has no `.git`, so
///    a bare `git` walks *up* the tree and silently resolves the `~/.cairn`
///    HOME repo (`git rev-parse --show-toplevel` returns `~/.cairn`,
///    `git status` reports `On branch main`) — answering about the wrong repo
///    with no error. A ceiling stops git's upward repo discovery at the
///    worktree boundary, so a bare `git` in a non-colocated workspace fails
///    loudly ("not a git repository") instead.
///
///    The ceiling is the worktree root's **parent**, not the worktree root
///    itself. git's `longest_ancestor_length` (setup.c) only honors a ceiling
///    entry that is a *strict* ancestor of the cwd — a prefix followed by `/` —
///    so a ceiling equal to the cwd is ignored. A bare `git` most often runs
///    *from* the worktree root (cwd == worktree root), so a worktree-root
///    ceiling would be inert there and git would ascend to `~/.cairn` anyway.
///    The parent is a strict ancestor of both the worktree root and any subdir
///    an agent `cd`-ed into, so git examines the worktree subtree, finds no
///    `.git`, and stops at the parent without ever reaching `~/.cairn`.
///
///    jj's own git-backend ops (`jj git push`/`fetch`, seal, log) address the
///    store by absolute path via libgit2/gitoxide, not by the git CLI's
///    cwd-anchored discovery walk, so this knob is inert for them — see the
///    bare-`jj git push` non-regression test in `mcp_run_commit_hygiene`. (The
///    jj store lives under `~/.cairn/jj-stores`, never between a worktree and
///    this parent ceiling, so even a discovery-based op could not be trapped.)
pub(crate) fn worktree_shell_vcs_env(
    orch: &crate::orchestrator::Orchestrator,
    cwd: &Path,
) -> Vec<(String, String)> {
    if !crate::jj::is_jj_dir(cwd) {
        return Vec::new();
    }
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let mut env = jj.shell_env();
    // Parent, not cwd: git ignores a ceiling equal to the cwd (see doc above).
    let ceiling = cwd.parent().unwrap_or(cwd);
    env.push((
        "GIT_CEILING_DIRECTORIES".into(),
        ceiling.to_string_lossy().into_owned(),
    ));
    // The universal `<cairn_home>/bin/jj` shim (installed at startup and already
    // on every agent PATH via `agent_shell_path`) intercepts
    // `jj workspace update-stale` and forwards every other jj to the bundled
    // binary, so no projection-specific shim dir is composed here anymore and the
    // interception now reaches Windows too. `shell_env` above already carries the
    // managed jj config and non-interactive editor.
    env
}

/// Publish immutable cloud coverage for a runner-sealed commit.
///
/// Call this after releasing the project-store lock. For the first cloud-visible
/// root we upload a self-contained
/// reachable pack. Once the direct parent is cataloged for this repository, later
/// seals upload only the complete (non-thin) range from that covered parent.
/// Bytes are put before the catalog/reference transaction.
pub(crate) async fn publish_sealed_commit_pack(
    db: &crate::storage::LocalDb,
    project_id: &str,
    repository: &Path,
    sealed_commit: &str,
) -> Result<(), String> {
    let Some(store) = db.team_content_store().cloned() else {
        return Ok(());
    };
    let project_id = project_id.to_string();
    // A project row whose `repository_id` is absent or empty has no durable
    // identity to catalog under, and there is no safe identity to guess: the
    // catalog is keyed by it. Read it NULL-tolerantly so that case reaches the
    // refusal below instead of surfacing as a row-conversion error.
    let repository_id = db
        .query_opt(
            "SELECT repository_id FROM projects WHERE id = ?1",
            (project_id.clone(),),
            |row| crate::storage::RowExt::opt_text(row, 0),
        )
        .await
        .map_err(|error| format!("resolve sealed-pack repository identity: {error}"))?
        .flatten()
        .filter(|repository_id| !repository_id.is_empty())
        .ok_or_else(|| "project has no durable repository identity".to_string())?;

    // Two git subprocesses, so off the runtime: a worker waiting on them in
    // `output()` is a worker the Runner cannot schedule anything else on.
    let (tip, parent) = {
        let repository = repository.to_path_buf();
        let sealed_commit = sealed_commit.to_string();
        tokio::task::spawn_blocking(move || {
            let tip = git_stdout(
                &repository,
                &["rev-parse", &format!("{sealed_commit}^{{commit}}")],
            )?;
            let parent = git_stdout(&repository, &["rev-parse", &format!("{tip}^")]).ok();
            Ok::<_, String>((tip, parent))
        })
        .await
        .map_err(|error| format!("resolve sealed commit: {error}"))??
    };
    let covered_parent = match parent.as_deref() {
        Some(parent) => {
            db.query_opt_i64(
                "SELECT COUNT(*) FROM pack_catalog c
                 JOIN pack_catalog_references r
                   ON r.content_hash = c.content_hash
                  AND r.project_id = c.project_id
                  AND r.repository_id = c.repository_id
                  AND r.object_format = c.object_format
                 WHERE c.project_id = ?1 AND c.repository_id = ?2
                   AND c.object_format = 'sha1' AND c.tip_commit = ?3
                   AND r.owner_kind = 'sealed_commit' AND r.owner_id = ?3",
                (
                    project_id.clone(),
                    repository_id.clone(),
                    parent.to_string(),
                ),
            )
            .await
            .map_err(|error| format!("inspect sealed-pack parent coverage: {error}"))?
            .unwrap_or(0)
                > 0
        }
        None => false,
    };

    let repository = repository.to_path_buf();
    let tip_for_pack = tip.clone();
    let parent_for_pack = parent.clone();
    let (pack, index, kind, base_commit) = tokio::task::spawn_blocking(move || {
        if covered_parent {
            let base = parent_for_pack
                .as_deref()
                .ok_or_else(|| "covered sealed root lost its parent coordinate".to_string())?;
            let (pack, index) = cairn_codec::packfile::build_reachable_range_pack(
                &repository,
                &tip_for_pack,
                base,
            )?;
            Ok::<_, String>((
                pack,
                index,
                crate::storage::pack_catalog::PackKind::ExecutionRange,
                Some(base.to_string()),
            ))
        } else {
            let (pack, index) =
                cairn_codec::packfile::build_reachable_pack(&repository, &tip_for_pack)?;
            Ok((
                pack,
                index,
                crate::storage::pack_catalog::PackKind::Reachable,
                None,
            ))
        }
    })
    .await
    .map_err(|error| format!("sealed-pack build task failed: {error}"))??;

    let validated = crate::orchestrator::object_plane::validate_pack_bytes(pack)
        .map_err(|error| format!("validate sealed pack: {error}"))?;
    if validated.index != index {
        return Err("sealed pack builder index disagrees with independent validation".into());
    }
    crate::orchestrator::object_plane::publish_validated_pack(
        db,
        Some(store.as_ref()),
        &validated,
        crate::storage::pack_catalog::PackCatalogPublication {
            content_hash: String::new(),
            project_id,
            repository_id,
            object_format: "sha1".into(),
            byte_count: 0,
            pack_checksum: String::new(),
            object_count: 0,
            kind,
            base_commit,
            tip_commit: tip.clone(),
            owner_kind: "sealed_commit".into(),
            owner_id: tip,
        },
    )
    .await
    .map_err(|error| format!("publish sealed pack: {error}"))
}

fn git_stdout(repository: &Path, args: &[&str]) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .map_err(|error| format!("spawn git {args:?}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// In-memory [`WorktreeVcs`] double for deterministic, binary-free coverage of
/// the commit barrier's control flow. Each query returns a programmed result,
/// and the seal counter lets a test assert whether a seal was attempted.
/// Defined at module scope (not inside `mod tests`) so the barrier tests in
/// other modules can reach it.
#[cfg(test)]
pub(crate) struct FakeVcs {
    dirty: Result<bool, String>,
    changed: Result<bool, String>,
    seal: Result<CommitResult, String>,
    capture: Option<String>,
    seal_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FakeVcs {
    pub(crate) fn new() -> Self {
        Self {
            dirty: Ok(true),
            changed: Ok(true),
            seal: Ok(CommitResult {
                sha: "abc123".to_string(),
                pr_number: None,
                amend_note: None,
            }),
            capture: None,
            seal_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn capture(mut self, v: Option<String>) -> Self {
        self.capture = v;
        self
    }

    pub(crate) fn dirty(mut self, v: Result<bool, String>) -> Self {
        self.dirty = v;
        self
    }

    pub(crate) fn changed(mut self, v: Result<bool, String>) -> Self {
        self.changed = v;
        self
    }

    pub(crate) fn seal(mut self, v: Result<CommitResult, String>) -> Self {
        self.seal = v;
        self
    }

    pub(crate) fn seals(&self) -> usize {
        self.seal_calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl WorktreeVcs for FakeVcs {
    fn snapshot(&self, _worktree: &Path) -> Result<VcsSnapshot, String> {
        Ok(VcsSnapshot("fake-change-id".to_string()))
    }

    fn changed_since(&self, _worktree: &Path, _before: &VcsSnapshot) -> Result<bool, String> {
        self.changed.clone()
    }

    fn is_dirty(&self, _worktree: &Path) -> Result<bool, String> {
        self.dirty.clone()
    }

    fn seal_all(
        &self,
        _worktree: &Path,
        _msg: &str,
        _author: Option<&PublicationIdentity>,
    ) -> Result<CommitResult, String> {
        self.seal_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.seal.clone()
    }

    fn capture_patch(&self, _worktree: &Path) -> Option<String> {
        self.capture.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    use crate::jj::tests::jj_bin;

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

    fn git_stdout(repo: &Path, args: &[&str]) -> String {
        let out = crate::env::git()
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?} failed");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn init_project(repo: &Path) {
        git(repo, &["init", "-q", "-b", "main"]);
        git(repo, &["config", "user.email", "p@e.com"]);
        git(repo, &["config", "user.name", "p"]);
        std::fs::write(repo.join("shared.rs"), "base\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "base"]);
    }

    mod sealed_packs {
        use super::{git, git_stdout, init_project};
        use crate::mcp::vcs::publish_sealed_commit_pack;
        use crate::orchestrator::object_plane::resolve_catalog_chain;
        use crate::storage::{
            migrated_test_db, ContentStore, InMemoryContentStore, LocalDb, RowExt,
            TeamReplicaContext,
        };
        use std::path::Path;
        use std::sync::Arc;
        use tempfile::TempDir;

        const PROJECT: &str = "project-sealed";
        const REPOSITORY: &str = "repository-sealed";

        /// A content store that accepts nothing, for proving that a failed put
        /// leaves no catalog reference behind.
        struct RefusingContentStore;

        #[async_trait::async_trait]
        impl ContentStore for RefusingContentStore {
            async fn put(
                &self,
                _hash: &str,
                _bytes: &[u8],
            ) -> Result<(), crate::storage::ContentStoreError> {
                Err(crate::storage::ContentStoreError::Remote(
                    "content store offline".into(),
                ))
            }

            async fn get(
                &self,
                _hash: &str,
            ) -> Result<Option<Vec<u8>>, crate::storage::ContentStoreError> {
                Ok(None)
            }
        }

        /// A migrated database that behaves like an open team replica: a team
        /// identity and a content store are together what makes cloud
        /// publication possible at all.
        async fn team_db(store: Arc<dyn ContentStore>) -> LocalDb {
            let mut db = migrated_test_db("sealed-pack.db").await;
            db.set_team_context(
                TeamReplicaContext {
                    team_id: "team-sealed".into(),
                    private_db: None,
                },
                store,
            );
            db
        }

        async fn seed_project(db: &LocalDb, repository_id: Option<&str>) {
            db.execute(
                "INSERT INTO projects
                 (id, workspace_id, name, key, repo_path, repository_id, created_at, updated_at)
                 VALUES (?1, 'default', 'Sealed', 'SP', '', ?2, 1, 1)",
                (PROJECT, repository_id),
            )
            .await
            .unwrap();
        }

        fn commit(repo: &Path, file: &str, contents: &str) -> String {
            std::fs::write(repo.join(file), contents).unwrap();
            git(repo, &["add", "-A"]);
            git(repo, &["commit", "-q", "-m", file]);
            git_stdout(repo, &["rev-parse", "HEAD"])
        }

        /// The catalog entry a sealed commit owns: its bytes, its kind, and the
        /// base it is expressed against.
        async fn sealed_entry(db: &LocalDb, tip: &str) -> Option<(String, String, Option<String>)> {
            let tip = tip.to_owned();
            db.query_opt(
                "SELECT c.content_hash, c.kind, c.base_commit FROM pack_catalog c
                 JOIN pack_catalog_references r
                   ON r.content_hash = c.content_hash
                  AND r.project_id = c.project_id
                  AND r.repository_id = c.repository_id
                 WHERE c.project_id = ?1 AND c.repository_id = ?2 AND c.tip_commit = ?3
                   AND c.publication_state = 'published'
                   AND r.owner_kind = 'sealed_commit' AND r.owner_id = ?3",
                (PROJECT, REPOSITORY, tip),
                |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
            )
            .await
            .unwrap()
        }

        /// Walk the published catalog exactly as a cold executor would, into an
        /// object database that starts empty, and prove the sealed commit's
        /// closure is complete once the chain is installed.
        async fn reconstruct(
            db: &LocalDb,
            store: &InMemoryContentStore,
            tip: &str,
            objects: &Path,
        ) -> Result<(), String> {
            let chain = resolve_catalog_chain(db, PROJECT, REPOSITORY, tip, &[])
                .await
                .unwrap();
            assert!(!chain.is_empty(), "a sealed commit must be resolvable");
            std::fs::create_dir_all(objects).unwrap();
            for (hash, byte_count, checksum, _base, _tip) in &chain {
                let framed = store
                    .get(hash)
                    .await
                    .unwrap()
                    .expect("a published reference must never outrun its bytes");
                assert_eq!(framed.len() as u64, *byte_count);
                let (pack, index) = cairn_codec::transfer::unframe_pack(&framed).unwrap();
                let validated = cairn_codec::transfer::validate_pack(
                    &pack,
                    cairn_codec::transfer::PackLimits::default(),
                    &std::env::temp_dir(),
                )
                .unwrap();
                assert_eq!(validated.index, index);
                assert_eq!(&validated.manifest.pack_checksum, checksum);
                cairn_codec::transfer::install_pack(objects, &validated).unwrap();
            }
            cairn_codec::transfer::verify_commit_closure(
                objects,
                &[],
                &[],
                tip,
                &std::env::temp_dir(),
            )
            .map(|_| ())
        }

        /// The first cloud-visible seal is self-contained: nothing is cataloged
        /// beneath it, so the pack must carry the whole reachable closure.
        #[tokio::test(flavor = "current_thread")]
        async fn a_first_seal_publishes_a_self_contained_reachable_pack() {
            let repo = TempDir::new().unwrap();
            init_project(repo.path());
            let sealed = commit(repo.path(), "first.rs", "first\n");
            let store = Arc::new(InMemoryContentStore::new());
            let db = team_db(store.clone()).await;
            seed_project(&db, Some(REPOSITORY)).await;

            publish_sealed_commit_pack(&db, PROJECT, repo.path(), &sealed)
                .await
                .unwrap();

            let (hash, kind, base) = sealed_entry(&db, &sealed).await.expect("catalog entry");
            assert_eq!(kind, "reachable");
            assert_eq!(base, None);
            assert!(store.contains(&hash).await);

            let cold = TempDir::new().unwrap();
            reconstruct(&db, &store, &sealed, &cold.path().join("objects"))
                .await
                .expect("a first seal must stand alone");
        }

        /// Once the direct parent is covered, a child seal ships only the range
        /// above it — complete on its own terms (non-thin, so it validates
        /// against an empty object database) but deliberately not self-
        /// sufficient, which is what makes the catalog chain load-bearing.
        #[tokio::test(flavor = "current_thread")]
        async fn a_covered_child_publishes_a_range_that_needs_its_parent() {
            let repo = TempDir::new().unwrap();
            init_project(repo.path());
            let parent = commit(repo.path(), "first.rs", "first\n");
            let store = Arc::new(InMemoryContentStore::new());
            let db = team_db(store.clone()).await;
            seed_project(&db, Some(REPOSITORY)).await;
            publish_sealed_commit_pack(&db, PROJECT, repo.path(), &parent)
                .await
                .unwrap();
            let child = commit(repo.path(), "second.rs", "second\n");

            publish_sealed_commit_pack(&db, PROJECT, repo.path(), &child)
                .await
                .unwrap();

            let (child_hash, kind, base) = sealed_entry(&db, &child).await.expect("catalog entry");
            assert_eq!(kind, "execution_range");
            assert_eq!(base.as_deref(), Some(parent.as_str()));
            let (parent_hash, _, _) = sealed_entry(&db, &parent).await.expect("catalog entry");
            assert_ne!(child_hash, parent_hash);

            // The range alone is a valid pack and an incomplete history.
            let partial = TempDir::new().unwrap();
            let partial_objects = partial.path().join("objects");
            std::fs::create_dir_all(&partial_objects).unwrap();
            let framed = store.get(&child_hash).await.unwrap().unwrap();
            let (pack, _) = cairn_codec::transfer::unframe_pack(&framed).unwrap();
            let validated = cairn_codec::transfer::validate_pack(
                &pack,
                cairn_codec::transfer::PackLimits::default(),
                &std::env::temp_dir(),
            )
            .unwrap();
            cairn_codec::transfer::install_pack(&partial_objects, &validated).unwrap();
            assert!(
                cairn_codec::transfer::verify_commit_closure(
                    &partial_objects,
                    &[],
                    &[],
                    &child,
                    &std::env::temp_dir()
                )
                .is_err(),
                "a range pack must not masquerade as a complete history"
            );

            let cold = TempDir::new().unwrap();
            reconstruct(&db, &store, &child, &cold.path().join("objects"))
                .await
                .expect("the resolved chain must reconstruct a complete closure");
        }

        /// Bytes come first. A store that cannot accept them publishes no
        /// catalog reference, so the catalog never advertises coverage that
        /// cannot be fetched.
        #[tokio::test(flavor = "current_thread")]
        async fn a_refused_put_publishes_no_catalog_reference() {
            let repo = TempDir::new().unwrap();
            init_project(repo.path());
            let sealed = commit(repo.path(), "first.rs", "first\n");
            let db = team_db(Arc::new(RefusingContentStore)).await;
            seed_project(&db, Some(REPOSITORY)).await;

            let error = publish_sealed_commit_pack(&db, PROJECT, repo.path(), &sealed)
                .await
                .expect_err("a store that refuses bytes must fail loudly");
            assert!(error.contains("content store offline"), "{error}");
            assert!(sealed_entry(&db, &sealed).await.is_none());
            assert!(
                resolve_catalog_chain(&db, PROJECT, REPOSITORY, &sealed, &[])
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        /// Catalog identity is the durable repository id, not the project id. A
        /// project that has none cannot be published under a guessed identity.
        #[tokio::test(flavor = "current_thread")]
        async fn a_project_without_a_durable_repository_identity_refuses_to_publish() {
            let repo = TempDir::new().unwrap();
            init_project(repo.path());
            let sealed = commit(repo.path(), "first.rs", "first\n");
            let store = Arc::new(InMemoryContentStore::new());
            let db = team_db(store.clone()).await;
            seed_project(&db, None).await;

            let error = publish_sealed_commit_pack(&db, PROJECT, repo.path(), &sealed)
                .await
                .expect_err("a missing repository identity must be explicit");
            assert!(error.contains("durable repository identity"), "{error}");
            assert!(store.is_empty().await);
        }

        /// Intentional: with no team there is no cloud to publish to, and none
        /// is needed — direct runner object transfer already materializes this
        /// commit on any executor. Publication is coverage, never a
        /// precondition of a write.
        #[tokio::test(flavor = "current_thread")]
        async fn a_teamless_database_publishes_nothing_and_succeeds() {
            let repo = TempDir::new().unwrap();
            init_project(repo.path());
            let sealed = commit(repo.path(), "first.rs", "first\n");
            let db = migrated_test_db("sealed-pack-local.db").await;
            seed_project(&db, Some(REPOSITORY)).await;

            publish_sealed_commit_pack(&db, PROJECT, repo.path(), &sealed)
                .await
                .unwrap();

            assert!(sealed_entry(&db, &sealed).await.is_none());
        }
    }
    /// On a path that is not a git repo the non-worktree sentinel is fully
    /// inert: snapshot/changed_since report nothing (git can't run), and it
    /// rejects every seal.
    #[test]
    fn non_worktree_vcs_is_inert_on_a_non_git_path() {
        let vcs = NonWorktreeVcs;
        let wt = Path::new("/tmp/not-a-jj-workspace");

        assert_eq!(vcs.snapshot(wt).unwrap().raw(), "");
        assert_eq!(vcs.is_dirty(wt), Ok(false));
        assert_eq!(
            vcs.changed_since(wt, &VcsSnapshot(String::new())),
            Ok(false)
        );
        assert_eq!(
            vcs.seal_all(wt, "work", None).unwrap_err(),
            NON_WORKTREE_SEAL_ERROR
        );
    }

    const OWNED_BRANCH: &str = "agent/cairn-3280-builder-0";
    const PLANTED_BRANCH: &str = "agent/planted-mid-batch";

    /// A workspace of a Cairn project store under `home/jj-stores`, the shape a
    /// legacy agent residence keeps when boot cleanup leaves it on disk.
    struct Residence {
        home: TempDir,
        _proj: TempDir,
        _wts: TempDir,
        ws: std::path::PathBuf,
    }

    impl Residence {
        fn stores_root(&self) -> std::path::PathBuf {
            self.home.path().join("jj-stores")
        }
    }

    fn residence(bin: &str) -> Residence {
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = crate::jj::environment(bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, OWNED_BRANCH, "main", None).unwrap();
        Residence {
            home,
            _proj: proj,
            _wts: wts,
            ws,
        }
    }

    /// A jj repository the user colocated in their own checkout, outside any
    /// Cairn store.
    fn users_own_jj_repository(bin: &str, home: &Path) -> TempDir {
        let repo = TempDir::new().unwrap();
        init_project(repo.path());
        crate::jj::environment(bin, home);
        let cfg = home.join("jj").join("config.toml");
        jj_raw(bin, &cfg, repo.path(), &["git", "init", "--colocate"]);
        repo
    }

    /// Membership of a Cairn store is what makes a checkout Cairn's, so a
    /// residence whose marker was deleted, before or during the batch, is still
    /// one. The comparison is canonical, so a symlinked home does not hide it.
    #[test]
    #[serial_test::serial(jj)]
    fn a_workspace_of_a_cairn_store_is_a_residence_with_or_without_its_marker() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping a_workspace_of_a_cairn_store_is_a_residence: jj not resolvable");
            return;
        };
        let fx = residence(&bin);
        let marked = CairnResidenceVcs::detect(&fx.ws, &fx.stores_root())
            .expect("a marked workspace of a Cairn store is a residence");
        assert_eq!(marked.branch.as_deref(), Some(OWNED_BRANCH));

        std::fs::remove_file(fx.ws.join(".jj").join(crate::jj::BRANCH_MARKER)).unwrap();
        let unmarked = CairnResidenceVcs::detect(&fx.ws, &fx.stores_root()).expect(
            "deleting the marker must not turn a checkout of Cairn's shared store into one an \
             ambient run may seal or revert",
        );
        assert_eq!(unmarked.branch, None);

        #[cfg(unix)]
        {
            let links = TempDir::new().unwrap();
            let home = links.path().join("home");
            std::os::unix::fs::symlink(fx.home.path(), &home).unwrap();
            assert!(
                CairnResidenceVcs::detect(&fx.ws, &home.join("jj-stores")).is_some(),
                "a stores root reached through a symlink is the same root"
            );
        }
    }

    /// The user's own jj repository keeps its behaviour; a Cairn marker inside
    /// one is still treated as Cairn's, so it is refused rather than published.
    #[test]
    #[serial_test::serial(jj)]
    fn a_users_own_jj_repository_is_a_residence_only_when_it_carries_cairns_marker() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping a_users_own_jj_repository_is_a_residence: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let stores_root = home.path().join("jj-stores");
        std::fs::create_dir_all(&stores_root).unwrap();
        let repo = users_own_jj_repository(&bin, home.path());
        assert!(CairnResidenceVcs::detect(repo.path(), &stores_root).is_none());

        crate::jj::write_branch_marker(repo.path(), PLANTED_BRANCH).unwrap();
        assert!(CairnResidenceVcs::detect(repo.path(), &stores_root).is_some());
    }

    /// In the user's own jj repository an ambient commit lands locally.
    #[test]
    #[serial_test::serial(jj)]
    fn a_users_own_jj_repository_commits_locally() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping a_users_own_jj_repository_commits_locally: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let repo = users_own_jj_repository(&bin, home.path());
        let backend = JjBackend::new(crate::jj::environment(&bin, home.path()));

        std::fs::write(repo.path().join("work.rs"), "user work\n").unwrap();
        let sealed = backend.seal_all(repo.path(), "local commit", None).unwrap();
        assert!(!sealed.sha.is_empty());
        assert!(!backend.is_dirty(repo.path()).unwrap());
    }

    /// The residence backend refuses without running jj, so its answers hold on
    /// a path where jj could not run at all.
    #[test]
    fn a_residence_refuses_every_mutation_and_names_its_branch() {
        let vcs = CairnResidenceVcs {
            branch: Some(OWNED_BRANCH.to_string()),
        };
        let wt = Path::new("/tmp/not-a-jj-workspace");
        let before = vcs.snapshot(wt).unwrap();
        assert_eq!(vcs.is_dirty(wt), Ok(true));
        let refusal = vcs.seal_all(wt, "work", None).unwrap_err();
        assert!(is_cairn_residence_refusal(&refusal), "{refusal}");
        assert!(refusal.contains(OWNED_BRANCH), "{refusal}");
        let unobserved = vcs.changed_since(wt, &before).unwrap_err();
        assert!(is_cairn_residence_refusal(&unobserved), "{unobserved}");
        assert!(!is_cairn_residence_refusal(NON_WORKTREE_SEAL_ERROR));
    }

    /// In a real checkout, the sentinel READ-ONLY detects dirt a run left behind
    /// so the barrier can warn, without ever mutating the checkout.
    #[test]
    fn non_worktree_vcs_detects_new_dirt_read_only() {
        let dir = TempDir::new().unwrap();
        let repo = dir.path();
        init_project(repo);
        let vcs = NonWorktreeVcs;

        // Clean entry: nothing changed yet.
        let before = vcs.snapshot(repo).unwrap();
        assert!(!vcs.changed_since(repo, &before).unwrap());

        // A stray write into the live checkout is detected as new dirt.
        std::fs::write(repo.join("stray.txt"), "oops\n").unwrap();
        assert!(
            vcs.changed_since(repo, &before).unwrap(),
            "a new untracked file is new dirt"
        );
        assert!(repo.join("stray.txt").exists());
    }

    /// The user's OWN pre-existing uncommitted work is not attributed to the
    /// batch: only dirt that appeared AFTER the entry snapshot is flagged.
    #[test]
    fn non_worktree_vcs_ignores_preexisting_user_dirt() {
        let dir = TempDir::new().unwrap();
        let repo = dir.path();
        init_project(repo);
        // The user already has uncommitted work before the batch runs.
        std::fs::write(repo.join("mine.txt"), "user work\n").unwrap();

        let vcs = NonWorktreeVcs;
        let before = vcs.snapshot(repo).unwrap();
        // The batch changed nothing new; the user's dirt is not blamed on it.
        assert!(
            !vcs.changed_since(repo, &before).unwrap(),
            "pre-existing user dirt must not be attributed to the batch"
        );
    }

    /// The FakeVcs double returns its programmed `capture_patch`, so barrier
    /// tests can name the files the marker scan reads. Default is `None`,
    /// matching the trait default where a backend produces no patch.
    #[test]
    fn fake_vcs_returns_programmed_capture_patch() {
        let none = FakeVcs::new();
        assert_eq!(
            none.capture_patch(Path::new("/tmp/x")),
            None,
            "default capture is None"
        );
        let some = FakeVcs::new().capture(Some("diff --git a/x b/x\n".to_string()));
        assert_eq!(
            some.capture_patch(Path::new("/tmp/x")).as_deref(),
            Some("diff --git a/x b/x\n")
        );
    }

    /// Run `git rev-parse --show-toplevel` in `cwd` with an optional
    /// `GIT_CEILING_DIRECTORIES`, returning `(success, trimmed_stdout)`. Does NOT
    /// assert success — the whole point is to observe git failing under the
    /// ceiling.
    fn git_toplevel(cwd: &Path, ceiling: Option<&Path>) -> (bool, String) {
        let mut cmd = crate::env::git();
        cmd.args(["rev-parse", "--show-toplevel"]).current_dir(cwd);
        if let Some(ceiling) = ceiling {
            cmd.env("GIT_CEILING_DIRECTORIES", ceiling);
        }
        let out = cmd.output().unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
    }

    /// The load-bearing empirical confirmation of the `GIT_CEILING_DIRECTORIES`
    /// hypothesis, with no orchestrator and no jj binary. Models production
    /// faithfully: an outer git repo (the `~/.cairn` HOME repo), a `worktrees`
    /// dir under it, and a non-colocated jj workspace `ws` (only a `.jj`, no
    /// `.git`) under that. A bare `git` from `ws` walks UP past `worktrees` to
    /// the HOME repo and answers about the wrong repository (the #146/#153 bug).
    /// The ceiling at the workspace's PARENT (`worktrees`) makes git stop at the
    /// boundary and fail loudly instead — and crucially binds even when git runs
    /// from the workspace root itself (cwd == root), which a worktree-root
    /// ceiling would NOT (git ignores a ceiling equal to the cwd).
    #[test]
    fn git_ceiling_directories_stops_upward_repo_resolution() {
        let home = TempDir::new().unwrap();
        init_project(home.path()); // the outer ~/.cairn-style HOME repo (.git)
        let home_top = std::fs::canonicalize(home.path()).unwrap();

        let worktrees = home.path().join("worktrees");
        let ws = worktrees.join("ws");
        std::fs::create_dir_all(ws.join(".jj")).unwrap(); // non-colocated: .jj, no .git
        let sub = ws.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        // Production ceiling = parent of the worktree root (`worktrees`).
        let ceiling = std::fs::canonicalize(&worktrees).unwrap();

        // Bug reproduces: with no ceiling, bare git resolves UP to the HOME repo
        // from both the workspace root and a nested subdir.
        let (ok_root, top_root) = git_toplevel(&ws, None);
        assert!(ok_root, "bare git resolves up to the HOME repo (the bug)");
        assert_eq!(
            std::fs::canonicalize(&top_root).unwrap(),
            home_top,
            "without the ceiling, bare git in the .jj workspace answers about the ~/.cairn HOME repo"
        );
        let (ok_sub, top_sub) = git_toplevel(&sub, None);
        assert!(ok_sub && std::fs::canonicalize(&top_sub).unwrap() == home_top);

        // Fix works: the parent ceiling stops the upward walk, so git fails to
        // find a repo instead of lying about the HOME repo — from the workspace
        // root (the cwd == ceiling-would-fail case) AND a nested subdir.
        let (ok_fixed_root, top_fixed_root) = git_toplevel(&ws, Some(&ceiling));
        assert!(
            !ok_fixed_root,
            "with the parent ceiling, bare git from the worktree root must fail, not resolve up: {top_fixed_root}"
        );
        let (ok_fixed_sub, top_fixed_sub) = git_toplevel(&sub, Some(&ceiling));
        assert!(
            !ok_fixed_sub,
            "the parent ceiling also stops the walk from a subdir the agent cd-ed into: {top_fixed_sub}"
        );
    }

    /// `worktree_shell_vcs_env` is empty for a non-`.jj` cwd (the live checkout,
    /// left untouched) and, for a `.jj` worktree, carries the managed jj env
    /// (`JJ_CONFIG` under the orchestrator config dir) plus
    /// `GIT_CEILING_DIRECTORIES` pinned to the worktree root. Needs an
    /// orchestrator but no jj binary: `shell_env` only ensures the managed config
    /// file exists.
    #[test]
    fn worktree_shell_vcs_env_shape() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let db = rt.block_on(crate::storage::migrated_test_db("vcs_shell_env.db"));
        let temp = TempDir::new().unwrap();
        let config_dir = temp.path().join("config");
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch =
            crate::orchestrator::Orchestrator::builder(db_state, services, config_dir.clone())
                .build();

        // Non-`.jj` cwd: empty, so the live-checkout path is untouched.
        let plain = temp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();
        assert!(
            worktree_shell_vcs_env(&orch, &plain).is_empty(),
            "a non-worktree cwd injects no VCS env"
        );

        // `.jj` worktree: managed JJ_CONFIG + ceiling at the worktree root.
        let ws = temp.path().join("ws");
        std::fs::create_dir_all(ws.join(".jj")).unwrap();
        let env: std::collections::HashMap<String, String> =
            worktree_shell_vcs_env(&orch, &ws).into_iter().collect();
        assert_eq!(
            env.get("GIT_CEILING_DIRECTORIES").map(String::as_str),
            Some(temp.path().to_string_lossy().as_ref()),
            "the ceiling is pinned to the worktree root's parent (a strict ancestor git honors)"
        );
        let jj_config = env.get("JJ_CONFIG").expect("managed JJ_CONFIG injected");
        assert!(
            jj_config.starts_with(config_dir.to_string_lossy().as_ref()),
            "JJ_CONFIG points at the managed config under the orchestrator config dir: {jj_config}"
        );
        assert_eq!(env.get("JJ_EDITOR").map(String::as_str), Some("true"));

        // The universal `<cairn_home>/bin/jj` shim (installed at startup) now
        // provides `update-stale` interception, so the worktree env no longer
        // overrides PATH or re-exports CAIRN_JJ_BIN — the spawn site's own
        // `agent_shell_path()` (which already carries the jj shim) is used as-is.
        assert!(
            !env.contains_key("PATH"),
            "worktree vcs env no longer injects PATH; the startup jj shim covers interception"
        );
        assert!(
            !env.contains_key("CAIRN_JJ_BIN"),
            "worktree vcs env no longer re-exports CAIRN_JJ_BIN"
        );
    }

    // ---- resolve_store_lock: the agent seal/discard serialization seam ----

    use crate::orchestrator::Orchestrator;
    use std::sync::Arc;

    /// Build an Orchestrator rooted at `config_dir` (the dir whose `jj-stores`
    /// subtree `project_store_dir` keys off). No jj binary required: the lock key
    /// is pure path + map logic.
    async fn orch_with_config(db_name: &str, config_dir: std::path::PathBuf) -> Orchestrator {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        let db = crate::storage::migrated_test_db(db_name).await;
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        crate::orchestrator::Orchestrator::builder(db_state, services, config_dir).build()
    }

    #[tokio::test]
    async fn store_lock_timeout_names_current_holder() {
        let root = TempDir::new().unwrap();
        let orch = orch_with_config("store_lock_holder", root.path().to_path_buf()).await;
        let store = root.path().join("store");
        let _holder = orch
            .hold_jj_store_lock_on_another_task(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "sibling reconcile (external advance on main)",
            )
            .await;

        let error = acquire_store_lock(
            &orch,
            Some(&store),
            crate::orchestrator::StoreLockClass::Foreground,
            "test waiter",
            Duration::from_millis(10),
        )
        .await
        .err()
        .expect("waiter times out");

        assert!(
            error.contains("sibling reconcile (external advance on main)"),
            "{error}"
        );
    }

    /// Run a jj command directly with the managed config, asserting success
    /// (`JjEnv::run` is private to the jj module, so vcs tests shell out).
    fn jj_raw(bin: &str, cfg: &Path, cwd: &Path, args: &[&str]) {
        let out = crate::env::command(bin)
            .args(args)
            .current_dir(cwd)
            .env("JJ_CONFIG", cfg)
            .output()
            .unwrap();
        assert!(out.status.success(), "jj {args:?} failed");
    }
}
