//! VCS mutation seam for the commit-barrier and write-commit paths.
//!
//! Every worktree-mutating VCS operation on these paths flows through
//! [`WorktreeVcs`]. An agent worktree is a `.jj` workspace over a shared store,
//! so [`JjBackend`] is the production backend there. A non-worktree cwd — the
//! project's live checkout behind a long-lived triage / read-only agent or other
//! non-worktree run — resolves to the read-only [`NonWorktreeVcs`]
//! sentinel instead: changes can only happen in worktrees, so the barrier is a
//! clean no-op there and never touches (or reverts) the user's checkout.
//!
//! The trait survives the single-backend collapse for one reason: it is the seam
//! that lets the commit barrier (`run_commit_barrier`) — the "a wrong edit breaks
//! every agent" code — be unit-tested deterministically without a VCS binary, via
//! the in-memory `FakeVcs` test double. That keeps always-on coverage of the
//! barrier's commit/restore/no-op control flow even where `jj` is absent.
//!
//! Change-detection lives behind the seam too, not just the mutations: under jj
//! the in-progress change lives in `@`, so dirty/changed detection is jj-aware.
//! See `docs/jj-migration.md`.

use std::path::Path;

use super::git::{CommitResult, GitAuthor};

/// Opaque pre-batch working-copy snapshot, captured before a batch runs so the
/// barrier can attribute new dirt to the call that caused it. Carries the jj `@`
/// change id.
#[derive(Debug, Clone)]
pub struct VcsSnapshot(pub String);

impl VcsSnapshot {
    /// The backend-internal string this snapshot carries.
    pub fn raw(&self) -> &str {
        &self.0
    }
}

/// All worktree-mutating VCS operations on the commit-barrier and write paths.
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
        author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String>;
    /// Seal only the specified paths into one addressable commit.
    fn seal_files(
        &self,
        worktree: &Path,
        files: &[&str],
        msg: &str,
        author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String>;
    /// Discard working-copy changes, returning the worktree to its committed state.
    fn discard(&self, worktree: &Path) -> Result<(), String>;
    /// Clear a STALE working copy by advancing `@` onto the rewritten/advanced
    /// commit (the one jj op staleness does not block). The stale-resilient
    /// `discard` leans on it internally; the write-path recovery calls it
    /// explicitly to re-base the worktree before re-applying a batch.
    fn update_stale(&self, worktree: &Path) -> Result<(), String>;
    /// Pre-flight reconcile of a workspace before a batch runs, so an agent's
    /// tool call never starts against a stale or behind-its-branch-tip working
    /// copy. Called at batch start under the per-store lock. No-op by default
    /// (the non-worktree checkout has no shared store to reconcile). See the
    /// [`JjBackend`] implementation for the two moves it makes.
    fn reconcile_workspace(&self, worktree: &Path) -> Result<(), String> {
        let _ = worktree;
        Ok(())
    }
    /// Capture the working copy's current edits as a unified patch, for the
    /// write-path stale-recovery to persist to scratch before a give-up discard
    /// (so "recoverable" is true from the agent's seat, not just the jj operation
    /// log). `None` when there is nothing to capture or the backend cannot
    /// produce one — the default, since only a real worktree recovers a batch.
    fn capture_patch(&self, worktree: &Path) -> Option<String> {
        let _ = worktree;
        None
    }
    /// Whether this backend can revert the working copy to its committed state.
    ///
    /// True for a real worktree (`discard` rolls `@` back). False for the
    /// project's live checkout, where reverting would destroy the user's own
    /// uncommitted work — so the no-`commit_msg` barrier must WARN about stray
    /// dirt rather than (no-op) "revert" it. See [`NonWorktreeVcs`].
    fn can_revert(&self) -> bool {
        true
    }
}

/// jj backend — seals/discards the workspace `@` over the shared store via
/// `crate::jj`. One addressable commit per tool call; discard is reversible
/// through the operation log; no blocking mid-transition state.
pub struct JjBackend {
    jj: crate::jj::JjEnv,
}

impl JjBackend {
    pub fn new(jj: crate::jj::JjEnv) -> Self {
        Self { jj }
    }

    /// Push the workspace's bookmark to origin after a seal so each `commit_msg`
    /// seal lands on origin. The branch comes from the workspace's marker;
    /// best-effort, so a local or remoteless jj project never fails a seal.
    ///
    /// Before the push, opportunistically HEAL a clean-tip / conflicted-
    /// intermediate branch (see [`Self::heal_conflicted_intermediates`]) so a
    /// coordinator's resolve-and-reseal immediately restores a pushable, mergeable
    /// branch instead of a silently-failing push whose origin head goes stale until
    /// the next base advance.
    fn push_after_seal(&self, worktree: &Path) {
        let Some(branch) = crate::jj::read_branch_marker(worktree) else {
            return;
        };
        self.heal_conflicted_intermediates(worktree, &branch);
        crate::jj::push_to_origin(&self.jj, worktree, &branch);
    }

    /// After a successful seal, collapse a clean-tip / conflicted-intermediate
    /// branch to one clean commit on its base so it is immediately pushable and
    /// mergeable. This closes the between-advances gap that re-wedges an
    /// integration branch: when a base advance bakes conflicts into a branch's
    /// intermediate commits and the agent resolves the markers at the TIP and
    /// re-seals, resealing `@` cannot clear the conflicted ancestors, so
    /// [`crate::jj::push_to_origin`] silently refuses (jj won't push a conflicted
    /// history) and origin's head goes stale until the next reconcile flatten
    /// fires. Running the same guarded flatten the reconcile path uses, here at
    /// reseal time, makes the resolve-and-reseal self-healing.
    ///
    /// Every step is BEST-EFFORT with logs — a heal failure must never fail a good
    /// seal. A `TipConflicted` branch is left untouched (the agent must resolve the
    /// markers; a flatten preserves the tip tree and cannot clear it). The jj ops
    /// run with the worktree as cwd: it is a workspace over the shared store, and
    /// every op is `--ignore-working-copy` and addresses commits by id/revset, so
    /// they mutate the shared graph exactly as the reconcile path's store-cwd ops
    /// do. `advance_workspace_onto` then re-parents this workspace's `@` onto the
    /// flattened commit (via `update-stale`).
    fn heal_conflicted_intermediates(&self, worktree: &Path, branch: &str) {
        let Some((base_branch, _base_rev)) = crate::jj::read_base_marker(worktree) else {
            return;
        };
        let Some(base_commit) = crate::jj::bookmark_commit(&self.jj, worktree, &base_branch) else {
            return;
        };
        // Only a clean tip over conflicted intermediates is flatten-recoverable.
        // Clean (nothing to do), TipConflicted (agent must resolve), and a probe
        // error all fall through untouched.
        if !matches!(
            crate::jj::flatten_state(&self.jj, worktree, &base_commit, branch),
            Ok(crate::jj::FlattenState::IntermediateOnly)
        ) {
            return;
        }
        let desc = crate::jj::branch_description(&self.jj, worktree, branch);
        let message = if desc.is_empty() {
            format!("Flatten {branch} onto base (auto-recovery)")
        } else {
            desc
        };
        match crate::jj::flatten_branch_recovery(&self.jj, worktree, branch, &base_commit, &message)
        {
            Ok(recovered) => {
                if let Err(e) = crate::jj::advance_workspace_onto(
                    &self.jj,
                    worktree,
                    worktree,
                    branch,
                    &recovered.flattened_commit,
                ) {
                    log::warn!(
                        "reseal heal: re-parent workspace {branch} onto flattened tip failed: {e}"
                    );
                }
                log::info!(
                    "reseal heal: flattened {branch} ({} conflicted intermediate(s) collapsed, {} rider(s) re-pointed)",
                    recovered.collapsed_conflicted_commits,
                    recovered.repointed_bookmarks.len(),
                );
            }
            Err(e) => log::warn!(
                "reseal heal: flatten of {branch} refused ({e}); leaving branch for the reconcile/merge-time recovery"
            ),
        }
    }
}

impl WorktreeVcs for JjBackend {
    fn snapshot(&self, worktree: &Path) -> Result<VcsSnapshot, String> {
        Ok(VcsSnapshot(crate::jj::snapshot_change_id(
            &self.jj, worktree,
        )?))
    }

    fn changed_since(&self, worktree: &Path, _before: &VcsSnapshot) -> Result<bool, String> {
        // Each tool call seals or discards `@`, so `@` is empty on entry; any
        // non-empty `@` is therefore new dirt. The `before` id is unused.
        crate::jj::is_working_copy_dirty(&self.jj, worktree)
    }

    fn is_dirty(&self, worktree: &Path) -> Result<bool, String> {
        crate::jj::is_working_copy_dirty(&self.jj, worktree)
    }

    fn seal_all(
        &self,
        worktree: &Path,
        msg: &str,
        author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String> {
        let result = crate::jj::seal(&self.jj, worktree, msg, author)?;
        self.push_after_seal(worktree);
        Ok(result)
    }

    fn seal_files(
        &self,
        worktree: &Path,
        files: &[&str],
        msg: &str,
        author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String> {
        // Path-scope the seal to exactly these paths so unrelated un-sealed dirt
        // in `@` (a prior failed or full-sandbox run's side effects) is NOT
        // folded into this write's commit: a file-scoped write seals only these
        // paths, never the whole working copy. The barrier and full-sandbox path
        // deliberately leave such dirt in `@`, so a later file-scoped write must
        // not claim it.
        let result = crate::jj::seal_paths(&self.jj, worktree, msg, author, files)?;
        self.push_after_seal(worktree);
        Ok(result)
    }

    fn discard(&self, worktree: &Path) -> Result<(), String> {
        crate::jj::discard(&self.jj, worktree)
    }

    fn update_stale(&self, worktree: &Path) -> Result<(), String> {
        crate::jj::update_stale(&self.jj, worktree)
    }

    fn reconcile_workspace(&self, worktree: &Path) -> Result<(), String> {
        // 1. Heal an on-disk STALE working copy. `jj workspace update-stale` is the
        //    one op staleness does not block; on a fresh (non-stale) workspace it
        //    is a fast no-op (exits 0, "not stale"). This is exactly the heal the
        //    incident's agent hand-ran — but here it is serialized on the per-store
        //    lock the caller holds, so it can never race a concurrent rebase.
        crate::jj::update_stale(&self.jj, worktree)?;

        let Some(branch) = crate::jj::read_branch_marker(worktree) else {
            return Ok(());
        };

        // 2. Convert the clean "behind its branch tip" seal refusal into automatic
        //    recovery: when the working copy is CLEAN, the bookmark has advanced
        //    PAST `@` (seal would not fast-forward), and the branch tip carries no
        //    conflict, re-parent `@` onto the workspace's own bookmark tip. A dirty
        //    tree or a conflicted tip is left untouched — seal-time handling and the
        //    conflicted-branch preservation path already own those, and re-parenting
        //    a dirty tree would mint a divergent recovery commit.
        match crate::jj::is_working_copy_dirty(&self.jj, worktree) {
            Ok(false) => {}
            // Dirty (or unprobeable) — leave it to seal-time handling.
            _ => return Ok(()),
        }
        if crate::jj::seal_is_fast_forward(&self.jj, worktree, &branch)? {
            return Ok(()); // Bookmark not ahead of `@`; nothing to re-parent.
        }
        if crate::jj::branch_has_conflict(&self.jj, worktree, &branch).unwrap_or(true) {
            return Ok(()); // Conflicted tip — the preservation path owns it.
        }
        let Some(tip) = crate::jj::bookmark_commit(&self.jj, worktree, &branch) else {
            return Ok(());
        };
        crate::jj::advance_workspace_onto(&self.jj, worktree, worktree, &branch, &tip)?;
        Ok(())
    }

    fn capture_patch(&self, worktree: &Path) -> Option<String> {
        // Best-effort: a diff failure (e.g. jj refusing on a stale copy) yields
        // `None`, so the give-up error simply omits the recovery path.
        crate::jj::working_copy_diff(&self.jj, worktree)
            .ok()
            .filter(|patch| !patch.trim().is_empty())
    }
}

/// Rejection returned when a seal is attempted in a non-worktree cwd. Changes
/// can only happen in a worktree; the project's live checkout is read-only for
/// agents.
pub(crate) const NON_WORKTREE_SEAL_ERROR: &str =
    "Changes can only be made in a worktree. This agent runs on the project's live \
     checkout (no worktree) and cannot commit.";

/// Read-only [`WorktreeVcs`] for a non-jj cwd: the project's live checkout used
/// by long-lived triage / read-only-analysis agents and other no-worktree runs.
/// Changes can only happen in worktrees, so there is nothing here for
/// Cairn to *manage* — but it is NOT inert. `snapshot`/`changed_since` perform a
/// read-only `git status` so the no-`commit_msg` barrier can detect when a run
/// left stray dirt in the live checkout and WARN about it. They never mutate the
/// checkout: `discard` stays a no-op and `can_revert` is false, because the old
/// single-backend resolver returned a `JjBackend` here whose `discard` would
/// `jj restore` in the plain checkout and DESTROY the user's uncommitted work.
/// Detection is read-only; the warning never reverts. See `docs/worktree-fence.md`.
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
        _author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String> {
        Err(NON_WORKTREE_SEAL_ERROR.to_string())
    }

    fn seal_files(
        &self,
        _worktree: &Path,
        _files: &[&str],
        _msg: &str,
        _author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String> {
        Err(NON_WORKTREE_SEAL_ERROR.to_string())
    }

    fn discard(&self, _worktree: &Path) -> Result<(), String> {
        // Never touch the user's live checkout — see the type docs.
        Ok(())
    }

    fn update_stale(&self, _worktree: &Path) -> Result<(), String> {
        // A plain checkout over one git repo is never a stale jj workspace.
        Ok(())
    }

    fn can_revert(&self) -> bool {
        // The live checkout is never reverted (it holds the user's own work);
        // the barrier warns about stray dirt instead.
        false
    }
}

/// Resolve the VCS backend for an agent cwd. A `.jj` workspace resolves to
/// [`JjBackend`] (the only place changes happen); any other cwd — the project's
/// live checkout behind a no-worktree triage / read-only agent —
/// resolves to the read-only [`NonWorktreeVcs`], so the commit barrier never
/// shells jj in (or reverts) a plain checkout.
pub fn resolve_worktree_vcs(
    orch: &crate::orchestrator::Orchestrator,
    worktree: &Path,
) -> Box<dyn WorktreeVcs> {
    if crate::jj::is_jj_dir(worktree) {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        Box::new(JjBackend::new(jj))
    } else {
        Box::new(NonWorktreeVcs)
    }
}

/// Resolve the per-store serialization lock for an agent cwd, keyed identically
/// to base-advance reconcile and merge-fold
/// (`project_store_dir(config_dir, repo_path)`), so the agent seal/discard path
/// serializes on the SAME lock instance those store mutators hold. An agent seal
/// running `jj` ops on a shared store concurrently with a reconcile/fold can fork
/// the operation log and mint divergent conflicted copies; holding this lock
/// across the seal closes that window.
///
/// Returns `None` for a non-worktree cwd (the project's live checkout behind a
/// no-worktree agent never mutates a shared store, so there is nothing to
/// serialize) or when the project store cannot be resolved. The `None` fallback
/// is best-effort by design: the seal still proceeds without the guard, matching
/// today's behavior — every real agent worktree resolves a run + repo_path, so
/// the fallback only fires where there is no shared store to protect.
pub async fn resolve_store_lock(
    orch: &crate::orchestrator::Orchestrator,
    request: &cairn_common::protocol::CallbackRequest,
) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
    let cwd = Path::new(&request.cwd);
    if !crate::jj::is_jj_dir(cwd) {
        return None; // NonWorktreeVcs — never touches a shared store.
    }
    let run = crate::mcp::handlers::run_context::lookup_run(&orch.db.local, request)
        .await
        .ok()?;
    let repo_path =
        crate::mcp::handlers::run_context::project_path(&orch.db.local, &run.project_id)
            .await
            .ok()??;
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    Some(orch.jj_store_lock(&store))
}

/// Env that makes a bare `git`/`jj` shell command run through the run tool
/// behave correctly inside a jj-only agent worktree. Empty for a non-worktree
/// cwd (the project's live checkout, where bare git correctly resolves the
/// checkout), so that path is untouched.
///
/// Two distinct fixes compose here, both scoped to a `.jj` worktree:
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
pub fn worktree_shell_vcs_env(
    orch: &crate::orchestrator::Orchestrator,
    cwd: &Path,
) -> Vec<(String, String)> {
    if !crate::jj::is_jj_dir(cwd) {
        return Vec::new();
    }
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let mut env = jj.shell_env();
    // Parent, not cwd: git ignores a ceiling equal to the cwd (see doc above).
    let ceiling = cwd.parent().unwrap_or(cwd);
    env.push((
        "GIT_CEILING_DIRECTORIES".into(),
        ceiling.to_string_lossy().into_owned(),
    ));
    // Intercept `jj workspace update-stale` from bare `jj` in agent shells: jj's
    // own stale hint names exactly the command that destroyed a workspace in the
    // incident (racing the shared store). Prepend a shim dir to PATH and point its
    // `CAIRN_JJ_BIN` at the real binary managed jj runs, so every other jj command
    // execs the real jj untouched. With `reconcile_workspace` in place the shim
    // should never trigger in practice; it exists so the one command jj advertises
    // can never again race the store from an agent's hands. Unix-only.
    #[cfg(unix)]
    match crate::jj::ensure_jj_shim_dir(&orch.config_dir) {
        Ok(shim_dir) => {
            // Compose on top of the agent shell PATH (which carries the
            // host-owned `cairn` shim dir), NOT bare get_user_path(): this entry
            // overrides the spawn site's own `agent_shell_path()` for a jj
            // worktree (a later env with the same key wins), so it must itself
            // keep the cairn bin dir or `cairn` stops resolving in the primary
            // (worktree) case. Final order: jj shim first (bare-`jj
            // update-stale` interception), then the cairn bin dir, then the
            // user PATH.
            let current_path = crate::env::agent_shell_path();
            let new_path = format!("{}:{}", shim_dir.display(), current_path);
            env.push(("PATH".into(), new_path));
            env.push(("CAIRN_JJ_BIN".into(), jj.binary_path().to_string()));
        }
        Err(e) => log::warn!("jj shim setup failed (bare `jj update-stale` not intercepted): {e}"),
    }
    env
}

/// In-memory [`WorktreeVcs`] double for deterministic, binary-free coverage of
/// the commit barrier's control flow (the "a wrong edit breaks every agent"
/// code). Each query returns a programmed result; the mutation counters let a
/// test assert whether a seal or discard happened. Defined at module scope (not
/// inside `mod tests`) so the barrier tests in other modules can reach it.
#[cfg(test)]
pub(crate) struct FakeVcs {
    dirty: Result<bool, String>,
    changed: Result<bool, String>,
    seal: Result<CommitResult, String>,
    discard_result: Result<(), String>,
    can_revert: bool,
    capture: Option<String>,
    seal_calls: std::sync::atomic::AtomicUsize,
    discard_calls: std::sync::atomic::AtomicUsize,
    update_stale_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FakeVcs {
    pub fn new() -> Self {
        Self {
            dirty: Ok(true),
            changed: Ok(true),
            seal: Ok(CommitResult {
                sha: "abc123".to_string(),
                pr_number: None,
                amend_note: None,
            }),
            discard_result: Ok(()),
            can_revert: true,
            capture: None,
            seal_calls: std::sync::atomic::AtomicUsize::new(0),
            discard_calls: std::sync::atomic::AtomicUsize::new(0),
            update_stale_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn can_revert(mut self, v: bool) -> Self {
        self.can_revert = v;
        self
    }

    pub fn capture(mut self, v: Option<String>) -> Self {
        self.capture = v;
        self
    }

    pub fn dirty(mut self, v: Result<bool, String>) -> Self {
        self.dirty = v;
        self
    }

    pub fn changed(mut self, v: Result<bool, String>) -> Self {
        self.changed = v;
        self
    }

    pub fn seal(mut self, v: Result<CommitResult, String>) -> Self {
        self.seal = v;
        self
    }

    pub fn seals(&self) -> usize {
        self.seal_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn discards(&self) -> usize {
        self.discard_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn update_stales(&self) -> usize {
        self.update_stale_calls
            .load(std::sync::atomic::Ordering::SeqCst)
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
        _author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String> {
        self.seal_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.seal.clone()
    }

    fn seal_files(
        &self,
        _worktree: &Path,
        _files: &[&str],
        _msg: &str,
        _author: Option<&GitAuthor>,
    ) -> Result<CommitResult, String> {
        self.seal_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.seal.clone()
    }

    fn discard(&self, _worktree: &Path) -> Result<(), String> {
        self.discard_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.discard_result.clone()
    }

    fn update_stale(&self, _worktree: &Path) -> Result<(), String> {
        self.update_stale_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn capture_patch(&self, _worktree: &Path) -> Option<String> {
        self.capture.clone()
    }

    fn can_revert(&self) -> bool {
        self.can_revert
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
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
        git(repo, &["config", "user.name", "P"]);
        std::fs::write(repo.join("shared.rs"), "base\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "base"]);
    }

    /// `JjBackend::seal_all` lands one addressable commit AND pushes the
    /// workspace's bookmark to a bare origin — the seam's git-parity push.
    #[test]
    #[serial_test::serial(jj)]
    fn jj_backend_seal_all_lands_commit_and_pushes() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping jj_backend_seal_all_lands_commit_and_pushes: jj not resolvable");
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

        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

        let branch = "agent/CAIRN-7-builder-0";
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        std::fs::write(ws.join("mod.rs"), "code\n").unwrap();

        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()));
        let result = backend.seal_all(&ws, "agent work", None).unwrap();
        assert!(
            !result.sha.is_empty(),
            "seal_all returns the sealed commit id"
        );
        assert!(
            !backend.is_dirty(&ws).unwrap(),
            "@ is empty again after seal_all"
        );

        let refs = git_stdout(
            origin.path(),
            &["for-each-ref", "--format=%(refname)", "refs/heads/"],
        );
        assert!(
            refs.contains(branch),
            "seal_all must push the bookmark {branch} to origin: {refs}"
        );
    }

    /// `seal_files` is path-scoped: a file-scoped write seals only its paths and
    /// leaves unrelated un-sealed `@` dirt (e.g. a prior failed/ungated run's
    /// side effects) in the working copy, never folding the whole working copy
    /// into the commit. This is the regression guard for the "stale dirt folded
    /// into a later write's commit" failure mode.
    #[test]
    #[serial_test::serial(jj)]
    fn jj_backend_seal_files_is_path_scoped() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping jj_backend_seal_files_is_path_scoped: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());

        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let branch = "agent/CAIRN-7-builder-0";
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

        // Stale dirt from an earlier failed/ungated run, plus the file this write
        // actually touches.
        std::fs::write(ws.join("stale.txt"), "scratch\n").unwrap();
        std::fs::write(ws.join("wanted.rs"), "wanted\n").unwrap();

        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()));
        backend
            .seal_files(&ws, &["wanted.rs"], "seal only wanted", None)
            .unwrap();

        // The bug would seal the whole `@` (clean after); the fix leaves the
        // stale change un-sealed in `@`.
        assert!(
            backend.is_dirty(&ws).unwrap(),
            "stale dirt must remain un-sealed in @ after a file-scoped seal"
        );
        assert!(ws.join("stale.txt").exists());

        // The sealed commit contains only wanted.rs, not the stale file.
        let cfg = home.path().join("jj").join("config.toml");
        let out = crate::env::command(&bin)
            .args(["diff", "-r", "@-", "--name-only"])
            .current_dir(&ws)
            .env("JJ_CONFIG", &cfg)
            .output()
            .unwrap();
        let names = String::from_utf8_lossy(&out.stdout);
        assert!(
            names.contains("wanted.rs"),
            "the write's file must be in the sealed commit: {names}"
        );
        assert!(
            !names.contains("stale.txt"),
            "stale dirt must NOT be folded into the write's commit: {names}"
        );
    }

    /// On a path that is not a git repo the non-worktree sentinel is fully
    /// inert: snapshot/changed_since report nothing (git can't run), it rejects
    /// every seal, and — the load-bearing safety property — it NEVER discards
    /// (the old JjBackend-everywhere bug would `jj restore` the user's live
    /// checkout and destroy uncommitted work). `can_revert` is false so the
    /// barrier warns instead of reverting.
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
        assert!(!vcs.can_revert(), "the live checkout is never reverted");
        assert_eq!(
            vcs.seal_all(wt, "work", None).unwrap_err(),
            NON_WORKTREE_SEAL_ERROR
        );
        assert_eq!(
            vcs.seal_files(wt, &["a.rs"], "work", None).unwrap_err(),
            NON_WORKTREE_SEAL_ERROR
        );
        assert_eq!(
            vcs.discard(wt),
            Ok(()),
            "discard must never touch the checkout"
        );
    }

    /// In a real checkout, the sentinel READ-ONLY detects dirt a run left behind
    /// so the barrier can warn — without ever mutating the checkout. A new file
    /// the agent wrote shows up as changed; `discard` leaves it in place.
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

        // Detection is read-only: discard must never delete the user's file.
        vcs.discard(repo).unwrap();
        assert!(
            repo.join("stray.txt").exists(),
            "the live checkout is never reverted"
        );
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

    /// The FakeVcs double returns its programmed `capture_patch`, so the
    /// write-path give-up preservation (Fix B) can be asserted to have captured
    /// the batch's would-be-lost edits. Default is `None`, matching the trait
    /// default where a backend produces no patch.
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

    /// The FakeVcs double counts `update_stale` calls, mirroring seals/discards,
    /// so the stale-recovery path can be asserted to have invoked it.
    #[test]
    fn fake_vcs_counts_update_stale() {
        let vcs = FakeVcs::new();
        assert_eq!(vcs.update_stales(), 0);
        vcs.update_stale(Path::new("/tmp/x")).unwrap();
        vcs.update_stale(Path::new("/tmp/x")).unwrap();
        assert_eq!(vcs.update_stales(), 2);
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

        let path = env.get("PATH").expect("jj shim PATH injected");
        let shim_prefix = format!("{}:", config_dir.join("shims").display());
        assert!(
            path.starts_with(&shim_prefix),
            "the managed jj shim directory stays first on PATH: {path}"
        );
        assert!(
            path.contains("/.bun/bin"),
            "the shim is composed onto env::agent_shell_path(), not the host process PATH: {path}"
        );
        // The composition source is agent_shell_path(), so the host-owned cairn
        // bin dir must survive into the worktree PATH — otherwise this entry
        // (which overrides the spawn site's PATH) would drop `cairn` in the
        // primary worktree case.
        let cairn_bin = crate::env::cairn_bin_dir();
        assert!(
            path.contains(cairn_bin.to_string_lossy().as_ref()),
            "the cairn CLI shim dir is composed into the worktree PATH so `cairn` resolves in agent worktree shells: {path}"
        );
    }

    // ---- resolve_store_lock: the agent seal/discard serialization seam ----

    use crate::orchestrator::Orchestrator;
    use crate::storage::LocalDb;
    use cairn_common::protocol::CallbackRequest;
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

    /// Seed the minimum rows so `lookup_run`/`project_path` resolve a worktree
    /// `cwd` to its project repo_path (mirrors the live worktree-run shape: a
    /// `live` run on a job whose `worktree_path` is the agent cwd).
    async fn seed_worktree_run(
        db: &LocalDb,
        project_id: String,
        repo_path: String,
        worktree: String,
    ) {
        db.write(move |conn| {
            let project_id = project_id.clone();
            let repo_path = repo_path.clone();
            let worktree = worktree.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES (?1, 'default', 'Project', 'PROJ', ?2, 'main', 1, 1)",
                    (project_id.as_str(), repo_path.as_str()),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1', ?1, 1, 'Issue', 'active', 1, 1)",
                    (project_id.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-1', 'recipe-default', 'issue-1', ?1, 'running', 1, 1)",
                    (project_id.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, worktree_path, base_branch, created_at, updated_at)
                     VALUES ('job-1', 'exec-1', 'node', 'issue-1', ?1, 'running', ?2, 'main', 1, 1)",
                    (project_id.as_str(), worktree.as_str()),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
                     VALUES ('run-1', 'issue-1', ?1, 'job-1', 'live', 1, 1)",
                    (project_id.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Provision a config dir, a project repo path, and a `.jj` worktree cwd, plus
    /// the DB rows resolving that cwd to the project. Returns the orchestrator, a
    /// run-tool `CallbackRequest` whose `cwd` is the worktree, the project
    /// repo_path string (for computing the expected store key), and the owning
    /// TempDir (held by the caller so the tree survives the test).
    async fn worktree_run_fixture(
        db_name: &str,
    ) -> (Orchestrator, CallbackRequest, String, TempDir) {
        let root = TempDir::new().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let repo = root.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let ws = root.path().join("ws");
        std::fs::create_dir_all(ws.join(".jj")).unwrap(); // is_jj_dir(cwd) == true
        let repo_path = repo.to_string_lossy().into_owned();
        let ws_path = ws.to_string_lossy().into_owned();

        let orch = orch_with_config(db_name, config_dir).await;
        seed_worktree_run(
            &orch.db.local,
            "proj-1".to_string(),
            repo_path.clone(),
            ws_path.clone(),
        )
        .await;

        let request = CallbackRequest {
            thread_id: None,
            cwd: ws_path,
            run_id: None,
            tool: "run".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        };
        (orch, request, repo_path, root)
    }

    /// Test A — key identity (the load-bearing wiring guarantee). The seal-side
    /// `resolve_store_lock` must return the SAME `Arc<Mutex>` instance that
    /// base-advance reconcile and merge-fold acquire via
    /// `jj_store_lock(project_store_dir(config_dir, repo_path))`. If these keys
    /// ever drift, serialization silently breaks with no error — this is the
    /// regression guard.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_store_lock_matches_reconcile_lock_instance() {
        let (orch, request, repo_path, _root) =
            worktree_run_fixture("vcs_store_lock_key_identity.db").await;

        let seal_lock = crate::mcp::vcs::resolve_store_lock(&orch, &request)
            .await
            .expect("a worktree cwd resolves a store lock");
        // The exact key reconcile/fold derive.
        let reconcile_lock = orch.jj_store_lock(&crate::jj::project_store_dir(
            &orch.config_dir,
            Path::new(&repo_path),
        ));
        assert!(
            Arc::ptr_eq(&seal_lock, &reconcile_lock),
            "seal/discard must serialize on the SAME lock instance as reconcile/fold"
        );
    }

    /// Test B — mutual exclusion through the real lock. With an in-flight
    /// reconcile holding the store lock, a seal-side acquisition via
    /// `resolve_store_lock(...).lock()` must block until the reconcile guard
    /// drops, then proceed. Proves serialization is exercised through the real
    /// `jj_store_lock`, not merely keyed the same.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_store_lock_serializes_behind_in_flight_reconcile() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let (orch, request, repo_path, _root) =
            worktree_run_fixture("vcs_store_lock_mutual_exclusion.db").await;

        // Simulate an in-flight reconcile/fold holding the store lock.
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
        let reconcile_lock = orch.jj_store_lock(&store);
        let reconcile_guard = reconcile_lock.lock().await;

        let acquired = Arc::new(AtomicBool::new(false));
        let seal_side = {
            let orch = orch.clone();
            let request = request.clone();
            let acquired = acquired.clone();
            tokio::spawn(async move {
                let lock = crate::mcp::vcs::resolve_store_lock(&orch, &request)
                    .await
                    .expect("a worktree cwd resolves a store lock");
                let _guard = lock.lock().await;
                acquired.store(true, Ordering::SeqCst);
            })
        };

        // While reconcile holds the lock, the seal side cannot proceed.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !acquired.load(Ordering::SeqCst),
            "seal must block while a reconcile holds the store lock"
        );

        // Releasing the reconcile guard lets the seal side acquire and proceed.
        drop(reconcile_guard);
        seal_side.await.unwrap();
        assert!(
            acquired.load(Ordering::SeqCst),
            "seal proceeds once the reconcile releases the store lock"
        );
    }

    /// Test C — a non-worktree cwd resolves no lock. The project's live checkout
    /// (NonWorktreeVcs) never mutates a shared store, so it must not acquire (or
    /// block on) a store lock. Returns `None` before any DB lookup.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_store_lock_is_none_for_non_worktree_cwd() {
        let root = TempDir::new().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let plain = root.path().join("plain"); // no `.jj` — a live checkout
        std::fs::create_dir_all(&plain).unwrap();

        let orch = orch_with_config("vcs_store_lock_non_worktree.db", config_dir).await;
        let request = CallbackRequest {
            thread_id: None,
            cwd: plain.to_string_lossy().into_owned(),
            run_id: None,
            tool: "run".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        };
        assert!(
            crate::mcp::vcs::resolve_store_lock(&orch, &request)
                .await
                .is_none(),
            "a non-worktree cwd must not resolve a store lock"
        );
    }

    // ---- Component D: reseal-time opportunistic heal in push_after_seal ----

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

    /// Count the commits a range revset resolves to, shelling `jj log` directly
    /// with the managed config (`JjEnv::run` is private to the jj module).
    fn count_commits(bin: &str, cfg: &Path, cwd: &Path, range: &str) -> usize {
        let out = crate::env::command(bin)
            .args([
                "log",
                "-r",
                range,
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ])
            .current_dir(cwd)
            .env("JJ_CONFIG", cfg)
            .output()
            .unwrap();
        assert!(out.status.success(), "jj log range failed");
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count()
    }

    /// A resolve-and-reseal that leaves a CLEAN tip over conflicted INTERMEDIATE
    /// commits is healed at reseal time: `push_after_seal` flattens the branch to
    /// one clean commit on its base, re-parents `@`, and pushes it to origin — so a
    /// coordinator's resolution immediately restores a pushable, mergeable branch
    /// instead of a silently-failing push whose origin head goes stale.
    #[test]
    #[serial_test::serial(jj)]
    fn reseal_heals_conflicted_intermediates_and_pushes() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping reseal_heals_conflicted_intermediates_and_pushes: jj not resolvable"
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
        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2288-coordinator-0";
        crate::jj::add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None)
            .unwrap();
        crate::jj::ensure_bookmark_on_origin(&jj, &store, int).unwrap();

        let builder = "agent/CAIRN-1-builder-0";
        let ws = wts.path().join("builder");
        crate::jj::add_workspace(&jj, &store, &ws, builder, int, None).unwrap();
        std::fs::write(ws.join("shared.rs"), "builder-edit\n").unwrap();
        crate::jj::seal(&jj, &ws, "builder edits shared", None).unwrap();
        crate::jj::ensure_bookmark_on_origin(&jj, &store, builder).unwrap();
        let origin_before = git_stdout(origin.path(), &["rev-parse", builder]);

        // The integration tip advances conflictingly; the builder rebases onto it
        // (recording a conflict on its INTERMEDIATE commit) and resolves at its tip.
        let cfg = home.path().join("jj").join("config.toml");
        jj_raw(&bin, &cfg, &store, &["new", int]);
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj_raw(&bin, &cfg, &store, &["describe", "-m", "int advances"]);
        jj_raw(
            &bin,
            &cfg,
            &store,
            &["bookmark", "set", int, "-r", "@", "--ignore-working-copy"],
        );
        crate::jj::rebase_branch_onto(&jj, &store, builder, int).unwrap();
        assert!(crate::jj::branch_has_conflict(&jj, &store, builder).unwrap());
        crate::jj::update_stale(&jj, &ws).unwrap();
        std::fs::write(ws.join("shared.rs"), "resolved\n").unwrap();
        crate::jj::seal(&jj, &ws, "resolve conflict", None).unwrap();
        assert!(!crate::jj::branch_has_conflict(&jj, &store, builder).unwrap());

        // Record the base marker (the integration branch) so the heal can find its
        // flatten dest, and confirm the pre-heal shape.
        let int_tip = crate::jj::bookmark_commit(&jj, &store, int).unwrap();
        crate::jj::write_base_marker(&ws, int, &int_tip).unwrap();
        assert_eq!(
            crate::jj::flatten_state(&jj, &store, &int_tip, builder).unwrap(),
            crate::jj::FlattenState::IntermediateOnly
        );
        // Before the heal, jj refuses to push the conflicted-ancestor branch.
        assert!(
            crate::jj::push_store_bookmark(&jj, &store, builder).is_err(),
            "the wedged branch is unpushable before the heal"
        );

        // The reseal heal flattens, re-parents `@`, and pushes.
        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()));
        backend.push_after_seal(&ws);

        assert!(!crate::jj::branch_has_conflict(&jj, &store, builder).unwrap());
        let range = format!("{int_tip}..bookmarks(exact:{builder:?})");
        assert_eq!(
            count_commits(&bin, &cfg, &ws, &range),
            1,
            "the branch is flattened to one commit on its base"
        );
        assert!(
            crate::jj::conflicted_commits(&jj, &ws, &range).is_empty(),
            "no conflicted commit survives the reseal heal"
        );
        let origin_after = git_stdout(origin.path(), &["rev-parse", builder]);
        assert_ne!(
            origin_before, origin_after,
            "the healed branch's head advanced on origin"
        );
    }

    /// A clean reseal takes NO extra rewrite: with no conflicted intermediate, the
    /// heal is a no-op and the branch tip is unchanged (only the ordinary push
    /// runs).
    #[test]
    #[serial_test::serial(jj)]
    fn clean_reseal_takes_no_extra_rewrite() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping clean_reseal_takes_no_extra_rewrite: jj not resolvable");
            return;
        };
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

        let int = "agent/CAIRN-2288-coordinator-0";
        crate::jj::add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None)
            .unwrap();
        let builder = "agent/CAIRN-1-builder-0";
        let ws = wts.path().join("builder");
        crate::jj::add_workspace(&jj, &store, &ws, builder, int, None).unwrap();
        std::fs::write(ws.join("clean.rs"), "clean\n").unwrap();
        crate::jj::seal(&jj, &ws, "clean builder work", None).unwrap();

        let int_tip = crate::jj::bookmark_commit(&jj, &store, int).unwrap();
        crate::jj::write_base_marker(&ws, int, &int_tip).unwrap();
        assert_eq!(
            crate::jj::flatten_state(&jj, &store, &int_tip, builder).unwrap(),
            crate::jj::FlattenState::Clean
        );

        let tip_before = crate::jj::bookmark_commit(&jj, &store, builder).unwrap();
        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()));
        backend.push_after_seal(&ws);
        let tip_after = crate::jj::bookmark_commit(&jj, &store, builder).unwrap();
        assert_eq!(
            tip_before, tip_after,
            "a clean seal is not rewritten by the reseal heal"
        );
    }
}
