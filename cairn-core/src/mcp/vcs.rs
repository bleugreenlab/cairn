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

use super::git::{CommitResult, GitAuthor};

/// How long a file-target write waits for the project store lock. It is the
/// shared constant rather than a mirror of one: the wait here and the socket
/// ceiling `cairn-cmd` sizes above it are the same fact, and a mirror is a place
/// for them to drift.
pub(crate) const STORE_LOCK_TIMEOUT: Duration =
    Duration::from_millis(cairn_common::write_contract::WRITE_STORE_LOCK_WAIT_MS);

pub(crate) async fn acquire_store_lock(
    orch: &crate::orchestrator::Orchestrator,
    store: Option<&Path>,
    operation: &str,
    timeout: Duration,
) -> Result<Option<crate::orchestrator::JjStoreGuard>, String> {
    let Some(store) = store else { return Ok(None) };
    orch.acquire_jj_store_lock_with_timeout(store, operation, Some(timeout))
        .await
        .map(Some)
        .map_err(|()| {
            let holder = orch
                .jj_store_lock_holder(store)
                .unwrap_or_else(|| "another version-control operation".to_string());
            format!("The project's version-control store is busy behind: {holder}; retry this operation.")
        })
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
    /// Capture the working copy's current edits as a unified patch, for the
    /// write-path stale-recovery to persist to scratch before a give-up discard
    /// (so "recoverable" is true from the agent's seat, not just the jj operation
    /// log). `None` when there is nothing to capture or the backend cannot
    /// produce one — the default, since only a real worktree recovers a batch.
    fn capture_patch(&self, worktree: &Path) -> Option<String> {
        let _ = worktree;
        None
    }
    /// Whether Cairn may revert this checkout to its committed state.
    ///
    /// The question is OWNERSHIP, not capability: `discard` is a destructive
    /// publication — jj's `restore` resets `@` to its parent and takes every
    /// uncommitted change with it, not merely the ones a batch just wrote — so it
    /// is only ever Cairn's to perform in a checkout Cairn provisioned. In
    /// somebody else's checkout the barrier must WARN about stray dirt and leave
    /// it in place.
    ///
    /// The answer takes NO arguments on purpose. The barrier consults it only
    /// after the batch has run, and the evidence of ownership lives in a file
    /// inside the very checkout the batch can write, so an implementation that
    /// re-derived the answer at that point would let a batch decide whether Cairn
    /// may destroy the surrounding uncommitted work. Every implementation must
    /// therefore fix its answer BEFORE the batch executes — [`JjBackend`] captures
    /// it when the backend is constructed — and this signature is what makes that
    /// the only expressible shape.
    fn can_revert(&self) -> bool {
        true
    }
}

/// jj backend — seals/discards the workspace `@` over the shared store via
/// `crate::jj`. One addressable commit per tool call; discard is reversible
/// through the operation log; no blocking mid-transition state.
pub struct JjBackend {
    jj: crate::jj::JjEnv,
    /// The branch Cairn owned in this checkout AT CONSTRUCTION — which is before
    /// the batch runs, since the backend is resolved during request preflight.
    ///
    /// Captured rather than read on demand because the marker lives in the
    /// checkout the batch can write. Re-reading it at cleanup time would hand a
    /// batch the power to turn Cairn's destructive rollback ON by planting
    /// `.jj/cairn-branch` (deliberately, or just by copying workspace metadata
    /// around), and to turn a legitimate rollback OFF by deleting it. Neither
    /// direction is the batch's to decide, so the decision predates it.
    owned_branch: Option<String>,
}

impl JjBackend {
    /// Resolve the backend for `worktree`, fixing the ownership answer now.
    pub(crate) fn new(jj: crate::jj::JjEnv, worktree: &Path) -> Self {
        Self {
            owned_branch: crate::jj::read_branch_marker(worktree),
            jj,
        }
    }

    /// Push the workspace's bookmark to origin after a seal so each `commit_msg`
    /// seal lands on origin. Gated on the ownership captured at construction, so a
    /// jj checkout Cairn did not provision is never healed or published into — and
    /// a marker the batch itself wrote cannot nominate a branch for Cairn to heal.
    /// Best-effort beyond that gate, so a local or remoteless jj project never
    /// fails a seal.
    ///
    /// Before the push, opportunistically HEAL a clean-tip / conflicted-
    /// intermediate branch (see [`Self::heal_conflicted_intermediates`]) so a
    /// coordinator's resolve-and-reseal immediately restores a pushable, mergeable
    /// branch instead of a silently-failing push whose origin head goes stale until
    /// the next base advance.
    fn heal_after_seal(&self, worktree: &Path) {
        let Some(branch) = self.owned_branch.clone() else {
            return;
        };
        self.heal_conflicted_intermediates(worktree, &branch);
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
        // The branch comes from the preflight capture, never from a fresh marker
        // read: a batch that deleted the marker would otherwise get a locally
        // committed, unpublished seal reported to it as a successful commit on its
        // branch, and one that planted a marker would opt an unowned checkout into
        // Cairn publication.
        let result = crate::jj::seal_paths(
            &self.jj,
            worktree,
            msg,
            author,
            &[],
            self.owned_branch.as_deref(),
        )?;
        self.heal_after_seal(worktree);
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
        let result = crate::jj::seal_paths(
            &self.jj,
            worktree,
            msg,
            author,
            files,
            self.owned_branch.as_deref(),
        )?;
        self.heal_after_seal(worktree);
        Ok(result)
    }

    fn discard(&self, worktree: &Path) -> Result<(), String> {
        crate::jj::discard(&self.jj, worktree)
    }

    fn update_stale(&self, worktree: &Path) -> Result<(), String> {
        crate::jj::update_stale(&self.jj, worktree)
    }

    fn capture_patch(&self, worktree: &Path) -> Option<String> {
        // Best-effort: a diff failure (e.g. jj refusing on a stale copy) yields
        // `None`, so the give-up error simply omits the recovery path.
        crate::jj::working_copy_diff(&self.jj, worktree)
            .ok()
            .filter(|patch| !patch.trim().is_empty())
    }

    fn can_revert(&self) -> bool {
        // A `.jj` directory is not a claim of ownership. A user who colocated
        // their own jj repo, and then ran an ambient agent whose cwd is it,
        // reaches this backend — and `discard` there would `jj restore` away every
        // uncommitted change they had, which is the very hazard [`NonWorktreeVcs`]
        // was introduced to close for plain git. The branch marker is the
        // predicate that separates the two: Cairn wrote it when it provisioned the
        // workspace, so its presence means Cairn owns a branch here and the
        // rollback is Cairn's to perform. Read at construction, never here — see
        // [`Self::owned_branch`].
        self.owned_branch.is_some()
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
/// left stray dirt in the live checkout and WARN about it. They never mutate the
/// checkout: `discard` stays a no-op and `can_revert` is false, because the old
/// single-backend resolver returned a `JjBackend` here whose `discard` would
/// `jj restore` in the plain checkout and DESTROY the user's uncommitted work.
/// Detection is read-only; the warning never reverts. See `docs/worktree-fence.md`.
///
/// This type closes that hazard only for a cwd with no `.jj`. The same hazard in
/// a jj repo the USER colocated is closed one layer up, by
/// [`JjBackend`] capturing the branch marker when it is constructed — selecting a
/// backend on `.jj` presence cannot tell a checkout Cairn provisioned from one it
/// merely found.
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
pub(crate) fn resolve_worktree_vcs(
    orch: &crate::orchestrator::Orchestrator,
    worktree: &Path,
) -> Box<dyn WorktreeVcs> {
    if crate::jj::is_jj_dir(worktree) {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        Box::new(JjBackend::new(jj, worktree))
    } else {
        Box::new(NonWorktreeVcs)
    }
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
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
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

/// In-memory [`WorktreeVcs`] double for deterministic, binary-free coverage of
/// the commit barrier's control flow (the "a wrong edit breaks every agent"
/// code). Each query returns a programmed result; the mutation counters let a
/// test assert whether a seal or discard happened. Defined at module scope (not
/// inside `mod tests`) so the barrier tests in other modules can reach it.
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
    let Some(store) = db.team_id().map(|_| db.content_store().clone()) else {
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

    let tip = git_stdout(
        repository,
        &["rev-parse", &format!("{sealed_commit}^{{commit}}")],
    )?;
    let parent = git_stdout(repository, &["rev-parse", &format!("{tip}^")]).ok();
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

#[cfg(test)]
pub(crate) struct FakeVcs {
    dirty: Result<bool, String>,
    changed: Result<bool, String>,
    seal: Result<CommitResult, String>,
    discard_result: Result<(), String>,
    discard_results: std::sync::Mutex<std::collections::VecDeque<Result<(), String>>>,
    can_revert: bool,
    capture: Option<String>,
    seal_calls: std::sync::atomic::AtomicUsize,
    discard_calls: std::sync::atomic::AtomicUsize,
    update_stale_calls: std::sync::atomic::AtomicUsize,
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
            discard_result: Ok(()),
            discard_results: std::sync::Mutex::new(std::collections::VecDeque::new()),
            can_revert: true,
            capture: None,
            seal_calls: std::sync::atomic::AtomicUsize::new(0),
            discard_calls: std::sync::atomic::AtomicUsize::new(0),
            update_stale_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn can_revert(mut self, v: bool) -> Self {
        self.can_revert = v;
        self
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

    pub(crate) fn discards(&self) -> usize {
        self.discard_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn update_stales(&self) -> usize {
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
        self.discard_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.discard_result.clone())
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
        git(repo, &["config", "user.name", "P"]);
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
            async fn put(&self, _hash: &str, _bytes: &[u8]) -> Result<(), String> {
                Err("content store offline".into())
            }

            async fn get(&self, _hash: &str) -> Result<Option<Vec<u8>>, String> {
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
                )
                .unwrap();
                assert_eq!(validated.index, index);
                assert_eq!(&validated.manifest.pack_checksum, checksum);
                cairn_codec::transfer::install_pack(objects, &validated).unwrap();
            }
            cairn_codec::transfer::verify_commit_closure(objects, &[], tip).map(|_| ())
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
            )
            .unwrap();
            cairn_codec::transfer::install_pack(&partial_objects, &validated).unwrap();
            assert!(
                cairn_codec::transfer::verify_commit_closure(&partial_objects, &[], &child)
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
    /// `JjBackend::seal_all` lands one addressable commit locally and publishes
    /// nothing; the store-addressed publication that runs after the caller
    /// releases the project-store lock is what reaches origin.
    #[test]
    #[serial_test::serial(jj)]
    fn jj_backend_seal_all_lands_commit_without_publishing_it() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping jj_backend_seal_all_lands_commit_without_publishing: jj not resolvable"
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

        let branch = "agent/CAIRN-7-builder-0";
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        std::fs::write(ws.join("mod.rs"), "code\n").unwrap();

        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()), &ws);
        let result = backend.seal_all(&ws, "agent work", None).unwrap();
        let refs_before_propagation = git_stdout(
            origin.path(),
            &["for-each-ref", "--format=%(refname)", "refs/heads/"],
        );
        assert!(
            !refs_before_propagation.contains(branch),
            "local sealing must not publish {branch} before the post-lock propagation seam"
        );
        crate::jj::push_store_bookmark(&jj, &store, branch).unwrap();
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
            "post-lock propagation must push the bookmark {branch} to origin: {refs}"
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

        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()), &ws);
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

    /// The branch [`owned_workspace`] provisions, and a name no fixture creates — so
    /// finding a bookmark or ref under it can only mean a planted marker was
    /// honoured.
    const OWNED_BRANCH: &str = "agent/CAIRN-3280-builder-0";
    const PLANTED_BRANCH: &str = "agent/planted-mid-batch";

    /// A jj workspace Cairn provisioned, with its ownership marker in place.
    struct OwnedWorkspace {
        home: TempDir,
        proj: TempDir,
        _wts: TempDir,
        ws: std::path::PathBuf,
        store: std::path::PathBuf,
    }

    impl OwnedWorkspace {
        fn marker(&self) -> std::path::PathBuf {
            self.ws.join(".jj").join(crate::jj::BRANCH_MARKER)
        }

        fn jj(&self, bin: &str) -> crate::jj::JjEnv {
            crate::jj::JjEnv::resolve(bin, self.home.path())
        }

        /// The backend as request preflight would build it, from the marker state
        /// on disk right now.
        fn preflight_backend(&self, bin: &str) -> JjBackend {
            JjBackend::new(self.jj(bin), &self.ws)
        }

        fn bookmark(&self, bin: &str, branch: &str) -> Option<String> {
            crate::jj::bookmark_commit(&self.jj(bin), &self.store, branch)
        }

        /// The backing checkout's `refs/heads/<branch>`, or `None` when absent.
        fn git_ref(&self, branch: &str) -> Option<String> {
            let out = crate::env::git()
                .args(["rev-parse", "--verify", &format!("refs/heads/{branch}")])
                .current_dir(self.proj.path())
                .output()
                .unwrap();
            out.status
                .success()
                .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        }
    }

    fn owned_workspace(bin: &str) -> OwnedWorkspace {
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = crate::jj::JjEnv::resolve(bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, OWNED_BRANCH, "main", None).unwrap();
        OwnedWorkspace {
            home,
            proj,
            _wts: wts,
            ws,
            store,
        }
    }

    /// `JjBackend::can_revert` answers from OWNERSHIP, not from the presence of a
    /// `.jj` directory.
    ///
    /// A revert is a destructive publication: `jj restore` resets `@` to its
    /// parent and takes every uncommitted change with it, not merely the ones a
    /// batch just wrote. In a jj repo the USER colocated, that is precisely the
    /// work-destroying hazard [`NonWorktreeVcs`] was introduced to close for plain
    /// git — and backend selection, which keys on `.jj` presence, cannot tell that
    /// checkout from one Cairn provisioned. The branch marker can.
    #[test]
    #[serial_test::serial(jj)]
    fn a_jj_checkout_cairn_does_not_own_is_never_reverted() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping jj_checkout_cairn_does_not_own_is_never_reverted: jj not resolvable"
            );
            return;
        };
        let fx = owned_workspace(&bin);

        assert!(
            fx.preflight_backend(&bin).can_revert(),
            "Cairn provisioned this workspace, so rolling it back is Cairn's to do"
        );

        // The same directory, minus the one file that says Cairn owns a branch
        // here: the shape of a checkout Cairn merely found.
        std::fs::remove_file(fx.marker()).unwrap();
        assert!(
            !fx.preflight_backend(&bin).can_revert(),
            "an unowned jj checkout must never be reverted — the discard would take the \
             user's own uncommitted work with it"
        );
    }

    /// THE TIME-OF-CHECK PROPERTY. The ownership evidence is a file inside the
    /// checkout the batch can write, and the barrier consults `can_revert` only
    /// AFTER the batch has run. So the answer is fixed when the backend is
    /// constructed — during request preflight, before any command executes — and
    /// nothing the batch does afterwards moves it.
    ///
    /// Both directions matter, and they fail differently:
    ///
    /// - Planting `.jj/cairn-branch` mid-batch must not turn the rollback ON. That
    ///   is the destructive direction: in a user-colocated checkout it would hand
    ///   Cairn a `jj restore` over the user's own pre-existing uncommitted work. A
    ///   batch need not be malicious to do it — copying workspace metadata around
    ///   is enough.
    /// - Deleting it mid-batch must not turn a legitimate rollback OFF, or a batch
    ///   could opt its own dirt out of the no-`commit_msg` hygiene gate and have it
    ///   persist.
    #[test]
    #[serial_test::serial(jj)]
    fn ownership_is_fixed_before_the_batch_and_a_batch_cannot_move_it() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping ownership_is_fixed_before_the_batch: jj not resolvable");
            return;
        };
        let fx = owned_workspace(&bin);

        // A checkout Cairn does not own, as the preflight sees it.
        std::fs::remove_file(fx.marker()).unwrap();
        let backend = fx.preflight_backend(&bin);
        assert!(!backend.can_revert());

        // ... and now the batch writes the marker, exactly as a stray copy of
        // workspace metadata would.
        crate::jj::write_branch_marker(&fx.ws, PLANTED_BRANCH).unwrap();
        assert!(
            !backend.can_revert(),
            "a marker written DURING the batch must not authorize Cairn to revert the \
             checkout: the barrier consults this after the batch, so re-reading the file \
             here would let a batch destroy the user's uncommitted work"
        );

        // The converse: ownership established before the batch survives the batch
        // deleting the evidence, so no batch can opt its dirt out of the gate.
        let owned = fx.preflight_backend(&bin);
        assert!(owned.can_revert());
        std::fs::remove_file(fx.marker()).unwrap();
        assert!(
            owned.can_revert(),
            "a marker deleted DURING the batch must not disable a legitimate rollback"
        );
    }

    /// The same time-of-check property on the PUBLICATION side, proven by a real
    /// seal rather than by the predicate alone.
    ///
    /// A batch that deletes the marker must not be able to silence its own
    /// publication. If the seal re-read the marker it would find `None`, commit
    /// locally, skip the bookmark advance and the export, and still return a
    /// `CommitResult` — so the barrier would print `✓ Committed changes (sha)`
    /// for a commit the branch never received. That is precisely the silent
    /// unpublished-commit failure this whole change exists to eliminate, so it must
    /// not be reachable by writing a file.
    #[test]
    #[serial_test::serial(jj)]
    fn a_marker_deleted_during_a_batch_still_publishes_to_the_captured_branch() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping marker_deleted_during_a_batch_still_publishes: jj not resolvable");
            return;
        };
        let fx = owned_workspace(&bin);
        let backend = fx.preflight_backend(&bin);
        let before = fx.bookmark(&bin, OWNED_BRANCH).unwrap();

        // The batch removes the ownership evidence, then leaves work to seal.
        std::fs::remove_file(fx.marker()).unwrap();
        assert!(crate::jj::read_branch_marker(&fx.ws).is_none());
        std::fs::write(fx.ws.join("work.rs"), "agent work\n").unwrap();

        backend.seal_all(&fx.ws, "agent work", None).unwrap();

        let sealed = crate::jj::head_commit(&fx.jj(&bin), &fx.ws).unwrap();
        assert_ne!(sealed, before, "the seal produced a new commit");
        assert_eq!(
            fx.bookmark(&bin, OWNED_BRANCH).as_deref(),
            Some(sealed.as_str()),
            "the seal must advance the branch captured at preflight; reporting a commit the \
             branch never received is the exact failure this change removes"
        );
        assert_eq!(
            fx.git_ref(OWNED_BRANCH).as_deref(),
            Some(sealed.as_str()),
            "and the export must reach the backing git ref, which is what everything \
             outside jj reads"
        );
    }

    /// The inverse, also by a real seal: a batch cannot opt an unowned checkout
    /// INTO Cairn publication by planting a marker. The commit still lands locally
    /// — withholding is not refusing — but no bookmark moves and no git ref appears,
    /// under the planted name or the workspace's real branch.
    #[test]
    #[serial_test::serial(jj)]
    fn a_marker_planted_during_a_batch_publishes_nothing() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping marker_planted_during_a_batch_publishes_nothing: jj not resolvable"
            );
            return;
        };
        let fx = owned_workspace(&bin);

        // Unowned as of preflight.
        std::fs::remove_file(fx.marker()).unwrap();
        let backend = fx.preflight_backend(&bin);
        let real_before = fx.bookmark(&bin, OWNED_BRANCH).unwrap();

        // The batch plants a marker naming a branch of its choosing.
        crate::jj::write_branch_marker(&fx.ws, PLANTED_BRANCH).unwrap();
        std::fs::write(fx.ws.join("work.rs"), "somebody else's work\n").unwrap();

        let sealed = backend.seal_all(&fx.ws, "local commit", None).unwrap();

        assert!(
            !sealed.sha.is_empty(),
            "the commit still lands locally — an unowned checkout withholds publication, \
             it does not refuse the commit"
        );
        assert_eq!(
            fx.bookmark(&bin, OWNED_BRANCH).as_deref(),
            Some(real_before.as_str()),
            "the workspace's real branch must not move in a checkout Cairn does not own"
        );
        assert_eq!(
            fx.bookmark(&bin, PLANTED_BRANCH),
            None,
            "a branch named by a marker the batch itself wrote must never be created"
        );
        assert_eq!(fx.git_ref(PLANTED_BRANCH), None, "and never exported");
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
            .acquire_jj_store_lock(&store, "sibling reconcile (external advance on main)")
            .await;

        let error = acquire_store_lock(
            &orch,
            Some(&store),
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
        crate::jj::seal_paths(&jj, &ws, "builder edits shared", None, &[], Some(builder)).unwrap();
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
        // Unguarded on purpose: this fixture needs the conflicted shape that
        // `rebase_branch_onto` now rolls back rather than leaves behind.
        crate::jj::tests::rebase_recording_conflict(&jj, &store, builder, int);
        assert!(crate::jj::branch_has_conflict(&jj, &store, builder).unwrap());
        crate::jj::update_stale(&jj, &ws).unwrap();
        std::fs::write(ws.join("shared.rs"), "resolved\n").unwrap();
        crate::jj::seal_paths(&jj, &ws, "resolve conflict", None, &[], Some(builder)).unwrap();
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

        // The local reseal heal flattens and re-parents `@`; propagation runs
        // separately after the caller releases the project-store lock.
        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()), &ws);
        backend.heal_after_seal(&ws);
        let origin_after_local_heal = git_stdout(origin.path(), &["rev-parse", builder]);
        assert_eq!(
            origin_before, origin_after_local_heal,
            "local reseal healing must not publish before the post-lock propagation seam"
        );
        crate::jj::push_store_bookmark(&jj, &store, builder).unwrap();

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
        crate::jj::seal_paths(&jj, &ws, "clean builder work", None, &[], Some(builder)).unwrap();

        let int_tip = crate::jj::bookmark_commit(&jj, &store, int).unwrap();
        crate::jj::write_base_marker(&ws, int, &int_tip).unwrap();
        assert_eq!(
            crate::jj::flatten_state(&jj, &store, &int_tip, builder).unwrap(),
            crate::jj::FlattenState::Clean
        );

        let tip_before = crate::jj::bookmark_commit(&jj, &store, builder).unwrap();
        let backend = JjBackend::new(crate::jj::JjEnv::resolve(&bin, home.path()), &ws);
        backend.heal_after_seal(&ws);
        let tip_after = crate::jj::bookmark_commit(&jj, &store, builder).unwrap();
        assert_eq!(
            tip_before, tip_after,
            "a clean seal is not rewritten by the reseal heal"
        );
    }
}
