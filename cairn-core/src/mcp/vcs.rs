//! VCS mutation seam for the commit-barrier and write-commit paths.
//!
//! Every worktree-mutating VCS operation on these paths flows through
//! [`WorktreeVcs`]. An agent worktree is a `.jj` workspace over a shared store,
//! so [`JjBackend`] is the production backend there. A non-worktree cwd — the
//! project's live checkout behind a long-lived manager / triage / read-only
//! agent (project chat included) — resolves to the read-only [`NonWorktreeVcs`]
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
    fn push_after_seal(&self, worktree: &Path) {
        if let Some(branch) = crate::jj::read_branch_marker(worktree) {
            crate::jj::push_to_origin(&self.jj, worktree, &branch);
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
}

/// Rejection returned when a seal is attempted in a non-worktree cwd. Changes
/// can only happen in a worktree; the project's live checkout is read-only for
/// agents.
pub(crate) const NON_WORKTREE_SEAL_ERROR: &str =
    "Changes can only be made in a worktree. This agent runs on the project's live \
     checkout (no worktree) and cannot commit.";

/// Read-only [`WorktreeVcs`] for a non-jj cwd: the project's live checkout used
/// by long-lived manager / triage / read-only-analysis agents (project chat
/// included). Changes can only happen in worktrees, so there is nothing here for
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

    fn can_revert(&self) -> bool {
        // The live checkout is never reverted (it holds the user's own work);
        // the barrier warns about stray dirt instead.
        false
    }
}

/// Resolve the VCS backend for an agent cwd. A `.jj` workspace resolves to
/// [`JjBackend`] (the only place changes happen); any other cwd — the project's
/// live checkout behind a no-worktree manager / triage / read-only agent —
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
    seal_calls: std::sync::atomic::AtomicUsize,
    discard_calls: std::sync::atomic::AtomicUsize,
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
            }),
            discard_result: Ok(()),
            can_revert: true,
            seal_calls: std::sync::atomic::AtomicUsize::new(0),
            discard_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn can_revert(mut self, v: bool) -> Self {
        self.can_revert = v;
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
}
