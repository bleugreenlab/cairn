//! Host-driven user file edit.
//!
//! Commits a file the user edited directly in the desktop file tab into its jj
//! worktree, reusing the same VCS seal seam the agent `write` verb uses
//! ([`crate::mcp::vcs::WorktreeVcs::seal_files`]). This is the convenience path
//! for the times — especially docs/writing — when it is faster for the user to
//! make a change themselves than to direct an agent to do it.
//!
//! The load-bearing invariant is the same as the agent path: the worktree always
//! ends either committed (the edit is now branch history) or discarded to HEAD
//! (the seal failed and the workspace was restored), never as stray dirt. The
//! seal is fail-closed; recording the change, refreshing the UI, and notifying
//! co-located agents are all best-effort and never undo a successful commit.

use std::path::{Component, Path, PathBuf};

use cairn_db::turso::params;

use super::file_mutations::{changed_line_counts, emit_worktree_changed, record_file_change_async};
#[cfg(test)]
use crate::mcp::git::GitAuthor;
#[cfg(test)]
use crate::mcp::vcs::WorktreeVcs;
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// Rejection when direct editing is attempted outside a jj agent worktree (the
/// project's live checkout, or any non-jj directory). Keeps the feature off the
/// user's own checkout, where committing would touch their working state.
const NON_JJ_WORKTREE_ERROR: &str = "Direct editing is only available in agent worktrees.";

/// Validate a worktree-relative file path: no absolute paths, no `..` traversal,
/// no root/prefix components. Mirrors the Tauri command's validation so the core
/// function is safe to call directly (tests, future callers).
fn validate_relative(file_path: &str) -> Result<PathBuf, String> {
    let relative = Path::new(file_path);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err("Invalid path: path traversal not allowed".to_string());
    }
    Ok(relative.to_path_buf())
}

/// Resolve `(project_id, repo_path)` for the job that owns this worktree, used to
/// key the per-store jj lock and resolve the project's git author identity.
/// The project that owns a worktree: its id, repo path, and default branch (an
/// empty/absent default branch maps to `None`). Drives the per-store jj lock, the
/// git author, and the default-branch gate.
struct WorktreeProject {
    job_id: String,
    project_id: String,
    repo_path: String,
    default_branch: Option<String>,
    branch: String,
}

async fn project_for_worktree(orch: &Orchestrator, worktree: &str) -> Option<WorktreeProject> {
    let worktree = worktree.to_string();
    orch.db
        .local
        .read(|conn| {
            let worktree = worktree.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.id, j.project_id, p.repo_path, p.default_branch, j.branch
                         FROM jobs j
                         JOIN projects p ON j.project_id = p.id
                         WHERE j.worktree_path = ?1
                         ORDER BY j.created_at DESC
                         LIMIT 1",
                        params![worktree.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some(WorktreeProject {
                    job_id: row.text(0)?,
                    project_id: row.text(1)?,
                    repo_path: row.text(2)?,
                    default_branch: row.opt_text(3)?.filter(|s| !s.is_empty()),
                    branch: row
                        .opt_text(4)?
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "main".to_string()),
                }))
            })
        })
        .await
        .ok()
        .flatten()
}

/// Job ids with a live (`starting`/`live`) run on this worktree. These are the
/// agents that share the working copy: the concurrency gate checks them for an
/// active turn, and the post-commit passive notification addresses them.
async fn live_jobs_on_worktree(orch: &Orchestrator, worktree: &str) -> Result<Vec<String>, String> {
    let worktree = worktree.to_string();
    orch.db
        .local
        .read(|conn| {
            let worktree = worktree.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT DISTINCT j.id
                         FROM runs r
                         JOIN jobs j ON r.job_id = j.id
                         WHERE r.status IN ('starting', 'live')
                           AND j.worktree_path = ?1",
                        params![worktree.as_str()],
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await
        .map_err(|e| e.to_string())
}

/// A compact, bounded line diff of a single file's change, for the passive edit
/// notification so an agent sees WHAT changed inline instead of blindly re-reading
/// the whole file. Emits only changed lines (`-` old, `+` new) via an LCS, capped
/// at `max_lines` with a truncation note. Empty string when there is no textual
/// change.
fn render_change_diff(before: &str, after: &str, max_lines: usize) -> String {
    let a: Vec<&str> = before.lines().collect();
    let b: Vec<&str> = after.lines().collect();
    let mut lcs = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in (0..a.len()).rev() {
        for j in (0..b.len()).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut lines: Vec<String> = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            lines.push(format!("-{}", a[i]));
            i += 1;
        } else {
            lines.push(format!("+{}", b[j]));
            j += 1;
        }
    }
    while i < a.len() {
        lines.push(format!("-{}", a[i]));
        i += 1;
    }
    while j < b.len() {
        lines.push(format!("+{}", b[j]));
        j += 1;
    }
    if lines.len() > max_lines {
        let extra = lines.len() - max_lines;
        lines.truncate(max_lines);
        lines.push(format!("… ({extra} more changed lines)"));
    }
    lines.join("\n")
}

/// Passively notify every live agent on this worktree that the user edited a
/// file, with a compact diff of the change. Minimal by design: `User edited
/// <file>` then the diff, nothing else. Sent strictly AFTER a successful seal.
/// `Passive` never wakes an idle agent. Best-effort: a delivery failure never
/// undoes the commit.
fn notify_agents(orch: &Orchestrator, job_ids: &[String], file_path: &str, diff: &str) {
    if job_ids.is_empty() {
        return;
    }
    let content = if diff.is_empty() {
        format!("User edited `{file_path}`")
    } else {
        format!("User edited `{file_path}`\n{diff}")
    };
    for job_id in job_ids {
        let Some(run_id) = crate::messages::delivery::latest_run_for_job(&orch.db.local, job_id)
        else {
            continue;
        };
        if let Err(e) = crate::messages::delivery::queue_system_direct(
            orch,
            &run_id,
            &content,
            DeliveryUrgency::Passive,
        ) {
            log::warn!("Failed to notify agent {job_id} of user file edit: {e}");
        }
    }
}

/// Build the user-facing error for a seal that failed and was rolled back. The
/// claim matches the post-call state: a stale/lost-seal failure restored the
/// workspace, a generic failure did too, and a failed restore is named.
#[cfg(test)]
fn seal_failure_message(seal_error: &str, restored: Result<(), String>) -> String {
    if let Err(re) = restored {
        return format!("Save failed and the workspace couldn't be restored: {re}");
    }
    // The conflicted-branch refusal is the seal fast-forward guard rejecting a
    // seal onto a bookmark whose tip carries a recorded conflict: the branch is in
    // a conflicted/mid-rebase state that `update_stale` can't heal, so retrying
    // does not help (a host edit is not a deliberate agent flatten). The other
    // recoverable cases (op-log stale, lost-seal) do clear on a retry. The shared
    // classifier is the single source of truth across the run/write/host paths.
    if crate::jj::is_conflicted_branch_seal_error(seal_error) {
        return "Couldn't save: this worktree's branch is mid-rebase or conflicted, so a \
             direct edit can't be committed here. Nothing was changed."
            .to_string();
    }
    if crate::jj::is_stale_error(seal_error) || crate::jj::is_lost_seal_error(seal_error) {
        return "Couldn't save: the worktree changed underneath. Nothing was changed — try again."
            .to_string();
    }
    format!("Couldn't save your edit: {seal_error}. Nothing was changed.")
}

/// Seal the user's edit into one commit, recovering from a stale workspace once
/// and discarding to HEAD on any unrecoverable failure so the worktree is never
/// left dirty. Returns the sealed commit's short sha, or a user-facing error
/// whose claim matches the post-call state.
///
/// Stale recovery mirrors the agent write path: a base-advance reconcile (or any
/// other shared-store op) can rebase this workspace's branch while its agent is
/// idle, leaving the working copy stale so the first seal's fast-forward check
/// fails ("behind its branch tip"/"working copy is stale"). A concurrent store
/// advance can likewise produce a lost-seal. Both are the same recoverable family
/// the agent path retries together; without recovery every retry fails
/// identically and the edit can never land. We advance the workspace onto the
/// branch tip (`update_stale`), re-write the user's full content (a whole-file
/// edit needs no anchored re-match), and seal once more.
///
/// Takes `&dyn WorktreeVcs` so the discard-on-failure safety property stays
/// unit-testable with a fake backend.
#[cfg(test)]
fn finish_commit(
    vcs: &dyn WorktreeVcs,
    worktree: &Path,
    file: &Path,
    repo_rel: &str,
    content: &str,
    commit_msg: &str,
    author: Option<&GitAuthor>,
) -> Result<String, String> {
    let files = [repo_rel];
    let mut result = vcs.seal_files(worktree, &files, commit_msg, author);

    // One recovery pass for the same recoverable family the agent write path
    // treats together (stale OR lost-seal): both recover by advancing onto the
    // current base and re-applying.
    if let Err(seal_error) = &result {
        // The conflicted-branch refusal (a divergent `@` over a conflicted
        // bookmark tip) is NOT stale-recoverable: `update_stale` can't heal it and
        // a host edit is not a deliberate agent flatten, so it must refuse cleanly
        // without a retry. The stale / lost-seal family does clear on a retry.
        let recoverable = !crate::jj::is_conflicted_branch_seal_error(seal_error)
            && (crate::jj::is_stale_error(seal_error) || crate::jj::is_lost_seal_error(seal_error));
        log::warn!(
            "host file edit: seal of `{repo_rel}` failed (recoverable={recoverable}): {seal_error}"
        );
        if recoverable {
            match vcs.update_stale(worktree) {
                Ok(()) => match std::fs::write(file, content) {
                    Ok(()) => {
                        log::info!(
                            "host file edit: advanced stale workspace and re-applied \
                             `{repo_rel}`, re-sealing"
                        );
                        result = vcs.seal_files(worktree, &files, commit_msg, author);
                        if let Err(e) = &result {
                            log::warn!(
                                "host file edit: re-seal of `{repo_rel}` after recovery failed: {e}"
                            );
                        }
                    }
                    Err(e) => log::warn!(
                        "host file edit: re-applying `{repo_rel}` after update-stale failed: {e}"
                    ),
                },
                Err(e) => {
                    log::warn!("host file edit: update-stale failed for `{repo_rel}`: {e}")
                }
            }
        }
    }

    match result {
        Ok(commit) => Ok(commit.sha),
        Err(seal_error) if crate::mcp::vcs::is_workspace_lineage_mismatch(&seal_error) => Err(
            format!(
                "{seal_error}. The on-disk edit was PRESERVED exactly; no stale retry or discard was attempted."
            ),
        ),
        Err(seal_error) => {
            // Restore worktree == HEAD before reporting failure, so the message's
            // "restored" claim is true and no uncommitted dirt is left behind.
            let restored = vcs.discard(worktree);
            log::warn!(
                "host file edit: discarding to HEAD after unrecoverable seal of `{repo_rel}`: \
                 {seal_error} (restore_ok={})",
                restored.is_ok()
            );
            Err(seal_failure_message(&seal_error, restored))
        }
    }
}

/// Commit a user's direct file edit into its jj worktree, returning the sealed
/// commit's short sha.
///
/// Steps, in order: reject a non-jj worktree and an empty commit message; hold
/// the per-store jj lock across the gate, write, and seal (serializing against
/// base-advance reconcile and concurrent store mutators); refuse if any agent on
/// the worktree has an active turn; write the content; record the change
/// (best-effort); seal via the VCS seam, discarding to HEAD on failure; on
/// success refresh the UI and passively notify co-located agents.
pub async fn commit_user_file_edit(
    orch: &Orchestrator,
    worktree_path: &str,
    file_path: &str,
    content: &str,
    commit_msg: &str,
) -> Result<String, String> {
    let worktree = Path::new(worktree_path);
    if !crate::jj::is_jj_dir(worktree) {
        return Err(NON_JJ_WORKTREE_ERROR.to_string());
    }
    if commit_msg.trim().is_empty() {
        return Err("A commit message is required to save this edit.".to_string());
    }
    let project = project_for_worktree(orch, worktree_path)
        .await
        .ok_or_else(|| "Direct edit worktree has no owning project job.".to_string())?;
    if project.default_branch.as_deref() == Some(project.branch.as_str()) {
        return Err(format!(
            "Direct editing is not available on the default branch (`{}`). Edit from a feature-branch worktree instead.",
            project.branch
        ));
    }
    let relative = validate_relative(file_path)?;
    let repo_rel = relative.to_string_lossy().replace('\\', "/");
    let jobs = live_jobs_on_worktree(orch, worktree_path).await?;
    for job_id in &jobs {
        if crate::messages::delivery::head_turn_active(&orch.db.local, job_id)
            .await
            .unwrap_or(false)
        {
            return Err(
                "An agent is currently working in this worktree; wait until it finishes, then try again."
                    .to_string(),
            );
        }
    }

    let managed_store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&project.repo_path));
    // Pre-store workspaces can still exist during migration and in imported
    // fixtures. They remain a valid jj repository coordinate; once the managed
    // store exists it is always authoritative.
    let store = if crate::jj::is_jj_dir(&managed_store) {
        managed_store
    } else {
        worktree.to_path_buf()
    };
    let guard = orch
        .acquire_jj_store_lock(&store, "host logical file edit")
        .await;
    let expected = cairn_vcs::resolve_coordinate(&store, &project.branch)
        .await
        .map_err(|error| error.to_string())?;
    let before = super::super::read::file_at_commit(
        PathBuf::from(&project.repo_path),
        expected.clone(),
        &repo_rel,
    )?
    .ok_or_else(|| format!("Cannot edit {file_path}: file does not exist at the logical head"))
    .and_then(|bytes| {
        String::from_utf8(bytes)
            .map_err(|_| "Direct editing is only available for UTF-8 regular files.".to_string())
    })?;
    let author = orch
        .resolve_git_identity_for_project(Some(project.project_id.as_str()))
        .map(|(name, email)| cairn_vcs::PublicationAuthor { name, email });
    let publication_store = store.clone();
    let publication_branch = project.branch.clone();
    let publication_expected = expected.clone();
    let publication_path = repo_rel.clone();
    let publication_content = content.as_bytes().to_vec();
    let publication_message = commit_msg.to_string();
    let publication = tokio::task::spawn_blocking(move || {
        cairn_vcs::publish_logical_mutations(
            &publication_store,
            &publication_branch,
            &publication_expected,
            vec![cairn_vcs::LogicalTreeMutation {
                path: publication_path,
                content: Some(publication_content),
            }],
            cairn_vcs::PublicationMode::Child {
                description: publication_message,
                author,
            },
        )
    })
    .await
    .map_err(|error| format!("Host edit publication worker failed: {error}"))??;
    let (additions, deletions) = changed_line_counts(Some(&before), Some(content));
    let trace = guard.trace();
    drop(guard);

    if crate::merge_requests::queries::publication_requirement_for_managed_branch(
        &orch.db.local,
        &project.job_id,
        &project.project_id,
        &project.branch,
    )
    .await
        == crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr
    {
        let _phase = trace.phase("origin push");
        let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, worktree);
        crate::mcp::vcs::publish_required_origin(vcs.as_ref(), worktree).map_err(|error| {
            format!(
                "Host edit commit {} landed locally but remains unpublished because the required open-PR origin push failed: {error}",
                publication.head
            )
        })?;
    }
    if let Err(error) = record_file_change_async(
        orch,
        worktree_path,
        &repo_rel,
        "modified",
        additions,
        deletions,
        None,
    )
    .await
    {
        log::warn!("Failed to record user file change for {repo_rel}: {error}");
    }
    emit_worktree_changed(orch, worktree_path);
    let edit_diff = render_change_diff(&before, content, 60);
    notify_agents(orch, &jobs, &repo_rel, &edit_diff);
    Ok(publication.head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::mcp::git::CommitResult;
    use crate::mcp::vcs::FakeVcs;
    use crate::mcp::vcs::{VcsSnapshot, WorktreeVcs};
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    /// A `WorktreeVcs` double whose `seal_files` returns a *sequence* of programmed
    /// results (one per call), so a test can model "first seal stale, second seal
    /// succeeds" — the recovery success path the shared `FakeVcs` (fixed result)
    /// cannot express. Counts update_stale/discard so the recovery flow is asserted.
    struct SeqVcs {
        seals: Mutex<std::collections::VecDeque<Result<CommitResult, String>>>,
        seal_calls: AtomicUsize,
        update_stale_calls: AtomicUsize,
        discard_calls: AtomicUsize,
    }

    impl SeqVcs {
        fn new(seals: Vec<Result<CommitResult, String>>) -> Self {
            Self {
                seals: Mutex::new(seals.into()),
                seal_calls: AtomicUsize::new(0),
                update_stale_calls: AtomicUsize::new(0),
                discard_calls: AtomicUsize::new(0),
            }
        }
        fn seals(&self) -> usize {
            self.seal_calls.load(Ordering::SeqCst)
        }
        fn update_stales(&self) -> usize {
            self.update_stale_calls.load(Ordering::SeqCst)
        }
        fn discards(&self) -> usize {
            self.discard_calls.load(Ordering::SeqCst)
        }
    }

    impl WorktreeVcs for SeqVcs {
        fn snapshot(&self, _: &Path) -> Result<VcsSnapshot, String> {
            Ok(VcsSnapshot("seq".to_string()))
        }
        fn changed_since(&self, _: &Path, _: &VcsSnapshot) -> Result<bool, String> {
            Ok(true)
        }
        fn is_dirty(&self, _: &Path) -> Result<bool, String> {
            Ok(true)
        }
        fn seal_all(
            &self,
            _: &Path,
            _: &str,
            _: Option<&GitAuthor>,
        ) -> Result<CommitResult, String> {
            unreachable!("host edit seals via seal_files")
        }
        fn seal_files(
            &self,
            _: &Path,
            _: &[&str],
            _: &str,
            _: Option<&GitAuthor>,
        ) -> Result<CommitResult, String> {
            self.seal_calls.fetch_add(1, Ordering::SeqCst);
            self.seals
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("no more programmed seal results".to_string()))
        }
        fn discard(&self, _: &Path) -> Result<(), String> {
            self.discard_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn update_stale(&self, _: &Path) -> Result<(), String> {
            self.update_stale_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

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

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("host-edit.db").await
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    async fn seed_project_job(
        db: &LocalDb,
        repo_path: &str,
        worktree_path: &str,
        branch: &str,
        base_commit: &str,
    ) {
        db.execute_script(&format!(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','{repo_path}',1,1);
             INSERT INTO jobs(id, project_id, status, worktree_path, branch, base_commit, created_at, updated_at)
              VALUES('j','p','running','{worktree_path}','{branch}','{base_commit}',1,1);
             INSERT INTO runs(id, job_id, status, created_at, updated_at)
              VALUES('r','j','live',1,1);"
        ))
        .await
        .unwrap();
    }

    /// Provision a real jj worktree over a fresh project store, seed the matching
    /// project/job/run rows, and return the orchestrator plus the worktree path.
    async fn provision(
        bin: &str,
        proj: &TempDir,
        wts: &TempDir,
    ) -> (Orchestrator, std::path::PathBuf, String) {
        init_project(proj.path());
        let db = migrated_db().await;
        let orch = test_orchestrator(db);
        let jj = crate::jj::JjEnv::resolve(bin, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, proj.path());
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("job");
        let branch = "agent/CAIRN-2061-builder-0";
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        let base_commit = crate::jj::head_commit(&jj, &ws).unwrap();
        crate::jj::write_base_marker(&ws, "main", &base_commit).unwrap();
        crate::jj::write_project_root_marker(&ws, proj.path()).unwrap();
        crate::jj::write_workspace_identity(
            &ws,
            &crate::jj::WorkspaceIdentity::new(
                "j",
                "j",
                "p",
                proj.path().to_path_buf(),
                ws.clone(),
                branch,
                crate::jj::workspace_name_for_branch(branch),
                base_commit.clone(),
            ),
        )
        .unwrap();
        let ws_str = ws.to_string_lossy().to_string();
        seed_project_job(
            &orch.db.local,
            proj.path().to_string_lossy().as_ref(),
            &ws_str,
            branch,
            &base_commit,
        )
        .await;
        (orch, ws, ws_str)
    }

    #[tokio::test]
    async fn non_jj_worktree_is_rejected() {
        let dir = TempDir::new().unwrap();
        let db = migrated_db().await;
        let orch = test_orchestrator(db);
        let err = commit_user_file_edit(
            &orch,
            dir.path().to_string_lossy().as_ref(),
            "a.txt",
            "x\n",
            "msg",
        )
        .await
        .unwrap_err();
        assert_eq!(err, NON_JJ_WORKTREE_ERROR);
    }

    #[test]
    fn finish_commit_returns_sha_on_success() {
        let vcs = FakeVcs::new();
        let sha = finish_commit(
            &vcs,
            Path::new("/tmp/ws"),
            Path::new("/tmp/ws/a.rs"),
            "a.rs",
            "x\n",
            "msg",
            None,
        )
        .unwrap();
        assert_eq!(sha, "abc123");
        assert_eq!(vcs.discards(), 0, "a successful seal must not discard");
        assert_eq!(
            vcs.update_stales(),
            0,
            "a clean seal needs no stale recovery"
        );
    }

    #[test]
    fn finish_commit_recovers_stale_then_discards_when_unrecoverable() {
        // A stale seal triggers one recovery pass (update_stale + re-apply content
        // + re-seal). The fake keeps returning stale, so it ultimately discards to
        // HEAD — but the recovery was attempted and the content was re-applied.
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("a.rs");
        let vcs = FakeVcs::new().seal(Err("working copy is stale".to_string()));
        let err = finish_commit(&vcs, dir.path(), &file, "a.rs", "x\n", "msg", None).unwrap_err();
        assert_eq!(vcs.update_stales(), 1, "a stale seal must attempt recovery");
        assert_eq!(vcs.seals(), 2, "recovery re-seals once after update_stale");
        assert_eq!(
            vcs.discards(),
            1,
            "an unrecoverable seal must discard to HEAD"
        );
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "x\n",
            "recovery re-applies the user's content before re-sealing"
        );
        assert!(
            err.contains("try again"),
            "an unrecoverable stale failure reports a retryable transient: {err}"
        );
    }

    #[test]
    fn render_change_diff_shows_only_changed_lines_and_caps() {
        let d = render_change_diff("a\nb\nc\n", "a\nB\nc\n", 60);
        assert!(d.contains("-b"), "old line shown: {d}");
        assert!(d.contains("+B"), "new line shown: {d}");
        assert!(
            !d.lines().any(|l| l == " a" || l == "a"),
            "unchanged lines omitted: {d}"
        );

        // New file (no `before`): all additions.
        let added = render_change_diff("", "x\ny\n", 60);
        assert_eq!(added, "+x\n+y");

        // Cap with a truncation note.
        let big = (0..100)
            .map(|n| format!("l{n}"))
            .collect::<Vec<_>>()
            .join("\n");
        let capped = render_change_diff("", &big, 10);
        assert!(
            capped.contains("more changed lines"),
            "caps with a note: {capped}"
        );
        assert_eq!(capped.lines().count(), 11, "10 lines + the truncation note");
    }

    #[test]
    fn seal_failure_message_distinguishes_divergence_from_stale() {
        // A conflicted-branch refusal (divergent `@` over a conflicted bookmark
        // tip) is named as conflicted/mid-rebase and is NOT a retryable transient.
        let diverged = seal_failure_message(crate::jj::CONFLICTED_BRANCH_SEAL_MSG, Ok(()));
        assert!(
            diverged.contains("conflicted"),
            "divergence message names the conflicted/mid-rebase state: {diverged}"
        );
        assert!(
            !diverged.contains("try again"),
            "a conflicted-branch divergence is not a retryable transient: {diverged}"
        );

        // The clean "behind its branch tip" refusal is the genuine stale /
        // coordinator-advance case, which DOES clear on a retry (update-stale
        // advances `@` onto the clean tip).
        let stale_ff = seal_failure_message(
            "seal refused: workspace `agent/x` is behind its branch tip",
            Ok(()),
        );
        assert!(
            stale_ff.contains("try again"),
            "a clean fast-forward refusal is retryable stale: {stale_ff}"
        );

        let stale = seal_failure_message("working copy is stale", Ok(()));
        assert!(
            stale.contains("try again"),
            "op-log staleness is retryable: {stale}"
        );

        let restore_failed = seal_failure_message("working copy is stale", Err("boom".to_string()));
        assert!(
            restore_failed.contains("boom"),
            "a failed restore is surfaced first: {restore_failed}"
        );
    }

    #[test]
    fn finish_commit_recovers_stale_seal_and_returns_retry_sha() {
        // The fix that matters: first seal is stale, recovery advances the
        // workspace and re-applies content, and the SECOND seal succeeds — the
        // commit lands with the retry's sha and nothing is discarded.
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("a.rs");
        let vcs = SeqVcs::new(vec![
            Err("working copy is stale".to_string()),
            Ok(CommitResult {
                sha: "recovered".to_string(),
                pr_number: None,
                amend_note: None,
            }),
        ]);
        let sha = finish_commit(&vcs, dir.path(), &file, "a.rs", "x\n", "msg", None).unwrap();
        assert_eq!(sha, "recovered", "returns the recovered seal's sha");
        assert_eq!(
            vcs.update_stales(),
            1,
            "recovery advances the stale workspace"
        );
        assert_eq!(vcs.seals(), 2, "first seal stale, second seal succeeds");
        assert_eq!(vcs.discards(), 0, "a recovered seal must not discard");
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "x\n",
            "the user's content is re-applied before the retry seal"
        );
    }

    #[test]
    fn finish_commit_recovers_lost_seal_like_stale() {
        // A lost-seal is the same recoverable family as stale (matching the agent
        // write path), so it must take the recovery branch and land on retry.
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("a.rs");
        let lost_seal =
            "seal captured no change (the working copy was reset under a concurrent store advance)";
        assert!(
            crate::jj::is_lost_seal_error(lost_seal) && !crate::jj::is_stale_error(lost_seal),
            "precondition: the message is a lost-seal, not a stale, error"
        );
        let vcs = SeqVcs::new(vec![
            Err(lost_seal.to_string()),
            Ok(CommitResult {
                sha: "recovered".to_string(),
                pr_number: None,
                amend_note: None,
            }),
        ]);
        let sha = finish_commit(&vcs, dir.path(), &file, "a.rs", "x\n", "msg", None).unwrap();
        assert_eq!(sha, "recovered", "a lost-seal also recovers and lands");
        assert_eq!(vcs.update_stales(), 1);
        assert_eq!(vcs.discards(), 0);
    }

    #[test]
    fn finish_commit_conflicted_branch_refuses_without_stale_retry() {
        // A conflicted-branch refusal is NOT stale-recoverable: a host edit is not
        // a deliberate agent flatten, so finish_commit refuses cleanly — no
        // update_stale retry (which could never converge), a single discard to
        // HEAD, and the conflicted/mid-rebase message.
        let vcs = FakeVcs::new().seal(Err(crate::jj::CONFLICTED_BRANCH_SEAL_MSG.to_string()));
        let err = finish_commit(
            &vcs,
            Path::new("/tmp/ws"),
            Path::new("/tmp/ws/a.rs"),
            "a.rs",
            "x\n",
            "msg",
            None,
        )
        .unwrap_err();
        assert_eq!(
            vcs.update_stales(),
            0,
            "a conflicted-branch refusal must not attempt stale recovery"
        );
        assert_eq!(
            vcs.seals(),
            1,
            "no re-seal: the refusal is terminal for a host edit"
        );
        assert_eq!(
            vcs.discards(),
            1,
            "it discards to HEAD once, refusing the host edit cleanly"
        );
        assert!(
            err.contains("mid-rebase or conflicted"),
            "reports the conflicted/mid-rebase state: {err}"
        );
    }

    #[test]
    fn finish_commit_reports_generic_seal_failure() {
        let vcs = FakeVcs::new().seal(Err("disk full".to_string()));
        let err = finish_commit(
            &vcs,
            Path::new("/tmp/ws"),
            Path::new("/tmp/ws/a.rs"),
            "a.rs",
            "x\n",
            "msg",
            None,
        )
        .unwrap_err();
        assert_eq!(
            vcs.update_stales(),
            0,
            "a non-stale failure does not recover"
        );
        assert_eq!(vcs.discards(), 1);
        assert!(
            err.contains("disk full"),
            "generic failure surfaces the cause: {err}"
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn user_edit_commits_logical_head_without_moving_worktree() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping user_edit_commits_and_leaves_worktree_clean: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        let (orch, ws, ws_str) = provision(&bin, &proj, &wts).await;

        let sha =
            commit_user_file_edit(&orch, &ws_str, "shared.rs", "edited by user\n", "user edit")
                .await
                .unwrap();
        assert!(!sha.is_empty(), "a successful commit returns its sha");

        let vcs = crate::mcp::vcs::resolve_worktree_vcs(&orch, &ws);
        assert!(
            !vcs.is_dirty(&ws).unwrap(),
            "the worktree must be clean (== HEAD) after the commit"
        );
        assert_eq!(
            std::fs::read_to_string(ws.join("shared.rs")).unwrap(),
            "base\n",
            "logical publication must not materialize the retained worktree"
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn empty_commit_message_is_rejected() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping empty_commit_message_is_rejected: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        let (orch, _ws, ws_str) = provision(&bin, &proj, &wts).await;

        let err = commit_user_file_edit(&orch, &ws_str, "shared.rs", "x\n", "   ")
            .await
            .unwrap_err();
        assert!(
            err.contains("commit message is required"),
            "an empty commit message must be rejected: {err}"
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn active_turn_blocks_the_save() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping active_turn_blocks_the_save: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        let (orch, _ws, ws_str) = provision(&bin, &proj, &wts).await;

        // An active (running) turn on the worktree's job blocks the save.
        orch.db
            .local
            .execute_script(
                "INSERT INTO turns(id, session_id, job_id, sequence, state, created_at, updated_at)
                 VALUES('t','s','j',1,'running',1,1);",
            )
            .await
            .unwrap();

        let err = commit_user_file_edit(&orch, &ws_str, "shared.rs", "x\n", "user edit")
            .await
            .unwrap_err();
        assert!(
            err.contains("currently working"),
            "the active-turn gate must refuse the save: {err}"
        );
    }

    /// A symlink target is refused: editing it would write through to the target
    /// while sealing the symlink path. The reader already refuses symlinks via its
    /// canonical boundary; the save path must match.
    #[cfg(unix)]
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn symlink_target_is_rejected() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping symlink_target_is_rejected: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        let (orch, ws, ws_str) = provision(&bin, &proj, &wts).await;

        std::os::unix::fs::symlink(ws.join("shared.rs"), ws.join("link.rs")).unwrap();
        let err = commit_user_file_edit(&orch, &ws_str, "link.rs", "x\n", "edit link")
            .await
            .unwrap_err();
        assert!(
            err.contains("does not exist"),
            "checkout-only path must be rejected: {err}"
        );
        assert_eq!(
            std::fs::read_to_string(ws.join("shared.rs")).unwrap(),
            "base\n"
        );
    }

    fn run_jj_raw(bin: &str, cfg: &std::path::Path, cwd: &Path, args: &[&str]) -> (bool, String) {
        let out = crate::env::command(bin)
            .args(args)
            .current_dir(cwd)
            .env("JJ_CONFIG", cfg)
            .env("EDITOR", "true")
            .env("JJ_EDITOR", "true")
            .output()
            .unwrap();
        (
            out.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    }

    /// Regression for the user's actual failure: when the branch bookmark sits ON
    /// `@` (the working-copy commit IS the branch tip — the post-agent-commit
    /// shape), a save must still commit. Before the `::@` fast-forward fix this
    /// failed with "behind its branch tip".
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn save_succeeds_when_bookmark_is_on_working_copy() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping save_succeeds_when_bookmark_is_on_working_copy: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        let (orch, ws, ws_str) = provision(&bin, &proj, &wts).await;
        let cfg = orch.config_dir.join("jj").join("config.toml");
        let branch = "agent/CAIRN-2061-builder-0";

        // Put work in @ and move the bookmark ONTO @ (working-copy commit == tip).
        std::fs::write(ws.join("shared.rs"), "agent work\n").unwrap();
        let (ok, msg) = run_jj_raw(&bin, &cfg, &ws, &["bookmark", "set", branch, "-r", "@"]);
        assert!(ok, "bookmark set failed: {msg}");

        let sha = commit_user_file_edit(&orch, &ws_str, "shared.rs", "user edit\n", "save")
            .await
            .expect("a save must succeed when the bookmark is on the working-copy commit");
        assert!(!sha.is_empty());
    }

    /// Reproduce the conflicted-rebase scenario the dev logs show ("local advance
    /// on main: 1 recorded a conflict"): a feature branch rebased onto an advanced
    /// `main` that conflicts. A user save on that worktree still commits cleanly
    /// forward, proving a conflicted base is NOT the cause of the user's failure.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn conflicted_rebased_base_save_still_commits() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping conflicted_rebased_base_save_still_commits: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path()); // main: shared.rs = "base\n"
        let db = migrated_db().await;
        let orch = test_orchestrator(db);
        let jj = crate::jj::JjEnv::resolve(&bin, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, proj.path());
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let branch = "agent/CAIRN-2061-builder-0";
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        let base_commit = crate::jj::head_commit(&jj, &ws).unwrap();
        crate::jj::write_base_marker(&ws, "main", &base_commit).unwrap();
        crate::jj::write_project_root_marker(&ws, proj.path()).unwrap();
        crate::jj::write_workspace_identity(
            &ws,
            &crate::jj::WorkspaceIdentity::new(
                "j",
                "j",
                "p",
                proj.path().to_path_buf(),
                ws.clone(),
                branch,
                crate::jj::workspace_name_for_branch(branch),
                base_commit.clone(),
            ),
        )
        .unwrap();
        let ws_str = ws.to_string_lossy().to_string();
        seed_project_job(
            &orch.db.local,
            proj.path().to_string_lossy().as_ref(),
            &ws_str,
            branch,
            &base_commit,
        )
        .await;

        // Feature edit on shared.rs, sealed on the agent branch.
        crate::mcp::vcs::resolve_worktree_vcs(&orch, &ws)
            .seal_files(&ws, &["shared.rs"], "feature", None)
            .unwrap();
        std::fs::write(ws.join("shared.rs"), "feature change\n").unwrap();
        crate::mcp::vcs::resolve_worktree_vcs(&orch, &ws)
            .seal_files(&ws, &["shared.rs"], "feature edit", None)
            .unwrap();

        // main advances with a CONFLICTING change to the same file.
        std::fs::write(proj.path().join("shared.rs"), "main change\n").unwrap();
        git(proj.path(), &["commit", "-aqm", "main change"]);
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

        // Reconcile: rebase the feature branch onto main -> conflict, materialize.
        crate::jj::rebase_branch_onto(&jj, &store, branch, "main").unwrap();
        let _ = crate::jj::update_stale(&jj, &ws);

        // The rebased feature commit carries a conflict and is materialized in the
        // worktree, but a user save still commits cleanly forward (the conflict is
        // in the parent, the user's edit seals on top) — so a conflicted base is
        // NOT the cause of the "worktree changed during save" failure.
        assert_eq!(crate::jj::conflicted_files(&jj, &ws), vec!["shared.rs"]);
        let sha = commit_user_file_edit(&orch, &ws_str, "shared.rs", "user edit\n", "user save")
            .await
            .expect("a save on a conflicted/rebased base still commits");
        assert!(!sha.is_empty());
    }

    /// Editing a worktree whose branch marker is the project default branch is
    /// refused, so a file-tab save can never advance the default branch locally
    /// and trip the base-advance sibling reconcile.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn default_branch_worktree_is_rejected() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping default_branch_worktree_is_rejected: jj not resolvable");
            return;
        };
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        let (orch, _ws, ws_str) = provision(&bin, &proj, &wts).await;

        // Make the project's default branch equal the worktree's branch marker
        // (`provision` adds the workspace on `agent/CAIRN-2061-builder-0`).
        orch.db
            .local
            .execute_script(
                "UPDATE projects SET default_branch = 'agent/CAIRN-2061-builder-0' WHERE id = 'p';",
            )
            .await
            .unwrap();

        let err = commit_user_file_edit(&orch, &ws_str, "shared.rs", "x\n", "edit")
            .await
            .unwrap_err();
        assert!(
            err.contains("default branch"),
            "editing the default-branch worktree must be rejected: {err}"
        );
    }
}
