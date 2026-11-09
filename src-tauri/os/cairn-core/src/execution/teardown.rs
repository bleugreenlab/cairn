//! Coordinate and owner-resource cleanup at issue and project lifecycle boundaries.
//!
//! Agent jobs have no filesystem checkout to follow or remove. Durable branches
//! remain the recoverable history, scratch is disposable process residence, and
//! terminals/REPLs are executor lifetime resources released by their owner fence.
//!
//! Resolving an issue is also the transcript immutability boundary: before its
//! branches are deleted, every execution of the issue is archived
//! ([`crate::archival`]) against its branch's live range. Project deletion and
//! issue deletion skip archival because the rows are deleted with them.

use crate::archival::{ArchiveRange, ArchiveTarget};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) enum TeardownScope {
    /// A resolved issue: its transcripts become immutable, so they are archived
    /// before its branches go.
    Issue(String),
    /// An issue being deleted: its jobs, runs, and events are deleted right
    /// after teardown, so archiving them would be wasted work.
    DeletedIssue(String),
}

/// Release the issue's dev-instance process trees while terminal resolution can
/// still refuse to commit. The later full cleanup repeats this idempotently.
pub(crate) async fn release_issue_dev_instances(
    orch: &Orchestrator,
    issue_id: &str,
) -> Result<(), String> {
    let db = crate::issues::crud::owning_db_for_issue(&orch.db, issue_id)
        .await
        .map_err(|error| error.to_string())?;
    let (job_ids, targets) = issue_targets(&db, issue_id).await?;
    let mut dev_targets: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in targets {
        dev_targets
            .entry(target.project_id)
            .or_default()
            .push(target.branch);
    }
    for (project_id, branches) in dev_targets {
        crate::dev_instances::release_issue_instances(orch, &project_id, &job_ids, &branches)
            .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownReason {
    Merged,
    Discarded,
}

#[derive(Debug, Clone)]
struct BranchTarget {
    project_id: String,
    branch: String,
    repo_path: String,
    job_ids: Vec<String>,
}

async fn issue_targets(
    db: &LocalDb,
    issue_id: &str,
) -> Result<(Vec<String>, Vec<BranchTarget>), String> {
    let issue_id = issue_id.to_string();
    let rows: Vec<(String, String, Option<String>, String)> = db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT j.id, j.project_id, j.branch, p.repo_path FROM jobs j JOIN projects p ON p.id = j.project_id WHERE j.issue_id = ?1",
                (issue_id.as_str(),),
            ).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?));
            }
            Ok(out)
        })
    }).await.map_err(|e| format!("Failed to load issue job coordinates: {e}"))?;

    let job_ids = rows.iter().map(|row| row.0.clone()).collect();
    let mut grouped: BTreeMap<(String, String, String), Vec<String>> = BTreeMap::new();
    for (job_id, project_id, branch, repo_path) in rows {
        if let Some(branch) = branch.filter(|branch| !branch.is_empty()) {
            grouped
                .entry((project_id, repo_path, branch))
                .or_default()
                .push(job_id);
        }
    }
    let targets = grouped
        .into_iter()
        .map(|((project_id, repo_path, branch), job_ids)| BranchTarget {
            project_id,
            branch,
            repo_path,
            job_ids,
        })
        .collect();
    Ok((job_ids, targets))
}

pub(crate) async fn cleanup_issue_jobs(
    orch: &Orchestrator,
    scope: TeardownScope,
    reason: TeardownReason,
) -> Result<(), String> {
    let (issue_id, archive) = match scope {
        TeardownScope::Issue(issue_id) => (issue_id, true),
        TeardownScope::DeletedIssue(issue_id) => (issue_id, false),
    };
    let db = crate::issues::crud::owning_db_for_issue(&orch.db, &issue_id)
        .await
        .map_err(|e| e.to_string())?;
    let (job_ids, targets) = issue_targets(&db, &issue_id).await?;
    // The issue's resolution has committed, and it moves every one of its
    // nodes' `/checks` projections from the live branch to the plan recorded
    // for its last head. Nothing else announces that move to the cache.
    for job_id in &job_ids {
        orch.invalidate_node_check_status(job_id, "issue-resolution");
    }

    kill_terminals_for_jobs(orch, &db, &job_ids).await;
    kill_repls_for_jobs(orch, &job_ids).await;
    let mut dev_targets: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in &targets {
        dev_targets
            .entry(target.project_id.clone())
            .or_default()
            .push(target.branch.clone());
    }
    for (project_id, branches) in dev_targets {
        crate::dev_instances::release_issue_instances(orch, &project_id, &job_ids, &branches)
            .await?;
    }
    for job_id in &job_ids {
        crate::scratch::remove_job_scratch_dir(job_id);
    }
    // Each of the issue's nodes owned a portable scratch; it ends with them,
    // on the runner and on every machine that held a replica.
    crate::node_scratch::lifecycle::close_for_issue(orch, &issue_id).await;

    // The issue's agents were stopped before teardown, so its transcripts are
    // now immutable. Archive them while their branches still resolve; the
    // per-execution pack keeps the branch's objects after deletion and `git gc`.
    if archive {
        archive_issue_executions(orch, &db, &issue_id).await;
    }

    for target in &targets {
        retire_branch(orch, &db, &issue_id, target, reason).await;
    }
    Ok(())
}

/// Retire one of a resolved issue's branches, or keep it and say why.
///
/// Retirement is a compare-and-swap of what the decision saw. The branch is
/// reserved before the decision and until the end, so no owner commit or
/// landing can move it in between unseen; the decision and the local delete run
/// under the store lock, off the runtime, in one worker that owns the lock; and
/// the decision's metadata reads hold the store's object lease. Anything
/// uncertain keeps the branch: a kept branch costs a stale ref, a wrong delete
/// costs commits. The origin copy goes only after both local copies are gone,
/// by a delete leased on the store's tracked origin value
/// ([`retire_remote_copy`]), never an unleased one.
async fn retire_branch(
    orch: &Orchestrator,
    db: &std::sync::Arc<LocalDb>,
    issue_id: &str,
    target: &BranchTarget,
    reason: TeardownReason,
) {
    let branch = target.branch.as_str();
    let keep = |why: String| log::info!("Teardown: keeping branch `{branch}`: {why}");
    let merge_target = match reason {
        TeardownReason::Discarded => None,
        TeardownReason::Merged => {
            match resolve_merge_target_for_source(db, issue_id, branch, &target.job_ids).await {
                Some(merge_target) => Some(merge_target),
                None => return keep("its merge target is unresolved".into()),
            }
        }
    };
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&target.repo_path));
    if !crate::jj::is_jj_dir(&store) {
        return keep(
            "the project has no branch store to retire it from by compare-and-swap".into(),
        );
    }
    let reservation = match orch
        .reserve_branches(
            &store,
            &[branch],
            "retire a resolved issue's branch",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => return keep(format!("it could not be reserved ({error})")),
    };
    let guard = match orch
        .acquire_jj_store_lock_with_timeout(
            &store,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("retire branch {branch}"),
            Some(crate::mcp::vcs::STORE_LOCK_TIMEOUT),
        )
        .await
    {
        Ok(guard) => guard,
        Err(()) => return keep("the store is busy".into()),
    };
    // Recording a recovery takes this same lock, so a fence recorded just
    // before is seen here. A head a fence holds is kept: its coordinates are
    // the repair's, not this retirement's to change.
    if let Err(refusal) = crate::orchestrator::branch_recovery::refuse_coordinate_if_fenced(
        orch,
        &store,
        &[branch],
        "Retiring this branch",
    )
    .await
    {
        return keep(refusal);
    }
    // Read under the reservation and the lock: work cut from the branch, or a
    // new owner, may have appeared while this waited for them.
    match protection(db, &target.project_id, branch, merge_target.as_deref()).await {
        Ok(None) => {}
        Ok(Some(why)) => return keep(why),
        Err(error) => return keep(format!("whether it is still in use is unknown ({error})")),
    }
    let recorded = match merge_target {
        Some(_) => match recorded_source_commit(db, issue_id, branch).await {
            Ok(recorded) => recorded,
            Err(error) => return keep(error),
        },
        None => None,
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let outcome = {
        // Admission immediately before the decision and the delete: the
        // branch is reserved by this caller and owes no acceptance (a write
        // whose acceptance is unsettled may or may not be in its bookmark, so
        // its content is not known). Refused means kept, locally and on origin.
        let permit = match guard
            .branch_write_permit(
                db,
                &reservation,
                &target.project_id,
                branch,
                crate::orchestrator::BatchAdmission::None,
            )
            .await
        {
            Ok(permit) => permit,
            Err(refusal) => return keep(refusal.to_string()),
        };
        // The judgement and the delete are blocking jj and git work; the
        // permit borrows the guard, so they run in place on this worker
        // thread, handed off from the runtime, rather than on a pooled task.
        crate::jj::in_place_blocking(|| {
            retire_locally(
                &jj,
                &store,
                branch,
                merge_target.as_deref(),
                recorded.as_deref(),
                &permit,
                &guard,
            )
        })
    };
    // The lock ends with the local half; the reservation outlives it, through
    // the remote delete and its record.
    drop(guard);
    match outcome {
        Ok(LocalRetirement::Retired {
            tracked_origin: Some(origin),
        }) => {
            retire_remote_copy(
                orch,
                db,
                &target.project_id,
                &store,
                branch,
                &origin,
                &reservation,
            )
            .await
        }
        Ok(LocalRetirement::Retired {
            tracked_origin: None,
        }) => log::info!("Teardown: retired `{branch}`; origin has no tracked copy to retire"),
        Ok(LocalRetirement::Kept(why)) => keep(why),
        Ok(LocalRetirement::Partial(why)) => {
            log::warn!("Teardown: {why}; its origin copy is kept")
        }
        Err(error) => keep(error),
    }
}

/// Delete `branch` on origin, leased on the copy the store last recorded
/// there (`origin`), after its local copies were retired.
///
/// The push and the read-back are network I/O, so they run off the store lock
/// while `reservation` still holds the branch. Recording the result re-takes
/// the lock, re-admits the branch, and retires only this branch's tracking
/// copy, by compare-and-swap on `origin`. Every failure keeps the remote copy: origin moving
/// off `origin` means someone pushed since, and their commits are not this
/// retirement's to drop.
async fn retire_remote_copy(
    orch: &Orchestrator,
    db: &std::sync::Arc<LocalDb>,
    project_id: &str,
    store: &Path,
    branch: &str,
    origin: &str,
    reservation: &crate::orchestrator::BranchReservation,
) {
    let (jj, path, name, expected) = (
        crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        store.to_path_buf(),
        branch.to_string(),
        origin.to_string(),
    );
    let pushed = tokio::task::spawn_blocking(move || {
        let heads = crate::jj::retire_remote_branch_unrecorded(&jj, &path, &name, &expected)
            .map_err(|error| error.to_string())?;
        let readback = crate::jj::readback_leased_push(&jj, &path, &heads).err();
        Ok::<_, String>((heads, readback))
    })
    .await;
    let (heads, readback) = match pushed {
        Ok(Ok(pushed)) => pushed,
        Ok(Err(error)) => {
            return log::warn!("Teardown: kept origin's copy of `{branch}`: {error}");
        }
        Err(error) => {
            return log::warn!(
                "Teardown: kept origin's copy of `{branch}`: worker failed ({error})"
            );
        }
    };
    if let Some(error) = readback {
        log::warn!("Teardown: deleted origin's `{branch}` but could not read it back: {error}");
    }
    let guard = match orch
        .acquire_jj_store_lock_with_timeout(
            store,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("record retirement of {branch} on origin"),
            Some(crate::mcp::vcs::STORE_LOCK_TIMEOUT),
        )
        .await
    {
        Ok(guard) => guard,
        Err(()) => {
            return log::warn!(
                "Teardown: deleted origin's `{branch}`; the store is busy, so its tracking copy is \
                 recorded by the next import"
            );
        }
    };
    // The lock was released for the network, so the branch is admitted again
    // under this one before the store records anything about it. The record
    // touches only this branch's tracking ref and its view entry, by
    // compare-and-swap, so the branch's own admission is the whole gate.
    // The permit borrows the guard, so it lives in this block and ends before
    // the guard is released.
    let recorded = {
        let permit = match guard
            .branch_write_permit(
                db,
                reservation,
                project_id,
                branch,
                crate::orchestrator::BatchAdmission::None,
            )
            .await
        {
            Ok(permit) => permit,
            Err(refusal) => {
                return log::warn!(
                    "Teardown: deleted origin's `{branch}`; recording it waits (next fetch): \
                     {refusal}"
                );
            }
        };
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        crate::jj::in_place_blocking(|| {
            let _admitted = &permit;
            crate::jj::record_leased_push(&jj, store, &heads)
        })
    };
    drop(guard);
    match recorded {
        Ok(()) => log::info!("Teardown: retired `{branch}` locally and on origin"),
        Err(error) => {
            log::warn!("Teardown: deleted origin's `{branch}`, but recording it refused: {error}")
        }
    }
}

/// What keeps a branch from retirement, first match wins. Every lookup binds
/// the project and the branch; the owner lookup is pinned to
/// `idx_jobs_project_branch` by `protection_seeks_jobs_on_project_and_branch`.
const PROTECTION_SQL: &str = "SELECT CASE
     WHEN EXISTS (SELECT 1 FROM projects p WHERE p.id = ?1 AND p.default_branch = ?2)
       THEN 'it is the project default branch'
     WHEN EXISTS (SELECT 1 FROM jobs j WHERE j.project_id = ?1 AND j.base_branch = ?2
                     AND j.status NOT IN ('complete', 'failed', 'cancelled'))
       THEN 'live work is cut from it'
     WHEN EXISTS (SELECT 1 FROM merge_requests m WHERE m.project_id = ?1
                     AND m.target_branch = ?2 AND m.status = 'open')
       THEN 'an open pull request targets it'
     WHEN EXISTS (SELECT 1 FROM merge_requests m WHERE m.project_id = ?1
                     AND m.source_branch = ?2 AND m.status = 'open')
       THEN 'an open pull request is published from it'
     WHEN EXISTS (SELECT 1 FROM jobs j WHERE j.project_id = ?1 AND j.branch = ?2
                     AND j.status NOT IN ('complete', 'failed', 'cancelled'))
       THEN 'live work still owns it'
   END";

/// Why `branch` must not be retired, if anything says so: it is the project's
/// default, it is the branch its own work merged into, or live work still uses
/// it as a base, a pull request target, or its own branch.
async fn protection(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    merge_target: Option<&str>,
) -> Result<Option<String>, String> {
    if merge_target == Some(branch) {
        return Ok(Some("it is the branch its work merged into".into()));
    }
    let reason = db
        .query_opt_text(
            PROTECTION_SQL,
            params![project_id.to_string(), branch.to_string()],
        )
        .await
        .map_err(|error| format!("read what uses `{branch}`: {error}"))?;
    Ok(reason)
}

/// The source commit the issue's merged pull request for `branch` recorded.
async fn recorded_source_commit(
    db: &LocalDb,
    issue_id: &str,
    branch: &str,
) -> Result<Option<String>, String> {
    db.query_opt_text(
        "SELECT merged_source_commit FROM merge_requests
          WHERE issue_id = ?1 AND source_branch = ?2 AND status = 'merged'
            AND merged_source_commit IS NOT NULL
          ORDER BY merged_at DESC LIMIT 1",
        params![issue_id.to_string(), branch.to_string()],
    )
    .await
    .map_err(|error| format!("the landing record is unreadable: {error}"))
}

/// What the local half of a retirement did.
#[derive(Debug, PartialEq, Eq)]
enum LocalRetirement {
    Retired { tracked_origin: Option<String> },
    Kept(String),
    Partial(String),
}

/// Judge and retire `branch`'s local copies. Runs on a blocking worker that
/// holds the store lock; the judgement's reads also hold the object lease,
/// which is released before anything is deleted and never upgraded.
///
/// `merge_target` is `None` for a discarded issue, whose branch needs no
/// landing proof. For a merged one, the branch landed exactly while its
/// bookmark holds the source commit the landing recorded; a row that names none
/// falls back to the ancestry proof alone.
fn retire_locally(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    merge_target: Option<&str>,
    recorded: Option<&str>,
    _admitted: &crate::orchestrator::BranchWritePermit<'_>,
    guard: &crate::orchestrator::JjStoreGuard,
) -> Result<LocalRetirement, String> {
    if !guard.covers(store) {
        return Err(format!(
            "retiring `{branch}` needs the lock of {}, which this caller does not hold",
            store.display()
        ));
    }
    let state = {
        let _lease = crate::jj::StoreObjectLease::for_store(store)?;
        let state = crate::jj::read_local_branch_state(store, branch)?;
        let landed = || match recorded {
            Some(recorded) => state.bookmark.as_deref() == Some(recorded),
            None => merge_target
                .is_some_and(|target| crate::jj::bookmark_landed_in(jj, store, branch, target)),
        };
        if let Some(why) = retirement_refusal(&state, merge_target.is_some(), landed) {
            return Ok(LocalRetirement::Kept(why));
        }
        state
    };
    Ok(
        match crate::jj::retire_bookmark_expecting(store, branch, &state)? {
            crate::jj::Retirement::Retired => LocalRetirement::Retired {
                tracked_origin: state.tracked_origin,
            },
            crate::jj::Retirement::Refused(why) => LocalRetirement::Kept(why),
            crate::jj::Retirement::Partial(why) => LocalRetirement::Partial(why),
        },
    )
}

/// Why the local copies in `state` must be kept, if anything says so.
fn retirement_refusal(
    state: &crate::jj::LocalBranchState,
    merged: bool,
    landed: impl FnOnce() -> bool,
) -> Option<String> {
    let Some(bookmark) = state.bookmark.as_deref() else {
        return Some(match state.git_ref {
            Some(_) => "its git ref has no bookmark to account for it".into(),
            None => "it has no local copy left".into(),
        });
    };
    if state
        .git_ref
        .as_deref()
        .is_some_and(|git_ref| git_ref != bookmark)
    {
        return Some(format!(
            "its git ref ({}) differs from its bookmark ({bookmark}), and nothing accounts for it",
            state.git_ref.as_deref().unwrap_or_default()
        ));
    }
    if merged && !landed() {
        return Some("it holds commits its landing did not include".into());
    }
    None
}

/// One issue job's archival coordinates.
struct ArchivalJob {
    execution_id: String,
    project_id: String,
    branch: Option<String>,
    integration_target: String,
    default_branch: String,
}

async fn load_archival_jobs(db: &LocalDb, issue_id: &str) -> Result<Vec<ArchivalJob>, String> {
    db.query_all(
        "SELECT j.execution_id, j.project_id, j.branch,
                COALESCE(j.base_branch, p.default_branch, 'main'),
                COALESCE(p.default_branch, 'main')
         FROM jobs j JOIN projects p ON p.id = j.project_id
         WHERE j.issue_id = ?1 AND j.execution_id IS NOT NULL
         ORDER BY j.created_at ASC",
        (issue_id.to_string(),),
        |row| {
            Ok(ArchivalJob {
                execution_id: row.text(0)?,
                project_id: row.text(1)?,
                branch: row.opt_text(2)?.filter(|branch| !branch.is_empty()),
                integration_target: row.text(3)?,
                default_branch: row.text(4)?,
            })
        },
    )
    .await
    .map_err(|e| format!("loading issue jobs for archival: {e}"))
}

/// Archive every execution of a resolved issue. Fail-soft: an error is logged
/// and leaves that execution's rows `full` for the recurring sweep, and never
/// blocks the branch deletion that follows.
///
/// Each execution is archived with the live range of its jobs' branch when that
/// branch still resolves. An execution with no branch of its own, or whose
/// branch no longer resolves, is archived without one (zstd and blobbed only).
async fn archive_issue_executions(orch: &Orchestrator, db: &LocalDb, issue_id: &str) {
    let jobs = match load_archival_jobs(db, issue_id).await {
        Ok(jobs) => jobs,
        Err(error) => {
            log::warn!("Teardown: archival skipped for issue {issue_id}: {error}");
            return;
        }
    };
    type RangeKey = (String, String, String, String);
    let mut resolved: BTreeMap<RangeKey, Option<ArchiveRange>> = BTreeMap::new();
    let mut executions: BTreeMap<String, Vec<ArchiveRange>> = BTreeMap::new();
    for job in jobs {
        let range = match job.branch {
            Some(branch) => {
                let key = (
                    job.project_id,
                    branch,
                    job.integration_target,
                    job.default_branch,
                );
                if !resolved.contains_key(&key) {
                    let range = resolve_archive_range(orch, &key.0, &key.1, &key.2, &key.3).await;
                    resolved.insert(key.clone(), range);
                }
                resolved[&key].clone()
            }
            None => None,
        };
        let ranges = executions.entry(job.execution_id).or_default();
        if let Some(range) = range {
            if !ranges.iter().any(|seen| seen.branch == range.branch) {
                ranges.push(range);
            }
        }
    }

    for (execution_id, ranges) in executions {
        let target = ArchiveTarget {
            execution_id: execution_id.clone(),
            ranges,
        };
        match crate::archival::archive_execution(db, target).await {
            Ok(summary) => {
                log::info!("Teardown: archived execution {execution_id} ({summary})");
                if summary.all_reads_mismatched() {
                    log::warn!(
                        "Teardown: execution {execution_id} archived {} reads but none to a git \
                         coordinate (all fell to mismatch-fallback); the live and archived read \
                         composition may have diverged",
                        summary.mismatch_fallback,
                    );
                }
            }
            Err(error) => {
                log::warn!("Teardown: archival failed for execution {execution_id}: {error}")
            }
        }
    }
}

/// The live range of `branch` against `integration_target`, in the project's
/// local repository on this machine, with the head of `default_branch` as its
/// durable head. `None` when the branch or the default branch no longer
/// resolves.
async fn resolve_archive_range(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
    integration_target: &str,
    default_branch: &str,
) -> Option<ArchiveRange> {
    let repo_path =
        match crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, project_id).await {
            Ok((Some(path), _)) if !path.is_empty() => PathBuf::from(path),
            Ok(_) => {
                log::info!(
                "Teardown: no local repository for project {project_id}; archiving without a range"
            );
                return None;
            }
            Err(error) => {
                log::warn!("Teardown: resolving repository for archival: {error}");
                return None;
            }
        };
    let range = match crate::diff::live_branch_range(
        &orch.jj_binary_path,
        &orch.config_dir,
        &repo_path,
        branch,
        integration_target,
    )
    .await
    {
        Ok(range) => range,
        Err(error) => {
            log::info!(
                "Teardown: branch `{branch}` has no live range ({error}); archiving without one"
            );
            return None;
        }
    };
    // A child issue integrates into its parent's agent branch, which is itself
    // rebased and deleted; only the default branch outlives every agent branch.
    let durable_head = if integration_target == default_branch {
        range.target_head.clone()
    } else {
        match crate::diff::live_branch_range(
            &orch.jj_binary_path,
            &orch.config_dir,
            &repo_path,
            default_branch,
            default_branch,
        )
        .await
        {
            Ok(default) => default.tip,
            Err(error) => {
                log::info!(
                    "Teardown: default branch `{default_branch}` has no live head ({error}); \
                     archiving `{branch}` without a range"
                );
                return None;
            }
        }
    };
    Some(ArchiveRange::from_branch_range(
        repo_path,
        branch.to_string(),
        range,
        durable_head,
    ))
}

async fn project_process_ids(
    db: &LocalDb,
    project_id: &str,
) -> Result<(Vec<String>, Vec<String>), String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut job_rows = conn
                .query(
                    "SELECT id FROM jobs WHERE project_id = ?1",
                    (project_id.as_str(),),
                )
                .await?;
            let mut job_ids = Vec::new();
            while let Some(row) = job_rows.next().await? {
                job_ids.push(row.text(0)?);
            }

            let mut run_rows = conn
                .query(
                    "SELECT id FROM runs
                     WHERE project_id = ?1
                        OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)",
                    (project_id.as_str(),),
                )
                .await?;
            let mut run_ids = Vec::new();
            while let Some(row) = run_rows.next().await? {
                run_ids.push(row.text(0)?);
            }
            Ok((job_ids, run_ids))
        })
    })
    .await
    .map_err(|e| format!("Failed to load project process ids for cleanup: {e}"))
}

/// Stop resources owned by a project's jobs before their rows are deleted.
pub async fn cleanup_project_jobs(orch: &Orchestrator, project_id: &str) -> Result<(), String> {
    let (job_ids, run_ids) = project_process_ids(&orch.db.local, project_id).await?;
    for run_id in &run_ids {
        let _ = crate::orchestrator::lifecycle::kill_session(orch, run_id);
    }
    kill_terminals_for_jobs(orch, &orch.db.local, &job_ids).await;
    kill_repls_for_jobs(orch, &job_ids).await;
    for job_id in &job_ids {
        crate::scratch::remove_job_scratch_dir(job_id);
    }
    crate::node_scratch::lifecycle::close_for_project(orch, project_id).await;
    Ok(())
}

/// Resolve the merge target branch a torn-down `source_branch` should have landed
/// in: the issue's MR `target_branch` first (the real destination), then a
/// producing job's `base_branch` (its fork point), then the project default
/// branch. `None` only when the issue's project cannot be resolved at all.
async fn resolve_merge_target_for_source(
    db: &LocalDb,
    issue_id: &str,
    source_branch: &str,
    job_ids: &[String],
) -> Option<String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        let source_branch = source_branch.clone();
        let job_ids = job_ids.clone();
        Box::pin(async move {
            // 1. The MR's recorded target branch (the true merge destination).
            let mut rows = conn
                .query(
                    "SELECT target_branch FROM merge_requests
                     WHERE issue_id = ?1 AND source_branch = ?2
                     ORDER BY opened_at DESC LIMIT 1",
                    params![issue_id.as_str(), source_branch.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                if let Some(target) = row.opt_text(0)?.filter(|t| !t.is_empty()) {
                    return Ok(Some(target));
                }
            }
            drop(rows);
            // 2. A producing job's base_branch (what it forked from).
            for job_id in &job_ids {
                let mut rows = conn
                    .query(
                        "SELECT base_branch FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    if let Some(base) = row.opt_text(0)?.filter(|b| !b.is_empty()) {
                        return Ok(Some(base));
                    }
                }
            }
            // 3. The project default branch.
            let mut rows = conn
                .query(
                    "SELECT p.default_branch FROM projects p
                     JOIN issues i ON i.project_id = p.id
                     WHERE i.id = ?1 LIMIT 1",
                    params![issue_id.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                if let Some(default) = row.opt_text(0)?.filter(|d| !d.is_empty()) {
                    return Ok(Some(default));
                }
            }
            Ok(None)
        })
    })
    .await
    .ok()
    .flatten()
}

/// Kill every live interpreter belonging to these jobs. The durable rows stay:
/// teardown ends the process, not the record, so the transcript remains readable
/// until the job's rows cascade away with it.
async fn kill_repls_for_jobs(orch: &Orchestrator, job_ids: &[String]) {
    let mut killed = false;
    for (_job_id, slug, session) in orch.repl_state.remove_for_jobs(job_ids) {
        let repl_id = session.repl_id.clone();
        let generation = session.generation;
        session.stop_and_release(orch).await;
        if let Err(error) = crate::mcp::handlers::repl::store::mark_exited(
            &orch.db.local,
            &repl_id,
            generation,
            crate::mcp::handlers::repl::store::ReplExitReason::Closed,
        )
        .await
        {
            tracing::warn!(%error, %slug, "failed to mark torn-down REPL exited");
        }
        killed = true;
    }
    if killed {
        crate::mcp::handlers::repl::emit_repl_change(orch, "update");
    }
}

/// Kill running PTY sessions for the given jobs and delete their terminal rows.
///
/// Uses the runner's terminal bindings to issue fenced executor stop/release
/// operations before clearing durable rows.
async fn kill_terminals_for_jobs(orch: &Orchestrator, db: &LocalDb, job_ids: &[String]) {
    if job_ids.is_empty() {
        return;
    }

    // Stop live executor-hosted PTYs before releasing their lease authority.
    match load_running_terminals_for_jobs(db, job_ids).await {
        Ok(running) => {
            for (terminal_id, session_id) in &running {
                if let Err(error) =
                    crate::mcp::handlers::terminal::stop_terminal_by_session(orch, session_id).await
                {
                    log::warn!("Teardown: failed to stop terminal {terminal_id}: {error}");
                } else {
                    log::info!("Teardown: stopped terminal {terminal_id} (session {session_id})");
                }
            }
        }
        Err(e) => {
            log::warn!("Teardown: failed to load running terminals: {}", e);
        }
    }

    // A job's terminals are processes inside the job's own execution
    // environment, so there is no separate terminal-owned thing to release: the
    // shells were stopped above, and releasing the job's residency gives back
    // the cell, its checkout, and its footprint in one move. A job holds that
    // residency for as long as it runs, whether or not it ever opened a
    // terminal, so this releases by job identity rather than by what the
    // terminal table happens to record.
    for job_id in job_ids {
        let holder = cairn_common::executor_protocol::ResidencyHolder::Job {
            job_id: job_id.clone(),
        };
        let Some(fence) = orch.fleet.residency_fence(&holder) else {
            continue;
        };
        let result = orch
            .fleet
            .operate_residency(
                orch,
                cairn_common::executor_protocol::ResidencyOperation::Release { fence },
            )
            .await;
        if let cairn_common::executor_protocol::ResidencyResult::Failed { diagnostic, .. } = result
        {
            log::warn!(
                "Teardown: failed to release the execution environment for {job_id}: {diagnostic}"
            );
        }
    }

    // Delete every terminal row for these jobs — the running ones we just killed
    // and any lingering exited rows retained for post-exit reads / exit wakes.
    if let Err(e) = delete_all_terminal_rows_for_jobs(db, job_ids).await {
        log::warn!("Teardown: failed to delete terminal rows: {}", e);
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "delete"}),
    );

    // Node-scoped browsers are torn down with their jobs (project browsers
    // persist). Core cannot touch the live webview handle, so it sends a Close
    // over the channel for the app-side drain task to destroy it, then deletes
    // the rows.
    close_browsers_for_jobs(orch, db, job_ids).await;
}

/// Close live native webviews for the given jobs and delete their browser rows.
///
/// The live `Webview` handles live app-side; core reaches them only by sending
/// [`BrowserCommand::Close`](crate::browsers::BrowserCommand) over the channel.
/// On hosts without a webview layer the channel is `None` and only the rows are
/// cleared.
async fn close_browsers_for_jobs(orch: &Orchestrator, db: &LocalDb, job_ids: &[String]) {
    if job_ids.is_empty() {
        return;
    }
    match crate::browsers::list_running_browsers_for_jobs(db, job_ids).await {
        Ok(running) => {
            for browser in &running {
                orch.browser_network.clear(&browser.id);
                if let Some(tx) = &orch.browser_command_tx {
                    let _ = tx.send(crate::browsers::BrowserCommand::Close {
                        id: browser.id.clone(),
                        label: browser.webview_label.clone(),
                    });
                }
            }
        }
        Err(e) => log::warn!("Teardown: failed to load running browsers: {e}"),
    }
    if let Err(e) = crate::browsers::delete_all_browser_rows_for_jobs(db, job_ids).await {
        log::warn!("Teardown: failed to delete browser rows: {e}");
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_browsers", "action": "delete"}),
    );
}

async fn load_running_terminals_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<Vec<(String, String)>, String> {
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            for job_id in &job_ids {
                let mut rows = conn
                    .query(
                        "SELECT id, session_id FROM job_terminals
                         WHERE job_id = ?1 AND status = 'running'",
                        (job_id.as_str(),),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?));
                }
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| format!("Failed to load running terminals for teardown: {e}"))
}

async fn delete_all_terminal_rows_for_jobs(db: &LocalDb, job_ids: &[String]) -> Result<(), String> {
    let job_ids = job_ids.to_vec();
    db.write(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            for job_id in &job_ids {
                conn.execute(
                    "DELETE FROM job_terminals WHERE job_id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to delete job terminals during teardown: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn project_process_ids_include_thread_and_project_runs() {
        let db = crate::storage::migrated_test_db("project-process-ids.db").await;
        db.execute_batch(
            r#"
            INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES ('ws-process', 'Process', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('project-process', 'ws-process', 'Process', 'PROCESS', '', 1, 1);
            INSERT INTO threads(id, project_id, name, created_at, updated_at)
              VALUES ('thread-process', 'project-process', 'thread', 1, 1);
            INSERT INTO jobs(id, project_id, thread_id, status, created_at, updated_at)
              VALUES ('thread-job', 'project-process', 'thread-process', 'running', 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
              VALUES ('thread-run', 'project-process', 'thread-job', 'running', 1, 1);
            INSERT INTO runs(id, project_id, status, created_at, updated_at)
              VALUES ('project-run', 'project-process', 'running', 1, 1);
            "#,
        )
        .await
        .unwrap();

        let (mut job_ids, mut run_ids) = project_process_ids(&db, "project-process").await.unwrap();
        job_ids.sort();
        run_ids.sort();
        assert_eq!(job_ids, ["thread-job"]);
        assert_eq!(run_ids, ["project-run", "thread-run"]);
    }

    async fn insert_event(db: &LocalDb, id: &str, seq: i64, event_type: &str, data: &str) {
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES (?1, 'run', ?2, ?2, ?3, ?4, ?2)",
            params![id, seq, event_type, data],
        )
        .await
        .unwrap();
    }

    async fn event_columns(db: &LocalDb, id: &str) -> (Option<String>, Option<String>) {
        db.query_one(
            "SELECT storage_mode, content_commit FROM events WHERE id = ?1",
            (id.to_string(),),
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
    }

    /// Resolving an issue archives its execution against the branch's live
    /// range in the project's jj store, before the branch is deleted: the
    /// sealed write and a read of its result become git coordinates and the
    /// execution's pack is recorded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(jj)]
    async fn issue_teardown_archives_against_the_live_branch() {
        use crate::storage::event_fixture::{
            assistant_read, assistant_write, render_targets, tool_result, write_result,
        };
        let Some(bin) = crate::jj::tests::jj_bin() else {
            eprintln!(
                "skipping issue_teardown_archives_against_the_live_branch: jj not resolvable"
            );
            return;
        };
        crate::mcp::handlers::read::register_archived_file_renderer();
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let workspaces = tempfile::tempdir().unwrap();
        crate::jj::tests::init_project(project.path());
        let jj = crate::jj::environment(&bin, home.path());
        let store = crate::jj::project_store_dir(home.path(), project.path());
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();
        let branch = "agent/archive-teardown";
        let node = workspaces.path().join("node");
        crate::jj::add_workspace(&jj, &store, &node, branch, "main", None).unwrap();
        std::fs::write(node.join("branch.rs"), "branch work\n").unwrap();
        // The files `assistant_write` targets: archival only rewrites a write
        // whose changes the sealed commit's diff can regenerate.
        std::fs::write(node.join("a.txt"), "HEAVYPAYLOADALPHA\n").unwrap();
        std::fs::write(node.join("c.txt"), "HEAVYPAYLOADNEWFILE\n").unwrap();
        crate::jj::seal(&jj, &node, "branch work", None).unwrap();
        let sealed = crate::jj::head_commit(&jj, &node).unwrap();

        let db = crate::storage::migrated_test_db("teardown-archival.db").await;
        db.execute_script(format!(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj','ws','P','p','{repo}','main',1,1);
             INSERT INTO issues(id, project_id, number, title, status, completed_at, created_at, updated_at)
               VALUES ('issue','proj',1,'T','closed',5,1,5);
             INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','complete',1);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, branch, base_branch, status, created_at, updated_at)
               VALUES ('job','exec','proj','issue','{branch}','main','complete',1,1);
             INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
               VALUES ('run','job','proj','exited',1,1);",
            repo = project.path().display(),
        ))
        .await
        .unwrap();
        let stored = render_targets(&[("file:branch.rs", b"branch work\n")]);
        insert_event(&db, "a-w1", 1, "assistant", &assistant_write("w1")).await;
        insert_event(&db, "e-w1", 2, "tool_result", &write_result("w1", &sealed)).await;
        insert_event(
            &db,
            "a-r1",
            3,
            "assistant",
            &assistant_read("r1", &["file:branch.rs"]),
        )
        .await;
        insert_event(&db, "e-r1", 4, "tool_result", &tool_result("r1", &stored)).await;

        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(home.path().join("search")).unwrap(),
        );
        let db_state =
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search));
        // Archival runs before the discarded branch is deleted; the mock git
        // client reports the branch already gone so deletion is a no-op.
        let mut git = crate::services::testing::MockGitClient::new();
        git.expect_branch_exists().returning(|_, _| Ok(false));
        let orch = crate::orchestrator::OrchestratorBuilder::new(
            db_state,
            std::sync::Arc::new(
                crate::services::testing::TestServicesBuilder::new()
                    .with_git(git)
                    .build(),
            ),
            home.path().to_path_buf(),
        )
        .jj_binary_path(bin.clone())
        .build();

        cleanup_issue_jobs(
            &orch,
            TeardownScope::Issue("issue".to_string()),
            TeardownReason::Discarded,
        )
        .await
        .unwrap();

        let db = &orch.db.local;
        assert_eq!(
            event_columns(db, "a-w1").await,
            (Some("gitcoord".to_string()), Some(sealed.clone())),
            "the sealed write is addressed by its commit"
        );
        assert_eq!(
            event_columns(db, "e-r1").await,
            (Some("gitcoord".to_string()), Some(sealed.clone())),
            "the read reproduces at the sealed tip"
        );
        let tip = db
            .query_one(
                "SELECT tip_sha FROM execution_history WHERE execution_id = 'exec'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(tip, sealed);
    }

    /// A resolved issue's branch is retired only as its landing recorded it,
    /// under the store's own exclusions, and only when nothing still uses it:
    /// busy maintenance, a git ref the bookmark does not account for, an owner
    /// commit after the landing, a row naming no commit with ancestry saying no,
    /// live work cut from the branch, and the project default all keep it. A
    /// retirement removes the bookmark and the git ref together.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(jj)]
    async fn a_resolved_branch_is_retired_only_as_its_landing_recorded_it() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            eprintln!("skipping: jj not resolvable");
            return;
        };
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let workspaces = tempfile::tempdir().unwrap();
        crate::jj::tests::init_project(project.path());
        let jj = crate::jj::environment(&bin, home.path());
        let store = crate::jj::project_store_dir(home.path(), project.path());
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();
        let sealed = |branch: &str, file: &str| {
            let node = workspaces.path().join(branch.replace('/', "-"));
            if !node.exists() {
                crate::jj::add_workspace(&jj, &store, &node, branch, "main", None).unwrap();
            }
            std::fs::write(node.join(file), format!("{file}\n")).unwrap();
            crate::jj::seal(&jj, &node, file, None).unwrap();
            crate::jj::head_commit(&jj, &node).unwrap()
        };
        let (landed, moved, unrecorded) = ("agent/landed", "agent/moved", "agent/unrecorded");
        let landed_at = sealed(landed, "landed.rs");
        let moved_at = sealed(moved, "moved.rs");
        sealed(moved, "after-the-landing.rs");
        sealed(unrecorded, "unrecorded.rs");
        jj.run(&store, &["git", "export"], "export the fixture")
            .unwrap();
        let git_dir = crate::jj::store_git_dir(&store).unwrap();
        let update_ref = |branch: &str, to: &str| {
            let status = std::process::Command::new("git")
                .arg(format!("--git-dir={}", git_dir.display()))
                .args(["update-ref", &format!("refs/heads/{branch}"), to])
                .status()
                .unwrap();
            assert!(status.success());
        };

        let db = crate::storage::migrated_test_db("teardown-retire.db").await;
        db.execute_script(format!(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj','ws','P','p','{repo}','main',1,1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue','proj',1,'T','merged',1,1), ('other','proj',2,'O','active',1,1);
             INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch,
                                        status, opened_at, updated_at, merged_at, merged_commit, merged_source_commit)
               VALUES ('mr-l','job-l','proj','issue','PR','{landed}','main','merged',1,1,2,'x','{landed_at}'),
                      ('mr-m','job-m','proj','issue','PR','{moved}','main','merged',1,1,2,'x','{moved_at}'),
                      ('mr-u','job-u','proj','issue','PR','{unrecorded}','main','merged',1,1,2,'x',NULL);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, branch, base_branch, status, created_at, updated_at)
               VALUES ('cut','exec','proj','other','agent/cut','{unrecorded}','running',1,1);",
            repo = project.path().display(),
        ))
        .await
        .unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(home.path().join("search")).unwrap(),
        );
        let db_state =
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search));
        let orch = crate::orchestrator::OrchestratorBuilder::new(
            db_state,
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            home.path().to_path_buf(),
        )
        .jj_binary_path(bin.clone())
        .build();
        let repo = project.path().to_str().unwrap().to_string();
        let retire = |branch: &str, reason: TeardownReason| {
            let target = BranchTarget {
                project_id: "proj".into(),
                branch: branch.into(),
                repo_path: repo.clone(),
                job_ids: Vec::new(),
            };
            let orch = &orch;
            async move { retire_branch(orch, &orch.db.local, "issue", &target, reason).await }
        };
        let state = |branch: &str| crate::jj::read_local_branch_state(&store, branch).unwrap();

        {
            let _maintenance = crate::jj::testing::hold_store_reclamation(&store).unwrap();
            retire(landed, TeardownReason::Merged).await;
        }
        assert!(
            state(landed).bookmark.is_some(),
            "busy maintenance keeps it"
        );

        let main = crate::jj::bookmark_commit(&jj, &store, "main").unwrap();
        update_ref(landed, &main);
        retire(landed, TeardownReason::Merged).await;
        assert!(
            state(landed).bookmark.is_some(),
            "a git ref its bookmark does not account for keeps it"
        );
        update_ref(landed, &landed_at);

        retire(landed, TeardownReason::Merged).await;
        let gone = state(landed);
        assert_eq!(
            (gone.bookmark, gone.git_ref),
            (None, None),
            "retired as its landing recorded it"
        );

        retire(moved, TeardownReason::Merged).await;
        assert!(
            state(moved).bookmark.is_some(),
            "an owner commit after the landing keeps it"
        );

        retire(unrecorded, TeardownReason::Discarded).await;
        assert!(
            state(unrecorded).bookmark.is_some(),
            "live work cut from it keeps it, even discarded"
        );
        orch.db
            .local
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'cut'", ())
            .await
            .unwrap();
        retire(unrecorded, TeardownReason::Merged).await;
        assert!(
            state(unrecorded).bookmark.is_some(),
            "no recorded commit, and ancestry says it has not landed"
        );
        retire(unrecorded, TeardownReason::Discarded).await;
        assert!(
            state(unrecorded).bookmark.is_none(),
            "discarded work needs no landing proof"
        );

        retire("main", TeardownReason::Discarded).await;
        assert!(
            state("main").bookmark.is_some(),
            "the project default is never retired"
        );

        // A write to the branch whose acceptance is unsettled: its content is
        // not known, so the branch owes a repair and is not retired.
        let owed = sealed("agent/owed", "owed.rs");
        jj.run(&store, &["git", "export"], "export the owed branch")
            .unwrap();
        let key =
            crate::orchestrator::base_advance::preparation::journal_store_key(&store).unwrap();
        orch.db
            .local
            .execute(
                "INSERT INTO branch_advance_acceptances
                   (id, project_id, store_path, branch, head_before, head_after, base_branch,
                    base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
                    status, created_at)
                 VALUES ('owed', 'proj', ?1, 'agent/owed', 'b', 'a', 'main', 'm', 'op', '[]', '[]',
                         'unknown', 1)",
                params![key],
            )
            .await
            .unwrap();
        retire("agent/owed", TeardownReason::Discarded).await;
        assert_eq!(
            state("agent/owed").bookmark.as_deref(),
            Some(owed.as_str()),
            "an unsettled write keeps the branch, even discarded"
        );

        // A recovery fence on the branch keeps it: recording a recovery takes
        // the store lock retirement holds, and the fenced head is the repair's.
        let fenced = sealed("agent/fenced", "fenced.rs");
        jj.run(&store, &["git", "export"], "export the fenced branch")
            .unwrap();
        let fenced_state = cairn_vcs::BookmarkState::normal(&fenced);
        crate::orchestrator::branch_recovery::record_recoveries(
            &orch.db.local,
            "proj",
            &store,
            Some("agent/fenced"),
            crate::orchestrator::branch_recovery::BUDGET_AUTOMATIC,
            "fixture",
            vec![crate::jj::RecoveryCoordinates {
                scope: crate::jj::RecoveryScope::Branch,
                phase: crate::jj::RollbackFailurePhase::ExportUnverified,
                pre_op: None,
                pre_view: None,
                pre_git: None,
                failed_view: None,
                bookmarks: vec![crate::jj::BookmarkRecovery {
                    bookmark: "agent/fenced".into(),
                    pre: fenced_state.clone(),
                    pre_git: Some(fenced.clone()),
                    failed: fenced_state,
                }],
                survivors: vec![],
                evidence: vec!["fixture".into()],
            }],
        )
        .await
        .unwrap();
        retire("agent/fenced", TeardownReason::Discarded).await;
        assert_eq!(
            state("agent/fenced").bookmark.as_deref(),
            Some(fenced.as_str()),
            "a head a recovery fence holds is kept, even discarded"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn teardown_preserves_a_head_held_by_a_recovery_fence() {
        use crate::orchestrator::branch_recovery;
        let home = tempfile::tempdir().unwrap();
        let db = crate::storage::migrated_test_db("teardown-recovery-fence.db").await;
        let repo = home.path().join("repo");
        db.execute_script(format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'P', 'p', '{}', 'main', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue', 'proj', 1, 'T', 'merged', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, branch, status, created_at, updated_at)
             VALUES ('job', 'proj', 'issue', 'agent/fenced', 'complete', 1, 1);",
            repo.to_string_lossy().replace('\'', "''"),
        )).await.unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(home.path().join("search")).unwrap(),
        );
        let mut git = crate::services::testing::MockGitClient::new();
        git.expect_branch_exists().times(0);
        let orch = crate::orchestrator::OrchestratorBuilder::new(
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search)),
            std::sync::Arc::new(
                crate::services::testing::TestServicesBuilder::new()
                    .with_git(git)
                    .build(),
            ),
            home.path().to_path_buf(),
        )
        .build();
        let store = crate::jj::project_store_dir(home.path(), &repo);
        let state = cairn_vcs::BookmarkState::normal(&"a".repeat(40));
        let coordinates = crate::jj::RecoveryCoordinates {
            scope: crate::jj::RecoveryScope::Branch,
            phase: crate::jj::RollbackFailurePhase::ExportUnverified,
            pre_op: None,
            pre_view: None,
            pre_git: None,
            failed_view: None,
            bookmarks: vec![crate::jj::BookmarkRecovery {
                bookmark: "agent/fenced".into(),
                pre: state.clone(),
                pre_git: Some("a".repeat(40)),
                failed: state,
            }],
            survivors: vec![],
            evidence: vec!["fixture".into()],
        };
        branch_recovery::record_recoveries(
            &orch.db.local,
            "proj",
            &store,
            Some("agent/fenced"),
            branch_recovery::BUDGET_AUTOMATIC,
            "fixture",
            vec![coordinates],
        )
        .await
        .unwrap();
        cleanup_issue_jobs(
            &orch,
            TeardownScope::DeletedIssue("issue".into()),
            TeardownReason::Discarded,
        )
        .await
        .unwrap();
        assert_eq!(
            branch_recovery::fences(&orch.db.local, &store)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// The owner lookup binds project and branch. Without an index leading
    /// with both it reads every job in the project; results are the same
    /// either way, so only the plan witnesses the regression.
    #[tokio::test]
    async fn protection_seeks_jobs_on_project_and_branch() {
        let db = crate::storage::migrated_test_db("teardown-protection-plan.db").await;
        let plan =
            crate::storage::query_plan(&db, PROTECTION_SQL, vec!["p".into(), "agent/x".into()])
                .await;
        assert!(
            plan.lines().any(|step| step
                == "SEARCH j USING INDEX idx_jobs_project_branch (project_id=? AND branch=?)"),
            "the owner lookup must seek jobs on project and branch, got\n{plan}"
        );
    }

    /// Deleting an issue deletes its events with it, so teardown skips archival.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleted_issue_teardown_does_not_archive() {
        let home = tempfile::tempdir().unwrap();
        let db = crate::storage::migrated_test_db("teardown-delete-no-archival.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj','ws','P','p','/nonexistent','main',1,1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue','proj',1,'T','active',1,1);
             INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','complete',1);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, status, created_at, updated_at)
               VALUES ('job','exec','proj','issue','complete',1,1);
             INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
               VALUES ('run','job','proj','exited',1,1);",
        )
        .await
        .unwrap();
        insert_event(
            &db,
            "u1",
            1,
            "user",
            &crate::storage::event_fixture::user_text("hi"),
        )
        .await;
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(home.path().join("search")).unwrap(),
        );
        let db_state =
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search));
        let orch = crate::orchestrator::OrchestratorBuilder::new(
            db_state,
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            home.path().to_path_buf(),
        )
        .build();

        cleanup_issue_jobs(
            &orch,
            TeardownScope::DeletedIssue("issue".to_string()),
            TeardownReason::Discarded,
        )
        .await
        .unwrap();
        assert_eq!(
            event_columns(&orch.db.local, "u1").await.0.as_deref(),
            Some("full")
        );
    }
}
