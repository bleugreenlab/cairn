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

/// Settle processes independently of the producing job's projected status.
/// Terminal rows live on the runner; job coordinates and durable batches belong
/// to the issue's metadata database. A stop acknowledgement is not an exit event.
pub(crate) async fn settle_issue_resources(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
    resolution: crate::transitions::Resolution,
) -> Vec<crate::issues::status::SettlementResource> {
    use crate::issues::status::{SettlementOutcome, SettlementResource};
    // Read before anything settles: an acquisition, re-resolution or
    // reopen-then-resolve that enrols the issue while this pass runs writes a
    // new token, and that enrolment must survive our clear.
    let enrolment = crate::execution::resolution_cleanups::issue_enrolment(db, issue_id).await;
    let issue_uri = db
        .query_opt_text(
            "SELECT 'cairn://p/' || p.key || '/' || i.number FROM issues i
         JOIN projects p ON p.id=i.project_id WHERE i.id=?1",
            (issue_id.to_string(),),
        )
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "cairn://".into());
    let job_ids = match issue_resource_jobs(db, issue_id).await {
        Ok(ids) => ids,
        Err(error) => {
            return vec![SettlementResource {
                uri: issue_uri.clone(),
                outcome: SettlementOutcome::CleanupPending,
                diagnostic: Some(error),
            }]
        }
    };
    let mut resources = Vec::new();
    let retention = match issue_output_jobs(db, issue_id).await {
        Ok(output_jobs) => {
            retain_terminal_outputs(
                orch,
                &output_jobs,
                OutputRetention::Resolved {
                    resolved_at: issue_resolved_at(db, issue_id).await,
                },
            )
            .await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = retention {
        resources.push(SettlementResource {
            uri: issue_uri.clone(),
            outcome: SettlementOutcome::CleanupPending,
            diagnostic: Some(format!("terminal output retention pending: {error}")),
        });
    }
    for job_id in &job_ids {
        let home = crate::jobs::queries::home_uri_for_job(db, job_id)
            .await
            .ok()
            .flatten();
        let terminals = orch
            .db
            .local
            .query_all(
                "SELECT session_id, COALESCE(slug,id), status FROM job_terminals WHERE job_id=?1",
                (job_id.clone(),),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await;
        match terminals {
            Ok(rows) => {
                for (session, slug, status) in rows {
                    let uri = match &home {
                        Some(home) => format!("{home}/terminal/{slug}"),
                        None => issue_uri.clone(),
                    };
                    let already_exited = status == "exited";
                    let result = if already_exited {
                        Ok(crate::mcp::handlers::terminal::StopOutcome::AlreadyEnded)
                    } else {
                        crate::mcp::handlers::terminal::stop_terminal_for_resolution(
                            orch, &session, resolution,
                        )
                        .await
                    };
                    // Only the exit event (or liveness reconciliation) finalizes output.
                    let ended = orch
                        .db
                        .local
                        .query_opt(
                            "SELECT status FROM job_terminals WHERE session_id=?1",
                            (session.clone(),),
                            |row| row.text(0),
                        )
                        .await;
                    let proven = matches!(ended, Ok(Some(ref state)) if state == "exited");
                    let (outcome, diagnostic) = if proven {
                        match crate::mcp::handlers::terminal::finalize_exited_terminal_capture(
                            orch, &session,
                        )
                        .await
                        {
                            Ok(note) => (
                                if already_exited {
                                    SettlementOutcome::AlreadySettled
                                } else {
                                    SettlementOutcome::Stopped
                                },
                                note,
                            ),
                            Err(error) => (SettlementOutcome::CleanupPending, Some(error)),
                        }
                    } else {
                        (
                            SettlementOutcome::CleanupPending,
                            Some(result.err().unwrap_or_else(|| {
                                "stop requested; waiting for process exit and final output".into()
                            })),
                        )
                    };
                    resources.push(SettlementResource {
                        uri,
                        outcome,
                        diagnostic,
                    });
                }
            }
            Err(error) => resources.push(SettlementResource {
                uri: home.clone().unwrap_or_else(|| issue_uri.clone()),
                outcome: SettlementOutcome::CleanupPending,
                diagnostic: Some(error.to_string()),
            }),
        }
    }
    resources.extend(
        crate::mcp::handlers::run::cancel::settle_owned_batches(
            orch, db, &job_ids, resolution, &issue_uri,
        )
        .await,
    );
    match crate::execution::checks::requests::settle_requests_for_resolution(
        orch, db, &job_ids, resolution,
    )
    .await
    {
        Ok(checks) => resources.extend(checks),
        Err(error) => resources.push(SettlementResource {
            uri: issue_uri.clone(),
            outcome: SettlementOutcome::CleanupPending,
            diagnostic: Some(format!("check-request settlement pending: {error}")),
        }),
    }
    resources.extend(release_settled_job_environments(orch, db, &job_ids, &issue_uri).await);
    let settled = !resources
        .iter()
        .any(|resource| resource.outcome == SettlementOutcome::CleanupPending);
    if let (true, Ok(Some(enrolment))) = (settled, enrolment) {
        // An agent process whose job has no environment is not reported
        // above, but is still owed its stop until it exits.
        match live_processes(orch, db, &job_ids).await {
            Ok(live) if live.is_empty() => {
                if let Err(error) =
                    crate::execution::resolution_cleanups::clear_issue(db, issue_id, &enrolment)
                        .await
                {
                    log::warn!(
                        "Could not retire settled resolution cleanup for {issue_id}: {error}"
                    );
                }
            }
            Ok(_) => {}
            Err(error) => log::warn!("Resolution process census pending for {issue_id}: {error}"),
        }
    }
    resources
}

/// The jobs among `job_ids` with an agent process starting, live, or retained
/// by this runner.
async fn live_processes(
    orch: &Orchestrator,
    db: &LocalDb,
    job_ids: &[String],
) -> Result<Vec<String>, String> {
    let retained = orch.process_state.run_ids();
    let mut live = Vec::new();
    for job_id in job_ids {
        let runs = db
            .query_all(
                "SELECT id, status FROM runs WHERE job_id=?1",
                (job_id.clone(),),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        if runs.iter().any(|(id, status)| {
            matches!(status.as_deref(), Some("starting" | "live")) || retained.contains(id)
        }) {
            live.push(job_id.clone());
        }
    }
    Ok(live)
}

/// Stopping branch-dependent work does not resolve its owning issue or start
/// the expiry clock for that other issue's retained output.
async fn issue_output_jobs(db: &LocalDb, issue_id: &str) -> Result<Vec<String>, String> {
    db.query_all(
        "WITH RECURSIVE owned(id) AS (
             SELECT id FROM jobs WHERE issue_id=?1
             UNION SELECT j.id FROM jobs j JOIN owned o ON j.parent_job_id=o.id
                 WHERE j.issue_id IS NULL OR j.issue_id=?1
         ) SELECT id FROM owned",
        (issue_id.to_string(),),
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

async fn issue_resource_jobs(db: &LocalDb, issue_id: &str) -> Result<Vec<String>, String> {
    db.query_all(
        "WITH RECURSIVE owned(id) AS (
             SELECT j.id FROM jobs j WHERE j.issue_id=?1 OR (
                 j.issue_id IS NOT NULL AND EXISTS (
                     SELECT 1 FROM jobs source WHERE source.issue_id=?1
                         AND source.project_id=j.project_id AND source.branch=j.base_branch
                 )
             )
             UNION SELECT j.id FROM jobs j JOIN owned o ON j.parent_job_id=o.id
         ) SELECT id FROM owned",
        (issue_id.to_string(),),
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

/// Release only environments whose output and execution owners have settled.
/// The residency remains the retry evidence while any boundary is unconfirmed.
pub(crate) async fn release_settled_job_environments(
    orch: &Orchestrator,
    db: &LocalDb,
    job_ids: &[String],
    fallback_uri: &str,
) -> Vec<crate::issues::status::SettlementResource> {
    use crate::issues::status::{SettlementOutcome, SettlementResource};
    use cairn_common::executor_protocol::ResidencyHolder;
    let mut resources = Vec::new();
    for job_id in job_ids {
        let holder = ResidencyHolder::Job {
            job_id: job_id.clone(),
        };
        let pending = job_environment_pending(orch, db, job_id).await;
        let diagnostic = match pending {
            Ok(Some(reason)) => Some(reason),
            Err(error) => Some(error),
            Ok(None) => {
                if let Some(fence) = orch.fleet.residency_fence(&holder) {
                    if let Some(occupancy) = orch.fleet.residency_occupancy(&holder) {
                        if !occupancy.is_quiet() {
                            Some("execution environment still has in-flight work or resident processes".into())
                        } else {
                            crate::fleet::residency::release(orch, &fence).await.err()
                        }
                    } else {
                        Some("execution environment occupancy is not confirmed".into())
                    }
                } else if orch.fleet.residency_route_executor(&holder).is_some() {
                    Some("execution environment is not currently answering".into())
                } else {
                    continue;
                }
            }
        };
        let home = crate::jobs::queries::home_uri_for_job(db, job_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| fallback_uri.to_string());
        resources.push(SettlementResource {
            uri: home,
            outcome: if diagnostic.is_some() {
                SettlementOutcome::CleanupPending
            } else {
                SettlementOutcome::Stopped
            },
            diagnostic,
        });
    }
    resources
}

async fn job_environment_pending(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<String>, String> {
    if !resolution_calls_pending(db, job_id).await?.is_empty() {
        return Ok(Some(
            "cancelled turn tool-call settlement is not confirmed".into(),
        ));
    }
    let runs = db
        .query_all(
            "SELECT id, status FROM runs WHERE job_id=?1",
            (job_id.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    {
        let responses = orch.resolution_responses.lock().unwrap();
        if runs.iter().any(|(id, _)| responses.contains_key(id)) {
            return Ok(Some(
                "waiting for the resolution tool response transport".into(),
            ));
        }
    }
    let retained = orch.process_state.run_ids();
    if runs
        .iter()
        .any(|(id, status)| status == "starting" || status == "live" || retained.contains(id))
    {
        return Ok(Some("agent process cleanup is not confirmed".into()));
    }
    let terminal_pending = orch
        .db
        .local
        .query_opt(
            "SELECT 1 FROM job_terminals WHERE job_id=?1 AND status!='exited'
         UNION ALL SELECT 1 FROM terminal_outputs WHERE job_id=?1 AND capture='capturing'
         LIMIT 1",
            (job_id.to_string(),),
            |row| row.i64(0),
        )
        .await
        .map_err(|error| error.to_string())?
        .is_some();
    if terminal_pending {
        return Ok(Some(
            "terminal exit or output capture is not finalized".into(),
        ));
    }
    let batches = crate::mcp::handlers::run::detached::all_running(db).await?;
    if batches
        .iter()
        .any(|batch| batch.job_id == job_id && batch.holds_branch)
        || crate::mcp::handlers::run::detached::live_environment_owners()
            .iter()
            .any(|(_, owner)| owner.job_id == job_id)
    {
        return Ok(Some("batch cleanup is not confirmed".into()));
    }
    let parked = db
        .query_all(
            "SELECT condition_json FROM agent_waits WHERE job_id=?1
         AND state IN ('pending','resolving') AND kind='run_batch'",
            (job_id.to_string(),),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    for json in parked {
        let condition: crate::mcp::handlers::durable_suspend::Condition =
            serde_json::from_str(&json).map_err(|error| error.to_string())?;
        if condition.holds_branch() {
            return Ok(Some("parked batch cleanup is not confirmed".into()));
        }
    }
    Ok(None)
}

/// Existing cancelled turns and unanswered transcript calls survive process
/// finalization. Scope retries to those turns, never to the job that may have
/// acquired a new session after reopening.
async fn resolution_calls_pending(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let turns = db
        .query_all(
            "SELECT id,run_id,end_reason FROM turns
         WHERE job_id=?1 AND state='cancelled' AND end_reason IN ('issue_merged','issue_closed')
           AND run_id IS NOT NULL",
            (job_id.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut pending = Vec::new();
    for (turn, run, reason) in turns {
        if !crate::orchestrator::lifecycle::open_call_names(db, &run, &turn)
            .await
            .map_err(|error| error.to_string())?
            .is_empty()
        {
            pending.push((turn, run, reason));
        }
    }
    Ok(pending)
}

/// Retry the resolution cleanups still owed, as `resolution_cleanups` records
/// them. Calls are settled before resources, and live processes are stopped
/// before resources are settled. An idle pass is one read per database.
pub(crate) async fn reconcile_resolved_resources(orch: &Orchestrator) {
    use crate::execution::resolution_cleanups::{self, Cleanup};
    use std::time::Instant;
    for db in orch.db.all_dbs().await {
        let started = Instant::now();
        let owed = match resolution_cleanups::pending(&db).await {
            Ok(owed) => owed,
            Err(error) => {
                log::warn!("Could not read owed resolution cleanups: {error}");
                continue;
            }
        };
        let read_ms = started.elapsed().as_millis() as u64;
        let db_name = db.path().display().to_string();
        if owed.is_empty() {
            tracing::debug!(target: "execution::teardown", db = %db_name, read_ms,
                trigger = "terminal_lifecycle", "resolved resource reconcile pass");
            continue;
        }
        let (mut calls, mut issues) = (0usize, 0usize);
        let calls_started = Instant::now();
        for cleanup in &owed {
            let Cleanup::Calls {
                turn_id,
                run_id,
                resolution,
            } = cleanup
            else {
                continue;
            };
            calls += 1;
            if orch
                .resolution_responses
                .lock()
                .unwrap()
                .contains_key(run_id)
            {
                continue;
            }
            settle_resolution_calls(orch, &db, turn_id, run_id, *resolution).await;
        }
        let calls_ms = calls_started.elapsed().as_millis() as u64;
        let settle_started = Instant::now();
        for cleanup in &owed {
            let Cleanup::Issue {
                issue_id,
                resolution,
                generation,
                token,
            } = cleanup
            else {
                continue;
            };
            issues += 1;
            retry_issue_cleanup(orch, &db, issue_id, *resolution, *generation, token).await;
        }
        let settle_ms = settle_started.elapsed().as_millis() as u64;
        tracing::info!(target: "execution::teardown", db = %db_name, issues, calls, read_ms,
            calls_ms, settle_ms, total_ms = started.elapsed().as_millis() as u64,
            trigger = "terminal_lifecycle", "resolved resource reconcile pass");
    }
}

/// Answer a resolution-cancelled turn's open calls, and retire its entry once
/// none is open.
async fn settle_resolution_calls(
    orch: &Orchestrator,
    db: &LocalDb,
    turn_id: &str,
    run_id: &str,
    resolution: crate::transitions::Resolution,
) {
    use crate::orchestrator::lifecycle::{open_call_names, settle_unanswered_calls};
    let answered = || async {
        open_call_names(db, run_id, turn_id)
            .await
            .map(|names| names.is_empty())
    };
    match answered().await {
        Ok(true) => {}
        Ok(false) => {
            if let Err(error) = settle_unanswered_calls(
                orch,
                run_id,
                turn_id,
                crate::orchestrator::lifecycle::InterruptCause::Resolution {
                    exit_reason: crate::orchestrator::lifecycle::resolution_exit_reason(resolution)
                        .to_string(),
                },
            ) {
                log::warn!("Resolution call cleanup pending for turn {turn_id}: {error}");
                return;
            }
            if !matches!(answered().await, Ok(true)) {
                return;
            }
        }
        Err(error) => {
            log::warn!("Could not read open calls of turn {turn_id}: {error}");
            return;
        }
    }
    if let Err(error) = crate::execution::resolution_cleanups::clear_calls(db, turn_id).await {
        log::warn!("Could not retire answered resolution calls of turn {turn_id}: {error}");
    }
}

/// Retry one resolved issue's settlement, if it still reads resolved at the
/// generation its entry names; otherwise the entry is stale and goes.
async fn retry_issue_cleanup(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
    resolution: crate::transitions::Resolution,
    generation: i64,
    token: &str,
) {
    let current = db
        .query_opt(
            "SELECT status, resolution_generation FROM issues WHERE id=?1",
            (issue_id.to_string(),),
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await;
    let current = match current {
        Ok(current) => current,
        Err(error) => {
            log::warn!("Could not read resolved issue {issue_id}: {error}");
            return;
        }
    };
    let still_owed = current
        .as_ref()
        .is_some_and(|(status, current_generation)| {
            crate::execution::resolution_cleanups::parse_resolution(status) == Some(resolution)
                && *current_generation == generation
        });
    if !still_owed {
        if let Err(error) =
            crate::execution::resolution_cleanups::clear_issue(db, issue_id, token).await
        {
            log::warn!("Could not retire stale resolution cleanup for {issue_id}: {error}");
        }
        return;
    }
    match issue_resource_jobs(db, issue_id).await {
        Ok(job_ids) => match live_processes(orch, db, &job_ids).await {
            Ok(live) => {
                for job in live {
                    if let Err(error) = crate::orchestrator::lifecycle::stop_job_for_resolution(
                        orch, &job, resolution,
                    ) {
                        log::warn!("Resolution process cleanup pending for {job}: {error}");
                    }
                }
            }
            Err(error) => log::warn!("Resolution process census pending for {issue_id}: {error}"),
        },
        Err(error) => log::warn!("Could not list resolved issue {issue_id}'s jobs: {error}"),
    }
    let _ = settle_issue_resources(orch, db, issue_id, resolution).await;
}

/// A delegated child may have no issue, and branch-dependent work may belong
/// to a different, still-open issue. Both retain their source resolution.
///
/// The issue lookup is a seek per source (`i.id IN (...)`), not a join that
/// re-walks the sources for every issue.
pub(crate) async fn resolved_resource_issues(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<(String, String)>, String> {
    db.query_all(RESOLVED_RESOURCE_ISSUES_SQL, (job_id.to_string(),), |row| {
        Ok((row.text(0)?, row.text(1)?))
    })
    .await
    .map_err(|error| error.to_string())
}

/// [`resolved_resource_issues`]' statement, binding the job id as `?1`.
pub(crate) const RESOLVED_RESOURCE_ISSUES_SQL: &str =
    "WITH RECURSIVE ancestors(id,issue_id,parent_job_id,project_id,base_branch) AS (
             SELECT id,issue_id,parent_job_id,project_id,base_branch FROM jobs WHERE id=?1
             UNION SELECT j.id,j.issue_id,j.parent_job_id,j.project_id,j.base_branch
                 FROM jobs j JOIN ancestors a ON j.id=a.parent_job_id
         ), sources(issue_id) AS (
             SELECT issue_id FROM ancestors
             UNION SELECT j.issue_id FROM jobs j JOIN ancestors a
                 ON j.project_id=a.project_id AND j.branch=a.base_branch
         ) SELECT i.id,i.status FROM issues i
             WHERE i.id IN (SELECT issue_id FROM sources) AND i.status IN ('merged','closed')";

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

    let resolution = match reason {
        TeardownReason::Merged => crate::transitions::Resolution::Merged,
        TeardownReason::Discarded => crate::transitions::Resolution::Closed,
    };
    let resources = if archive {
        settle_issue_resources(orch, &db, &issue_id, resolution).await
    } else {
        kill_terminals_for_jobs(orch, &db, &job_ids, OutputRetention::Deleted).await;
        Vec::new()
    };
    let cleanup_pending = resources.iter().any(|resource| {
        resource.outcome == crate::issues::status::SettlementOutcome::CleanupPending
    });
    if archive {
        close_browsers_for_jobs(orch, &db, &job_ids).await;
    }
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
    // Retained terminal output is independent of scratch, but an unconfirmed
    // exit or an in-flight batch may still own capture state or a recoverable delta.
    let has_terminals = match orch
        .db
        .local
        .query_all(
            "SELECT DISTINCT job_id FROM job_terminals WHERE job_id IS NOT NULL AND status != 'exited'",
            (),
            |row| row.text(0),
        )
        .await
    {
        Ok(owners) => owners.iter().any(|owner| job_ids.contains(owner)),
        Err(_) => true,
    };
    if !cleanup_pending && !has_terminals {
        for job_id in &job_ids {
            crate::scratch::remove_job_scratch_dir(job_id);
        }
        crate::node_scratch::lifecycle::close_for_issue(orch, &issue_id).await;
    }

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
    let outcome = retire_local_copies(
        orch,
        db,
        &guard,
        &reservation,
        &target.project_id,
        branch,
        issue_id,
        merge_target.as_deref(),
    )
    .await;
    // The lock ends with the local half; the reservation outlives it, through
    // the remote delete and its record.
    drop(guard);
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(kept) => return keep(kept.why),
    };
    match outcome {
        LocalRetirement::Retired {
            tracked_origin: Some(origin),
        } => {
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
        LocalRetirement::Retired {
            tracked_origin: None,
        } => log::info!("Teardown: retired `{branch}`; origin has no tracked copy to retire"),
        LocalRetirement::Kept(why) => keep(why),
        LocalRetirement::Partial(why) => {
            log::warn!("Teardown: {why}; its origin copy is kept")
        }
    }
}

/// Why a retirement kept a branch: a stable key to count kept branches by,
/// and the sentence that says why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Kept {
    pub(crate) reason: &'static str,
    pub(crate) why: String,
}

impl Kept {
    fn new(reason: &'static str, why: impl Into<String>) -> Self {
        Self {
            reason,
            why: why.into(),
        }
    }
}

/// The one judgement of whether a resolved branch's local copies may go, and
/// their retirement when they may. Teardown runs it when an issue resolves;
/// the ref-store prune runs it for branches finished before teardown retired
/// bookmarks at all.
///
/// The caller holds `branch` reserved and then the store lock (`guard`), in
/// that order. `merge_target` is `None` for a discarded (closed) issue, whose
/// branch needs no landing proof; a merged one is retired only as its landing
/// recorded it (see [`retire_locally`]). Only the local copies are touched:
/// a retired branch's origin copy, if the store still tracks one, is named in
/// the result for the caller to retire or leave.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn retire_local_copies(
    orch: &Orchestrator,
    db: &LocalDb,
    guard: &crate::orchestrator::JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
    project_id: &str,
    branch: &str,
    issue_id: &str,
    merge_target: Option<&str>,
) -> Result<LocalRetirement, Kept> {
    let store = guard.store_path().to_path_buf();
    // Recording a recovery takes this same lock, so a fence recorded just
    // before is seen here. A head a fence holds is kept: its coordinates are
    // the repair's, not this retirement's to change.
    crate::orchestrator::branch_recovery::refuse_coordinate_if_fenced(
        orch,
        &store,
        &[branch],
        "Retiring this branch",
    )
    .await
    .map_err(|refusal| Kept::new("fenced", refusal))?;
    // Read under the reservation and the lock: work cut from the branch, or a
    // new owner, may have appeared while this waited for them.
    match protection(db, project_id, branch, merge_target).await {
        Ok(None) => {}
        Ok(Some(why)) => return Err(Kept::new("in-use", why)),
        Err(error) => {
            return Err(Kept::new(
                "use-unknown",
                format!("whether it is still in use is unknown ({error})"),
            ))
        }
    }
    let recorded = match merge_target {
        Some(_) => recorded_source_commit(db, issue_id, branch)
            .await
            .map_err(|error| Kept::new("landing-record-unreadable", error))?,
        None => None,
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    // Admission immediately before the decision and the delete: the branch is
    // reserved by this caller and owes no acceptance (a write whose acceptance
    // is unsettled may or may not be in its bookmark, so its content is not
    // known). Refused means kept, locally and on origin.
    let permit = guard
        .branch_write_permit(
            db,
            reservation,
            project_id,
            branch,
            crate::orchestrator::BatchAdmission::None,
        )
        .await
        .map_err(|refusal| Kept::new("not-admitted", refusal.to_string()))?;
    // The judgement and the delete are blocking jj and git work; the permit
    // borrows the guard, so they run in place on this worker thread, handed
    // off from the runtime, rather than on a pooled task.
    // Timed as a store-lock phase, so its cost under the lock is in the log.
    let outcome = guard.blocking_phase("judge and retire a branch's local copies", || {
        crate::jj::in_place_blocking(|| {
            retire_locally(
                &jj,
                &store,
                branch,
                merge_target,
                recorded.as_deref(),
                &permit,
                guard,
            )
        })
    });
    match outcome {
        Ok(LocalRetirement::Kept(why)) => Err(Kept::new("not-proven", why)),
        Ok(outcome) => Ok(outcome),
        Err(error) => Err(Kept::new("failed", error)),
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
pub(crate) enum LocalRetirement {
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
    kill_terminals_for_jobs(orch, &orch.db.local, &job_ids, OutputRetention::Deleted).await;
    if let Err(error) = crate::terminal_output::expire_for_project(
        &orch.db.local,
        project_id,
        chrono::Utc::now().timestamp(),
    )
    .await
    {
        log::warn!("Teardown: failed to expire the project's retained terminal output: {error}");
    }
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
pub(crate) async fn resolve_merge_target_for_source(
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

/// Retained output is runner-local even when the issue belongs to a team.
#[derive(Debug, Clone, Copy)]
enum OutputRetention {
    Resolved { resolved_at: i64 },
    Deleted,
}

async fn issue_resolved_at(db: &LocalDb, issue_id: &str) -> i64 {
    db.query_opt(
        "SELECT completed_at FROM issues WHERE id = ?1",
        params![issue_id.to_string()],
        |row| row.opt_i64(0),
    )
    .await
    .ok()
    .flatten()
    .flatten()
    .unwrap_or_else(|| chrono::Utc::now().timestamp())
}

async fn retain_terminal_outputs(
    orch: &Orchestrator,
    job_ids: &[String],
    retention: OutputRetention,
) -> Result<(), String> {
    match retention {
        OutputRetention::Resolved { resolved_at } => {
            crate::terminal_output::record_resolution(&orch.db.local, job_ids, resolved_at)
                .await
                .map(|_| ())
        }
        OutputRetention::Deleted => {
            crate::terminal_output::expire_for_jobs(
                &orch.db.local,
                job_ids,
                chrono::Utc::now().timestamp(),
            )
            .await
        }
    }
    .map_err(|error| error.to_string())
}

/// Destructive deletion stops PTYs and expires retained output. Normal issue
/// resolution uses `settle_issue_resources` and preserves pending exit evidence.
async fn kill_terminals_for_jobs(
    orch: &Orchestrator,
    db: &LocalDb,
    job_ids: &[String],
    retention: OutputRetention,
) {
    if job_ids.is_empty() {
        return;
    }

    // Stop live executor-hosted PTYs before releasing their lease authority.
    match load_running_terminals_for_jobs(&orch.db.local, job_ids).await {
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

    // Explicit deletion is destructive, unlike normal resolution: the rows
    // below disappear and cannot serve as evidence for a deferred release.
    for job_id in job_ids {
        let holder = cairn_common::executor_protocol::ResidencyHolder::Job {
            job_id: job_id.clone(),
        };
        if let Some(fence) = orch.fleet.residency_fence(&holder) {
            if let Err(error) = crate::fleet::residency::release(orch, &fence).await {
                log::warn!("Teardown: failed to release deleted job {job_id}: {error}");
            }
        }
    }

    if let Err(error) = retain_terminal_outputs(orch, job_ids, retention).await {
        log::warn!("Teardown: failed to set retained terminal output deadlines: {error}");
    }
    if let Err(error) = delete_all_terminal_rows_for_jobs(&orch.db.local, job_ids).await {
        log::warn!("Teardown: failed to delete terminal rows: {error}");
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
    .map_err(|error| format!("Failed to delete job terminals during teardown: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completed_producers_keep_pending_terminal_and_batch_cleanup_retryable() {
        use crate::issues::status::SettlementOutcome;
        use crate::transitions::Resolution;
        let root = tempfile::tempdir().unwrap();
        let db = crate::storage::migrated_test_db("resource-settlement-retry.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','',1,1);
             INSERT INTO issues(id,project_id,number,title,status,completed_at,created_at,updated_at)
               VALUES('i','p',1,'I','merged',100,1,100);
             INSERT INTO executions(id,recipe_id,project_id,issue_id,status,seq,started_at)
               VALUES('e','r','p','i','complete',1,1);
             INSERT INTO jobs(id,execution_id,project_id,issue_id,uri_segment,status,created_at,updated_at)
               VALUES('producer','e','p','i','builder','complete',1,1);
             INSERT INTO job_terminals(id,job_id,project_id,session_id,command,status,created_at,slug)
               VALUES('terminal','producer','p','session','sleep 100','running',1,'watch');
             INSERT INTO terminal_outputs(id,terminal_id,job_id,project_id,slug,terminal_uri,expiry_basis,created_at,updated_at)
               VALUES('output','terminal','producer','p','watch',
                      'cairn://p/p/1/1/builder/terminal/watch','resolution',1,1);
             INSERT INTO run_batch_detached(request_id,job_id,run_id,commits,holds_branch,state,reason,created_ms)
               VALUES('mutating','producer','run',1,1,'running','detached',1),
                     ('verdict-only','producer','run',0,0,'running','detached',1);
             CREATE TRIGGER retention_unavailable BEFORE UPDATE ON terminal_outputs
               BEGIN SELECT RAISE(ABORT,'retention unavailable'); END;"
        ).await.unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search)),
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        // The fixture resolved the issue by raw SQL, so it records the owed
        // cleanup the resolve write would have.
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    crate::execution::resolution_cleanups::record_issue(
                        conn,
                        "i",
                        Resolution::Merged,
                    )
                    .await
                })
            })
            .await
            .unwrap();
        let first = settle_issue_resources(&orch, &orch.db.local, "i", Resolution::Merged).await;
        assert!(first.iter().any(|r| r
            .diagnostic
            .as_deref()
            .is_some_and(|d| d.contains("retention unavailable"))));
        for suffix in ["/terminal/watch", "/batches/mutating"] {
            let resource = first
                .iter()
                .find(|r| r.uri.ends_with(suffix))
                .expect(suffix);
            assert_eq!(resource.outcome, SettlementOutcome::CleanupPending);
        }
        assert!(!first
            .iter()
            .any(|r| r.uri.ends_with("/batches/verdict-only")));
        let terminal = orch
            .db
            .local
            .query_one(
                "SELECT status,restart_policy,exit_reason FROM job_terminals WHERE id='terminal'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            terminal,
            (
                "running".into(),
                "never".into(),
                "issue resolved: merged".into()
            )
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT state FROM run_batch_detached WHERE request_id='mutating'",
                    (),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("running"),
            "an absent executor is not death evidence"
        );
        orch.db
            .local
            .execute_script("DROP TRIGGER retention_unavailable;")
            .await
            .unwrap();
        reconcile_resolved_resources(&orch).await;
        let output = crate::terminal_output::load(&orch.db.local, "output")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.resolved_at, Some(100));
        assert_eq!(output.expires_at, Some(100 + 7 * 24 * 60 * 60));
        assert!(
            output.is_capturing(),
            "retry never fabricates an exit or output barrier"
        );
        // Model an authoritative exit arriving between settlement attempts.
        orch.db
            .local
            .execute(
                "UPDATE job_terminals SET status='exited',exited_at=150 WHERE id='terminal'",
                (),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute_script(
                "CREATE TRIGGER capture_unavailable BEFORE UPDATE ON terminal_outputs
             WHEN NEW.capture != OLD.capture
             BEGIN SELECT RAISE(ABORT,'capture unavailable'); END;",
            )
            .await
            .unwrap();
        let pending = settle_issue_resources(&orch, &orch.db.local, "i", Resolution::Merged).await;
        let terminal = pending
            .iter()
            .find(|r| r.uri.ends_with("/terminal/watch"))
            .unwrap();
        assert_eq!(terminal.outcome, SettlementOutcome::CleanupPending);
        assert!(terminal
            .diagnostic
            .as_deref()
            .unwrap()
            .contains("capture unavailable"));
        assert!(crate::terminal_output::load(&orch.db.local, "output")
            .await
            .unwrap()
            .unwrap()
            .is_capturing());
        orch.db
            .local
            .execute_script("DROP TRIGGER capture_unavailable;")
            .await
            .unwrap();
        orch.db.local.execute_script(
            "UPDATE issues SET status='active',resolution_generation=resolution_generation+1,
                 merged_at=NULL,completed_at=NULL WHERE id='i';
             INSERT INTO jobs(id,execution_id,project_id,issue_id,uri_segment,status,created_at,updated_at)
                 VALUES('fresh','e','p','i','fresh','pending',200,200);"
        ).await.unwrap();
        crate::terminal_host::reconcile_terminal_lifecycle_with(&orch, false)
            .await
            .unwrap();
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT status FROM jobs WHERE id='fresh'", (),)
                .await
                .unwrap()
                .as_deref(),
            Some("pending"),
            "capture retry must not stop reopened work"
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT status FROM issues WHERE id='i'", (),)
                .await
                .unwrap()
                .as_deref(),
            Some("active")
        );
        let capture = crate::terminal_output::load(&orch.db.local, "output")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            capture.capture, "incomplete",
            "a lost live capture must not become complete"
        );
        assert_eq!(capture.exit_code, None, "no synthetic exit code");
        assert_eq!(capture.exited_at, Some(150));
        crate::mcp::handlers::terminal::finalize_exited_terminal_capture(&orch, "session")
            .await
            .expect("capture finalization is idempotent");
        assert_eq!(
            crate::terminal_output::load(&orch.db.local, "output")
                .await
                .unwrap()
                .unwrap()
                .resolved_at,
            Some(100)
        );
    }

    #[tokio::test]
    async fn retry_finds_resolved_source_of_branch_dependent_descendants() {
        let db = crate::storage::migrated_test_db("dependent-resource-retry.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','',1,1),('other','w','O','other','',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
               VALUES('source','p',1,'S','merged',1,1),('dependent','p',2,'D','active',1,1),
                     ('unrelated','other',1,'O','closed',1,1);
             INSERT INTO jobs(id,project_id,issue_id,parent_job_id,branch,base_branch,status,created_at,updated_at)
               VALUES('producer','p','source',NULL,'agent/source','main','complete',1,1),
                     ('reviewer','p','dependent',NULL,'agent/reviewer','agent/source','cancelled',1,1),
                     ('child','p',NULL,'reviewer',NULL,NULL,'cancelled',1,1),
                     ('foreign','other','unrelated',NULL,'agent/source','main','complete',1,1);"
        ).await.unwrap();
        assert_eq!(
            resolved_resource_issues(&db, "child").await.unwrap(),
            vec![("source".into(), "merged".into())]
        );
        assert!(issue_resource_jobs(&db, "source")
            .await
            .unwrap()
            .contains(&"child".into()));
        assert_eq!(
            issue_output_jobs(&db, "source").await.unwrap(),
            vec!["producer".to_string()],
            "dependent issue output must not expire before its own issue resolves"
        );
    }

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
               VALUES ('run','job','proj','exited',1,1);
             INSERT INTO job_browsers(id,job_id,project_id,slug,webview_label,status,created_at,last_active_at)
               VALUES('node-browser','job','proj','node','node-view','running',1,1),
                     ('project-browser',NULL,'proj','project','project-view','running',1,1);",
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
            db.query_all("SELECT id FROM job_browsers ORDER BY id", (), |row| row
                .text(0))
                .await
                .unwrap(),
            vec!["project-browser".to_string()],
            "normal issue resolution closes only node-scoped browsers"
        );
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
