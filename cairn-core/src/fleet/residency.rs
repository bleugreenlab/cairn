use std::path::Path;

use cairn_common::executor_protocol::{
    CellOwnerRef, CellPriority, OwnerDeathPolicy, RepositoryLocator, ResidencyAcquireRequest,
    ResidencyFailureKind, ResidencyFence, ResidencyFootprint, ResidencyHolder, ResidencyOperation,
    ResidencyResult,
};

use crate::fleet::placement::{classify_residency_failure, PlacementRefusal};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// How long a job's execution environment outlives an owner that stopped
/// answering. Job teardown releases the residency deterministically, so this
/// bound exists only to reclaim environments whose job died without saying so.
/// It is generous on purpose: an agent legitimately thinks for a long time
/// between two batches, and the whole point of the environment is that the
/// second batch lands where the first did.
const JOB_EXECUTION_HEARTBEAT_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const JOB_EXECUTION_RECLAIM_GRACE_MS: u64 = 5 * 60 * 1000;

/// Build the one request every surface of a job presents to acquire its
/// execution environment.
///
/// Every surface of an agent job — synchronous `run` batches, inline code,
/// REPLs, and terminals — executes inside the one cell this residency holds.
/// That is what makes a package installed by one surface importable from
/// another, `$TMPDIR` shared across them, and a generated file reachable at one
/// absolute path. Sharing is bounded by the residency's life: reclaim stays
/// legal at any time, and the next execution regenerates the environment from
/// scratch.
///
/// Identity is the holder and the repository, and nothing else, so the owner
/// reference read from the job row below is free to drift between callers
/// without splitting them across two cells.
pub(crate) async fn job_residency_request(
    db: &LocalDb,
    job_id: &str,
    base_commit: &str,
    wait_horizon_unix_ms: u64,
    waiting_since_unix_ms: u64,
) -> Result<ResidencyAcquireRequest, String> {
    let owned_job_id = job_id.to_string();
    let (project_id, project_key, repo_path, node_name, issue_number, exec_seq) = db
        .read(move |conn| {
            let job_id = owned_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.project_id, p.key, p.repo_path, j.node_name, i.number, e.seq
                        FROM jobs j
                        JOIN projects p ON j.project_id = p.id
                        LEFT JOIN executions e ON j.execution_id = e.id
                        LEFT JOIN runs r ON r.job_id = j.id
                        LEFT JOIN issues i ON r.issue_id = i.id
                        WHERE j.id = ?1
                        ORDER BY r.created_at DESC
                        LIMIT 1
                        ",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                })?;
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.opt_text(3)?,
                    row.opt_i64(4)?.map(|value| value as i32),
                    row.opt_i64(5)?.map(|value| value as i32),
                ))
            })
        })
        .await
        .map_err(|error| format!("resolve job execution environment: {error}"))?;
    Ok(ResidencyAcquireRequest {
        holder: ResidencyHolder::Job {
            job_id: job_id.to_string(),
        },
        owner_ref: Some(CellOwnerRef {
            project_id: project_id.clone(),
            project_key: Some(project_key),
            issue_number,
            job_id: Some(job_id.to_string()),
            execution_seq: exec_seq,
            node_kind: node_name,
        }),
        selector: None,
        executor: None,
        repository: RepositoryLocator::ColocatedPath {
            project_id: project_id.clone(),
            repository_id: project_id,
            absolute_path: repo_path,
        },
        initial_base_commit: base_commit.to_string(),
        // What the environment costs while it sits idle, and nothing more. Its
        // checkout is real and durable for the residency's life, and the shells
        // and REPLs it keeps resident between batches have real RSS that nothing
        // else declares. It holds no CPU: an idle environment runs nothing, and
        // the batches that do run inside it arrive as their own cell requests,
        // which charge and release a unit through ordinary admission. There is
        // no concurrency field here to declare otherwise.
        footprint: ResidencyFootprint {
            memory_bytes: 64 * 1024 * 1024,
            disk_growth_bytes: 1024 * 1024 * 1024,
        },
        death_policy: OwnerDeathPolicy {
            heartbeat_timeout_ms: JOB_EXECUTION_HEARTBEAT_TIMEOUT_MS,
            reclaim_grace_ms: JOB_EXECUTION_RECLAIM_GRACE_MS,
        },
        priority: CellPriority::AgentInteractive,
        // The acquiring caller's horizon, not this function's. A batch that is
        // acquiring its job's home before running has its own answer to how long
        // that is worth waiting for, and acquiring the home is part of placing
        // the batch rather than a precondition with a separate clock.
        wait_horizon_unix_ms,
        waiting_since_unix_ms: waiting_since_unix_ms.min(crate::fleet::unix_time_ms()),
    })
}

/// Acquire or rejoin a job's execution environment. Acquisition is idempotent,
/// so every surface calls this and the second caller lands in the first caller's
/// cell. The renewal that follows is what lets an environment survive a long
/// stretch of agent thinking between two batches.
pub(crate) async fn acquire_job_residency(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    base_commit: &str,
    wait_horizon_unix_ms: u64,
    waiting_since_unix_ms: u64,
) -> Result<ResidencyFence, PlacementRefusal> {
    let request = job_residency_request(
        db,
        job_id,
        base_commit,
        wait_horizon_unix_ms,
        waiting_since_unix_ms,
    )
    .await
    .map_err(PlacementRefusal::structural)?;
    let fence = acquire(orch, request).await?;
    let _ = renew(orch, &fence).await;
    Ok(fence)
}

pub(crate) async fn resolve_logical_commit(
    orch: &Orchestrator,
    repository_path: &Path,
    branch: &str,
) -> Result<String, String> {
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let repository_path = repository_path.to_path_buf();
    let branch = branch.to_string();
    tokio::task::spawn_blocking(move || {
        let jj = crate::jj::JjEnv::resolve(&jj_binary_path, &config_dir);
        let store = crate::jj::project_store_dir(&config_dir, &repository_path);
        crate::jj::bookmark_commit(&jj, &store, &branch).ok_or_else(|| {
            format!("logical branch `{branch}` does not resolve to a committed head")
        })
    })
    .await
    .map_err(|error| format!("logical branch resolution task failed: {error}"))?
}

/// Acquire an execution environment, reporting a failure as a classified
/// [`PlacementRefusal`] rather than a sentence.
///
/// The sentence is still what reaches the agent, but the verdict is what a
/// caller able to wait needs: acquiring an environment is a cell placement, so
/// "the machine had no room for it just now" and "this environment cannot be
/// created" arrive here as the same shape and must not be answered the same way.
pub(crate) async fn acquire(
    orch: &Orchestrator,
    request: ResidencyAcquireRequest,
) -> Result<ResidencyFence, PlacementRefusal> {
    let holder = request.holder.clone();
    let result = orch
        .fleet
        .operate_residency(orch, ResidencyOperation::Acquire { request })
        .await;
    let cell = match result {
        ResidencyResult::State { cell } => cell,
        // Only `Acquire` reaches here, and it never materializes conflicts.
        ResidencyResult::ConflictMaterialized { cell, .. } => cell,
        // An acquisition failure reaches the agent verbatim through the surface
        // that asked for the environment, so it carries the diagnostic sentence
        // alone. The typed kind describes machinery an agent cannot act on and
        // belongs in the log; what survives into the return value is the verdict
        // derived from it and from the placement evidence it carries.
        ResidencyResult::Failed {
            kind,
            diagnostic,
            cell_outcome,
        } => {
            let verdict = classify_residency_failure(
                &kind,
                cell_outcome.as_deref(),
                orch.fleet.link_restoration(),
            );
            tracing::warn!(%holder, ?kind, ?verdict, %diagnostic, "residency acquisition failed");
            return Err(PlacementRefusal {
                verdict,
                diagnostic,
            });
        }
        ResidencyResult::Released { .. } => {
            return Err(PlacementRefusal::structural(
                "its execution environment was released while it was being acquired",
            ))
        }
    };
    let residency = cell.residency.as_ref().ok_or_else(|| {
        PlacementRefusal::structural("acquisition returned a cell with no residency")
    })?;
    Ok(ResidencyFence {
        holder,
        incarnation_id: residency.incarnation_id.clone(),
        cell_epoch: cell.cell_epoch,
    })
}

async fn operation(
    orch: &Orchestrator,
    operation: ResidencyOperation,
    action: &str,
) -> Result<ResidencyResult, String> {
    let result = orch.fleet.operate_residency(orch, operation).await;
    match result {
        ResidencyResult::State { .. } | ResidencyResult::Released { .. } => Ok(result),
        other => Err(format!("execution environment {action} failed: {other:?}")),
    }
}

pub(crate) async fn refresh(
    orch: &Orchestrator,
    fence: &ResidencyFence,
    commit: &str,
) -> Result<(), String> {
    operation(
        orch,
        ResidencyOperation::RefreshCheckout {
            fence: fence.clone(),
            base_commit: commit.to_string(),
        },
        "checkout refresh",
    )
    .await?;
    Ok(())
}

pub(crate) async fn renew(orch: &Orchestrator, fence: &ResidencyFence) -> Result<(), String> {
    operation(
        orch,
        ResidencyOperation::Renew {
            fence: fence.clone(),
        },
        "renewal",
    )
    .await?;
    Ok(())
}

/// Run an operation whose success is a state of absence, so an environment that
/// is already gone counts as done.
///
/// Stopping and releasing are statements about the end state, not about who
/// reached it. A residency the executor no longer knows about is already in the
/// state these ask for, and teardown races reach it routinely — a settled
/// terminal process releasing behind a reclaim that already happened. Reporting
/// that as a failure produces log noise that hides the failures that matter.
async fn teardown(
    orch: &Orchestrator,
    operation: ResidencyOperation,
    action: &str,
) -> Result<(), String> {
    match orch.fleet.operate_residency(orch, operation).await {
        ResidencyResult::State { .. } | ResidencyResult::Released { .. } => Ok(()),
        ResidencyResult::Failed {
            kind: ResidencyFailureKind::NotFound,
            diagnostic,
            ..
        } => {
            tracing::debug!(%diagnostic, "execution environment {action} found nothing to undo");
            Ok(())
        }
        other => Err(format!("execution environment {action} failed: {other:?}")),
    }
}

pub(crate) async fn stop(
    orch: &Orchestrator,
    fence: &ResidencyFence,
    process_key: &str,
) -> Result<(), String> {
    teardown(
        orch,
        ResidencyOperation::StopProcess {
            fence: fence.clone(),
            process_key: process_key.to_string(),
        },
        "process stop",
    )
    .await
}

pub(crate) async fn release(orch: &Orchestrator, fence: &ResidencyFence) -> Result<(), String> {
    teardown(
        orch,
        ResidencyOperation::Release {
            fence: fence.clone(),
        },
        "release",
    )
    .await
}

pub(crate) async fn rollback(orch: &Orchestrator, fence: &ResidencyFence, process_key: &str) {
    let _ = stop(orch, fence, process_key).await;
    let _ = release(orch, fence).await;
}
