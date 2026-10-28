use std::path::Path;

use cairn_common::executor_protocol::{
    CellOutcome, CellOwnerRef, CellPriority, ExecutorSelector, OwnerDeathPolicy, RepositoryLocator,
    ResidencyAcquireRequest, ResidencyFailureKind, ResidencyFence, ResidencyFootprint,
    ResidencyHolder, ResidencyOperation, ResidencyResult,
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
    let JobResidence {
        owner_ref,
        repo_path,
    } = job_residence(db, job_id).await?;
    let project_id = owner_ref.project_id.clone();
    Ok(ResidencyAcquireRequest {
        holder: ResidencyHolder::Job {
            job_id: job_id.to_string(),
        },
        owner_ref: Some(owner_ref),
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
        // The acquiring caller's horizon, not this function's. A `run` batch
        // passes its ceiling rather than its declared budget: the home is the
        // job's, and the budget measures the batch's own wait for room once the
        // home is in hand.
        wait_horizon_unix_ms,
        waiting_since_unix_ms: waiting_since_unix_ms.min(crate::fleet::unix_time_ms()),
    })
}

/// Who a job's cell belongs to, and which repository it is a checkout of.
///
/// One read answers both, so every surface that acquires on a job's behalf
/// attributes the cell identically whether the job runs on a branch of its own
/// or in the project's live checkout.
pub(crate) struct JobResidence {
    pub owner_ref: CellOwnerRef,
    pub repo_path: String,
}

pub(crate) async fn job_residence(db: &LocalDb, job_id: &str) -> Result<JobResidence, String> {
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
    Ok(JobResidence {
        owner_ref: CellOwnerRef {
            project_id,
            project_key: Some(project_key),
            issue_number,
            job_id: Some(job_id.to_string()),
            execution_seq: exec_seq,
            node_kind: node_name,
        },
        repo_path,
    })
}

/// The request a **branchless** owner presents to acquire a residency over the
/// project's live checkout.
///
/// A thread session owns no branch by construction — it is commit-fenced, has no
/// worktree and no PR — so there is no managed checkout to materialize for it.
/// Every long-lived process it runs (its terminals, its REPLs) therefore lives in
/// the project's own checkout, read-only, under one holder keyed by the session's
/// job so those processes share an environment with each other and with nobody
/// else. A project terminal is the same shape with the project as its owner.
///
/// The live checkout is externally owned and always at its own HEAD: Cairn never
/// moves it, which is why this resolves the head with plain `git` rather than a
/// logical bookmark, and why a caller must not follow this acquisition with a
/// checkout refresh.
pub(crate) async fn live_checkout_residency_request(
    owner_id: &str,
    project_id: &str,
    repo_path: &str,
    owner_ref: Option<CellOwnerRef>,
    wait_horizon_unix_ms: u64,
) -> Result<ResidencyAcquireRequest, String> {
    let checkout = repo_path.to_string();
    let base_commit = tokio::task::spawn_blocking(move || {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&checkout)
            .output()
            .map_err(|error| format!("resolve project checkout head: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "resolve project checkout head: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    })
    .await
    .map_err(|error| format!("live checkout head resolution task failed: {error}"))??;

    Ok(ResidencyAcquireRequest {
        holder: ResidencyHolder::ProjectTerminals {
            project_id: owner_id.to_string(),
        },
        owner_ref,
        selector: None,
        executor: None,
        repository: RepositoryLocator::ExistingCheckout {
            project_id: project_id.to_string(),
            repository_id: project_id.to_string(),
            absolute_path: repo_path.to_string(),
        },
        initial_base_commit: base_commit,
        // A place to live in, not a workload: the checkout already exists and is
        // not Cairn's to grow, so the only declared cost is what the resident
        // shells and interpreters hold in memory.
        footprint: ResidencyFootprint {
            memory_bytes: 64 * 1024 * 1024,
            disk_growth_bytes: 0,
        },
        // The terminal window, because terminal recovery is sized from these
        // exact constants and a thread's REPL shares the holder its terminals
        // hold. One owner cannot have two answers for when it is considered dead.
        death_policy: OwnerDeathPolicy {
            heartbeat_timeout_ms: crate::terminal_host::TERMINAL_HEARTBEAT_TIMEOUT_MS,
            reclaim_grace_ms: crate::terminal_host::TERMINAL_RECLAIM_GRACE_MS,
        },
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms,
        waiting_since_unix_ms: crate::fleet::unix_time_ms(),
    })
}

/// Acquire or rejoin a job's execution environment. Acquisition is idempotent,
/// so every surface calls this and the second caller lands in the first caller's
/// cell. The renewal that follows is what lets an environment survive a long
/// stretch of agent thinking between two batches.
///
/// `selector` is set only by a terminal or REPL create that names a machine,
/// after [`place_for_resident_process`] has settled that the environment may
/// be placed there. A run batch passes `None`: its environment is placement,
/// and its own selector is decided per batch (`BatchResidence`).
pub(crate) async fn acquire_job_residency(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    base_commit: &str,
    wait_horizon_unix_ms: u64,
    waiting_since_unix_ms: u64,
    selector: Option<&ExecutorSelector>,
) -> Result<ResidencyFence, PlacementRefusal> {
    let mut request = job_residency_request(
        db,
        job_id,
        base_commit,
        wait_horizon_unix_ms,
        waiting_since_unix_ms,
    )
    .await
    .map_err(PlacementRefusal::structural)?;
    request.executor = selector.cloned();
    let fence = acquire(orch, request).await?;
    let _ = renew(orch, &fence).await;
    Ok(fence)
}

/// The acquisition any surface of one job presents, branch or no branch.
///
/// A job on a managed branch acquires over that branch's checkout; a branchless
/// owner — a thread session, which has a job but is commit-fenced and owns no
/// worktree — acquires over the project's live checkout instead. The split is the
/// same one a terminal makes, and it lives here so every surface of a job
/// converges on ONE cell rather than each rediscovering the rule.
pub(crate) async fn job_residency_acquisition(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    wait_horizon_unix_ms: u64,
) -> Result<ResidencyAcquireRequest, String> {
    let residence = job_residence(db, job_id).await?;
    let owned_job_id = job_id.to_string();
    let branch = db
        .query_opt_text("SELECT branch FROM jobs WHERE id = ?1", (owned_job_id,))
        .await
        .map_err(|error| format!("resolve job branch: {error}"))?
        .filter(|branch| !branch.is_empty());
    match branch {
        Some(branch) => {
            let base_commit =
                resolve_logical_commit(orch, std::path::Path::new(&residence.repo_path), &branch)
                    .await?;
            job_residency_request(
                db,
                job_id,
                &base_commit,
                wait_horizon_unix_ms,
                crate::fleet::unix_time_ms(),
            )
            .await
        }
        None => {
            live_checkout_residency_request(
                job_id,
                &residence.owner_ref.project_id.clone(),
                &residence.repo_path,
                Some(residence.owner_ref),
                wait_horizon_unix_ms,
            )
            .await
        }
    }
}

/// Write one durable attachment's bytes into a held cell, returning the path the
/// executor derived for them.
///
/// The path comes back from the executor rather than being predicted here, which
/// is what entitles the caller to state it: a runner that could not read where
/// the bytes landed must not claim they landed.
pub(crate) async fn materialize_attachment(
    orch: &Orchestrator,
    fence: &ResidencyFence,
    request: cairn_common::executor_protocol::AttachmentMaterializationRequest,
) -> Result<String, String> {
    match orch
        .fleet
        .operate_residency(
            orch,
            ResidencyOperation::MaterializeAttachment {
                fence: fence.clone(),
                request,
            },
        )
        .await
    {
        ResidencyResult::AttachmentMaterialized { path, .. } => Ok(path),
        ResidencyResult::Failed { diagnostic, .. } => Err(diagnostic),
        other => Err(format!("unexpected materialization reply: {other:?}")),
    }
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
        let jj = crate::jj::environment(&jj_binary_path, &config_dir);
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
    acquire_with_logging(orch, request, true).await
}

/// Acquire residency without emitting the generic failure warning.
///
/// Supervisors that retry an ambient service own failure reporting so they can
/// collapse an unchanged outage instead of amplifying it on every probe.
pub(crate) async fn acquire_quietly(
    orch: &Orchestrator,
    request: ResidencyAcquireRequest,
) -> Result<ResidencyFence, PlacementRefusal> {
    acquire_with_logging(orch, request, false).await
}

async fn acquire_with_logging(
    orch: &Orchestrator,
    request: ResidencyAcquireRequest,
    log_failure: bool,
) -> Result<ResidencyFence, PlacementRefusal> {
    let holder = request.holder.clone();
    let result = orch
        .fleet
        .operate_residency(orch, ResidencyOperation::Acquire { request })
        .await;
    let cell = match result {
        ResidencyResult::State { cell } => cell,
        // Only `Acquire` reaches here, and it materializes nothing.
        ResidencyResult::ConflictMaterialized { cell, .. } => cell,
        ResidencyResult::AttachmentMaterialized { .. } => {
            return Err(PlacementRefusal::structural(
                "acquisition returned an attachment materialization",
            ))
        }
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
            // The link this failure came from is the one holding this
            // environment, which for a job placed on a remote is not the
            // colocated one. Reading the wrong link is how a machine that is
            // simply reconnecting produced a refusal instead of a wait.
            let verdict = classify_residency_failure(
                &kind,
                cell_outcome.as_deref(),
                orch.fleet.residency_link_restoration(&holder),
            );
            // The stage label says where acquisition stopped; the placement
            // sentence underneath it says why, naming the machines and what
            // each of them answered. Dropping the second is how an agent came
            // to read only "select execution environment executor".
            let diagnostic = match cell_outcome.as_deref() {
                Some(CellOutcome::Unavailable {
                    diagnostic: placement,
                    ..
                }) if !placement.is_empty() => format!("{diagnostic}: {placement}"),
                _ => diagnostic,
            };
            if log_failure {
                tracing::warn!(%holder, ?kind, ?verdict, %diagnostic, "residency acquisition failed");
            }
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
            require_clean: false,
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

/// The sentence an acquisition refusal carries to the agent: where acquisition
/// stopped, and the placement reason underneath it naming the machines. `None`
/// for a reply that is not a refusal.
pub(crate) fn acquisition_refusal_text(result: &ResidencyResult) -> Option<String> {
    let ResidencyResult::Failed {
        diagnostic,
        cell_outcome,
        ..
    } = result
    else {
        return None;
    };
    Some(match cell_outcome.as_deref() {
        Some(CellOutcome::Unavailable {
            diagnostic: placement,
            ..
        }) if !placement.is_empty() => format!("{diagnostic}: {placement}"),
        _ => diagnostic.clone(),
    })
}

// --- placing a terminal or REPL that names a machine ---
//
// A terminal or REPL is the one thing that binds a job's environment to a
// machine: it is a live stateful process the job's later batches use. Without
// a selector it joins the environment wherever it already is. With one, the
// environment must be on a machine that satisfies it, or the create is refused
// naming what prevents it. It is never started somewhere else quietly.
//
// An environment with nothing live in it binds nothing (execution homes
// dissolve: a residency is placement, not a constraint), so one sitting on the
// wrong machine is released and acquired again where the create asked. The
// warmth the released cell held is the cost the explicit pin pays.

/// The machine holding an environment, as a create that names a machine sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResidentHome {
    pub executor_name: String,
    /// Whether that machine satisfies the create's selector. `None` when it is
    /// away and the selector asks what only a live advertisement answers.
    pub satisfies_selector: Option<bool>,
    /// What is live inside the environment, in the words a refusal uses. Empty
    /// means nothing binds it to its machine.
    pub live: Vec<String>,
}

/// What a terminal or REPL create does about where its environment is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResidentPlacement {
    /// Acquire, carrying the selector if there is one: there is no environment
    /// yet, or it is already somewhere acceptable.
    Acquire,
    /// Nothing live holds the environment on `from`, which does not satisfy the
    /// selector: release it there and acquire it where the selector asks.
    Rehome { from: String },
    /// Something live holds the environment on a machine the selector refuses.
    Refuse(String),
}

/// The create-time rule, pure so every case can be pinned in a test.
///
/// A machine that is away and cannot answer the selector is not a mismatch:
/// the create acquires with the selector, and [`verify_resident_landing`] judges
/// the machine the environment actually came up on.
pub(crate) fn decide_resident_placement(
    surface: &str,
    selector: Option<&ExecutorSelector>,
    home: Option<&ResidentHome>,
) -> ResidentPlacement {
    let Some(selector) = selector.filter(|selector| !selector.is_empty()) else {
        return ResidentPlacement::Acquire;
    };
    let Some(home) = home.filter(|home| home.satisfies_selector == Some(false)) else {
        return ResidentPlacement::Acquire;
    };
    if home.live.is_empty() {
        return ResidentPlacement::Rehome {
            from: home.executor_name.clone(),
        };
    }
    let (holders, verb, pronoun) = match home.live.as_slice() {
        [only] => (only.clone(), "holds", "it"),
        many => (many.join(", "), "hold", "them"),
    };
    ResidentPlacement::Refuse(format!(
        "{surface} was not started: {holders} {verb} this job's environment on **{machine}**, which does not match {destination}. Stop {pronoun} first, or create {surface} without `executor` to join it on **{machine}**. Nothing was started.",
        machine = home.executor_name,
        destination = selector_destination(selector),
    ))
}

fn selector_destination(selector: &ExecutorSelector) -> String {
    match &selector.name {
        Some(name) => format!("**{name}**"),
        None => format!("an executor matching {}", selector.describe()),
    }
}

/// Settle where a terminal's or REPL's environment must be before its process
/// starts. With no selector there is nothing to settle. Returns the machine the
/// environment was moved off, when [`ResidentPlacement::Rehome`] moved it, so
/// the create can say so.
pub(crate) async fn place_for_resident_process(
    orch: &Orchestrator,
    db: &LocalDb,
    holder: &ResidencyHolder,
    job_id: Option<&str>,
    selector: Option<&ExecutorSelector>,
    surface: &str,
) -> Result<Option<String>, String> {
    let Some(selector) = selector.filter(|selector| !selector.is_empty()) else {
        return Ok(None);
    };
    let home = match orch.fleet.residency_route_executor(holder) {
        None => None,
        Some(executor_id) => {
            let satisfies_selector = orch
                .fleet
                .recorded_executor_satisfies_selector(&executor_id, selector);
            let live = match satisfies_selector {
                Some(false) => resident_occupants(orch, db, holder, job_id).await,
                _ => Vec::new(),
            };
            Some(ResidentHome {
                executor_name: orch.fleet.executor_name_for_text(&executor_id),
                satisfies_selector,
                live,
            })
        }
    };
    match decide_resident_placement(surface, Some(selector), home.as_ref()) {
        ResidentPlacement::Acquire => Ok(None),
        ResidentPlacement::Refuse(refusal) => Err(refusal),
        ResidentPlacement::Rehome { from } => {
            if let Some(fence) = orch.fleet.residency_fence(holder) {
                release(orch, &fence).await.map_err(|error| {
                    format!("{surface} was not started: this job's environment on **{from}** could not be released to move it ({error}). Nothing was started.")
                })?;
            }
            tracing::info!(
                %holder,
                from = %from,
                wanted = %selector.describe(),
                "{surface} named a machine its environment's host does not satisfy, and nothing live held the environment there, so it is re-homed"
            );
            Ok(Some(from))
        }
    }
}

/// After acquisition: the machine the environment came up on, or a refusal when
/// it does not satisfy the create's selector. Acquisition rejoins an existing
/// environment wherever it is, so this is what makes a selector binding even
/// when a batch re-created the environment between the decision and now.
pub(crate) fn verify_resident_landing(
    orch: &Orchestrator,
    holder: &ResidencyHolder,
    selector: Option<&ExecutorSelector>,
    surface: &str,
) -> Result<Option<String>, String> {
    let Some(executor_id) = orch.fleet.residency_route_executor(holder) else {
        return Ok(None);
    };
    let machine = orch.fleet.executor_name_for_text(&executor_id);
    if let Some(selector) = selector.filter(|selector| !selector.is_empty()) {
        if orch
            .fleet
            .recorded_executor_satisfies_selector(&executor_id, selector)
            == Some(false)
        {
            return Err(format!(
                "{surface} was not started: this job's environment is on **{machine}**, which does not match {}. Nothing was started.",
                selector_destination(selector)
            ));
        }
    }
    Ok(Some(machine))
}

/// Where a terminal or REPL came up, for the create's own reply.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ResidentLanding {
    /// The machine its environment is on.
    pub machine: Option<String>,
    /// The machine the environment was moved off to honour the selector.
    pub rehomed_from: Option<String>,
}

impl ResidentLanding {
    /// " on **X**", plus what moving the environment cost, appended to a
    /// create's "Started ..." line.
    pub(crate) fn describe(&self) -> String {
        let mut text = match &self.machine {
            Some(machine) => format!(" on **{machine}**"),
            None => String::new(),
        };
        if let Some(from) = &self.rehomed_from {
            text.push_str(&format!(
                ". This job's environment moved here from **{from}**, where nothing live held it; uncommitted files and installed packages there did not move"
            ));
        }
        text
    }
}

/// Parse the `executor` a terminal or REPL create carries: the same selector
/// `run` takes (`{name}` or `{os}`, optionally with `requiredToolchains`), and
/// nothing a create cannot honour.
pub(crate) fn parse_resident_selector(
    value: &serde_json::Value,
) -> Result<ExecutorSelector, String> {
    let selector: ExecutorSelector = serde_json::from_value(value.clone()).map_err(|_| {
        "payload.executor takes `name` (one machine, as cairn://executors lists it) or `os` (any machine on that platform), optionally with `requiredToolchains`".to_string()
    })?;
    if selector.requires_macos_nested_sandbox_write || selector.containment.is_some() {
        return Err(
            "payload.executor takes `name`, `os`, and `requiredToolchains` only".to_string(),
        );
    }
    selector
        .validate()
        .map_err(|error| format!("payload.executor: {error}"))?;
    Ok(selector)
}

/// The machine a residency is on, by the name an operator uses, for a page that
/// must say where its process runs.
pub(crate) fn residency_machine_name(
    orch: &Orchestrator,
    holder: &ResidencyHolder,
) -> Option<String> {
    orch.fleet
        .residency_route_executor(holder)
        .map(|executor_id| orch.fleet.executor_name_for_text(&executor_id))
}

/// What is live in an environment, named: the job's running terminals and live
/// REPLs from the durable side, then anything else its executor reports (a
/// process the rows do not name, a batch in flight).
async fn resident_occupants(
    orch: &Orchestrator,
    db: &LocalDb,
    holder: &ResidencyHolder,
    job_id: Option<&str>,
) -> Vec<String> {
    let mut live = match job_id {
        Some(job_id) => job_live_processes(orch, db, job_id, false).await,
        None => Vec::new(),
    };
    if let Some(occupancy) = orch.fleet.residency_occupancy(holder) {
        if live.is_empty() {
            live.extend(
                occupancy
                    .live_processes()
                    .map(|(key, _)| format!("a live process (`{key}`)")),
            );
        }
        if occupancy.command.is_some() || occupancy.draining.is_some() {
            live.push("a run batch in flight".to_string());
        }
    }
    live
}

/// A job's running terminals and live REPLs, as `terminal `slug`` and
/// `REPL `slug``.
///
/// `managed_branch_only` keeps a branchless owner's processes out: those live
/// in the project's own checkout under a separate holder, and share nothing
/// with the job's run batches.
pub(crate) async fn job_live_processes(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    managed_branch_only: bool,
) -> Vec<String> {
    let owned_job_id = job_id.to_string();
    let terminals = db
        .read(move |conn| {
            let job_id = owned_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT t.slug FROM job_terminals t
                         JOIN jobs j ON j.id = t.job_id
                         WHERE t.job_id = ?1
                           AND t.status = 'running'
                           AND (?2 = 0 OR COALESCE(j.branch, '') != '')
                         ORDER BY t.slug",
                        (job_id.as_str(), i64::from(managed_branch_only)),
                    )
                    .await?;
                let mut slugs = Vec::new();
                while let Some(row) = rows.next().await? {
                    slugs.push(row.text(0)?);
                }
                Ok(slugs)
            })
        })
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(%job_id, %error, "could not list running terminals");
            Vec::new()
        });
    terminals
        .into_iter()
        .map(|slug| format!("terminal `{slug}`"))
        .chain(
            orch.repl_state
                .resident_slugs(job_id)
                .into_iter()
                .map(|slug| format!("REPL `{slug}`")),
        )
        .collect()
}

#[cfg(test)]
mod resident_placement_tests {
    use super::*;

    fn named(name: &str) -> ExecutorSelector {
        ExecutorSelector {
            name: Some(name.to_string()),
            ..ExecutorSelector::default()
        }
    }

    fn home(satisfies: Option<bool>, live: &[&str]) -> ResidentHome {
        ResidentHome {
            executor_name: "bginf-halo".to_string(),
            satisfies_selector: satisfies,
            live: live.iter().map(ToString::to_string).collect(),
        }
    }

    /// No selector: the terminal joins the environment wherever it is.
    #[test]
    fn no_selector_joins_the_environment() {
        assert_eq!(
            decide_resident_placement(
                "terminal `lab`",
                None,
                Some(&home(Some(false), &["REPL `py`"]))
            ),
            ResidentPlacement::Acquire
        );
    }

    /// A selector with no environment yet, or one already on a matching
    /// machine, acquires carrying the selector.
    #[test]
    fn a_selector_acquires_when_absent_or_satisfied() {
        let selector = named("bgsrv-mac");
        assert_eq!(
            decide_resident_placement("terminal `lab`", Some(&selector), None),
            ResidentPlacement::Acquire
        );
        assert_eq!(
            decide_resident_placement(
                "terminal `lab`",
                Some(&selector),
                Some(&home(Some(true), &[]))
            ),
            ResidentPlacement::Acquire
        );
    }

    /// A machine that is away is not a mismatch: acquire, and judge the landing.
    #[test]
    fn an_unanswerable_home_is_not_a_mismatch() {
        assert_eq!(
            decide_resident_placement(
                "terminal `lab`",
                Some(&named("bgsrv-mac")),
                Some(&home(None, &["REPL `py`"]))
            ),
            ResidentPlacement::Acquire
        );
    }

    /// #428: a lab pinned to bgsrv-mac, environment idle on bginf-halo. Nothing
    /// live binds it there, so it moves.
    #[test]
    fn an_idle_environment_on_the_wrong_machine_is_re_homed() {
        assert_eq!(
            decide_resident_placement(
                "terminal `lab`",
                Some(&named("bgsrv-mac")),
                Some(&home(Some(false), &[]))
            ),
            ResidentPlacement::Rehome {
                from: "bginf-halo".to_string()
            }
        );
    }

    /// A live process on the wrong machine is the one binding fact, and the
    /// refusal names it and the machine, never a 'home'.
    #[test]
    fn a_live_process_on_the_wrong_machine_refuses_naming_it() {
        let ResidentPlacement::Refuse(refusal) = decide_resident_placement(
            "REPL `py`",
            Some(&named("bgsrv-mac")),
            Some(&home(Some(false), &["terminal `dev`"])),
        ) else {
            panic!("a live terminal on the wrong machine refuses");
        };
        assert_eq!(
            refusal,
            "REPL `py` was not started: terminal `dev` holds this job's environment on **bginf-halo**, which does not match **bgsrv-mac**. Stop it first, or create REPL `py` without `executor` to join it on **bginf-halo**. Nothing was started."
        );
        assert!(!refusal.contains("home"), "{refusal}");

        let ResidentPlacement::Refuse(many) = decide_resident_placement(
            "terminal `lab`",
            Some(&ExecutorSelector {
                os: Some("macos".to_string()),
                ..ExecutorSelector::default()
            }),
            Some(&home(
                Some(false),
                &["terminal `dev`", "a run batch in flight"],
            )),
        ) else {
            panic!("live work on the wrong machine refuses");
        };
        assert!(
            many.contains("terminal `dev`, a run batch in flight hold"),
            "{many}"
        );
        assert!(many.contains("Stop them first"), "{many}");
        assert!(many.contains("an executor matching"), "{many}");
    }
}
