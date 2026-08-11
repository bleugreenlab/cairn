use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cairn_common::dev_instance_protocol::{
    DevInstanceLaunchControl, DevInstanceLaunchEvent, DevInstanceLaunchFailure,
    DevInstanceLaunchRequest, DevInstanceReadiness,
};
use cairn_common::executor_protocol::{
    CellOwnerRef, CellPriority, OwnerDeathPolicy, ProcessSandboxMode, RepositoryLocator,
    ResidencyAcquireRequest, ResidencyFootprint, ResidencyHolder, ResidencyOperation,
    ResidencyResult, ResidentProcessCwdRoot, ResidentProcessEvent, ResidentProcessEventKind,
    ResidentProcessIoMode, ResidentProcessKind, ResidentProcessSpec, ResidentProcessStatus,
    ResourceReservation, ResourceReservationSource,
};
use cairn_common::uri::CairnResource;
use cairn_db::turso::params;
use tokio::sync::mpsc;

use crate::messages::delivery::{latest_run_for_job, queue_system_direct};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::services::GitClient;
use crate::storage::RowExt;

const PROCESS_KEY: &str = "dev-instance";
/// The entrypoint the executor runs inside the materialized checkout, relative
/// to that checkout's root.
const RUNTIME_ENTRYPOINT: &str = "scripts/dev-instance-runtime.ts";
const READY_MARKER: &str = "CAIRN_DEV_INSTANCE_READY=";
const LEASE_TIMEOUT_MS: u64 = 30_000;
const RECLAIM_GRACE_MS: u64 = 10_000;
const RENEW_INTERVAL: Duration = Duration::from_secs(10);

/// Advance every live dev server launched from `branch` to the commit that now
/// names that branch. The executor serializes this mutation with every other
/// operation on the residency, while the caller performs it only after the
/// branch publication lock has been released.
///
/// A dev checkout is a projection, not an authoring surface. Its refresh is
/// therefore stricter than a terminal's: tracked dirt refuses the move and the
/// owner receives a blocking state instead of the old tree continuing silently.
pub(crate) async fn sync_live_branch_instances(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
    new_tip: &str,
) -> Vec<String> {
    let fences = live_branch_instance_fences(orch.fleet.snapshot(), project_id, branch);

    let mut failures = Vec::new();
    for fence in fences {
        let result = orch
            .fleet
            .operate_residency(
                orch,
                ResidencyOperation::RefreshCheckout {
                    fence: fence.clone(),
                    base_commit: new_tip.to_string(),
                    require_clean: true,
                },
            )
            .await;
        if let ResidencyResult::Failed {
            kind, diagnostic, ..
        } = result
        {
            let state = if kind
                == cairn_common::executor_protocol::ResidencyFailureKind::InvalidState
                && diagnostic.contains("dirty checkout")
            {
                "dirty tree"
            } else if kind == cairn_common::executor_protocol::ResidencyFailureKind::InvalidState {
                "checkout conflict"
            } else {
                "executor unavailable"
            };
            let message = format!(
                "⛔ BLOCKING [Dev instance sync: {state}] The live dev instance for `{branch}` could not advance to `{new_tip}` and may still be serving stale code. The running process was left intact. Exact diagnostic: {diagnostic}"
            );
            log::error!("{message}");
            notify_branch_owner(orch, project_id, branch, &message).await;
            failures.push(message);
        }
    }

    failures
}

fn orphan_dev_runner_candidates(
    processes: Vec<DevRunnerProcess>,
    terminal: &HashSet<String>,
    protected: &HashSet<String>,
    current_pid: u32,
) -> Vec<DevRunnerProcess> {
    processes
        .into_iter()
        .filter(|process| {
            process.pid != current_pid
                && terminal.contains(&process.branch_key)
                && !protected.contains(&process.branch_key)
        })
        .collect()
}

/// Reconcile development runners which predate executor-owned process
/// supervision. These processes are not represented by a residency after their
/// launcher and executor disappear, so the residency sweep above cannot see
/// them. The branch-keyed target path is their remaining durable identity.
///
/// A branch is protected whenever any database still describes non-terminal
/// work on it, or the connected executor reports a live dev residency for it.
/// Protection wins over terminal ownership so a held cell can never be reaped
/// because another replica has an older terminal record for the same branch.
#[cfg(unix)]
pub async fn sweep_terminal_dev_runners(orch: &Orchestrator) -> Result<usize, String> {
    let mut terminal = HashSet::new();
    let mut protected = HashSet::new();
    let mut job_branches = std::collections::HashMap::new();
    for db in orch.db.all_dbs().await {
        let rows: Vec<(String, String, String)> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT j.id, j.branch, i.status
                             FROM jobs j
                             JOIN issues i ON i.id = j.issue_id
                             WHERE j.branch IS NOT NULL AND j.branch != ''",
                            (),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push((row.text(0)?, row.text(1)?, row.text(2)?));
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| format!("load dev-runner owners: {error}"))?;
        for (job_id, branch, status) in rows {
            let key = stable_branch_key(&branch);
            job_branches.insert(job_id, key.clone());
            if matches!(status.as_str(), "merged" | "closed") {
                terminal.insert(key);
            } else {
                protected.insert(key);
            }
        }
    }
    for cell in orch.fleet.snapshot().cells {
        if let Some(residency) = cell.residency {
            match residency.holder {
                ResidencyHolder::DevInstance { .. } => {
                    if let Some(selector) = residency.selector {
                        protected.insert(stable_branch_key(&selector));
                    }
                }
                ResidencyHolder::Job { job_id } => {
                    if let Some(key) = job_branches.get(&job_id) {
                        protected.insert(key.clone());
                    }
                }
                _ => {}
            }
        }
    }

    let candidates = orphan_dev_runner_candidates(
        dev_runner_processes()?,
        &terminal,
        &protected,
        std::process::id(),
    );
    let mut reaped = 0;
    let mut failures = Vec::new();
    for candidate in candidates {
        let pid = candidate.pid;
        let key = &candidate.branch_key;
        if !dev_runner_identity_is_current(&candidate)? {
            reaped += 1;
            continue;
        }
        match nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        ) {
            Ok(()) => {}
            Err(nix::errno::Errno::ESRCH) => {
                reaped += 1;
                continue;
            }
            Err(error) => {
                failures.push(format!("stop orphan dev runner {pid} ({key}): {error}"));
                continue;
            }
        }
        for _ in 0..50 {
            if !dev_runner_identity_is_current(&candidate)? {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if dev_runner_identity_is_current(&candidate)? {
            failures.push(format!(
                "orphan dev runner {pid} ({key}) remained alive after SIGTERM"
            ));
            continue;
        }
        reaped += 1;
    }
    if failures.is_empty() {
        Ok(reaped)
    } else {
        Err(format!(
            "legacy dev-runner sweep was not fully verified (reaped {reaped}): {}",
            failures.join("; ")
        ))
    }
}

#[cfg(not(unix))]
pub async fn sweep_terminal_dev_runners(_orch: &Orchestrator) -> Result<usize, String> {
    Ok(0)
}

#[cfg(unix)]
fn dev_runner_identity_is_current(candidate: &DevRunnerProcess) -> Result<bool, String> {
    Ok(dev_runner_processes()?
        .into_iter()
        .any(|process| process == *candidate))
}

#[cfg(unix)]
fn dev_runner_processes() -> Result<Vec<DevRunnerProcess>, String> {
    let output = std::process::Command::new("ps")
        .args(["-axo", "pid=,lstart=,command="])
        .output()
        .map_err(|error| format!("enumerate development runners: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "enumerate development runners: ps exited with {}",
            output.status
        ));
    }
    Ok(parse_dev_runner_processes(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DevRunnerProcess {
    pid: u32,
    started_at: String,
    branch_key: String,
}

fn parse_dev_runner_processes(output: &str) -> Vec<DevRunnerProcess> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let started_at = (0..5)
                .map(|_| fields.next())
                .collect::<Option<Vec<_>>>()?
                .join(" ");
            let executable = fields.next()?;
            (fields.next() == Some("run")).then_some(())?;
            let components: Vec<_> = Path::new(executable).components().collect();
            let branch = components.windows(3).find_map(|parts| {
                (parts[0].as_os_str() == ".cairn-dev-target" && parts[1].as_os_str() == "branches")
                    .then(|| parts[2].as_os_str().to_string_lossy().into_owned())
            })?;
            let target = components.windows(2).any(|parts| {
                parts[0].as_os_str() == branch.as_str() && parts[1].as_os_str() == "target"
            });
            (target
                && Path::new(executable)
                    .file_name()
                    .is_some_and(|name| name == "cairn-runner"))
            .then_some(DevRunnerProcess {
                pid,
                started_at,
                branch_key: branch,
            })
        })
        .collect()
}

/// Reconcile dev residencies left behind by issues that reached a terminal
/// state before deterministic teardown was introduced.
pub async fn sweep_terminal_issue_instances(orch: &Orchestrator) -> Result<usize, String> {
    let mut targets: std::collections::BTreeMap<String, (Vec<String>, Vec<String>)> =
        std::collections::BTreeMap::new();
    for db in orch.db.all_dbs().await {
        let rows: Vec<(String, String, Option<String>)> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT j.id, j.project_id, j.branch
                             FROM jobs j
                             JOIN issues i ON i.id = j.issue_id
                             WHERE i.status IN ('merged', 'closed')",
                            (),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?));
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| format!("load terminal dev-instance owners: {error}"))?;
        for (job_id, project_id, branch) in rows {
            let target = targets.entry(project_id).or_default();
            if !target.0.contains(&job_id) {
                target.0.push(job_id);
            }
            if let Some(branch) = branch.filter(|branch| !branch.is_empty()) {
                if !target.1.contains(&branch) {
                    target.1.push(branch);
                }
            }
        }
    }

    let mut released = 0;
    for (project_id, (job_ids, branches)) in targets {
        let fences = issue_instance_fences(orch.fleet.snapshot(), &project_id, &job_ids, &branches);
        released += fences.len();
        let mut failures = Vec::new();
        for fence in fences {
            let holder = fence.holder.clone();
            if let Err(diagnostic) = crate::fleet::residency::release(orch, &fence).await {
                failures.push(format!("{holder:?}: {diagnostic}"));
            }
        }
        if !failures.is_empty() {
            return Err(format!(
                "startup dev-instance sweep was not verified: {}",
                failures.join("; ")
            ));
        }
    }
    Ok(released)
}

/// Select dev-instance residencies owned by jobs being torn down. Older
/// residencies may predate owner attribution, so an unattributed instance is
/// matched by its project and agent branch. An instance attributed to a
/// different job is never consumed merely because it shares a selector.
fn issue_instance_fences(
    snapshot: cairn_common::executor_protocol::FleetSnapshot,
    project_id: &str,
    job_ids: &[String],
    branches: &[String],
) -> Vec<cairn_common::executor_protocol::ResidencyFence> {
    let job_ids: HashSet<&str> = job_ids.iter().map(String::as_str).collect();
    let branches: HashSet<&str> = branches.iter().map(String::as_str).collect();
    snapshot
        .cells
        .into_iter()
        .filter_map(|cell| {
            let residency = cell.residency?;
            if cell.project_id != project_id
                || !matches!(residency.holder, ResidencyHolder::DevInstance { .. })
            {
                return None;
            }
            let owned = residency
                .owner_ref
                .as_ref()
                .and_then(|owner| owner.job_id.as_deref())
                .map(|job_id| job_ids.contains(job_id))
                .unwrap_or_else(|| {
                    residency.owner_ref.is_none()
                        && residency
                            .selector
                            .as_deref()
                            .is_some_and(|selector| branches.contains(selector))
                });
            owned.then_some(cairn_common::executor_protocol::ResidencyFence {
                holder: residency.holder,
                incarnation_id: residency.incarnation_id,
                cell_epoch: cell.cell_epoch,
            })
        })
        .collect()
}

pub(crate) async fn release_issue_instances(
    orch: &Orchestrator,
    project_id: &str,
    job_ids: &[String],
    branches: &[String],
) -> Result<(), String> {
    let fences = issue_instance_fences(orch.fleet.snapshot(), project_id, job_ids, branches);
    let mut failures = Vec::new();
    for fence in fences {
        let holder = fence.holder.clone();
        if let Err(diagnostic) = crate::fleet::residency::release(orch, &fence).await {
            failures.push(format!("{holder:?}: {diagnostic}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "dev-instance teardown was not verified: {}",
            failures.join("; ")
        ))
    }
}

fn live_branch_instance_fences(
    snapshot: cairn_common::executor_protocol::FleetSnapshot,
    project_id: &str,
    branch: &str,
) -> Vec<cairn_common::executor_protocol::ResidencyFence> {
    snapshot
        .cells
        .into_iter()
        .filter_map(|cell| {
            let residency = cell.residency?;
            let is_instance = matches!(residency.holder, ResidencyHolder::DevInstance { .. });
            (cell.project_id == project_id
                && is_instance
                && residency.selector.as_deref() == Some(branch))
            .then_some(cairn_common::executor_protocol::ResidencyFence {
                holder: residency.holder,
                incarnation_id: residency.incarnation_id,
                cell_epoch: cell.cell_epoch,
            })
        })
        .collect()
}

async fn notify_branch_owner(orch: &Orchestrator, project_id: &str, branch: &str, message: &str) {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    let job_id = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs WHERE project_id = ?1 AND branch = ?2 ORDER BY created_at DESC LIMIT 1",
                        params![project_id.as_str(), branch.as_str()],
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .ok()
        .flatten();
    if let Some(run_id) = job_id
        .as_deref()
        .and_then(|job| latest_run_for_job(&orch.db.local, job))
    {
        if let Err(error) = queue_system_direct(orch, &run_id, message, DeliveryUrgency::Steer) {
            log::error!(
                "could not notify dev instance owner after synchronization failure: {error}"
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevInstanceResolution {
    pub project_id: String,
    pub repository_path: PathBuf,
    pub selector: String,
    pub commit_id: String,
    /// The work the selector names, when it names an agent branch this project
    /// minted for a job. Descriptive: it is what a surface calls the instance,
    /// not who holds its lease.
    pub owner_ref: Option<CellOwnerRef>,
}

pub async fn resolve_launch_coordinate(
    orch: &Orchestrator,
    request: &DevInstanceLaunchRequest,
) -> Result<DevInstanceResolution, DevInstanceLaunchFailure> {
    if request.project_id.trim().is_empty() {
        return Err(DevInstanceLaunchFailure::InvalidProject {
            diagnostic: "projectId must not be empty".into(),
        });
    }
    let project_id = request.project_id.clone();
    let row = orch
        .db
        .local
        .read(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT repo_path, default_branch FROM projects WHERE id = ?1 LIMIT 1",
                        params![project_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((row.opt_text(0)?, row.opt_text(1)?)))
            })
        })
        .await
        .map_err(|error| DevInstanceLaunchFailure::InvalidProject {
            diagnostic: error.to_string(),
        })?
        .ok_or_else(|| DevInstanceLaunchFailure::ProjectNotFound {
            diagnostic: format!("no registered project has id '{}'", request.project_id),
        })?;
    let repository_path =
        row.0
            .map(PathBuf::from)
            .ok_or_else(|| DevInstanceLaunchFailure::InvalidProject {
                diagnostic: format!("project '{}' has no local repository", request.project_id),
            })?;
    let managed_store = crate::jj::project_store_dir(&orch.config_dir, &repository_path);
    // The store's git backend is the project's own `.git`, but its view of refs
    // is only as current as its last import: a commit that reached the checkout
    // after the last job provisioning is otherwise unresolvable here. Preparing
    // the store is what makes both an explicit selector and the checkout's own
    // head resolve against the refs that exist right now.
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = managed_store.clone();
    let repository = repository_path.clone();
    tokio::task::spawn_blocking(move || crate::jj::ensure_project_store(&jj, &store, &repository))
        .await
        .map_err(|error| error.to_string())
        .and_then(|prepared| prepared)
        .map_err(|diagnostic| DevInstanceLaunchFailure::InvalidProject {
            diagnostic: format!(
                "project '{}' is not ready to launch: {diagnostic}",
                request.project_id
            ),
        })?;

    let explicit = request
        .selector
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    // The selector is the instance's identity — it keys the lease, the instance
    // home, and the build cache — while the coordinate is what gets resolved to
    // a commit. They differ only for an implicit launch, where the branch names
    // the instance and the checkout's own head names the commit.
    let (selector, coordinate) = match explicit {
        Some(selector) => {
            let selector =
                canonical_selector(orch, &request.project_id, selector, row.1.as_deref()).await?;
            (selector.clone(), selector)
        }
        None => {
            let git = orch.services.git.clone();
            let repository = repository_path.clone();
            let caller = request
                .checkout
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from);
            let (source, checkout) = tokio::task::spawn_blocking(move || {
                let source = caller
                    .and_then(|candidate| caller_checkout(git.as_ref(), &candidate, &repository))
                    .unwrap_or_else(|| repository.clone());
                let coordinate = live_checkout_coordinate(git.as_ref(), &source);
                (source, coordinate)
            })
            .await
            .map_err(|error| {
                DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven {
                    diagnostic: format!("could not read the launching checkout: {error}"),
                }
            })?;
            let checkout = checkout.ok_or_else(|| {
                DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven {
                    diagnostic: format!(
                        "nothing is checked out at {}; launch with --branch <selector>",
                        source.display()
                    ),
                }
            })?;
            let name = match checkout.branch {
                Some(branch) => branch,
                // A detached checkout's own commit names nothing on its own, so
                // the branch has to come from somewhere. Provenance first: when
                // the runner materialized this checkout for a job, it knows
                // which branch that job works on and never has to guess.
                None => {
                    match checkout_provenance_branch(orch, &request.project_id, &source).await {
                        Some(branch) => branch,
                        None => {
                            let jj =
                                crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
                            let store = managed_store.clone();
                            let commit = checkout.commit.clone();
                            let default_branch = row.1.clone();
                            tokio::task::spawn_blocking(move || {
                            branch_naming_commit(&jj, &store, &commit, default_branch.as_deref())
                        })
                        .await
                        .ok()
                        .flatten()
                        .ok_or_else(|| {
                            DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven {
                                diagnostic: format!(
                                    "the checkout at {} is not on a branch, and no branch in this project names the commit it is on, so this launch has no instance to be; launch with --branch <selector>",
                                    source.display()
                                ),
                            }
                        })?
                        }
                    }
                }
            };
            (name, checkout.commit)
        }
    };

    let commit_id = cairn_vcs::resolve_coordinate(&managed_store, &coordinate)
        .await
        .map_err(|error| {
            if explicit.is_none() {
                return DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven {
                    diagnostic: format!(
                        "the commit checked out at {} is not one this project can build; launch with --branch <selector>",
                        repository_path.display()
                    ),
                };
            }
            match error {
                cairn_vcs::CoordinateResolutionError::Invalid(_) => {
                    DevInstanceLaunchFailure::InvalidSelector {
                        diagnostic: format!("invalid selector '{coordinate}'"),
                    }
                }
                cairn_vcs::CoordinateResolutionError::Ambiguous(_) => {
                    DevInstanceLaunchFailure::AmbiguousSelector {
                        diagnostic: format!(
                            "selector '{coordinate}' resolves to more than one commit"
                        ),
                    }
                }
                other => DevInstanceLaunchFailure::SelectorNotFound {
                    diagnostic: format!("selector '{coordinate}' is not runner-resolvable: {other}"),
                },
            }
        })?;

    let git = orch.services.git.clone();
    let repository = repository_path.clone();
    let proven = {
        let selector = selector.clone();
        let commit_id = commit_id.clone();
        tokio::task::spawn_blocking(move || {
            prove_buildable_coordinate(git.as_ref(), &repository, &selector, &commit_id)
        })
        .await
        .map_err(|error| error.to_string())
    };
    proven
        .and_then(|proven| proven)
        .map_err(|diagnostic| DevInstanceLaunchFailure::UnbuildableCoordinate { diagnostic })?;

    let owner_ref = issue_owner_for_selector(orch, &request.project_id, &selector).await;
    Ok(DevInstanceResolution {
        project_id: request.project_id.clone(),
        repository_path,
        selector,
        commit_id,
        owner_ref,
    })
}

/// The issue a selector belongs to, when the selector names an agent branch the
/// runner minted for a job on this project. An instance built from that branch
/// is the operator's build *of that work*, so the row listing it can say `#3104`
/// the way every other row does instead of naming a lease slug.
///
/// `job_id` is deliberately left unset. It is the field lease adoption keys off
/// (`fleet::find_adoptable_residency`), and a dev instance is emphatically
/// not that job's execution home: claiming the id would let a running job move
/// into the operator's instance. An unresolvable selector — a plain branch, a
/// commit, a branch whose job is gone — yields `None`, and the instance names
/// itself by its selector.
async fn issue_owner_for_selector(
    orch: &Orchestrator,
    project_id: &str,
    selector: &str,
) -> Option<CellOwnerRef> {
    let owned_project = project_id.to_string();
    let owned_selector = selector.to_string();
    let row = orch
        .db
        .local
        .read(move |conn| {
            let project_id = owned_project.clone();
            let selector = owned_selector.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT p.key, j.node_name, i.number
                        FROM jobs j
                        JOIN projects p ON j.project_id = p.id
                        JOIN issues i ON j.issue_id = i.id
                        WHERE j.project_id = ?1 AND j.branch = ?2
                        ORDER BY j.created_at DESC
                        LIMIT 1
                        ",
                        params![project_id.as_str(), selector.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.opt_i64(2)?.map(|value| value as i32),
                )))
            })
        })
        .await
        .ok()
        .flatten()?;
    Some(CellOwnerRef {
        project_id: project_id.to_string(),
        project_key: row.0,
        issue_number: Some(row.2?),
        job_id: None,
        execution_seq: None,
        node_kind: row.1,
    })
}

/// The coordinate an explicit selector names, in the one spelling that keys the
/// instance.
///
/// A node URI names work rather than a revision, so it resolves to the branch
/// that node minted. Canonicalizing here is what makes
/// `--branch cairn://p/CAIRN/3104/1/builder` and
/// `--branch agent/CAIRN-3104-builder-1` the same instance rather than two
/// builds of one commit with separate homes, databases, ports, and Cargo
/// caches — the same reason `main` resolves to the project's own default
/// branch. Everything downstream, the lease key and the row's attribution
/// alike, sees only the resolved branch.
async fn canonical_selector(
    orch: &Orchestrator,
    project_id: &str,
    selector: &str,
    default_branch: Option<&str>,
) -> Result<String, DevInstanceLaunchFailure> {
    if selector == "main" {
        return Ok(default_branch.unwrap_or("main").to_string());
    }
    if !selector.starts_with("cairn://") {
        return Ok(selector.to_string());
    }
    let Some(CairnResource::Node {
        project,
        number,
        exec_seq,
        node_id,
    }) = cairn_common::uri::parse_uri(selector)
    else {
        return Err(DevInstanceLaunchFailure::InvalidSelector {
            diagnostic: format!(
                "'{selector}' is not a node URI; a Cairn selector names one node, as in cairn://p/CAIRN/3104/1/builder"
            ),
        });
    };
    let node = format!("{project}/{number}/{exec_seq}/{node_id}");
    let owned_project = project_id.to_string();
    let key = cairn_common::uri::canonical_project(project);
    let branch = orch
        .db
        .local
        .read(move |conn| {
            let project_id = owned_project.clone();
            let key = key.clone();
            let node_id = node_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.branch
                        FROM jobs j
                        JOIN issues i ON j.issue_id = i.id
                        JOIN projects p ON i.project_id = p.id
                        JOIN executions e ON j.execution_id = e.id
                        WHERE p.id = ?1 AND p.key = ?2 AND i.number = ?3 AND e.seq = ?4
                          AND j.parent_job_id IS NULL AND j.uri_segment = ?5
                        LIMIT 1
                        ",
                        params![
                            project_id.as_str(),
                            key.as_str(),
                            number,
                            exec_seq,
                            node_id.as_str()
                        ],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some(row.opt_text(0)?))
            })
        })
        .await
        .map_err(|error| DevInstanceLaunchFailure::InvalidProject {
            diagnostic: error.to_string(),
        })?;
    match branch {
        Some(Some(branch)) => Ok(branch),
        Some(None) => Err(DevInstanceLaunchFailure::SelectorNotFound {
            diagnostic: format!(
                "node {node} minted no branch of its own to build; launch with --branch <branch>"
            ),
        }),
        None => Err(DevInstanceLaunchFailure::SelectorNotFound {
            diagnostic: format!("this project has no node {node}"),
        }),
    }
}

/// Prove the resolved commit can actually host a development instance before any
/// lease is acquired: the object must be present in the project's own database,
/// and its tree must carry the runtime entrypoint the executor is about to run.
///
/// Resolution answers a coordinate against the project's managed store, whose
/// git backend is this repository's `.git` — so a coordinate that survives
/// resolution but fails here names a commit this project cannot build. The
/// diagnostic names the selector and the commit, and the caller raises it as a
/// typed `UnbuildableCoordinate` failure. Refusing at resolution is what keeps
/// the alternative from happening: a checkout materialized at a commit that was
/// never the Cairn application, failing later as a relative-path module error
/// that says nothing about what was resolved.
fn prove_buildable_coordinate(
    git: &dyn GitClient,
    repository_path: &Path,
    selector: &str,
    commit_id: &str,
) -> Result<(), String> {
    let present = git
        .run(
            repository_path,
            vec![
                "cat-file".into(),
                "-e".into(),
                format!("{commit_id}^{{commit}}"),
            ],
        )
        .map_err(|diagnostic| {
            format!(
                "could not read commit {commit_id} (selector '{selector}') from {}: {diagnostic}",
                repository_path.display()
            )
        })?;
    if !present.success {
        return Err(format!(
            "selector '{selector}' resolved to commit {commit_id}, which is not in the object database at {}",
            repository_path.display()
        ));
    }
    let carries_runtime = git
        .run(
            repository_path,
            vec![
                "cat-file".into(),
                "-e".into(),
                format!("{commit_id}:{RUNTIME_ENTRYPOINT}"),
            ],
        )
        .map_err(|diagnostic| {
            format!(
                "could not read {RUNTIME_ENTRYPOINT} at commit {commit_id} (selector '{selector}') in {}: {diagnostic}",
                repository_path.display()
            )
        })?;
    if !carries_runtime.success {
        return Err(format!(
            "selector '{selector}' resolved to commit {commit_id} in {}, which does not contain {RUNTIME_ENTRYPOINT}: that repository is not a checkout of the Cairn application",
            repository_path.display()
        ));
    }
    Ok(())
}

/// What the operator currently has checked out: the branch it is on, when it is
/// on one, and the commit a build from that checkout would contain.
///
/// A checkout Cairn materializes is a git worktree with a DETACHED head, so
/// `branch` is absent for every launch from a job's own shell. The commit is
/// still there — it is what gets built — but it is a storage address, and an
/// address cannot serve as the instance's name. Which branch names such a
/// checkout is a question for the project's store, not for git's HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckoutCoordinate {
    branch: Option<String>,
    commit: String,
}

/// The branch a checkout was materialized for, when the runner materialized it.
///
/// This is the authoritative answer for a launch from a job's own shell, and it
/// has to come before any commit lookup: a job's cell is a detached worktree,
/// and an agent branch begins life at the tip of the branch it was cut from, so
/// `main` and the agent's own branch routinely name the same commit until the
/// job's first commit lands. Inverting the commit to a bookmark would therefore
/// answer `main` for exactly the launch most worth attributing — one made from
/// a job terminal before its branch diverged — losing the `#<issue>` identity
/// and sharing `main`'s instance and cache rather than the job's.
///
/// The provenance is read from the runner's own state: the cell records the
/// residency holding it, and that job records the branch it works on. Nothing
/// here consults the launching shell's environment, so an exported
/// `CAIRN_WORKTREE_BRANCH` still cannot speak for a launch (CAIRN-3107) while
/// the launch nevertheless knows whose checkout it was made from.
async fn checkout_provenance_branch(
    orch: &Orchestrator,
    project_id: &str,
    checkout: &Path,
) -> Option<String> {
    let job_id = cell_job_for_checkout(&orch.fleet.snapshot().cells, project_id, checkout)?;
    let owned_project = project_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let project_id = owned_project.clone();
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT branch FROM jobs WHERE id = ?1 AND project_id = ?2 LIMIT 1",
                        params![job_id.as_str(), project_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                row.opt_text(0)
            })
        })
        .await
        .ok()
        .flatten()
        .map(|branch| branch.trim().to_string())
        .filter(|branch| !branch.is_empty())
}

/// The job holding the cell a checkout path lives in, if any.
///
/// The launch carries the directory it was run from, which may be anywhere
/// inside the checkout, so a cell claims it by containment rather than
/// equality. Paths are canonicalized before comparison because a cell root and
/// a caller's `cwd` can spell the same directory differently.
fn cell_job_for_checkout(
    cells: &[cairn_common::executor_protocol::PersistentCellState],
    project_id: &str,
    checkout: &Path,
) -> Option<String> {
    let real = |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let checkout = real(checkout);
    cells.iter().find_map(|cell| {
        if cell.project_id != project_id || !checkout.starts_with(real(Path::new(&cell.path))) {
            return None;
        }
        match &cell.residency.as_ref()?.holder {
            ResidencyHolder::Job { job_id } => Some(job_id.clone()),
            _ => None,
        }
    })
}

/// The branch that names an instance built from a detached checkout the fleet
/// does not own — an operator's own checkout of some commit.
///
/// The commit alone cannot name it: it tells two dev builds apart only by hash,
/// it keys a fresh cold build cache on every commit instead of reusing the
/// branch's, and it is what a Running-panel row was left rendering as the
/// operator's identity (CAIRN-3241). The store knows which branch that commit
/// is the tip of, so the launch asks it rather than inventing a name.
///
/// More than one bookmark can sit on one commit. No job's identity is at stake
/// here — a checkout the fleet does not own belongs to no job, and one that
/// does is answered by [`checkout_provenance_branch`] before this is reached —
/// so the project's default branch wins, because that commit IS the default
/// branch's code. Past that it is the first name in sorted order, so repeated
/// launches of one checkout always agree on which instance they are.
fn branch_naming_commit(
    jj: &crate::jj::JjEnv,
    store: &Path,
    commit: &str,
    default_branch: Option<&str>,
) -> Option<String> {
    let mut bookmarks = crate::jj::local_bookmarks_at(jj, store, commit).ok()?;
    bookmarks.sort();
    let default = default_branch
        .map(str::trim)
        .filter(|branch| !branch.is_empty());
    if let Some(default) = default {
        if bookmarks.iter().any(|bookmark| bookmark == default) {
            return Some(default.to_string());
        }
    }
    bookmarks.into_iter().next()
}

/// The checkout a caller launched from, when it is genuinely a checkout of this
/// project's repository. An implicit launch names the branch the caller is
/// standing on, and only the caller knows which working tree that is — but a
/// path arriving over the wire is a claim, so it is proven against the
/// repository the project is registered at before any ref is read. Worktrees cut
/// from one repository share a common directory, which is exactly the identity
/// that makes them the same repository and different checkouts. An unrelated or
/// unreadable path yields `None`, and the caller falls back to the project's own
/// checkout.
fn caller_checkout(
    git: &dyn GitClient,
    candidate: &Path,
    repository_path: &Path,
) -> Option<PathBuf> {
    let common_directory = |path: &Path| {
        git.rev_parse(
            path,
            vec![
                "--path-format=absolute".to_string(),
                "--git-common-dir".to_string(),
            ],
        )
        .ok()
        .map(|value| PathBuf::from(value.trim()))
        .and_then(|value| std::fs::canonicalize(value).ok())
    };
    let project = common_directory(repository_path)?;
    (common_directory(candidate)? == project).then(|| candidate.to_path_buf())
}

/// Read the live checkout's committed coordinate. `None` when the path is not a
/// checkout with a readable commit, which the caller turns into a typed refusal
/// rather than a guess about what to build.
fn live_checkout_coordinate(
    git: &dyn GitClient,
    repository_path: &Path,
) -> Option<CheckoutCoordinate> {
    let commit = git
        .rev_parse(repository_path, vec!["HEAD".to_string()])
        .ok()
        .map(|commit| commit.trim().to_string())
        .filter(|commit| !commit.is_empty())?;
    let branch = git
        .current_branch(repository_path)
        .ok()
        .map(|branch| branch.trim().to_string())
        .filter(|branch| !branch.is_empty());
    Some(CheckoutCoordinate { branch, commit })
}

struct EventForwarder {
    sender: Mutex<Option<mpsc::UnboundedSender<ResidentProcessEvent>>>,
}

impl EventForwarder {
    fn send(&self, event: ResidentProcessEvent) {
        if let Ok(sender) = self.sender.lock() {
            if let Some(sender) = sender.as_ref() {
                let _ = sender.send(event);
            }
        }
    }

    fn close(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
    }
}

pub async fn run_launch_session(
    orch: Orchestrator,
    request: DevInstanceLaunchRequest,
    events: mpsc::UnboundedSender<DevInstanceLaunchEvent>,
    resolution: DevInstanceResolution,
    mut controls: mpsc::UnboundedReceiver<DevInstanceLaunchControl>,
) {
    let _ = events.send(DevInstanceLaunchEvent::Resolved {
        selector: resolution.selector.clone(),
        commit_id: resolution.commit_id.clone(),
    });
    if orch.fleet.executor_generation().is_none() {
        let _ = events.send(DevInstanceLaunchEvent::Failed {
            failure: DevInstanceLaunchFailure::ColocatedExecutorUnavailable {
                diagnostic: "the colocated executor is not connected".into(),
            },
        });
        return;
    }

    let branch_key = stable_branch_key(&resolution.selector);
    let holder = ResidencyHolder::DevInstance {
        instance_id: format!("{}:{branch_key}", resolution.project_id),
    };
    let _ = events.send(DevInstanceLaunchEvent::Acquiring);
    let acquire = orch
        .fleet
        .operate_residency(
            &orch,
            ResidencyOperation::Acquire {
                request: ResidencyAcquireRequest {
                    holder: holder.clone(),
                    owner_ref: resolution.owner_ref.clone(),
                    selector: Some(resolution.selector.clone()),
                    executor: None,
                    repository: RepositoryLocator::ColocatedPath {
                        project_id: resolution.project_id.clone(),
                        repository_id: resolution.project_id.clone(),
                        absolute_path: resolution.repository_path.to_string_lossy().into_owned(),
                    },
                    initial_base_commit: resolution.commit_id.clone(),
                    // A dev instance's checkout carries its own node_modules and
                    // build cache, which is what makes this footprint large. The
                    // concurrency its server holds is declared on the server
                    // process, not here.
                    footprint: ResidencyFootprint {
                        memory_bytes: 2 * 1024 * 1024 * 1024,
                        disk_growth_bytes: 8 * 1024 * 1024 * 1024,
                    },
                    death_policy: OwnerDeathPolicy {
                        heartbeat_timeout_ms: LEASE_TIMEOUT_MS,
                        reclaim_grace_ms: RECLAIM_GRACE_MS,
                    },
                    priority: CellPriority::AgentInteractive,
                    wait_horizon_unix_ms: crate::fleet::default_wait_horizon_unix_ms(
                        &crate::config::settings::load_fleet(&orch.config_dir),
                    ),
                    waiting_since_unix_ms: unix_time_ms(),
                },
            },
        )
        .await;
    let fence = match acquire {
        ResidencyResult::State { ref cell } => match cell.residency.as_ref() {
            Some(residency) => cairn_common::executor_protocol::ResidencyFence {
                holder,
                incarnation_id: residency.incarnation_id.clone(),
                cell_epoch: cell.cell_epoch,
            },
            None => {
                fail(
                    &events,
                    DevInstanceLaunchFailure::LeaseUnavailable {
                        diagnostic: "acquisition returned a cell with no residency".into(),
                    },
                );
                return;
            }
        },
        ResidencyResult::Failed {
            kind,
            diagnostic,
            cell_outcome,
        } => {
            fail(
                &events,
                DevInstanceLaunchFailure::from_residency_failure(
                    kind,
                    diagnostic,
                    cell_outcome.map(|outcome| *outcome),
                ),
            );
            return;
        }
        other => {
            fail(
                &events,
                DevInstanceLaunchFailure::LeaseUnavailable {
                    diagnostic: format!("unexpected acquisition result: {other:?}"),
                },
            );
            return;
        }
    };
    let _ = events.send(DevInstanceLaunchEvent::Acquired);
    if let Err(diagnostic) =
        crate::fleet::residency::refresh(&orch, &fence, &resolution.commit_id).await
    {
        fail(
            &events,
            DevInstanceLaunchFailure::LeaseUnavailable { diagnostic },
        );
        let _ = crate::fleet::residency::release(&orch, &fence).await;
        return;
    }

    let (process_tx, mut process_rx) = mpsc::unbounded_channel();
    let forwarder = Arc::new(EventForwarder {
        sender: Mutex::new(Some(process_tx)),
    });
    let callback_forwarder = forwarder.clone();
    let callback_fence = fence.clone();
    let generation = Arc::new(AtomicU64::new(0));
    let callback_generation = generation.clone();
    orch.fleet.subscribe_resident_process_events(move |event| {
        let expected = callback_generation.load(Ordering::Acquire);
        if event.holder == callback_fence.holder
            && event.incarnation_id == callback_fence.incarnation_id
            && event.cell_epoch == callback_fence.cell_epoch
            && event.process_key == PROCESS_KEY
            && (expected == 0 || event.process_generation == expected)
        {
            callback_forwarder.send(event);
        }
    });

    let mut args = vec![
        RUNTIME_ENTRYPOINT.into(),
        "--branch".into(),
        resolution.selector.clone(),
        "--seed".into(),
        request.seed.clone(),
    ];
    if request.force_copy {
        args.push("--force-copy".into());
    }
    let _ = events.send(DevInstanceLaunchEvent::Starting);
    let started = orch
        .fleet
        .operate_residency(
            &orch,
            ResidencyOperation::StartProcess {
                fence: fence.clone(),
                process_key: PROCESS_KEY.into(),
                kind: ResidentProcessKind::DevInstance {
                    source_terminal_session_id: request.source_terminal_session_id.clone(),
                },
                // The dev server is the thing that works, so it is the thing
                // that is charged. Its memory and disk are the residency's
                // footprint, declared at acquire.
                reservation: Some(ResourceReservation {
                    memory_bytes: 0,
                    disk_growth_bytes: 0,
                    concurrency_units: 1,
                    source: ResourceReservationSource::Declared,
                }),
                process: ResidentProcessSpec {
                    program: "bun".into(),
                    args,
                    cwd: String::new(),
                    cwd_root: ResidentProcessCwdRoot::Checkout,
                    env: vec![("CAIRN_WORKTREE_BRANCH".into(), resolution.selector.clone())],
                    sandbox_mode: ProcessSandboxMode::Unconfined,
                    sandbox_policy: None,
                    runtime_assets: Vec::new(),
                    io: ResidentProcessIoMode::Pipe,
                },
            },
        )
        .await;
    let process_generation = match started {
        ResidencyResult::State { ref cell } => cell
            .occupancy
            .processes
            .get(PROCESS_KEY)
            .map(|process| process.generation),
        ResidencyResult::Failed {
            kind,
            diagnostic,
            cell_outcome,
        } => {
            fail(
                &events,
                DevInstanceLaunchFailure::from_residency_failure(
                    kind,
                    diagnostic,
                    cell_outcome.map(|outcome| *outcome),
                ),
            );
            None
        }
        other => {
            fail(
                &events,
                DevInstanceLaunchFailure::ProcessStart {
                    diagnostic: format!("unexpected process start result: {other:?}"),
                },
            );
            None
        }
    };
    let Some(process_generation) = process_generation else {
        crate::fleet::residency::rollback(&orch, &fence, PROCESS_KEY).await;
        forwarder.close();
        return;
    };
    generation.store(process_generation, Ordering::Release);
    let _ = events.send(DevInstanceLaunchEvent::Running);

    let mut renew = tokio::time::interval(RENEW_INTERVAL);
    renew.tick().await;
    let mut stdout = Vec::new();
    loop {
        tokio::select! {
            control = controls.recv() => {
                let disconnected = matches!(
                    control,
                    Some(DevInstanceLaunchControl::ConnectionClosing) | None
                );
                if let Err(diagnostic) =
                    crate::fleet::residency::stop(&orch, &fence, PROCESS_KEY).await
                {
                    fail(&events, DevInstanceLaunchFailure::LeaseLost { diagnostic });
                }
                if disconnected {
                    fail(
                        &events,
                        DevInstanceLaunchFailure::Cancelled {
                            diagnostic: "launch client disconnected".into(),
                        },
                    );
                }
                wait_for_stopped_process(
                    &mut process_rx,
                    &events,
                    &mut stdout,
                    process_generation,
                ).await;
                break;
            }
            _ = renew.tick() => {
                if let Err(diagnostic) = crate::fleet::residency::renew(&orch, &fence).await {
                    fail(&events, DevInstanceLaunchFailure::LeaseLost { diagnostic });
                    let _ = crate::fleet::residency::stop(&orch, &fence, PROCESS_KEY).await;
                    break;
                }
            }
            event = process_rx.recv() => {
                let Some(event) = event else { break; };
                if !is_current_process_generation(&event, process_generation) {
                    continue;
                }
                match event.event {
                    ResidentProcessEventKind::Output { sequence, stream, data } => {
                        let is_stdout = stream
                            == cairn_common::executor_protocol::ResidentProcessStream::Stdout;
                        let _ = events.send(DevInstanceLaunchEvent::Output {
                            sequence,
                            stream,
                            data: data.clone(),
                        });
                        if is_stdout {
                            stdout.extend_from_slice(&data);
                            emit_readiness(&events, &mut stdout);
                        }
                    }
                    ResidentProcessEventKind::State { status: ResidentProcessStatus::Exited { exit_code, restartable, executor_lost, .. } } => {
                        if executor_lost {
                            let _ = events.send(DevInstanceLaunchEvent::ExecutorLost { restartable });
                        } else {
                            let _ = events.send(DevInstanceLaunchEvent::Exited { exit_code, restartable });
                        }
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    forwarder.close();
    let _ = events.send(DevInstanceLaunchEvent::Releasing);
    match crate::fleet::residency::release(&orch, &fence).await {
        Ok(()) => {
            let _ = events.send(DevInstanceLaunchEvent::Released);
        }
        Err(diagnostic) => fail(
            &events,
            DevInstanceLaunchFailure::ReleaseFailure { diagnostic },
        ),
    }
}

async fn wait_for_stopped_process(
    process_rx: &mut mpsc::UnboundedReceiver<ResidentProcessEvent>,
    events: &mpsc::UnboundedSender<DevInstanceLaunchEvent>,
    stdout: &mut Vec<u8>,
    process_generation: u64,
) {
    let wait = async {
        while let Some(event) = process_rx.recv().await {
            if event.process_generation != process_generation {
                continue;
            }
            match event.event {
                ResidentProcessEventKind::Output {
                    sequence,
                    stream,
                    data,
                } => {
                    let is_stdout =
                        stream == cairn_common::executor_protocol::ResidentProcessStream::Stdout;
                    let _ = events.send(DevInstanceLaunchEvent::Output {
                        sequence,
                        stream,
                        data: data.clone(),
                    });
                    if is_stdout {
                        stdout.extend_from_slice(&data);
                        emit_readiness(events, stdout);
                    }
                }
                ResidentProcessEventKind::State {
                    status:
                        ResidentProcessStatus::Exited {
                            exit_code,
                            restartable,
                            executor_lost,
                            ..
                        },
                } => {
                    if executor_lost {
                        let _ = events.send(DevInstanceLaunchEvent::ExecutorLost { restartable });
                    } else {
                        let _ = events.send(DevInstanceLaunchEvent::Exited {
                            exit_code,
                            restartable,
                        });
                    }
                    return;
                }
                _ => {}
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), wait).await;
}

fn emit_readiness(events: &mpsc::UnboundedSender<DevInstanceLaunchEvent>, buffer: &mut Vec<u8>) {
    while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
        let line = String::from_utf8_lossy(&buffer[..newline]);
        if let Some(json) = line.trim().strip_prefix(READY_MARKER) {
            if let Ok(readiness) = serde_json::from_str::<DevInstanceReadiness>(json) {
                let _ = events.send(DevInstanceLaunchEvent::Ready { readiness });
            }
        }
        buffer.drain(..=newline);
    }
    if buffer.len() > 64 * 1024 {
        buffer.clear();
    }
}

fn is_current_process_generation(event: &ResidentProcessEvent, expected: u64) -> bool {
    event.process_generation == expected
}

fn stable_branch_key(selector: &str) -> String {
    let mut key = selector
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while key.contains("--") {
        key = key.replace("--", "-");
    }
    let key = key.trim_matches('-');
    if key.is_empty() {
        "default".into()
    } else {
        key.chars().take(48).collect()
    }
}

/// The launch session a resolved coordinate belongs to: the instance it names.
///
/// A session is shared so that a client whose transport blips reattaches to its
/// own running launch instead of starting a second one. What may share it is the
/// instance, not the wire request that asked for it — an implicit launch carries
/// no branch at all, so keying on the request made every implicit launch on the
/// machine one session, and the second caller silently inherited a stream that
/// had already been delivered. Keying on the resolved instance keeps reconnect
/// working and lets two branches launch at once.
pub fn launch_session_key(resolution: &DevInstanceResolution) -> String {
    format!(
        "{}:{}",
        resolution.project_id,
        stable_branch_key(&resolution.selector)
    )
}

fn fail(events: &mpsc::UnboundedSender<DevInstanceLaunchEvent>, failure: DevInstanceLaunchFailure) {
    let _ = events.send(DevInstanceLaunchEvent::Failed { failure });
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::services::RealGitClient;
    use crate::storage::{LocalDb, SearchIndex};

    #[test]
    fn legacy_dev_runner_discovery_is_narrow_and_hold_aware() {
        let processes = parse_dev_runner_processes(
            " 12 Fri Aug  7 16:00:00 2026 /Users/a/.cairn-dev-target/branches/agent-cairn-1-builder-0/target/debug/cairn-runner run\n\
             13 Fri Aug  7 16:01:00 2026 /Users/a/.cairn-dev-target/branches/agent-cairn-2-builder-0/target/release/cairn-runner run\n\
             14 Fri Aug  7 16:02:00 2026 /repo/target/debug/cairn-runner run\n\
             15 Fri Aug  7 16:03:00 2026 /Users/a/.cairn-dev-target/branches/agent-cairn-3-builder-0/target/debug/cairn-runner serve\n",
        );
        assert_eq!(
            processes,
            vec![
                DevRunnerProcess {
                    pid: 12,
                    started_at: "Fri Aug 7 16:00:00 2026".into(),
                    branch_key: "agent-cairn-1-builder-0".into(),
                },
                DevRunnerProcess {
                    pid: 13,
                    started_at: "Fri Aug 7 16:01:00 2026".into(),
                    branch_key: "agent-cairn-2-builder-0".into(),
                }
            ]
        );

        let terminal = HashSet::from([
            "agent-cairn-1-builder-0".into(),
            "agent-cairn-2-builder-0".into(),
        ]);
        let protected = HashSet::from(["agent-cairn-2-builder-0".into()]);
        assert_eq!(
            orphan_dev_runner_candidates(processes, &terminal, &protected, 99),
            vec![DevRunnerProcess {
                pid: 12,
                started_at: "Fri Aug 7 16:00:00 2026".into(),
                branch_key: "agent-cairn-1-builder-0".into(),
            }],
            "a live residency or non-terminal job protects its branch even when another replica calls it terminal"
        );
    }

    /// A cell as the fleet snapshot reports one, carrying only what claiming a
    /// checkout depends on.
    fn cell_state(
        project_id: &str,
        path: &str,
        holder: Option<ResidencyHolder>,
    ) -> cairn_common::executor_protocol::PersistentCellState {
        use cairn_common::executor_protocol::{
            CellOccupancy, GitObjectFormat, OwnerDeathPolicy, PersistentCellLifecycle,
            PersistentCellState, ResidencyPhase,
        };
        PersistentCellState {
            warm_command_classes: Vec::new(),
            executor_id: "executor-a".into(),
            executor_display_name: None,
            project_id: project_id.into(),
            cell_id: "cell".into(),
            path: path.into(),
            workspace_name: "workspace".into(),
            repository: "/repo".into(),
            checkout_kind: Default::default(),
            git_common_dir: None,
            authority_path: "/authority".into(),
            lifecycle: PersistentCellLifecycle::Running,
            cell_epoch: 1,
            last_sealed_commit: None,
            last_used_unix_ms: 0,
            last_affinity_key: None,
            preparation_fingerprint: None,
            residency: holder.map(|holder| cairn_common::executor_protocol::CellResidency {
                holder,
                repository: RepositoryLocator::ManagedObjects {
                    project_id: project_id.into(),
                    repository_id: "repository".into(),
                    object_format: GitObjectFormat::Sha1,
                },
                owner_ref: None,
                selector: None,
                incarnation_id: "incarnation".into(),
                current_base_commit: "commit".into(),
                phase: ResidencyPhase::Active,
                last_heartbeat_unix_ms: 0,
                reclaim_deadline_unix_ms: 0,
                death_policy: OwnerDeathPolicy {
                    heartbeat_timeout_ms: 30_000,
                    reclaim_grace_ms: 10_000,
                },
                footprint: ResidencyFootprint {
                    memory_bytes: 0,
                    disk_growth_bytes: 0,
                },
                state_revision: 1,
                events: Vec::new(),
            }),
            occupancy: CellOccupancy::default(),
        }
    }

    fn test_orchestrator(db: LocalDb, jj_binary: String) -> (Orchestrator, tempfile::TempDir) {
        let config = tempfile::tempdir().unwrap();
        let index = SearchIndex::open_or_create(config.path().join("search-index.db")).unwrap();
        let db_state = Arc::new(DbState::new(Arc::new(db), Arc::new(index)));
        let services = Arc::new(TestServicesBuilder::new().with_git(RealGitClient).build());
        let orch = Orchestrator::builder(db_state, services, config.path().to_path_buf())
            .jj_binary_path(jj_binary)
            .build();
        (orch, config)
    }

    async fn registered_project(db: &LocalDb, repository: &Path) {
        let repository = repository.to_string_lossy().into_owned();
        db.write(move |conn| {
            let repository = repository.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', ?1, 'main', 1, 1)",
                    params![repository.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Make a checkout one a dev instance can be built from: the launch proves
    /// the runtime entrypoint is present at the resolved commit before leasing.
    fn commit_runtime_entrypoint(repo: &Path) {
        std::fs::create_dir_all(repo.join("scripts")).unwrap();
        std::fs::write(repo.join(RUNTIME_ENTRYPOINT), "// runtime\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "runtime"]);
    }

    fn launch(selector: Option<&str>) -> DevInstanceLaunchRequest {
        launch_from(selector, None)
    }

    fn launch_from(selector: Option<&str>, checkout: Option<&Path>) -> DevInstanceLaunchRequest {
        DevInstanceLaunchRequest {
            project_id: "proj-1".into(),
            selector: selector.map(str::to_string),
            checkout: checkout.map(|path| path.to_string_lossy().into_owned()),
            source_terminal_session_id: None,
            seed: "empty".into(),
            force_copy: false,
        }
    }

    fn resolution(selector: &str) -> DevInstanceResolution {
        DevInstanceResolution {
            project_id: "proj-1".into(),
            repository_path: PathBuf::from("/repo"),
            selector: selector.into(),
            commit_id: "abc123".into(),
            owner_ref: None,
        }
    }

    /// Two operators on two branches must produce two instances. Before this,
    /// an implicit launch read the *project's* checkout no matter where it came
    /// from, so both resolved the same coordinate and only one ever built.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_implicit_launch_names_the_callers_own_checkout() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping an_implicit_launch_names_the_callers_own_checkout: jj not resolvable"
            );
            return;
        };
        let checkout = tempfile::TempDir::new().unwrap();
        init_project(checkout.path());
        commit_runtime_entrypoint(checkout.path());

        let worktrees = tempfile::TempDir::new().unwrap();
        let sibling = worktrees.path().join("sibling");
        git(
            checkout.path(),
            &[
                "worktree",
                "add",
                "-b",
                "feature/sibling",
                sibling.to_str().unwrap(),
            ],
        );

        let db = crate::storage::migrated_test_db("dev-instance-caller-checkout.db").await;
        registered_project(&db, checkout.path()).await;
        let (orch, _config) = test_orchestrator(db, bin);

        let from_project = resolve_launch_coordinate(&orch, &launch_from(None, None))
            .await
            .unwrap();
        assert_eq!(from_project.selector, "main");

        let from_sibling =
            resolve_launch_coordinate(&orch, &launch_from(None, Some(sibling.as_path())))
                .await
                .unwrap();
        assert_eq!(
            from_sibling.selector, "feature/sibling",
            "an implicit launch names the branch the caller is standing on"
        );

        assert_ne!(
            launch_session_key(&from_project),
            launch_session_key(&from_sibling),
            "two branches must not share one launch session"
        );

        // A path that is not this project's is a claim, and the runner refuses
        // to read a stranger's refs on it.
        let stranger = tempfile::TempDir::new().unwrap();
        init_project(stranger.path());
        git(stranger.path(), &["checkout", "-q", "-b", "unrelated"]);
        let from_stranger =
            resolve_launch_coordinate(&orch, &launch_from(None, Some(stranger.path())))
                .await
                .unwrap();
        assert_eq!(
            from_stranger.selector, "main",
            "an unproven checkout falls back to the project's own"
        );
    }

    /// The session key is the instance's identity. Reconnect depends on two
    /// launches of the same instance agreeing; concurrency depends on two
    /// launches of different instances disagreeing.
    #[test]
    fn a_launch_session_is_keyed_by_the_instance_it_names() {
        assert_eq!(
            launch_session_key(&resolution("agent/CAIRN-1-builder-0")),
            launch_session_key(&resolution("agent/CAIRN-1-builder-0")),
        );
        assert_ne!(
            launch_session_key(&resolution("main")),
            launch_session_key(&resolution("agent/CAIRN-1-builder-0")),
        );
        assert!(launch_session_key(&resolution("main")).starts_with("proj-1:"));
    }

    #[test]
    fn checkout_coordinate_names_the_branch_and_leaves_a_detached_head_unnamed() {
        let mut on_branch = MockGitClient::new();
        on_branch
            .expect_rev_parse()
            .returning(|_, _| Ok("  abc123\n".into()));
        on_branch
            .expect_current_branch()
            .returning(|_| Ok("main\n".into()));
        assert_eq!(
            live_checkout_coordinate(&on_branch, Path::new("/repo")),
            Some(CheckoutCoordinate {
                branch: Some("main".into()),
                commit: "abc123".into(),
            })
        );

        let mut detached = MockGitClient::new();
        detached
            .expect_rev_parse()
            .returning(|_, _| Ok("abc123".into()));
        detached
            .expect_current_branch()
            .returning(|_| Ok(String::new()));
        assert_eq!(
            live_checkout_coordinate(&detached, Path::new("/repo")),
            Some(CheckoutCoordinate {
                branch: None,
                commit: "abc123".into(),
            }),
            "git names no branch for a detached head, and the commit is not a name"
        );
    }

    /// The reported defect (CAIRN-3241): a dev instance launched from a job's
    /// own checkout showed a 40-character commit as its operator identity and
    /// keyed a cold cargo partition by that same hash. Every checkout Cairn
    /// materializes is detached, so this is the ordinary case, not an edge.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_detached_checkout_is_named_by_the_branch_at_its_commit() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping a_detached_checkout_is_named_by_the_branch_at_its_commit: jj not resolvable"
            );
            return;
        };
        let checkout = tempfile::TempDir::new().unwrap();
        init_project(checkout.path());
        commit_runtime_entrypoint(checkout.path());
        git(
            checkout.path(),
            &["checkout", "-q", "-b", "agent/PROJ-3232-builder-1"],
        );
        for step in ["first", "second"] {
            std::fs::write(checkout.path().join(format!("{step}.rs")), step).unwrap();
            git(checkout.path(), &["add", "-A"]);
            git(checkout.path(), &["commit", "-q", "-m", step]);
        }
        git(checkout.path(), &["checkout", "-q", "main"]);

        // How the executor materializes a cell: a worktree detached at a
        // branch's tip, so `git branch --show-current` is empty inside it.
        let worktrees = tempfile::TempDir::new().unwrap();
        let cell = worktrees.path().join("slot-1");
        git(
            checkout.path(),
            &[
                "worktree",
                "add",
                "--detach",
                cell.to_str().unwrap(),
                "agent/PROJ-3232-builder-1",
            ],
        );
        assert!(
            git_stdout(&cell, &["branch", "--show-current"]).is_empty(),
            "the fixture only means anything if the cell's head is detached"
        );

        let db = crate::storage::migrated_test_db("dev-instance-detached.db").await;
        registered_project(&db, checkout.path()).await;
        let (orch, _config) = test_orchestrator(db, bin);

        let resolved = resolve_launch_coordinate(&orch, &launch_from(None, Some(cell.as_path())))
            .await
            .unwrap();
        assert_eq!(
            resolved.selector, "agent/PROJ-3232-builder-1",
            "the branch at the commit names the instance; the commit is a storage address"
        );
        assert_eq!(
            resolved.commit_id,
            git_stdout(&cell, &["rev-parse", "HEAD"]),
            "naming the instance by a branch does not change which commit gets built"
        );

        // A commit in the middle of a branch's history has nothing to name it,
        // and the launch says what to pass rather than minting a hash-shaped
        // instance nobody can tell from another.
        git(&cell, &["checkout", "-q", "HEAD~1"]);
        let refusal = resolve_launch_coordinate(&orch, &launch_from(None, Some(cell.as_path())))
            .await
            .expect_err("a commit no branch names cannot key an instance");
        let DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven { diagnostic } = refusal else {
            panic!("expected a typed working-copy refusal, got {refusal:?}");
        };
        assert!(
            diagnostic.contains("--branch <selector>"),
            "the refusal says what to do: {diagnostic}"
        );
    }

    /// The review finding on CAIRN-3241, pinned at the seam that decides it.
    ///
    /// An agent's cell is detached at its branch tip, and until the job's first
    /// commit lands that tip is also `main`'s. Inverting the commit answers
    /// `main` and loses the issue; provenance answers the job's own branch,
    /// which is what carries `#<issue>` into the row. The cell is matched from
    /// the caller's `cwd`, which is normally somewhere inside it rather than its
    /// root.
    #[test]
    fn a_jobs_own_checkout_is_claimed_by_the_cell_that_holds_it() {
        let cells = vec![
            cell_state(
                "proj-1",
                "/cells/slot-1",
                Some(ResidencyHolder::Job {
                    job_id: "job-3241".into(),
                }),
            ),
            cell_state(
                "proj-1",
                "/cells/slot-2",
                Some(ResidencyHolder::DevInstance {
                    instance_id: "proj-1:main".into(),
                }),
            ),
            cell_state(
                "proj-2",
                "/cells/slot-3",
                Some(ResidencyHolder::Job {
                    job_id: "job-other".into(),
                }),
            ),
        ];

        assert_eq!(
            cell_job_for_checkout(&cells, "proj-1", Path::new("/cells/slot-1/src/deep")),
            Some("job-3241".to_string()),
            "a launch from anywhere inside a job's cell is that job's checkout"
        );
        assert_eq!(
            cell_job_for_checkout(&cells, "proj-1", Path::new("/cells/slot-2")),
            None,
            "a dev instance's own cell is not a job's work"
        );
        assert_eq!(
            cell_job_for_checkout(&cells, "proj-1", Path::new("/cells/slot-3")),
            None,
            "another project's cell never claims this project's launch"
        );
        assert_eq!(
            cell_job_for_checkout(&cells, "proj-1", Path::new("/somewhere/else")),
            None,
            "an operator's own checkout belongs to no job, and falls through to the store"
        );
        // Prefix matching is on path components, not characters: a sibling
        // directory whose name merely begins with a cell's name is not inside it.
        assert_eq!(
            cell_job_for_checkout(&cells, "proj-1", Path::new("/cells/slot-10")),
            None,
            "a sibling cell whose name shares a prefix is a different checkout"
        );
    }

    /// Seed the fleet with the cells an executor reports, which is where a
    /// launch reads a checkout's provenance from.
    ///
    /// The returned receiver is the executor's end of the connection; dropping
    /// it closes the channel, so a caller holds it for the test's duration.
    fn attach_cells(
        orch: &Orchestrator,
        cells: Vec<cairn_common::executor_protocol::PersistentCellState>,
    ) -> mpsc::UnboundedReceiver<cairn_common::executor_protocol::ExecutorMessage> {
        use cairn_common::executor_protocol::{
            ExecutorAdvertisement, ExecutorCapabilities, ExecutorIdentity, ExecutorSubstrateReport,
            FleetSnapshot,
        };
        let (sender, receiver) = mpsc::unbounded_channel();
        let advertisement = ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "device-a".into(),
                executor_id: "executor-a".into(),
                display_name: "Executor A".into(),
            },
            capabilities: ExecutorCapabilities {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                logical_cores: 1,
                toolchains: Vec::new(),
                projects_served: Vec::new(),
                disk_budget_bytes: None,
                memory_budget_bytes: None,
                toolchain_detection: None,
            },
            current_load: 0,
            warm_roots: Vec::new(),
            observed_at_unix_ms: 0,
            liveness_observed_at_unix_ms: None,
        };
        let generation = orch
            .fleet
            .attach_advertised_executor(advertisement, sender, false, None);
        assert!(
            orch.fleet.set_executor_snapshot(
                "executor-a",
                generation,
                FleetSnapshot {
                    cells,
                    ..FleetSnapshot::default()
                },
                ExecutorSubstrateReport::default(),
            ),
            "the fixture only means anything if the fleet accepted the snapshot"
        );
        receiver
    }

    /// The review finding on CAIRN-3241, end to end through the launch.
    ///
    /// An agent branch begins life at the tip of the branch it was cut from, so
    /// until the job's first commit lands one commit carries both names and the
    /// inversion cannot tell them apart. The negative control runs first and
    /// shows what that costs: with no residency over the checkout the inversion
    /// is reached, and it answers `main` — the operator's dev build of an
    /// agent's work would share main's instance, home, database, and cache, and
    /// the Running panel would name it `main` rather than the issue. Seeding the
    /// residency that actually holds the cell flips the same launch to the job's
    /// own branch, which is what carries `#<issue>` into the row.
    ///
    /// Ordering is the whole assertion: both halves resolve here, and only
    /// asking provenance first gets the right one.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_launch_from_a_job_cell_sharing_mains_tip_is_named_by_the_job() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping a_launch_from_a_job_cell_sharing_mains_tip_is_named_by_the_job: jj not resolvable"
            );
            return;
        };
        let checkout = tempfile::TempDir::new().unwrap();
        init_project(checkout.path());
        commit_runtime_entrypoint(checkout.path());
        // The job's branch is cut at main's tip and has not diverged, so one
        // commit answers to both names. This is the ordinary state of an agent
        // branch before its first commit lands, not a contrived edge.
        git(
            checkout.path(),
            &["branch", "agent/PROJ-3241-builder-1", "main"],
        );

        let worktrees = tempfile::TempDir::new().unwrap();
        let cell = worktrees.path().join("slot-1");
        git(
            checkout.path(),
            &[
                "worktree",
                "add",
                "--detach",
                cell.to_str().unwrap(),
                "agent/PROJ-3241-builder-1",
            ],
        );
        assert_eq!(
            git_stdout(&cell, &["rev-parse", "HEAD"]),
            git_stdout(checkout.path(), &["rev-parse", "main"]),
            "the fixture only means anything if the job's tip and main's are one commit"
        );

        let db = crate::storage::migrated_test_db("dev-instance-shared-tip.db").await;
        registered_project(&db, checkout.path()).await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 3241, 'Running list', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, triggered_by, seq)
                     VALUES ('exec-1', 'recipe-1', 'issue-1', 'proj-1', 'running', 1, 'user', 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch)
                     VALUES ('job-3241', 'exec-1', 'issue-1', 'proj-1', 'Builder', 'running', 1, 1, 'builder', 'agent/PROJ-3241-builder-1')",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let (orch, _config) = test_orchestrator(db, bin);

        let unowned = resolve_launch_coordinate(&orch, &launch_from(None, Some(cell.as_path())))
            .await
            .unwrap();
        assert_eq!(
            unowned.selector, "main",
            "with nothing holding the checkout, inverting the shared commit answers the default \
             branch — the loss the provenance step exists to prevent"
        );

        let _executor = attach_cells(
            &orch,
            vec![cell_state(
                "proj-1",
                cell.to_str().unwrap(),
                Some(ResidencyHolder::Job {
                    job_id: "job-3241".into(),
                }),
            )],
        );

        // The launch carries a directory inside the cell, as a shell running in
        // the job's own checkout would.
        let owned = resolve_launch_coordinate(
            &orch,
            &launch_from(None, Some(cell.join("scripts").as_path())),
        )
        .await
        .unwrap();
        assert_eq!(
            owned.selector, "agent/PROJ-3241-builder-1",
            "the job the cell is held for names the instance, so provenance has to be asked \
             before the commit is inverted"
        );
        assert_eq!(
            owned.commit_id, unowned.commit_id,
            "naming the instance by its job does not change which commit gets built"
        );
        assert_eq!(
            owned.owner_ref.and_then(|owner| owner.issue_number),
            Some(3241),
            "this is what the Running panel renders as `#<issue>` instead of `main`"
        );
    }

    /// A checkout the fleet does not own belongs to no job, so no issue identity
    /// is at stake when several bookmarks share its commit. The default branch
    /// wins, because that commit is exactly the default branch's code — and
    /// either way the answer has to be the same on every launch, or one checkout
    /// would sprawl into several instances.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_commit_several_branches_share_is_named_the_same_way_every_time() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping a_commit_several_branches_share_is_named_the_same_way_every_time: jj not resolvable"
            );
            return;
        };
        let checkout = tempfile::TempDir::new().unwrap();
        init_project(checkout.path());
        commit_runtime_entrypoint(checkout.path());
        git(checkout.path(), &["branch", "agent/PROJ-1-builder-0"]);
        git(checkout.path(), &["branch", "agent/PROJ-2-builder-0"]);

        let worktrees = tempfile::TempDir::new().unwrap();
        let cell = worktrees.path().join("slot-1");
        git(
            checkout.path(),
            &[
                "worktree",
                "add",
                "--detach",
                cell.to_str().unwrap(),
                "main",
            ],
        );

        let db = crate::storage::migrated_test_db("dev-instance-shared-tip.db").await;
        registered_project(&db, checkout.path()).await;
        let (orch, _config) = test_orchestrator(db, bin);

        let first = resolve_launch_coordinate(&orch, &launch_from(None, Some(cell.as_path())))
            .await
            .unwrap();
        let second = resolve_launch_coordinate(&orch, &launch_from(None, Some(cell.as_path())))
            .await
            .unwrap();
        assert_eq!(first.selector, "main");
        assert_eq!(
            launch_session_key(&first),
            launch_session_key(&second),
            "one checkout is one instance, however many branches sit on its commit"
        );
    }

    #[test]
    fn a_coordinate_is_buildable_only_when_it_carries_the_runtime() {
        let repo = tempfile::TempDir::new().unwrap();
        init_project(repo.path());
        let git_client = RealGitClient;
        let without_runtime = git_stdout(repo.path(), &["rev-parse", "HEAD"]);

        // The reported failure: a launch that resolved a coordinate from a
        // repository that is not the Cairn application at all. It must die here,
        // naming the selector and the commit, rather than downstream as a
        // module-not-found from a checkout that never had the runtime.
        let diagnostic =
            prove_buildable_coordinate(&git_client, repo.path(), "main", &without_runtime)
                .expect_err("a checkout without the runtime entrypoint cannot host an instance");
        assert!(diagnostic.contains(&without_runtime), "{diagnostic}");
        assert!(diagnostic.contains("main"), "{diagnostic}");
        assert!(diagnostic.contains(RUNTIME_ENTRYPOINT), "{diagnostic}");

        // A commit the object database cannot produce is refused at resolution
        // too, instead of being handed to a materialization that half-succeeds.
        let absent = "0000000000000000000000000000000000000000";
        let diagnostic = prove_buildable_coordinate(&git_client, repo.path(), "main", absent)
            .expect_err("an absent commit cannot host an instance");
        assert!(diagnostic.contains(absent), "{diagnostic}");
        assert!(diagnostic.contains("object database"), "{diagnostic}");

        commit_runtime_entrypoint(repo.path());
        let buildable = git_stdout(repo.path(), &["rev-parse", "HEAD"]);
        assert!(prove_buildable_coordinate(&git_client, repo.path(), "main", &buildable).is_ok());
        assert!(
            prove_buildable_coordinate(&git_client, repo.path(), "main", &without_runtime).is_err(),
            "the proof is about the resolved commit, not the checkout's current state"
        );
    }

    #[test]
    fn checkout_coordinate_is_absent_without_a_readable_commit() {
        let mut git = MockGitClient::new();
        git.expect_rev_parse()
            .returning(|_, _| Err("not a git repository".into()));
        assert_eq!(live_checkout_coordinate(&git, Path::new("/nowhere")), None);
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn implicit_launch_resolves_the_live_checkouts_head() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping implicit_launch_resolves_the_live_checkouts_head: jj not resolvable"
            );
            return;
        };
        let checkout = tempfile::TempDir::new().unwrap();
        init_project(checkout.path());
        commit_runtime_entrypoint(checkout.path());
        let db = crate::storage::migrated_test_db("dev-instance-implicit.db").await;
        registered_project(&db, checkout.path()).await;
        let (orch, _config) = test_orchestrator(db, bin);

        let resolved = resolve_launch_coordinate(&orch, &launch(None))
            .await
            .unwrap();
        assert_eq!(
            resolved.selector,
            "main",
            "the instance is keyed by the branch, not the commit, so its build cache survives a commit"
        );
        assert_eq!(
            resolved.commit_id,
            git_stdout(checkout.path(), &["rev-parse", "HEAD"])
        );

        // A commit made after the store already existed still resolves: the
        // launch is what the operator has checked out right now, not the last
        // coordinate the store happened to import.
        std::fs::write(checkout.path().join("later.rs"), "later\n").unwrap();
        git(checkout.path(), &["add", "-A"]);
        git(checkout.path(), &["commit", "-q", "-m", "later"]);
        let advanced = resolve_launch_coordinate(&orch, &launch(None))
            .await
            .unwrap();
        assert_eq!(
            advanced.commit_id,
            git_stdout(checkout.path(), &["rev-parse", "HEAD"])
        );
        assert_ne!(advanced.commit_id, resolved.commit_id);

        // An explicit selector keeps resolving through the store, and an
        // unresolvable one keeps its typed refusal.
        let explicit = resolve_launch_coordinate(&orch, &launch(Some("main")))
            .await
            .unwrap();
        assert_eq!(explicit.commit_id, advanced.commit_id);
        assert!(matches!(
            resolve_launch_coordinate(&orch, &launch(Some("agent/never-published"))).await,
            Err(DevInstanceLaunchFailure::SelectorNotFound { .. })
        ));
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn implicit_launch_refuses_when_no_coordinate_can_be_proven() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping implicit_launch_refuses_when_no_coordinate_can_be_proven: jj not resolvable"
            );
            return;
        };
        let checkout = tempfile::TempDir::new().unwrap();
        git(checkout.path(), &["init", "-q", "-b", "main"]);
        let db = crate::storage::migrated_test_db("dev-instance-unproven.db").await;
        registered_project(&db, checkout.path()).await;
        let (orch, _config) = test_orchestrator(db, bin);

        assert!(
            matches!(
                resolve_launch_coordinate(&orch, &launch(None)).await,
                Err(DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven { .. })
            ),
            "a checkout with no commit has nothing to build and says so"
        );
    }

    #[tokio::test]
    async fn a_selector_naming_an_agent_branch_resolves_the_work_it_belongs_to() {
        let (orch, _config, _checkout) =
            orchestrator_with_a_builder_node("dev-instance-owner.db").await;

        let owner = issue_owner_for_selector(&orch, "proj-1", "agent/PROJ-3104-builder-1")
            .await
            .expect("an agent branch names the issue its job belongs to");
        assert_eq!(owner.issue_number, Some(3104));
        assert_eq!(owner.project_key.as_deref(), Some("PROJ"));
        assert_eq!(
            owner.node_kind.as_deref(),
            Some("Builder"),
            "the node's display name, read from the same job column an execution home reads"
        );
        assert_eq!(
            owner.job_id, None,
            "a dev instance is the operator's build of that work, not the job's execution home, \
             and lease adoption keys off job_id"
        );

        assert!(
            issue_owner_for_selector(&orch, "proj-1", "main")
                .await
                .is_none(),
            "a branch no job minted names no work, and the instance names itself"
        );
    }

    /// A project with one execution and one builder node holding a minted
    /// branch — the shape both selector spellings have to agree about.
    async fn orchestrator_with_a_builder_node(
        db_name: &str,
    ) -> (Orchestrator, tempfile::TempDir, tempfile::TempDir) {
        let db = crate::storage::migrated_test_db(db_name).await;
        let checkout = tempfile::TempDir::new().unwrap();
        registered_project(&db, checkout.path()).await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 3104, 'Running list', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, triggered_by, seq)
                     VALUES ('exec-1', 'recipe-1', 'issue-1', 'proj-1', 'running', 1, 'user', 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch)
                     VALUES ('job-1', 'exec-1', 'issue-1', 'proj-1', 'Builder', 'running', 1, 1, 'builder', 'agent/PROJ-3104-builder-1')",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let (orch, config) = test_orchestrator(db, "jj".to_string());
        (orch, config, checkout)
    }

    #[tokio::test]
    async fn a_node_uri_and_its_branch_name_the_same_instance() {
        let (orch, _config, _checkout) =
            orchestrator_with_a_builder_node("dev-instance-node-uri.db").await;
        let canonical = async |selector: &str| {
            canonical_selector(&orch, "proj-1", selector, Some("main")).await
        };

        assert_eq!(
            canonical("cairn://p/PROJ/3104/1/builder").await.unwrap(),
            "agent/PROJ-3104-builder-1",
            "a node URI names work, and the branch that node minted is what gets built"
        );
        assert_eq!(
            canonical("agent/PROJ-3104-builder-1").await.unwrap(),
            "agent/PROJ-3104-builder-1",
            "both spellings key one instance, so they share its home, database, ports, and cache"
        );
        assert_eq!(
            canonical("main").await.unwrap(),
            "main",
            "the project's own default branch still resolves as before"
        );

        // The launch refuses what it cannot resolve rather than handing a URI
        // to the revision resolver, which would report it as an unknown commit.
        assert!(matches!(
            canonical("cairn://p/PROJ/9999/1/builder").await,
            Err(DevInstanceLaunchFailure::SelectorNotFound { .. })
        ));
        assert!(matches!(
            canonical("cairn://p/PROJ/3104").await,
            Err(DevInstanceLaunchFailure::InvalidSelector { .. })
        ));
    }

    #[tokio::test]
    async fn a_node_uri_launch_is_attributed_to_the_issue_it_names() {
        let (orch, _config, _checkout) =
            orchestrator_with_a_builder_node("dev-instance-node-uri-owner.db").await;

        let selector = canonical_selector(&orch, "proj-1", "cairn://p/PROJ/3104/1/builder", None)
            .await
            .unwrap();
        let owner = issue_owner_for_selector(&orch, "proj-1", &selector)
            .await
            .expect("the canonical branch carries the attribution both spellings share");
        assert_eq!(owner.issue_number, Some(3104));
        assert_eq!(owner.node_kind.as_deref(), Some("Builder"));
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_launch_resolved_against_another_repository_is_refused() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping a_launch_resolved_against_another_repository_is_refused: jj not resolvable"
            );
            return;
        };
        // CAIRN-3120: the launch client identified the wrong registered project
        // and the runner resolved that project's own head, which materialized a
        // checkout with no development runtime in it. Resolution now refuses
        // before a lease is acquired.
        let checkout = tempfile::TempDir::new().unwrap();
        init_project(checkout.path());
        let db = crate::storage::migrated_test_db("dev-instance-foreign-repo.db").await;
        registered_project(&db, checkout.path()).await;
        let (orch, _config) = test_orchestrator(db, bin);

        let refusal = resolve_launch_coordinate(&orch, &launch(None))
            .await
            .expect_err("a repository that is not the Cairn application cannot be launched");
        let DevInstanceLaunchFailure::UnbuildableCoordinate { diagnostic } = refusal else {
            panic!("expected a typed unbuildable-coordinate refusal, got {refusal:?}");
        };
        assert!(diagnostic.contains(RUNTIME_ENTRYPOINT), "{diagnostic}");
        assert!(
            diagnostic.contains(&git_stdout(checkout.path(), &["rev-parse", "HEAD"])),
            "the refusal names the commit that was resolved: {diagnostic}"
        );

        commit_runtime_entrypoint(checkout.path());
        assert!(
            resolve_launch_coordinate(&orch, &launch(None))
                .await
                .is_ok(),
            "the same checkout launches once it can host the runtime"
        );
    }

    #[test]
    fn branch_key_is_stable_and_bounded() {
        assert_eq!(stable_branch_key("Feature/My Branch"), "feature-my-branch");
        assert!(stable_branch_key(&"a".repeat(100)).len() <= 48);
    }

    #[test]
    fn dev_instance_placement_is_colocated() {
        let acquisition = ResidencyAcquireRequest {
            holder: ResidencyHolder::DevInstance {
                instance_id: "p:main".into(),
            },
            executor: None,
            owner_ref: None,
            selector: Some("main".into()),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/repo".into(),
            },
            initial_base_commit: "abc".into(),
            footprint: ResidencyFootprint::default(),
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: LEASE_TIMEOUT_MS,
                reclaim_grace_ms: RECLAIM_GRACE_MS,
            },
            priority: CellPriority::AgentInteractive,
            wait_horizon_unix_ms: 1,
            waiting_since_unix_ms: 0,
        };
        let request = crate::fleet::residency_placement_request_for_test(&acquisition);
        assert_eq!(request.pinned_executor_id, Some("colocated".into()));
    }

    #[test]
    fn stale_process_generations_are_rejected_after_start() {
        let event = ResidentProcessEvent {
            holder: ResidencyHolder::DevInstance {
                instance_id: "p:main".into(),
            },
            incarnation_id: "incarnation".into(),
            cell_epoch: 1,
            process_key: PROCESS_KEY.into(),
            process_generation: 4,
            event: ResidentProcessEventKind::State {
                status: ResidentProcessStatus::Exited {
                    finished_at_unix_ms: 1,
                    exit_code: Some(0),
                    restartable: true,
                    executor_lost: false,
                },
            },
        };
        assert!(!is_current_process_generation(&event, 5));
        assert!(is_current_process_generation(&event, 4));
    }

    #[test]
    fn branch_advance_targets_only_the_matching_live_dev_instance() {
        let mut matching = cell_state(
            "proj-1",
            "/matching",
            Some(ResidencyHolder::DevInstance {
                instance_id: "proj-1:feature".into(),
            }),
        );
        matching.residency.as_mut().unwrap().selector = Some("agent/PROJ-1-builder-0".into());
        matching.cell_epoch = 7;

        let mut other_branch = matching.clone();
        other_branch.cell_id = "other-branch".into();
        other_branch.residency.as_mut().unwrap().selector = Some("agent/PROJ-2-builder-0".into());

        let mut terminal = matching.clone();
        terminal.cell_id = "terminal".into();
        terminal.residency.as_mut().unwrap().holder = ResidencyHolder::Job {
            job_id: "job".into(),
        };

        let fences = live_branch_instance_fences(
            cairn_common::executor_protocol::FleetSnapshot {
                cells: vec![matching, other_branch, terminal],
                ..Default::default()
            },
            "proj-1",
            "agent/PROJ-1-builder-0",
        );
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].cell_epoch, 7);
        assert!(matches!(
            fences[0].holder,
            ResidencyHolder::DevInstance { .. }
        ));
    }

    #[test]
    fn both_agent_commit_verbs_sync_live_dev_instances_after_publication() {
        let write = include_str!("mcp/handlers/write/mod.rs");
        let run = include_str!("mcp/handlers/run/mod.rs");
        for (verb, source) in [("write", write), ("run", run)] {
            assert!(
                source.contains("sync_live_branch_instances"),
                "{verb} can advance a branch without refreshing its live dev instance"
            );
        }
    }

    #[test]
    fn readiness_marker_survives_chunk_boundaries() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut buffer = b"noise\nCAIRN_DEV_INSTANCE_READY={\"appUrl\":\"http://app\",".to_vec();
        emit_readiness(&tx, &mut buffer);
        assert!(rx.try_recv().is_err());
        buffer.extend_from_slice(b"\"runnerUrl\":\"http://runner\"}\n");
        emit_readiness(&tx, &mut buffer);
        assert_eq!(
            rx.try_recv().unwrap(),
            DevInstanceLaunchEvent::Ready {
                readiness: DevInstanceReadiness {
                    app_url: "http://app".into(),
                    runner_url: "http://runner".into(),
                },
            }
        );
    }
    #[test]
    fn issue_teardown_targets_completed_owner_and_preserves_active_owner() {
        let mut completed = cell_state(
            "proj-1",
            "/completed",
            Some(ResidencyHolder::DevInstance {
                instance_id: "proj-1:completed".into(),
            }),
        );
        completed.residency.as_mut().unwrap().owner_ref = Some(CellOwnerRef {
            project_id: "proj-1".into(),
            project_key: Some("PROJ".into()),
            issue_number: Some(1),
            job_id: Some("completed-job".into()),
            execution_seq: Some(1),
            node_kind: Some("builder".into()),
        });
        completed.residency.as_mut().unwrap().selector = Some("agent/PROJ-1-builder".into());

        let mut active = completed.clone();
        active.cell_id = "active".into();
        active
            .residency
            .as_mut()
            .unwrap()
            .owner_ref
            .as_mut()
            .unwrap()
            .job_id = Some("active-job".into());

        let fences = issue_instance_fences(
            cairn_common::executor_protocol::FleetSnapshot {
                cells: vec![completed, active],
                ..Default::default()
            },
            "proj-1",
            &["completed-job".into()],
            &["agent/PROJ-1-builder".into()],
        );
        assert_eq!(fences.len(), 1);
        assert_eq!(
            fences[0].holder,
            ResidencyHolder::DevInstance {
                instance_id: "proj-1:completed".into()
            }
        );
    }

    #[test]
    fn issue_teardown_sweeps_legacy_unattributed_instance_by_branch() {
        let mut orphan = cell_state(
            "proj-1",
            "/orphan",
            Some(ResidencyHolder::DevInstance {
                instance_id: "proj-1:orphan".into(),
            }),
        );
        orphan.residency.as_mut().unwrap().selector = Some("agent/PROJ-1-builder".into());

        let fences = issue_instance_fences(
            cairn_common::executor_protocol::FleetSnapshot {
                cells: vec![orphan],
                ..Default::default()
            },
            "proj-1",
            &["completed-job".into()],
            &["agent/PROJ-1-builder".into()],
        );
        assert_eq!(fences.len(), 1);
    }
}
