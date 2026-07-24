//! Workflow process supervision (CAIRN-2487).
//!
//! A workflow node's process is a plain `bun <script>` subprocess, NOT an agent
//! CLI. Unlike the claude/codex/openrouter backends it implements no protocol
//! and streams no events: the script talks to Cairn exclusively through the MCP
//! callback the `@cairn/sdk` speaks (the same `read`/`write`/`run` verbs every
//! agent uses) — `agent()` calls, phase/log, and the final output artifact all
//! travel that one channel. So this module is pure supervision: inject the
//! callback env, register the process for restart-recovery/GC, forward stderr to
//! the log for crash diagnostics, and finalize on the real process exit code.
//!
//! Finalize mapping (the run's turn was started at spawn):
//! - a deliberate user stop (marker set) -> Exited with `exit_reason=user_stop`
//!   (reads as Stopped), record KEPT for Restart (CAIRN-2516).
//! - exit 0 with the output artifact written (terminal-tool armed) -> Exited
//!   (turn Complete, job Complete, the suspended caller resumes with the
//!   artifact); the ONLY outcome that drops the re-dispatch record.
//! - exit 0 WITHOUT the output artifact -> Failed (ran to completion but never
//!   satisfied its output contract), record KEPT so Restart can retry.
//! - non-zero exit -> Failed (a script throw), record KEPT so Restart can retry.
//! - indeterminate exit (no code) -> Crashed (resumable), record KEPT for the
//!   startup re-dispatch sweep.
//!
//! Every non-success outcome keeps the record; the startup sweep skips any
//! terminal-status run, so a kept record is re-run only by an explicit Restart
//! (which resets the run to `starting`), never on boot.
//!
//! `start_workflow_run` and startup recovery both drive the spawn helper, making
//! this module the canonical process-supervision path for workflow runs.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::run_state::{resolve_run_db, transition_run_to_live};
use crate::models::RunStatus;
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::LifetimeRuntimeAsset;

const WORKFLOW_BACKEND_NAME: &str = "Workflow";

/// Parameters required to dispatch a workflow into an executor lease.
pub(crate) struct WorkflowSpawnParams {
    pub run_id: String,
    pub project_id: String,
    pub repository_path: PathBuf,
    pub anchor_branch: String,
    pub worktree_mode: String,
    pub package_path: PathBuf,
    pub script_path: PathBuf,
    pub args_json: String,
    pub turn_id: String,
    pub session_id: Option<String>,
    pub output_name: String,
}

#[cfg(test)]
mod runtime_asset_tests {
    use super::*;

    #[test]
    fn installed_runtime_resolves_without_a_source_checkout() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("runtime/node_modules/@cairn/harness");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(package.join("package.json"), "{}").unwrap();

        assert_eq!(
            installed_workflow_runtime_package(temp.path(), "harness"),
            Some(package)
        );
    }
}

pub(crate) async fn spawn_workflow_process(
    orch: &Orchestrator,
    params: WorkflowSpawnParams,
) -> Result<(), String> {
    use cairn_common::executor_protocol::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    let run_db = resolve_run_db(WORKFLOW_BACKEND_NAME, orch, &params.run_id)?;
    // Resolve every runner-owned input before admission so validation/auth errors
    // cannot strand an acquired cell until heartbeat reclamation.
    let secret = orch
        .mcp_auth
        .get_secret_for_mcp()
        .map_err(|e| format!("Failed to get MCP auth secret: {e}"))?;
    let tip = crate::fleet::lifetime::resolve_logical_commit(
        orch,
        &params.repository_path,
        &params.anchor_branch,
    )
    .await?;
    let fence = crate::fleet::lifetime::acquire(
        orch,
        LifetimeLeaseAcquireRequest {
            declaration: LifetimeLeaseDeclaration {
                lease_id: format!("workflow:{}", params.run_id),
                owner: crate::fleet::lifetime::owner(
                    LifetimeLeaseOwnerKind::Workflow,
                    &params.run_id,
                ),
                owner_ref: None,
                name: params.run_id.clone(),
                purpose: "workflow runtime".into(),
                repository: RepositoryLocator::ColocatedPath {
                    project_id: params.project_id.clone(),
                    repository_id: params.project_id.clone(),
                    absolute_path: params.repository_path.to_string_lossy().into_owned(),
                },
                initial_base_commit: tip.clone(),
                resource_reservation: ResourceReservation {
                    memory_bytes: 256 * 1024 * 1024,
                    disk_growth_bytes: 1024 * 1024 * 1024,
                    concurrency_units: 1,
                    source: ResourceReservationSource::Declared,
                },
                owner_death_policy: LifetimeOwnerDeathPolicy {
                    heartbeat_timeout_ms: 30_000,
                    reclaim_grace_ms: 10_000,
                },
            },
            priority: CellPriority::AgentInteractive,
            deadline_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
                + 30_000,
        },
    )
    .await?;
    if let Err(error) = crate::fleet::lifetime::refresh(orch, &fence, &tip).await {
        let _ = crate::fleet::lifetime::release(orch, &fence).await;
        return Err(error);
    }
    let assets = workflow_runtime_assets(&params.package_path)?;
    let entry = params
        .script_path
        .strip_prefix(&params.package_path)
        .map_err(|_| "workflow entry is outside its package".to_string())?
        .to_string_lossy()
        .replace('\\', "/");
    let generation = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let early_exit = Arc::new(Mutex::new(None::<(u64, Option<i32>, bool)>));
    let (event_orch, event_fence, event_key) = (orch.clone(), fence.clone(), params.run_id.clone());
    let (event_generation, event_finished) = (generation.clone(), finished.clone());
    let event_early_exit = early_exit.clone();
    let session_id = params.session_id.clone();
    orch.fleet.subscribe_lifetime_process_events(move |event| {
        let expected = event_generation.load(Ordering::Acquire);
        if event.lease_id != event_fence.lease_id
            || event.incarnation_id != event_fence.incarnation_id
            || event.lease_epoch != event_fence.lease_epoch
            || event.process_key != event_key
            || (expected != 0 && event.process_generation != expected)
        {
            return;
        }
        if expected == 0 {
            if let LifetimeProcessEventKind::State {
                status:
                    LifetimeProcessStatus::Exited {
                        exit_code,
                        executor_lost,
                        ..
                    },
            } = event.event
            {
                if let Ok(mut pending) = event_early_exit.lock() {
                    *pending = Some((event.process_generation, exit_code, executor_lost));
                }
            }
            return;
        }
        match event.event {
            LifetimeProcessEventKind::Output { stream, data, .. } => {
                let text = String::from_utf8_lossy(&data);
                if stream == LifetimeProcessStream::Stderr {
                    log::error!("workflow stderr: {text}")
                } else if stream == LifetimeProcessStream::Stdout {
                    log::info!("workflow stdout: {text}")
                }
            }
            LifetimeProcessEventKind::State {
                status:
                    LifetimeProcessStatus::Exited {
                        exit_code,
                        executor_lost,
                        ..
                    },
            } if !event_finished.swap(true, Ordering::AcqRel) => {
                let (orch, fence, run_id, session_id) = (
                    event_orch.clone(),
                    event_fence.clone(),
                    event_key.clone(),
                    session_id.clone(),
                );
                tokio::spawn(async move {
                    orch.process_state.clear_workflow_lease(&run_id);
                    finalize_workflow_run(
                        &orch,
                        &run_id,
                        session_id.as_deref(),
                        if executor_lost { None } else { exit_code },
                    );
                    let _ = crate::fleet::lifetime::release(&orch, &fence).await;
                });
            }
            _ => {}
        }
    });
    let loader = format!(
        "await import(process.env.CAIRN_RUNTIME_ASSETS + \"/workflow/{}\")",
        entry.replace('"', "\\\"")
    );
    let result = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::StartProcess {
                fence: fence.clone(),
                process_key: params.run_id.clone(),
                process: LifetimeProcessSpec {
                    program: "bun".into(),
                    args: vec!["-e".into(), loader],
                    cwd: if params.worktree_mode == "inherit" {
                        String::new()
                    } else {
                        "runtime-assets/workflow".into()
                    },
                    cwd_root: if params.worktree_mode == "inherit" {
                        LifetimeProcessCwdRoot::Checkout
                    } else {
                        LifetimeProcessCwdRoot::LeaseScratch
                    },
                    env: vec![
                        ("CAIRN_RUN_ID".into(), params.run_id.clone()),
                        ("CAIRN_MCP_SECRET".into(), secret),
                        (
                            "CAIRN_CALLBACK_URL".into(),
                            format!("http://127.0.0.1:{}/api/mcp", orch.mcp_callback_port),
                        ),
                        ("CAIRN_ENV".into(), cairn_common::paths::env_str().into()),
                        (
                            "CAIRN_HOME".into(),
                            cairn_common::paths::cairn_home()
                                .to_string_lossy()
                                .into_owned(),
                        ),
                        ("CAIRN_WORKFLOW_ARGS".into(), params.args_json),
                        ("CAIRN_WORKFLOW_OUTPUT".into(), params.output_name),
                    ],
                    sandbox_mode: ProcessSandboxMode::Confined,
                    sandbox_policy: None,
                    runtime_assets: assets,
                    io: LifetimeProcessIoMode::Pipe,
                },
            },
        )
        .await;
    let LifetimeLeaseResult::State { cell } = result else {
        crate::fleet::lifetime::rollback(orch, &fence, &params.run_id).await;
        return Err(format!("failed to start workflow process: {result:?}"));
    };
    let Some(process_generation) = cell
        .occupant
        .as_ref()
        .and_then(CellOccupant::lifetime)
        .and_then(|lease| lease.processes.get(&params.run_id))
        .map(|p| p.generation)
    else {
        crate::fleet::lifetime::rollback(orch, &fence, &params.run_id).await;
        return Err("workflow start returned no process generation".to_string());
    };
    generation.store(process_generation, Ordering::Release);
    orch.process_state
        .bind_workflow_lease(params.run_id.clone(), fence.clone());
    let _ = transition_run_to_live(WORKFLOW_BACKEND_NAME, orch, &run_db, &params.run_id);
    let _ = crate::execution::jobs::start_turn(orch, &params.turn_id, &params.run_id);
    if let Some((event_generation, exit_code, executor_lost)) = early_exit
        .lock()
        .ok()
        .and_then(|mut pending| pending.take())
    {
        if event_generation == process_generation && !finished.swap(true, Ordering::AcqRel) {
            orch.process_state.clear_workflow_lease(&params.run_id);
            finalize_workflow_run(
                orch,
                &params.run_id,
                params.session_id.as_deref(),
                if executor_lost { None } else { exit_code },
            );
            let _ = crate::fleet::lifetime::release(orch, &fence).await;
            return Ok(());
        }
    }
    let renew_orch = orch.clone();
    tokio::spawn(async move {
        while !finished.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if finished.load(Ordering::Acquire)
                || crate::fleet::lifetime::renew(&renew_orch, &fence)
                    .await
                    .is_err()
            {
                break;
            }
        }
    });
    Ok(())
}

fn workflow_runtime_assets(package: &Path) -> Result<Vec<LifetimeRuntimeAsset>, String> {
    let mut assets = Vec::new();
    collect_assets(package, Path::new("workflow"), &mut assets)?;
    collect_assets(
        &workflow_runtime_package("harness")?,
        Path::new("workflow/node_modules/@cairn/harness"),
        &mut assets,
    )?;
    collect_assets(
        &workflow_runtime_package("sdk")?,
        Path::new("workflow/node_modules/@cairn/sdk"),
        &mut assets,
    )?;
    let total = assets.iter().map(|asset| asset.data.len()).sum::<usize>();
    if assets.len() > cairn_common::executor_protocol::MAX_LIFETIME_RUNTIME_ASSETS
        || total > cairn_common::executor_protocol::MAX_LIFETIME_RUNTIME_ASSETS_BYTES
        || assets.iter().any(|asset| {
            asset.data.len() > cairn_common::executor_protocol::MAX_LIFETIME_RUNTIME_ASSET_BYTES
        })
    {
        return Err(
            "workflow package and runtime exceed executor runtime-asset limits".to_string(),
        );
    }
    Ok(assets)
}

fn workflow_runtime_package(name: &str) -> Result<PathBuf, String> {
    if let Some(installed) =
        installed_workflow_runtime_package(&cairn_common::paths::cairn_home(), name)
    {
        return Ok(installed);
    }
    #[cfg(debug_assertions)]
    {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo = manifest
            .ancestors()
            .nth(4)
            .ok_or("cannot locate Cairn development repository root")?;
        let dev = repo.join("packages").join(name);
        if dev.join("package.json").is_file() {
            return Ok(dev);
        }
    }
    Err(format!(
        "Cairn workflow runtime package @cairn/{name} is not installed under CAIRN_HOME/runtime/node_modules"
    ))
}

fn installed_workflow_runtime_package(home: &Path, name: &str) -> Option<PathBuf> {
    let installed = home.join("runtime/node_modules/@cairn").join(name);
    if installed.join("package.json").is_file() {
        Some(installed)
    } else {
        None
    }
}

fn collect_assets(
    source: &Path,
    destination: &Path,
    out: &mut Vec<LifetimeRuntimeAsset>,
) -> Result<(), String> {
    for entry in std::fs::read_dir(source).map_err(|e| format!("read {}: {e}", source.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let target = destination.join(entry.file_name());
        let ty = entry.file_type().map_err(|e| e.to_string())?;
        if ty.is_dir() {
            collect_assets(&entry.path(), &target, out)?;
        } else if ty.is_file() {
            out.push(LifetimeRuntimeAsset {
                path: target.to_string_lossy().replace('\\', "/"),
                data: std::fs::read(entry.path()).map_err(|e| e.to_string())?,
            });
        }
    }
    Ok(())
}

/// Drop a workflow run's durable spawn record (private, runner-transient) once
/// it reaches a non-recoverable terminal outcome, so startup re-dispatch never
/// re-runs a finished or deterministically-failed workflow. Best-effort.
fn clear_workflow_run_record(orch: &Orchestrator, run_id: &str) {
    let result = crate::storage::run_db_blocking({
        let db = orch.db.local.clone();
        let run_id = run_id.to_string();
        move || async move { crate::execution::jobs::delete_workflow_run_row(&db, &run_id).await }
    });
    if let Err(e) = result {
        log::warn!("Failed to clear workflow_run record for {run_id}: {e}");
    }
}

/// Map the process exit to a run outcome and finalize (resuming the suspended
/// caller), then deregister the process.
///
/// Record lifecycle (CAIRN-2516): the `workflow_run` re-dispatch record is
/// dropped ONLY on a clean success (a completed workflow is not restartable).
/// Every other terminal outcome — a deliberate stop, a script failure, or a
/// crash — KEEPS the record so an explicit user Restart can re-dispatch the run
/// and replay its journal. This does not reintroduce a startup boot-loop: the
/// startup sweep skips any terminal-status run, so a kept record is re-run only
/// by an explicit Restart (which resets the run to `starting`), never on boot.
fn finalize_workflow_run(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    exit_code: Option<i32>,
) {
    // Check-and-clear the deliberate-stop marker first: a workflow Stop hard-kills
    // the (stdin-less) process, so its exit is indeterminate and would otherwise
    // read as a crash — which startup re-dispatch resurrects. This is the trap the
    // whole issue turns on.
    let deliberate_stop = orch.process_state.take_workflow_stop_requested(run_id);
    let artifact_written = orch.process_state.terminal_tool_armed(run_id);

    if deliberate_stop {
        // Terminal + non-crashed (reads as Stopped via `exit_reason`), record
        // KEPT so Restart can re-dispatch it. Fail-closed: this runs only after
        // the supervisor observes the process gone (the kill path escalates
        // SIGTERM->SIGKILL), so there is no window where the UI reads Stopped
        // while a zombie keeps spawning calls. The kill path also set
        // `exit_reason`; set it here too so the outcome is correct regardless of
        // which finalizer wins the status race.
        let _ = crate::orchestrator::lifecycle::set_exit_reason(orch, run_id, "user_stop");
        crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
    } else {
        match exit_code {
            Some(0) if artifact_written => {
                // Clean completion: drop the re-dispatch record so a later restart
                // does not re-run a finished workflow (CAIRN-2498).
                clear_workflow_run_record(orch, run_id);
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
            }
            Some(0) => {
                // Ran to completion but never satisfied its output contract. KEEP
                // the record (CAIRN-2516): a transient failure can be Restarted to
                // replay its journal from the completed prefix; the sweep still
                // skips it (terminal), so this is not a boot-loop.
                let reason = "Workflow script exited 0 without writing its output artifact.";
                insert_error_event(orch, run_id, session_id, reason);
                crate::orchestrator::lifecycle::fail_run(orch, run_id, reason);
            }
            Some(code) => {
                // A script throw. KEEP the record so a Restart can re-run it after
                // the underlying bug is fixed (CAIRN-2516); the sweep skips a
                // terminal `failed` run, so it never auto-re-dispatches.
                let reason = format!("Workflow script exited with code {code}.");
                insert_error_event(orch, run_id, session_id, &reason);
                crate::orchestrator::lifecycle::fail_run(orch, run_id, &reason);
            }
            None => {
                // Indeterminate exit (a genuine crash, not a deliberate stop):
                // finalize Crashed, leaving the run resumable. The record is KEPT
                // so startup re-dispatch re-spawns this run_id and its journal
                // replays the calls already completed (CAIRN-2498).
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
            }
        }
    }

    if let Ok(mut processes) = orch.process_state.processes.lock() {
        processes.remove(run_id);
    }
}
