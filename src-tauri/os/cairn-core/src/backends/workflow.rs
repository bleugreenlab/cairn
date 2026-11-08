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
use cairn_common::executor_protocol::ResidentRuntimeAsset;

const WORKFLOW_BACKEND_NAME: &str = "Workflow";

/// Parameters required to dispatch a workflow into an executor lease.
pub(crate) struct WorkflowSpawnParams {
    pub run_id: String,
    /// What this workflow is invoked as. It is what a surface showing the
    /// running runtime calls it, so the row names the workflow rather than the
    /// directory its package happens to sit in.
    pub workflow_id: String,
    pub project_id: String,
    pub repository_path: PathBuf,
    pub anchor_branch: String,
    pub branch_mode: String,
    pub package_path: PathBuf,
    pub script_path: PathBuf,
    pub args_json: String,
    pub turn_id: String,
    pub session_id: Option<String>,
    pub output_name: String,
    /// Re-dispatch replaces a resident runtime that may have survived its host.
    pub replace_existing: bool,
}

pub(crate) async fn spawn_workflow_process(
    orch: &Orchestrator,
    params: WorkflowSpawnParams,
) -> Result<(), String> {
    use cairn_common::executor_protocol::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let run_db = resolve_run_db(WORKFLOW_BACKEND_NAME, orch, &params.run_id)?;
    let tip = crate::fleet::residency::resolve_logical_commit(
        orch,
        &params.repository_path,
        &params.anchor_branch,
    )
    .await?;
    let fence = crate::fleet::residency::acquire(
        orch,
        ResidencyAcquireRequest {
            holder: ResidencyHolder::Workflow {
                run_id: params.run_id.clone(),
            },
            owner_ref: None,
            selector: None,
            executor: None,
            repository: RepositoryLocator::ColocatedPath {
                project_id: params.project_id.clone(),
                repository_id: params.project_id.clone(),
                absolute_path: params.repository_path.to_string_lossy().into_owned(),
            },
            initial_base_commit: tip.clone(),
            // What the checkout costs while nothing runs in it. The runtime's
            // concurrency unit is declared on the process that does the work.
            footprint: ResidencyFootprint {
                memory_bytes: 256 * 1024 * 1024,
                disk_growth_bytes: 1024 * 1024 * 1024,
            },
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
            priority: CellPriority::AgentInteractive,
            // A workflow run has no bound of its own on waiting for room, only
            // the machine-wide default. Its checkout is large and cold
            // provisioning is slow, so a tight number here refuses runs on a
            // machine that was working correctly the whole time.
            wait_horizon_unix_ms: crate::fleet::default_wait_horizon_unix_ms(
                &crate::config::settings::load_fleet(&orch.config_dir),
            ),
            waiting_since_unix_ms: crate::fleet::unix_time_ms(),
        },
    )
    .await
    .map_err(|refusal| refusal.diagnostic)?;
    // Reacquiring a holder preserves its processes. A host restart must run
    // the current package, not leave an old runtime waiting on an old question.
    // Stop before subscribing so its exit cannot finalize the replacement run.
    if params.replace_existing {
        if let Err(error) = crate::fleet::residency::stop(orch, &fence, &params.run_id).await {
            let _ = crate::fleet::residency::release(orch, &fence).await;
            return Err(error);
        }
    }
    if let Err(error) = crate::fleet::residency::refresh(orch, &fence, &tip).await {
        let _ = crate::fleet::residency::release(orch, &fence).await;
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
    // The subscription must outlive this function, until the runtime exits, so
    // a started run parks it here and whichever side finalizes the run drops it.
    let subscription_slot = Arc::new(Mutex::new(
        None::<crate::fleet::ResidentProcessSubscription>,
    ));
    let event_subscription = subscription_slot.clone();
    let subscription = orch.fleet.subscribe_resident_process_events(move |event| {
        let expected = event_generation.load(Ordering::Acquire);
        if event.holder != event_fence.holder
            || event.incarnation_id != event_fence.incarnation_id
            || event.cell_epoch != event_fence.cell_epoch
            || event.process_key != event_key
            || (expected != 0 && event.process_generation != expected)
        {
            return;
        }
        if expected == 0 {
            if let ResidentProcessEventKind::State {
                status:
                    ResidentProcessStatus::Exited {
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
            ResidentProcessEventKind::Output { stream, data, .. } => {
                let text = String::from_utf8_lossy(&data);
                if stream == ResidentProcessStream::Stderr {
                    log::error!("workflow stderr: {text}")
                } else if stream == ResidentProcessStream::Stdout {
                    log::info!("workflow stdout: {text}")
                }
            }
            ResidentProcessEventKind::State {
                status:
                    ResidentProcessStatus::Exited {
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
                let subscription = event_subscription.clone();
                tokio::spawn(async move {
                    drop(subscription.lock().unwrap().take());
                    orch.process_state.clear_workflow_lease(&run_id);
                    finalize_workflow_run(
                        &orch,
                        &run_id,
                        session_id.as_deref(),
                        if executor_lost { None } else { exit_code },
                    );
                    let _ = crate::fleet::residency::release(&orch, &fence).await;
                });
            }
            _ => {}
        }
    });
    let loader = format!(
        "await import(process.env.CAIRN_RUNTIME_ASSETS + \"/workflow/{}\")",
        entry.replace('"', "\\\"")
    );
    // A workflow runtime is an agent process like any other, so its fence
    // profile follows the run's fence dial and nothing else. Declaring
    // `Confined` outright would confine a `fence: allow` workflow the moment the
    // resident-process seam starts honoring the declaration. Unfenced, it still
    // carries the operator credential boundary.
    let sandbox_mode =
        match crate::mcp::handlers::permission::resolve_fence_policy(orch, Some(&params.run_id))
            .await
        {
            Some(fence) if crate::services::sandbox::sandbox_applies(fence) => {
                ProcessSandboxMode::Confined
            }
            _ => ProcessSandboxMode::CredentialFloor,
        };
    // The runtime starts wherever acquisition put its environment, which need
    // not be this runner's machine.
    let placed = orch.fleet.residency_route_executor(&fence.holder);
    let routed = match workflow_runtime_env(
        orch,
        &fence.holder,
        placed.as_deref(),
        &params.run_id,
        params.args_json,
        params.output_name,
    ) {
        Ok(routed) => routed,
        Err(error) => {
            crate::fleet::residency::rollback(orch, &fence, &params.run_id).await;
            return Err(format!("failed to start workflow process: {error}"));
        }
    };
    let result = orch
        .fleet
        .operate_residency(
            orch,
            ResidencyOperation::StartProcess {
                fence: fence.clone(),
                process_key: params.run_id.clone(),
                kind: ResidentProcessKind::WorkflowRuntime {
                    workflow: params.workflow_id.clone(),
                },
                // The runtime is the thing that works, so it is the thing that
                // is charged.
                reservation: Some(ResourceReservation {
                    memory_bytes: 0,
                    disk_growth_bytes: 0,
                    concurrency_units: 1,
                    source: ResourceReservationSource::Declared,
                    measured_peak_bytes: None,
                }),
                process: ResidentProcessSpec {
                    stdout_framing: ResidentStreamFraming::Bytes,
                    runtime_packages: Vec::new(),
                    program: "bun".into(),
                    args: vec!["-e".into(), loader],
                    launch_command: None,
                    cwd: if params.branch_mode == "inherit" {
                        String::new()
                    } else {
                        "runtime-assets/workflow".into()
                    },
                    cwd_root: if params.branch_mode == "inherit" {
                        ResidentProcessCwdRoot::Checkout
                    } else {
                        ResidentProcessCwdRoot::ResidencyScratch
                    },
                    // `CAIRN_HOME` is the executor's own, layered beneath this.
                    env: routed.env,
                    sandbox_mode,
                    sandbox_policy: None,
                    runtime_assets: assets,
                    io: ResidentProcessIoMode::Pipe,
                    node_scratch: None,
                    relay: Some(routed.relay),
                },
            },
        )
        .await;
    let ResidencyResult::State { cell } = result else {
        crate::fleet::residency::rollback(orch, &fence, &params.run_id).await;
        return Err(format!("failed to start workflow process: {result:?}"));
    };
    let Some(process_generation) = cell
        .occupancy
        .processes
        .get(&params.run_id)
        .map(|p| p.generation)
    else {
        crate::fleet::residency::rollback(orch, &fence, &params.run_id).await;
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
            let _ = crate::fleet::residency::release(orch, &fence).await;
            return Ok(());
        }
    }
    *subscription_slot.lock().unwrap() = Some(subscription);
    // An exit that finalized the run before the slot was filled found it empty.
    if finished.load(Ordering::Acquire) {
        drop(subscription_slot.lock().unwrap().take());
    }
    let renew_orch = orch.clone();
    tokio::spawn(async move {
        while !finished.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if finished.load(Ordering::Acquire)
                || crate::fleet::residency::renew(&renew_orch, &fence)
                    .await
                    .is_err()
            {
                break;
            }
        }
    });
    Ok(())
}

/// The workflow runtime's environment, routed through the resident relay of the
/// machine `placed` names.
///
/// The runtime talks to Cairn only through its callback, so where that callback
/// goes is the whole question. Wherever it runs, this runner's own colocated
/// executor included, it calls its executor's relay with a capability minted
/// for this one process, speaking for `run_id`, the way a terminal or REPL
/// does. Neither the machine secret, which speaks for no run, nor the loopback
/// goes in its environment. `CAIRN_HOME` is the executor's own, layered beneath
/// this.
pub(crate) fn workflow_runtime_env(
    orch: &Orchestrator,
    holder: &cairn_common::executor_protocol::ResidencyHolder,
    placed: Option<&str>,
    run_id: &str,
    args_json: String,
    output_name: String,
) -> Result<crate::fleet::residency::RoutedResidentEnv, String> {
    crate::fleet::residency::route_resident_callbacks_on(
        orch,
        placed,
        holder,
        run_id,
        Some(run_id),
        vec![
            ("CAIRN_RUN_ID".into(), run_id.to_string()),
            ("CAIRN_ENV".into(), cairn_common::paths::env_str().into()),
            ("CAIRN_WORKFLOW_ARGS".into(), args_json),
            ("CAIRN_WORKFLOW_OUTPUT".into(), output_name),
        ],
    )
}

fn workflow_runtime_assets(package: &Path) -> Result<Vec<ResidentRuntimeAsset>, String> {
    let mut assets = Vec::new();
    crate::runtime::collect_package_assets(package, Path::new("workflow"), &mut assets)?;
    crate::runtime::collect_package_assets(
        &crate::runtime::runtime_package("harness")?,
        Path::new("workflow/node_modules/@cairn/harness"),
        &mut assets,
    )?;
    crate::runtime::collect_package_assets(
        &crate::runtime::runtime_package("sdk")?,
        Path::new("workflow/node_modules/@cairn/sdk"),
        &mut assets,
    )?;
    cairn_common::runtime::validate_runtime_assets(&assets)
        .map_err(|error| format!("workflow package and runtime rejected: {error}"))?;
    Ok(assets)
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

/// No callback credential reaches a workflow runtime's launch environment on
/// macOS and Linux (tenet #1381): its capability is held by the executor that
/// relays it, the colocated machine included. A canary for every credential is
/// planted in this process's own environment, which the colocated executor's
/// children inherit.
#[cfg(all(test, unix))]
mod launch_env_tests {
    use super::*;
    use cairn_common::auth::launch_canary;

    fn git(repo: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    /// Whether this host can confine the runtime, which starts under the
    /// credential floor's profile. On macOS the only cause it cannot is a test
    /// process already inside a Seatbelt profile, the declared
    /// `nested-sandbox-exec` gap, recorded as a skip.
    fn workflow_confines_here(test: &str) -> bool {
        match cairn_sandbox::IsolatedChild::availability() {
            Ok(()) => true,
            Err(why) if cfg!(target_os = "macos") => {
                eprintln!("skipping {test}: {why}");
                cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
                false
            }
            Err(why) => panic!("{test} needs a host that can confine a workflow runtime: {why}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_workflow_runtime_carries_no_callback_credential() {
        if !workflow_confines_here("a_workflow_runtime_carries_no_callback_credential") {
            return;
        }
        if !launch_canary::in_planted_child(
            module_path!(),
            "a_workflow_runtime_carries_no_callback_credential",
        ) {
            return;
        }
        let socket = crate::fleet::service_placement::tests::name_callback_socket();
        let (orch, config) = crate::fleet::service_placement::tests::attached_orchestrator().await;
        let repo = config.path().join("repository");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        git(&repo, &["config", "user.email", "probe@example.com"]);
        git(&repo, &["config", "user.name", "Probe"]);
        std::fs::write(repo.join("README.md"), "probe\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-qm", "base"]);
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
        crate::jj::ensure_project_store(&jj, &store, &repo).unwrap();
        let repo_path = repo.to_string_lossy().into_owned();
        orch.db
            .local
            .execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES('p', 'default', 'Probe', 'probe', ?1, 1, 1)",
                cairn_db::turso::params![repo_path.as_str()],
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "INSERT INTO runs(id, project_id, status, created_at, updated_at)
                 VALUES('workflow-probe','p','starting',1,1)",
                (),
            )
            .await
            .unwrap();
        let observed = config.path().join("workflow-env.json");
        let package = config.path().join("package");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(package.join("package.json"), "{}").unwrap();
        let script = package.join("main.ts");
        std::fs::write(
            &script,
            format!(
                // Held until the test has read it, so its capability is
                // still registered while it is searched for.
                "const fs = require('fs');\nawait Bun.write({part:?}, JSON.stringify(process.env));\nfs.renameSync({part:?}, {path:?});\nwhile (!fs.existsSync({release:?})) await Bun.sleep(100);\n",
                part = format!("{}.part", observed.display()),
                path = observed.display().to_string(),
                release = format!("{}.release", observed.display()),
            ),
        )
        .unwrap();
        spawn_workflow_process(
            &orch,
            WorkflowSpawnParams {
                run_id: "workflow-probe".into(),
                workflow_id: "probe".into(),
                project_id: "p".into(),
                repository_path: repo,
                anchor_branch: "main".into(),
                branch_mode: "inherit".into(),
                package_path: package,
                script_path: script,
                args_json: "{}".into(),
                turn_id: "unused-turn".into(),
                session_id: None,
                output_name: "return".into(),
                replace_existing: false,
            },
        )
        .await
        .expect("the workflow starts");
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while !observed.is_file() {
            if std::time::Instant::now() >= deadline {
                let run = orch
                    .db
                    .local
                    .read(|conn| {
                        Box::pin(async move {
                            let mut rows = conn
                                .query(
                                    "SELECT COALESCE(r.status, '') || ' ' || COALESCE(r.error_message, '') || ' ' ||
                                            COALESCE((SELECT group_concat(data, ' | ') FROM events WHERE run_id = r.id), '')
                                     FROM runs r WHERE r.id = 'workflow-probe'",
                                    (),
                                )
                                .await?;
                            Ok(match rows.next().await? {
                                Some(row) => row.get::<String>(0)?,
                                None => String::new(),
                            })
                        })
                    })
                    .await;
                panic!("the workflow never wrote its environment; its run: {run:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let env = std::fs::read_to_string(&observed).unwrap();
        let leaks = launch_canary::leaks(&env, &[]);
        std::fs::write(format!("{}.release", observed.display()), "").unwrap();
        assert!(
            leaks.is_empty(),
            "a workflow runtime's launch environment carries a credential: {leaks:?}\n{env}"
        );
        launch_canary::assert_names_socket("a workflow runtime", &env, &socket);
    }
}
