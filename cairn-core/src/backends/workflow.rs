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
//! The spawn helper is driven by `start_workflow_run`; the run-target invocation
//! that reaches it lands in a later stage, so the module is dead-code-allowed
//! until then.
#![allow(dead_code)]

use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::run_state::{resolve_run_db, run_job_id, transition_run_to_live};
use crate::agent_process::process::ActiveProcess;
use crate::models::RunStatus;
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::services::SpawnConfig;

pub(crate) const WORKFLOW_BACKEND_NAME: &str = "Workflow";

/// Everything the supervisor needs to spawn and finalize a workflow process.
pub(crate) struct WorkflowSpawnParams {
    pub run_id: String,
    pub working_dir: String,
    /// Absolute path to the workflow's script entry.
    pub script_path: PathBuf,
    /// The validated named args, forwarded to the script as `CAIRN_WORKFLOW_ARGS`.
    pub args_json: String,
    /// The run's initial turn, started once the process spawns.
    pub turn_id: String,
    pub session_id: Option<String>,
    /// The declared output artifact name, exported as `CAIRN_WORKFLOW_OUTPUT` so
    /// the harness `output()` targets `cairn:~/{name}`.
    pub output_name: String,
}

/// Spawn `bun <script>` for a prepared workflow run: inject the callback env,
/// mark the run live and its turn running, register the process, and start the
/// supervisor thread that finalizes on exit. Returns immediately.
pub(crate) fn spawn_workflow_process(
    orch: &Orchestrator,
    params: WorkflowSpawnParams,
) -> Result<(), String> {
    let run_db = resolve_run_db(WORKFLOW_BACKEND_NAME, orch, &params.run_id)?;

    let bun_path = crate::env::find_binary("bun")?;
    let mcp_secret = orch
        .mcp_auth
        .get_secret_for_mcp()
        .map_err(|e| format!("Failed to get MCP auth secret: {e}"))?;
    // The script's SDK reads these env vars directly (see packages/sdk/src/config.ts):
    // the callback URL, the bearer secret, and its own run id. The server resolves
    // `cairn:~/` from CAIRN_RUN_ID, so no home-uri env is required.
    let callback_url = format!("http://127.0.0.1:{}/api/mcp", orch.mcp_callback_port);
    let cairn_home = cairn_common::paths::cairn_home();
    let script_arg = params.script_path.to_string_lossy().into_owned();

    let spawn_config = SpawnConfig::new(&bun_path)
        .args([script_arg])
        .cwd(&params.working_dir)
        .env("CAIRN_RUN_ID", &params.run_id)
        .env("CAIRN_MCP_SECRET", &mcp_secret)
        .env("CAIRN_CALLBACK_URL", &callback_url)
        .env("CAIRN_ENV", cairn_common::paths::env_str())
        .env("CAIRN_HOME", cairn_home.to_string_lossy().as_ref())
        .env("CAIRN_WORKFLOW_ARGS", &params.args_json)
        .env("CAIRN_WORKFLOW_OUTPUT", &params.output_name)
        .stdin(false);
    // Make the workflow script resolve `@cairn/harness` + `@cairn/sdk` from the
    // Cairn-owned runtime, independent of the INVOKING project (CAIRN-2504). Bun
    // resolves bare ESM specifiers by walking `node_modules` up from the script's
    // OWN directory (NODE_PATH is honored only for CommonJS `require`), so the
    // harness is linked into a `node_modules` beside the script rather than passed
    // via NODE_PATH. Best-effort: a failure is logged, not fatal.
    ensure_harness_link(&params.script_path);

    let mut child = orch.services.process.spawn(spawn_config).map_err(|e| {
        insert_error_event(
            orch,
            &params.run_id,
            params.session_id.as_deref(),
            &format!("Failed to start workflow process: {e}"),
        );
        e
    })?;

    if let Err(e) = transition_run_to_live(WORKFLOW_BACKEND_NAME, orch, &run_db, &params.run_id) {
        log::warn!("Failed to transition workflow run to live: {e}");
    }
    // Move the run's initial turn from pending -> running so a clean process exit
    // reduces it to Complete (finalize_run maps a running turn on Exited).
    if let Err(e) = crate::execution::jobs::start_turn(orch, &params.turn_id, &params.run_id) {
        log::warn!("Failed to start workflow turn: {e}");
    }

    let stdout = child.take_stdout();
    let stderr = child.take_stderr();
    let child_arc = Arc::new(Mutex::new(Some(child)));
    let job_id = run_job_id(WORKFLOW_BACKEND_NAME, &run_db, &params.run_id);

    {
        let mut processes = orch
            .process_state
            .processes
            .lock()
            .map_err(|e| e.to_string())?;
        let active = ActiveProcess::new(
            child_arc.clone(),
            Arc::new(Mutex::new(None)),
            params.session_id.clone(),
            job_id,
        );
        processes.register(params.run_id.clone(), active);
    }

    // Forward stderr to the log for crash diagnostics (a thrown script's stack
    // trace must be readable).
    if let Some(stderr) = stderr {
        thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                log::error!("workflow stderr: {line}");
            }
        });
    }

    // Supervisor: drain stdout (diagnostics only — NOT an event channel), wait for
    // exit, and finalize.
    let orch = orch.clone();
    let run_id = params.run_id.clone();
    let session_id = params.session_id.clone();
    thread::spawn(move || {
        if let Some(stdout) = stdout {
            for line in stdout.lines().map_while(Result::ok) {
                log::info!("workflow stdout: {line}");
            }
        }
        let exit_code = wait_for_exit(&child_arc);
        finalize_workflow_run(&orch, &run_id, session_id.as_deref(), exit_code);
    });

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

/// Poll the child for its exit status. stdout draining above already blocked
/// until the process closed stdout, so exit is imminent; a bounded poll catches
/// it. `None` means the code could not be determined (finalized as Crashed).
fn wait_for_exit(
    child_arc: &Arc<Mutex<Option<Box<dyn crate::services::ChildProcess>>>>,
) -> Option<i32> {
    for _ in 0..600 {
        {
            let mut guard = child_arc.lock().ok()?;
            match guard.as_mut() {
                Some(child) => match child.try_wait() {
                    Ok(Some(status)) => return Some(status.code().unwrap_or(-1)),
                    Ok(None) => {}
                    Err(_) => return None,
                },
                None => return None,
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    None
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

// ============================================================================
// Harness resolution (CAIRN-2504)
//
// A workflow script imports `@cairn/harness` (+ `@cairn/sdk`). Those packages
// live only in a Cairn checkout, so a workflow invoked from any other project
// failed to resolve them. bun resolves bare ESM specifiers by walking
// `node_modules` up from the SCRIPT's own directory (NODE_PATH is a CommonJS
// mechanism and is ignored for ESM), so the fix links the Cairn-owned runtime's
// `@cairn` scope into a `node_modules` beside the script. That placement also
// gives the intended precedence for free: the runtime scope is nearest, so it
// resolves first, while the invoking project's own `node_modules` higher up the
// tree remains available as a secondary source for any project deps.
// ============================================================================

/// The bundled workflow runtime's `node_modules`, relative to `CAIRN_HOME`. A
/// packaged app provisions it here via `workspace::bundle::provision_workflow_runtime`.
const CAIRN_HOME_RUNTIME_NODE_MODULES: [&str; 2] = ["runtime", "node_modules"];

/// Whether a `node_modules` directory provides the `@cairn/harness` package
/// (the marker package every workflow imports).
fn harness_present(node_modules: &Path) -> bool {
    node_modules.join("@cairn").join("harness").is_dir()
}

/// Resolve the Cairn-owned `node_modules` that provides `@cairn/harness` +
/// `@cairn/sdk` for a workflow spawn, independent of the invoking project.
///
/// Precedence:
/// 1. `CAIRN_HARNESS_RUNTIME` — explicit override (packaging / tests).
/// 2. `<CAIRN_HOME>/runtime/node_modules` — the runtime a packaged app installs
///    via bundle sync.
/// 3. Dev fallback — the Cairn repo's own `node_modules`, discovered by walking
///    up from the crate manifest dir (compile-time) and the process working dir
///    (runtime), returning the first that carries `@cairn/harness`. A dev build
///    compiles from the worktree and a `dev:instance` runner launches with the
///    repo's `src-tauri` as its cwd, so both reach the repo-root `node_modules`.
fn harness_runtime_node_modules() -> Option<PathBuf> {
    if let Some(raw) = std::env::var_os("CAIRN_HARNESS_RUNTIME").filter(|v| !v.is_empty()) {
        let path = PathBuf::from(raw);
        if harness_present(&path) {
            return Some(path);
        }
    }

    let mut provisioned = cairn_common::paths::cairn_home();
    for part in CAIRN_HOME_RUNTIME_NODE_MODULES {
        provisioned.push(part);
    }
    if harness_present(&provisioned) {
        return Some(provisioned);
    }

    dev_repo_node_modules()
}

/// Walk up from the crate manifest dir and the current working dir looking for a
/// `node_modules` that provides `@cairn/harness`.
fn dev_repo_node_modules() -> Option<PathBuf> {
    let mut roots: Vec<PathBuf> = vec![PathBuf::from(env!("CARGO_MANIFEST_DIR"))];
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }
    for root in roots {
        for ancestor in root.ancestors() {
            let candidate = ancestor.join("node_modules");
            if harness_present(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Link the Cairn-owned runtime's `@cairn` scope into a `node_modules` beside
/// the workflow script so bun resolves `@cairn/harness` + `@cairn/sdk` from the
/// script's own directory, regardless of the invoking project. Best-effort: a
/// missing runtime or a link failure is logged, not fatal — resolution then
/// falls back to the script's existing `node_modules` ancestry.
fn ensure_harness_link(script_path: &Path) {
    let Some(runtime) = harness_runtime_node_modules() else {
        log::warn!(
            "workflow: no @cairn/harness runtime resolved; script imports may fail for {script_path:?}"
        );
        return;
    };
    let Some(script_dir) = script_path.parent() else {
        return;
    };
    let node_modules = script_dir.join("node_modules");
    if let Err(e) = link_scope(
        &crate::services::RealFileSystem,
        &node_modules,
        &runtime.join("@cairn"),
    ) {
        log::warn!("workflow: failed to link @cairn runtime into {node_modules:?}: {e}");
    }
}

/// Ensure `<node_modules>/@cairn` links to `scope_target` (the runtime's `@cairn`
/// directory). Idempotent and non-destructive:
/// - a symlink already pointing at `scope_target` is left as-is;
/// - a stale symlink (the runtime moved) is replaced;
/// - a real `@cairn` directory (a Cairn checkout owns it), or a Windows junction
///   that `is_symlink` cannot introspect, is respected untouched.
///
/// Link creation goes through the production [`crate::services::FileSystem::symlink`],
/// which falls back to a directory junction (`mklink /J`) on Windows so a normal
/// user WITHOUT symlink privileges still gets a working link — otherwise a
/// best-effort raw `symlink_dir` would silently fail and reintroduce the original
/// unresolved-import bug on Windows. `scope_target` is a stable path
/// (`<CAIRN_HOME>/runtime/...` or the Cairn repo), so a junction is never
/// effectively stale. Split out from [`ensure_harness_link`] so the link contract
/// is unit-testable without resolving a live runtime.
fn link_scope(
    fs: &dyn crate::services::FileSystem,
    node_modules: &Path,
    scope_target: &Path,
) -> Result<(), String> {
    let link = node_modules.join("@cairn");
    if fs.is_symlink(&link) {
        if std::fs::read_link(&link)
            .map(|existing| existing == *scope_target)
            .unwrap_or(false)
        {
            return Ok(()); // already linked to the current runtime
        }
        // Stale symlink (the runtime moved): remove it before recreating. A
        // symlink to a directory is cleared by unlink on unix (remove_file) and
        // rmdir on windows (remove_dir); try both so either link type is removed
        // without touching the target's contents.
        let _ = std::fs::remove_file(&link).or_else(|_| std::fs::remove_dir(&link));
    } else if fs.exists(&link) {
        // A real @cairn dir (a Cairn checkout) or a junction: respect it.
        return Ok(());
    }
    fs.create_dir_all(node_modules)?;
    match fs.symlink(scope_target, &link) {
        Ok(()) => Ok(()),
        // Lost a race with a concurrent spawn that created the link first.
        Err(e) if fs.exists(&link) => {
            log::debug!("workflow: @cairn link already present after race: {e}");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod harness_link_tests {
    use super::*;
    use crate::services::RealFileSystem;

    #[test]
    fn harness_present_checks_scoped_package() {
        let temp = tempfile::tempdir().unwrap();
        let node_modules = temp.path().join("node_modules");
        assert!(!harness_present(&node_modules));
        std::fs::create_dir_all(node_modules.join("@cairn").join("harness")).unwrap();
        assert!(harness_present(&node_modules));
    }

    #[test]
    fn link_scope_creates_symlink_to_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_scope = temp.path().join("runtime/node_modules/@cairn");
        std::fs::create_dir_all(runtime_scope.join("harness")).unwrap();
        let node_modules = temp.path().join("pkg/node_modules");

        link_scope(&RealFileSystem, &node_modules, &runtime_scope).unwrap();
        let link = node_modules.join("@cairn");
        assert_eq!(std::fs::read_link(&link).unwrap(), runtime_scope);
        // Resolves through the link to the runtime's harness package.
        assert!(link.join("harness").is_dir());
    }

    #[test]
    fn link_scope_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_scope = temp.path().join("@cairn");
        std::fs::create_dir_all(&runtime_scope).unwrap();
        let node_modules = temp.path().join("node_modules");
        link_scope(&RealFileSystem, &node_modules, &runtime_scope).unwrap();
        // A second call over the existing correct link is a no-op success.
        link_scope(&RealFileSystem, &node_modules, &runtime_scope).unwrap();
        assert_eq!(
            std::fs::read_link(node_modules.join("@cairn")).unwrap(),
            runtime_scope
        );
    }

    #[test]
    fn link_scope_replaces_a_stale_symlink() {
        let temp = tempfile::tempdir().unwrap();
        let old = temp.path().join("old/@cairn");
        let new = temp.path().join("new/@cairn");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::create_dir_all(&new).unwrap();
        let node_modules = temp.path().join("node_modules");
        link_scope(&RealFileSystem, &node_modules, &old).unwrap();
        link_scope(&RealFileSystem, &node_modules, &new).unwrap();
        assert_eq!(
            std::fs::read_link(node_modules.join("@cairn")).unwrap(),
            new
        );
    }

    #[test]
    fn link_scope_respects_a_real_directory() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_scope = temp.path().join("@cairn");
        std::fs::create_dir_all(&runtime_scope).unwrap();
        let node_modules = temp.path().join("node_modules");
        // A project that owns a real @cairn dir (a Cairn checkout) is left alone.
        std::fs::create_dir_all(node_modules.join("@cairn").join("harness")).unwrap();
        link_scope(&RealFileSystem, &node_modules, &runtime_scope).unwrap();
        assert!(std::fs::read_link(node_modules.join("@cairn")).is_err());
        assert!(node_modules.join("@cairn/harness").is_dir());
    }
}
