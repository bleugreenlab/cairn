//! Synchronous project-check command runners and run/check stream-id helpers.

use super::process::execute_process;
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;

pub(crate) fn run_item_stream_id(tool_use_id: &str, index: usize) -> String {
    format!("{tool_use_id}:{index}")
}

/// Stream id for a synchronous when:write check's live output. Namespaced with
/// `check-` so a check's stream never collides with a run item's stream id
/// (`run_item_stream_id`) at the same index: the frontend matches run items by
/// the bare `:{index}` suffix, and a check runs while the committing batch's item
/// rows are still in flight (the tool result lands only after checks finish), so
/// a shared `:{index}` id would mis-attribute the check's output to the command
/// row with the same index. The frontend `activeCheckStream` matcher keys off
/// this `:check-` namespace instead.
pub(crate) fn check_stream_id(tool_use_id: &str, index: usize) -> String {
    format!("{tool_use_id}:check-{index}")
}

/// Run one project-declared `when:write` check command to completion under the
/// same OS sandbox / fence env / live-streaming machinery the `run` verb uses,
/// returning its exit code plus combined captured output.
///
/// Spawned via `bash -c`, exactly how a [`RunSpec::Shell`] item executes, so the
/// check inherits `CAIRN_WORKTREE`, the git/jj env, the sccache build-service
/// env, and the per-job `TMPDIR` scratch. A sandbox denial surfaces here as a
/// non-zero / `None` exit (a clear check failure), never an interactive fence
/// prompt: the synchronous when:write runner intentionally does not route checks
/// through `run_one`/`raise_fence` (that is the when:review cargo path, Wave 4).
pub(crate) async fn run_check_command(
    orch: &Orchestrator,
    cwd: &str,
    stream_id: &str,
    run_context: Option<&RunContext>,
    command: &str,
    timeout_ms: u32,
) -> Result<(Option<i32>, String), String> {
    let args = ["-c".to_string(), command.to_string()];
    let out = execute_process(
        orch,
        cwd,
        stream_id,
        run_context,
        "bash",
        &args,
        timeout_ms,
        Some(command),
        true,  // sandbox_enabled
        false, // branch_scoped_run
    )
    .await?;
    let combined = match (out.stdout.is_empty(), out.stderr.is_empty()) {
        (false, false) => format!("{}\n{}", out.stdout, out.stderr),
        (false, true) => out.stdout,
        (true, false) => out.stderr,
        (true, true) => String::new(),
    };
    Ok((out.exit_code, combined))
}

/// Run one project-declared check command UNSANDBOXED and with no run context,
/// teeing combined output to a host-readable, job-scoped log file so a PR-node or
/// `/checks` render can tail it live while the run is in flight.
///
/// This is the turn-end (`when:idle`/`when:review`) cadence's vehicle: at
/// turn-end the agent is idle, so a fence permission prompt would hang with no
/// one to answer. Passing `sandbox_enabled=false` takes the same unconditional
/// `sandbox = None` path the post-fence-grant re-execution uses (run.rs), so no
/// interactive fence prompt is reachable. `set -o pipefail` preserves the
/// check's own exit code through the `tee` pipe (otherwise `bash -c` would return
/// tee's exit, masking a failing check).
pub(crate) async fn run_check_command_unsandboxed(
    orch: &Orchestrator,
    cwd: &str,
    stream_id: &str,
    command: &str,
    timeout_ms: u32,
    log_path: &std::path::Path,
) -> Result<(Option<i32>, String), String> {
    // Single-quote the log path for the shell; escape any embedded single quote.
    let log = log_path.to_string_lossy().replace('\'', "'\\''");
    let wrapped = format!("set -o pipefail; {{ {command}; }} 2>&1 | tee -a '{log}'");
    let args = ["-c".to_string(), wrapped];
    let out = execute_process(
        orch,
        cwd,
        stream_id,
        None, // no run context — turn is over, nothing to stream into
        "bash",
        &args,
        timeout_ms,
        Some(command),
        false, // sandbox_enabled=false — idle agent can't answer a fence prompt
        false, // branch_scoped_run
    )
    .await?;
    let combined = match (out.stdout.is_empty(), out.stderr.is_empty()) {
        (false, false) => format!("{}\n{}", out.stdout, out.stderr),
        (false, true) => out.stdout,
        (true, false) => out.stderr,
        (true, true) => String::new(),
    };
    Ok((out.exit_code, combined))
}
