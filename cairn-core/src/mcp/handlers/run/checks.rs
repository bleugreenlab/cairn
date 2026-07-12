//! Synchronous project-check command runners and run/check stream-id helpers.

use super::process::execute_process;
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::BuildSlotExecutionMeta;

pub(crate) fn run_item_stream_id(tool_use_id: &str, index: usize) -> String {
    format!("{tool_use_id}:{index}")
}

/// Outcome of running one project check to completion. `exit_code` is `None`
/// when the process produced no code (a signal kill OR a timeout); `timed_out`
/// disambiguates the two so the runner can record a budget kill AS a timeout
/// rather than an opaque crash. Combined stdout+stderr rides in `output`.
#[derive(Debug)]
pub(crate) struct CheckExecResult {
    pub exit_code: Option<i32>,
    pub output: String,
    pub timed_out: bool,
    pub provenance: Option<BuildSlotExecutionMeta>,
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
///
/// `sandbox_enabled` is `true` for the shared-worktree (fallback) path, matching
/// the pre-isolation behavior where the sandbox applies and the check-command
/// exemption lifts it. It is `false` when the check runs in a disposable
/// per-check COW clone (the isolated concurrent path): the clone is not the live
/// checkout and not a registered worktree — stripping `.jj` makes it a non-jj
/// cwd that the sandbox would otherwise treat as a read-only live checkout and
/// deny a formatter's writes — so the clone runs UNCONFINED. The isolation, not
/// the sandbox, is what keeps one check's writes out of another's view.
pub(crate) async fn run_check_command(
    orch: &Orchestrator,
    cwd: &str,
    stream_id: &str,
    run_context: Option<&RunContext>,
    command: &str,
    timeout_ms: u32,
    sandbox_enabled: bool,
) -> Result<CheckExecResult, String> {
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
        None, // no stdin payload — check commands run through `bash -c`
        sandbox_enabled,
        false, // promote_on_timeout: a timed-out check is KILLED, never detached
    )
    .await?;
    let combined = match (out.stdout.is_empty(), out.stderr.is_empty()) {
        (false, false) => format!("{}\n{}", out.stdout, out.stderr),
        (false, true) => out.stdout,
        (true, false) => out.stderr,
        (true, true) => String::new(),
    };
    Ok(CheckExecResult {
        exit_code: out.exit_code,
        output: combined,
        timed_out: out.timed_out,
        provenance: None,
    })
}
