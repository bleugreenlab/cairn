//! Synchronous project-check command runners and run/check stream-id helpers.

use cairn_common::executor_protocol::CellExecutionMeta;

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
    /// Authoritative process execution time when the result was completed before
    /// this consumer began awaiting it, as with sequential executor batches.
    pub duration_ms: Option<i64>,
    pub provenance: Option<CellExecutionMeta>,
    pub publication: Option<crate::fleet::PublicationCoordination>,
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
