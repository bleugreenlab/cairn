use crate::{agent_process::stream::TokenCounts, orchestrator::Orchestrator, storage::LocalDb};
pub use cairn_session::backends::codex::events::*;
use std::sync::{atomic::Ordering, Arc};
pub(crate) fn terminal_tool_called_for_run(orch: &Orchestrator, run_id: &str) -> bool {
    orch.process_state
        .processes
        .lock()
        .ok()
        .and_then(|processes| {
            processes
                .get(run_id)
                .map(|process| process.terminal_tool_called.load(Ordering::Acquire))
        })
        .unwrap_or(false)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn store_usage_result_event(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    event_type: &str,
    is_error: bool,
    counts: TokenCounts,
    store_when_empty: bool,
) {
    let host = super::runtime::codex_runtime(
        super::super::transcript_host::SessionTranscriptHost {
            orch: orch.clone(),
            emitter: emitter.clone(),
            run_id: run_id.to_string(),
            session_id: session_id.map(str::to_string),
            run_db: run_db.clone(),
        },
        None,
        None,
        None,
    );
    cairn_session::backends::codex::events::store_usage_result_event(
        &host,
        run_id,
        session_id,
        event_type,
        is_error,
        counts,
        store_when_empty,
    );
}
