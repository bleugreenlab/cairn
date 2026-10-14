//! Stopping a model session through the cairn-llm facet.
//!
//! The one write the facet's read side accepts. It is the node stop reached by
//! another address: the session resolves to its live run, and that run goes
//! through the same `stop_session` a node's `action:"stop"` does, so the turn
//! is interrupted, the session parks warm and resumable, and child runs stop
//! with it.

use super::super::{build_failure, payload_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;
use cairn_llm::interface::Llm;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::LlmSession { id }, ChangeMode::Patch) = (resource, item.mode) else {
        return Ok(None);
    };
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
    let action = payload_str(payload, "action", &[])
        .ok_or_else(|| build_failure(index, item, "payload.action must be the string \"stop\""))?;
    if action != "stop" {
        return Err(build_failure(
            index,
            item,
            format!("payload.action '{action}' is not supported; stop is the only action a session accepts"),
        ));
    }
    let Some(session) = orch
        .sessions()
        .into_iter()
        .find(|session| session.is_addressed_by(id))
    else {
        return Err(build_failure(
            index,
            item,
            format!("No live session `{id}` on this host. Live sessions are listed at cairn://llm/sessions."),
        ));
    };
    let key = session.key().to_string();
    let run_id = session.run_id;
    if dry_run {
        return Ok(Some(format!(
            "Would stop session {key}: interrupt run {run_id}'s active turn and park the session warm (resumable)"
        )));
    }
    crate::orchestrator::lifecycle::stop_session(orch, &run_id)
        .map_err(|error| build_failure(index, item, error))?;
    Ok(Some(format!(
        "Stopped session {key}: interrupted run {run_id}'s active turn and parked the session warm (resumable; cascades to child runs)"
    )))
}
