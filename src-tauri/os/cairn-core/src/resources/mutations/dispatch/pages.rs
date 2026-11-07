//! Page writes: `create` and `patch` of `.../pages/<name>`.
//!
//! Routed to the page store rather than the generic artifact path: a page has
//! no terminal contract, so its write gates nothing, interrupts no run, and
//! advances no DAG.

use super::super::{build_failure, mode_name, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::NodePage { .. }, mode @ (ChangeMode::Create | ChangeMode::Patch)) =
        (resource, item.mode)
    else {
        return Ok(None);
    };
    let payload = item.payload.as_ref().ok_or_else(|| {
        build_failure(
            index,
            item,
            format!("mode={} requires a page payload", mode_name(mode)),
        )
    })?;
    let address =
        crate::pages::PageAddress::from_resource(resource).expect("a NodePage names a page");
    // Pages are written by their owning thread, so the writer is always a run
    // Cairn can resolve. A request with no run identity is refused rather than
    // read as the operator: it is indistinguishable from an agent shell that
    // dropped its own.
    if request.run_id.as_deref().is_none_or(str::is_empty) {
        return Err(build_failure(
            index,
            item,
            "Pages are written by their owning thread: this request carries no run identity, \
             and a run-less write cannot be told apart from an agent that dropped its own.",
        ));
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            build_failure(
                index,
                item,
                format!("A page write must come from a run Cairn can resolve: {error}"),
            )
        })?;
    let writer = crate::pages::PageWriter {
        job_id: agent.job_id,
    };
    crate::pages::write_page(
        orch,
        &address,
        &writer,
        payload,
        matches!(mode, ChangeMode::Patch),
        dry_run,
    )
    .await
    .map(Some)
    .map_err(|error| build_failure(index, item, error))
}
