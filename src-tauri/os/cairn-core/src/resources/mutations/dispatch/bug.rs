//! Bug report resource mutation dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::handlers::bug_report;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::Bug, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let title = payload_non_empty_str(payload, "title", &[])
                .ok_or_else(|| build_failure(index, item, "payload.title is required"))?;
            if dry_run {
                format!("Would submit bug report: {title}")
            } else {
                bug_report::submit_bug_report(orch, request, payload)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
