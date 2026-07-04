//! Workspace settings resource mutation dispatch, relocated from dispatch.rs.

use super::super::settings::apply_settings_patch;
use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::Settings, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            apply_settings_patch(orch, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
