//! Label resource mutation dispatch, relocated from dispatch.rs.

use super::super::labels::{apply_label_create, apply_label_delete, apply_label_patch};
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
        (CairnResource::Labels, ChangeMode::Create) => {
            if dry_run {
                "Would create workspace label".to_string()
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_label_create(orch, payload)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Label { label_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch label '{label_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_label_patch(orch, payload, label_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Label { label_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete label '{label_id}'")
            } else {
                apply_label_delete(orch, label_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
