//! Action resource mutation dispatch, relocated from dispatch.rs.

use super::super::actions::{apply_action_create, apply_action_delete, apply_action_patch};
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
    let summary =
        match (resource, item.mode) {
            (CairnResource::Actions, ChangeMode::Create) => {
                if dry_run {
                    "Would create workspace action".to_string()
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_action_create(orch, payload, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectActions { project }, ChangeMode::Create) => {
                if dry_run {
                    format!("Would create project action in {project}")
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_action_create(orch, payload, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Action { action_id }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch action '{action_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_action_patch(orch, payload, action_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectAction { project, action_id }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch project action '{project}/{action_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_action_patch(orch, payload, action_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Action { action_id }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete action '{action_id}'")
                } else {
                    apply_action_delete(orch, action_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectAction { project, action_id }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete project action '{project}/{action_id}'")
                } else {
                    apply_action_delete(orch, action_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            _ => return Ok(None),
        };
    Ok(Some(summary))
}
