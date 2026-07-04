//! Agent resource mutation dispatch, relocated from dispatch.rs.

use super::super::agents::{apply_agent_create, apply_agent_delete, apply_agent_patch};
use super::super::{build_failure, ResourceMutationResult};
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
    let summary =
        match (resource, item.mode) {
            (CairnResource::Agents, ChangeMode::Create) => {
                if dry_run {
                    "Would create workspace agent".to_string()
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_agent_create(orch, request, payload, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectAgents { project }, ChangeMode::Create) => {
                if dry_run {
                    format!("Would create project agent in {project}")
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_agent_create(orch, request, payload, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Agent { agent_id }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch agent '{agent_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_agent_patch(orch, request, payload, agent_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectAgent { project, agent_id }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch project agent '{project}/{agent_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_agent_patch(orch, request, payload, agent_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Agent { agent_id }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete agent '{agent_id}'")
                } else {
                    apply_agent_delete(orch, request, agent_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectAgent { project, agent_id }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete project agent '{project}/{agent_id}'")
                } else {
                    apply_agent_delete(orch, request, agent_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            _ => return Ok(None),
        };
    Ok(Some(summary))
}
