//! Project, project-settings, and project-reference mutation dispatch, relocated from dispatch.rs.

use super::super::projects::{
    apply_project_patch, apply_project_reference_create, apply_project_reference_delete,
    apply_project_reference_patch, apply_project_settings_patch, apply_projects_create,
};
use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
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
            (CairnResource::ProjectReferences { project }, ChangeMode::Create) => {
                if dry_run {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    let name = payload_non_empty_str(payload, "name", &[])
                        .ok_or_else(|| build_failure(index, item, "payload.name is required"))?;
                    format!("Would create project reference '{project}/{name}'")
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_project_reference_create(orch, project, payload, false)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectReference { project, name }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch project reference '{project}/{name}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_project_reference_patch(orch, project, name, payload, false)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectReference { project, name }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete project reference '{project}/{name}'")
                } else {
                    apply_project_reference_delete(orch, project, name, false)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Projects, ChangeMode::Create) => {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_projects_create(orch, payload, dry_run)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
            (CairnResource::Project { project }, ChangeMode::Patch) => {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_project_patch(orch, project, payload, dry_run)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
            (CairnResource::ProjectSettings { project }, ChangeMode::Patch) => {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_project_settings_patch(orch, project, payload, dry_run)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
            _ => return Ok(None),
        };
    Ok(Some(summary))
}
