//! Skill resource mutation dispatch, relocated from dispatch.rs.

use super::super::skills::{apply_skill_create, apply_skill_delete, apply_skill_patch};
use super::super::{
    build_failure, payload_non_empty_str, payload_str, payload_trimmed_non_empty_str,
    ResourceMutationResult,
};
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
            (CairnResource::Skills, ChangeMode::Create) => {
                if dry_run {
                    preview_skill_create(index, item, "workspace")?
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_skill_create(orch, request, payload, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectSkills { project }, ChangeMode::Create) => {
                if dry_run {
                    preview_skill_create(index, item, project)?
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_skill_create(orch, request, payload, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Skill { skill_id, path }, ChangeMode::Patch) => {
                require_skill_root(index, item, path)?;
                if dry_run {
                    format!("Would patch skill '{skill_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_skill_patch(orch, request, payload, skill_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (
                CairnResource::ProjectSkill {
                    project,
                    skill_id,
                    path,
                },
                ChangeMode::Patch,
            ) => {
                require_skill_root(index, item, path)?;
                if dry_run {
                    format!("Would patch project skill '{project}/{skill_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_skill_patch(orch, request, payload, skill_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Skill { skill_id, path }, ChangeMode::Delete) => {
                require_skill_root(index, item, path)?;
                if dry_run {
                    format!("Would delete skill '{skill_id}'")
                } else {
                    apply_skill_delete(orch, request, item.payload.as_ref(), skill_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (
                CairnResource::ProjectSkill {
                    project,
                    skill_id,
                    path,
                },
                ChangeMode::Delete,
            ) => {
                require_skill_root(index, item, path)?;
                if dry_run {
                    format!("Would delete project skill '{project}/{skill_id}'")
                } else {
                    apply_skill_delete(
                        orch,
                        request,
                        item.payload.as_ref(),
                        skill_id,
                        Some(project),
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?
                }
            }
            _ => return Ok(None),
        };
    Ok(Some(summary))
}

/// Reject skill mutations that target a package sub-path (only the skill root is mutable).
fn require_skill_root(
    index: usize,
    item: &ChangeItem,
    path: &[String],
) -> ResourceMutationResult<()> {
    if path.is_empty() {
        Ok(())
    } else {
        Err(build_failure(
            index,
            item,
            "Skill mutations target the skill root (cairn://skills/ID); authoring package files is not supported",
        ))
    }
}

fn preview_skill_create(
    index: usize,
    item: &ChangeItem,
    scope: &str,
) -> ResourceMutationResult<String> {
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
    let name = payload_trimmed_non_empty_str(payload, "name", &[]).ok_or_else(|| {
        build_failure(
            index,
            item,
            "payload.name is required and must be non-empty",
        )
    })?;
    if payload_non_empty_str(payload, "description", &[]).is_none() {
        return Err(build_failure(
            index,
            item,
            "payload.description is required and must be non-empty",
        ));
    }
    if payload_str(payload, "prompt", &[]).is_none() {
        return Err(build_failure(index, item, "payload.prompt is required"));
    }
    Ok(format!("Would create {scope} skill '{name}'"))
}
