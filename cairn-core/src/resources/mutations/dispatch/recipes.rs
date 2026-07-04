//! Recipe resource mutation dispatch, relocated from dispatch.rs.

use super::super::recipes::{apply_recipe_create, apply_recipe_delete, apply_recipe_patch};
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
            (CairnResource::Recipes, ChangeMode::Create) => {
                if dry_run {
                    "Would create workspace recipe".to_string()
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_recipe_create(orch, request, payload, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectRecipes { project }, ChangeMode::Create) => {
                if dry_run {
                    format!("Would create project recipe in {project}")
                } else {
                    let payload = item.payload.as_ref().ok_or_else(|| {
                        build_failure(index, item, "mode=create requires payload")
                    })?;
                    apply_recipe_create(orch, request, payload, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Recipe { recipe_id }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch recipe '{recipe_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_recipe_patch(orch, request, payload, recipe_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectRecipe { project, recipe_id }, ChangeMode::Patch) => {
                if dry_run {
                    format!("Would patch project recipe '{project}/{recipe_id}'")
                } else {
                    let payload = item
                        .payload
                        .as_ref()
                        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                    apply_recipe_patch(orch, request, payload, recipe_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::Recipe { recipe_id }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete recipe '{recipe_id}'")
                } else {
                    apply_recipe_delete(orch, request, recipe_id, None)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            (CairnResource::ProjectRecipe { project, recipe_id }, ChangeMode::Delete) => {
                if dry_run {
                    format!("Would delete project recipe '{project}/{recipe_id}'")
                } else {
                    apply_recipe_delete(orch, request, recipe_id, Some(project))
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
            _ => return Ok(None),
        };
    Ok(Some(summary))
}
