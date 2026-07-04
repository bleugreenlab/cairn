//! Execution collection/edit dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use crate::mcp::handlers::executions;
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
        (CairnResource::IssueExecutions { project, number }, ChangeMode::Append) => {
            let recipe = match item.payload.as_ref().and_then(|p| p.get("recipe")) {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.recipe must be a string")
                })?),
                None => None,
            };
            let backend = match item.payload.as_ref().and_then(|p| p.get("backend")) {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.backend must be a string")
                })?),
                None => None,
            };
            if dry_run {
                format!(
                    "Would start an execution for {project}-{number}{}",
                    recipe
                        .map(|r| format!(" (recipe '{r}')"))
                        .unwrap_or_default()
                )
            } else {
                executions::start_execution_from_collection(orch, project, *number, recipe, backend)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::IssueExecution {
                project,
                number,
                exec_seq,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let agent = payload_non_empty_str(payload, "agent", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.agent is required and must be a non-empty string",
                )
            })?;
            let snapshot_patch = payload
                .get("snapshot")
                .ok_or_else(|| build_failure(index, item, "payload.snapshot is required"))?
                .clone();
            executions::edit_execution_agent(
                orch,
                request,
                project,
                *number,
                *exec_seq,
                agent,
                snapshot_patch,
                dry_run,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
