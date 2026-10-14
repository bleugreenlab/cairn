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
            let branch_target = match item.payload.as_ref().and_then(|p| p.get("branch")) {
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(index, item, "payload.branch must be a string")
                    })?;
                    Some(
                        raw.parse::<crate::models::BranchTarget>()
                            .map_err(|error| {
                                build_failure(
                                    index,
                                    item,
                                    format!("payload.branch: {error} (new|base)"),
                                )
                            })?,
                    )
                }
                None => None,
            };
            let continuity = match item.payload.as_ref().and_then(|p| p.get("continuity")) {
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(index, item, "payload.continuity must be a string")
                    })?;
                    Some(raw.parse::<crate::models::Continuity>().map_err(|error| {
                        build_failure(
                            index,
                            item,
                            format!("payload.continuity: {error} (auto|clean|<coordinate>)"),
                        )
                    })?)
                }
                None => None,
            };
            let overrides = parse_launch_overrides(index, item, item.payload.as_ref())?;
            // CAIRN-4665: the default is the issue's next ATTEMPT, which
            // supersedes whichever execution owns the issue now. A caller that
            // wants a second opinion running beside the canonical one must
            // declare it, and gets an execution that never claims to be the
            // issue's implementation and is never fenced by a successor.
            let intent = match item.payload.as_ref().and_then(|p| p.get("lifecycle")) {
                Some(value) => match value.as_str() {
                    Some("parallel") => crate::execution::recipe::LaunchIntent::ParallelAlternative,
                    Some("canonical") => crate::execution::recipe::LaunchIntent::NewAttempt,
                    _ => {
                        return Err(build_failure(
                            index,
                            item,
                            "payload.lifecycle must be 'canonical' (the issue's next attempt, \
                             superseding the current one) or 'parallel' (a declared alternative)",
                        ))
                    }
                },
                None => crate::execution::recipe::LaunchIntent::NewAttempt,
            };
            if dry_run {
                format!(
                    "Would start an execution for {project}/{number}{}",
                    recipe
                        .map(|r| format!(" (recipe '{r}')"))
                        .unwrap_or_default()
                )
            } else {
                executions::start_execution_from_collection(
                    orch,
                    project,
                    *number,
                    recipe,
                    backend,
                    branch_target,
                    continuity,
                    overrides,
                    intent,
                )
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

            if payload.get("reevaluate").is_some() {
                let node_id =
                    payload_non_empty_str(payload, "reevaluate", &[]).ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            "payload.reevaluate must be the id of a blocked condition node",
                        )
                    })?;
                let summary = executions::reevaluate_execution_condition(
                    orch, project, *number, *exec_seq, node_id, dry_run,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                return Ok(Some(summary));
            }

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

/// Parse the optional `overrides` object shared by both launch doors: an append
/// to an issue's executions collection and the `execution` block on an
/// issue-create. Every refusal the grammar can raise lands here, at write time,
/// before an execution row exists — an override that cannot be honoured must not
/// become a run the caller then has to stop.
pub(super) fn parse_launch_overrides(
    index: usize,
    item: &ChangeItem,
    payload: Option<&serde_json::Value>,
) -> ResourceMutationResult<Option<crate::models::LaunchDeltas>> {
    let Some(value) = payload
        .and_then(|payload| payload.get("overrides"))
        .filter(|value| !value.is_null())
    else {
        return Ok(None);
    };
    let deltas = crate::models::LaunchDeltas::parse(value)
        .map_err(|error| build_failure(index, item, format!("payload.{error}")))?;
    Ok((!deltas.is_empty()).then_some(deltas))
}
