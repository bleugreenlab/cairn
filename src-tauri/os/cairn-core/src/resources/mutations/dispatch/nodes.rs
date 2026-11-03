//! Bare-node patch dispatch for session control and PR publication/resolution.

use super::super::{build_failure, payload_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

async fn resolution_attribution(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<crate::pr_data::actions::PrResolutionAttribution> {
    if request
        .payload
        .get("_cairn_origin")
        .and_then(|v| v.as_str())
        == Some("cli")
    {
        return Some(crate::pr_data::actions::PrResolutionAttribution::operator_cli());
    }
    crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
        .await
        .ok()
        .map(crate::pr_data::actions::PrResolutionAttribution::agent)
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Patch,
        ) => {
            // A bare-node patch carries an `action`. `stop` interrupts the
            // node's active turn and parks the session warm (resumable, not a
            // kill), so it works on ANY running node and branches BEFORE the PR
            // gate. merge/close/refresh operate on the PR a `pr` action node
            // owns (the action analogue of the NodeArtifact action patch,
            // CAIRN-1222) and stay behind the merge_requests gate.
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            // The contract gate admits `executor` or `action`, never both.
            if let Some(value) = payload.get("executor") {
                return placement_default_patch(
                    orch,
                    request,
                    PlacementDefaultHome {
                        project,
                        number: *number,
                        exec_seq: *exec_seq,
                        node_id,
                        task_name: None,
                    },
                    value,
                    dry_run,
                )
                .await
                .map(Some)
                .map_err(|error| build_failure(index, item, error));
            }
            let action = payload_str(payload, "action", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.action must be a string (stop|retry|merge|close|refresh|publish)",
                )
            })?;
            let precondition = crate::pr_data::actions::parse_merge_precondition(
                payload.get("precondition"),
                action,
            )
            .map_err(|error| build_failure(index, item, error))?;
            let owner_id = crate::resources::resolve_node_owner_id(
                &orch.db.local,
                project,
                *number,
                *exec_seq,
                node_id,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if action == "stop" {
                // Interrupt the node's live run. `stop_session` cascades to
                // child runs and parks the session warm rather than killing it,
                // so the node can be resumed by a later message.
                match crate::orchestrator::lifecycle::live_run_id_for_job(orch, &owner_id) {
                    Some(run_id) => {
                        if dry_run {
                            format!(
                                "Would stop {node_id}: interrupt run {run_id}'s active turn and park the session warm (resumable)"
                            )
                        } else {
                            crate::orchestrator::lifecycle::stop_session(orch, &run_id)
                                .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Stopped {node_id}: interrupted run {run_id}'s active turn and parked the session warm (resumable; cascades to child runs)"
                            )
                        }
                    }
                    None => {
                        // No live run attached. A job can still be non-terminal yet
                        // runless when it suspended on a foreground question or an
                        // inline delegated task and its run finalized (the
                        // OpenRouter owned loop keeps no warm process). Idle it at
                        // the job level (CAIRN-1907): cancel the open prompt, drop
                        // the pending successor, cascade-stop children, and recompute
                        // to a steerable state. A genuinely terminal job has nothing
                        // to stop.
                        let job = crate::jobs::queries::get_job(&orch.db.local, &owner_id)
                            .await
                            .map_err(|error| build_failure(index, item, error.to_string()))?;
                        if job.status.is_terminal() {
                            format!("node {node_id} has no active run to stop")
                        } else if dry_run {
                            format!(
                                "Would stop {node_id}: cancel its pending prompt, drop the pending successor, stop child runs, and idle the job (no live run attached)"
                            )
                        } else {
                            crate::orchestrator::lifecycle::stop_job(orch, &owner_id)
                                .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Stopped {node_id}: cancelled pending input, stopped child runs, and idled the job (steerable for a follow-up)"
                            )
                        }
                    }
                }
            } else if action == "retry" {
                // A failed publication may have no merge request yet. Validate
                // the PR node itself before the published-PR resolution gate.
                crate::execution::advancement::validate_pr_publish_retry(orch, &owner_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                if dry_run {
                    format!("Would retry PR publication for {node_id} in place")
                } else {
                    crate::execution::advancement::retry_pr_publish(orch, &owner_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    format!("Queued PR publication retry for {node_id} on the same node")
                }
            } else {
                let mr_context = crate::pr_data::actions::try_resolve_mr_context_for_job(
                    &orch.db.local,
                    &owner_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let Some(mr_context) = mr_context else {
                    return Err(build_failure(
                        index,
                        item,
                        format!(
                            "node {node_id} has no PR yet; merge/close/refresh/publish require a merge_requests row for the node"
                        ),
                    ));
                };
                // Drive merge/close/refresh through the PR's durable producing
                // job id (`merge_requests.job_id`). For first-class `pr` nodes,
                // action-run completion and port firing resolve back through
                // action_runs.parent_job_id. Shadowing keeps the match arms below
                // unchanged.
                let owner_id = mr_context.job_id;
                match action {
                    "merge" => {
                        let method =
                            payload_str(payload, "method", &[]).map(|value| value.to_string());
                        if dry_run {
                            let suffix = method
                                .as_deref()
                                .map(|m| format!(" (method={m})"))
                                .unwrap_or_default();
                            let gate = precondition
                                .map(|_| " (precondition=pinBump)")
                                .unwrap_or_default();
                            format!("Would merge PR for {node_id}{suffix}{gate}")
                        } else {
                            crate::pr_data::actions::merge_pr_for_job(
                                orch,
                                &owner_id,
                                method,
                                resolution_attribution(orch, request).await,
                                precondition,
                            )
                            .await
                            .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "close" => {
                        if dry_run {
                            format!("Would close PR for {node_id}")
                        } else {
                            crate::pr_data::actions::close_pr_for_job(
                                orch,
                                &owner_id,
                                resolution_attribution(orch, request).await,
                            )
                            .await
                            .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "refresh" => {
                        if dry_run {
                            format!("Would refresh PR for {node_id}")
                        } else {
                            let barrier =
                                crate::mcp::handlers::run::held::job_commit_barrier(orch, &owner_id)
                                    .await;
                            let cache =
                                crate::pr_data::actions::refresh_pr_for_job(orch, &owner_id)
                                    .await
                                    .map_err(|error| {
                                        build_failure(index, item, barrier.prefix(error))
                                    })?;
                            barrier.prefix(format!(
                                "Refreshed {} for {node_id}",
                                crate::pr_data::publication::refreshed_summary(&cache)
                            ))
                        }
                    }
                    "publish" => {
                        if dry_run {
                            format!("Would publish the PR branch for {node_id}")
                        } else {
                            crate::pr_data::actions::publish_pr_branch_for_job(orch, &owner_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    other => {
                        return Err(build_failure(
                            index,
                            item,
                            format!(
                                "unknown node action '{other}'; expected stop|retry|merge|close|refresh|publish"
                            ),
                        ))
                    }
                }
            }
        }
        (
            CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            ChangeMode::Patch,
        ) => {
            let value = item
                .payload
                .as_ref()
                .and_then(|payload| payload.get("executor"))
                .ok_or_else(|| build_failure(index, item, "payload.executor is required"))?;
            placement_default_patch(
                orch,
                request,
                PlacementDefaultHome {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: Some(task_name),
                },
                value,
                dry_run,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

/// The node or task coordinate an `executor` patch addresses.
struct PlacementDefaultHome<'a> {
    project: &'a str,
    number: i32,
    exec_seq: i32,
    node_id: &'a str,
    task_name: Option<&'a str>,
}

/// Set or clear the placement default of the job a node or task URI names.
/// A thread's own session is keyed by its thread, which `DefaultOwner` decides
/// from the job, so a thread addressed through its reserved node coordinate
/// lands on the same row as one addressed by name.
async fn placement_default_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    home: PlacementDefaultHome<'_>,
    value: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    use crate::fleet::placement_default::{apply_patch, DefaultOwner};
    let db = orch.db.for_project(home.project).await;
    let job_id = crate::resources::resolve_node_or_task_job_id(
        &db,
        home.project,
        home.number,
        home.exec_seq,
        home.node_id,
        home.task_name,
    )
    .await?;
    let owner = DefaultOwner::for_job(&db, &job_id).await?;
    apply_patch(orch, request, &db, &owner, value, dry_run).await
}
