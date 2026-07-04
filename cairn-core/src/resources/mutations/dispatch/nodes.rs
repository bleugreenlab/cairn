//! Bare-node patch (stop/merge/close/refresh) dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_str, ResourceMutationResult};
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
            let action = payload_str(payload, "action", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.action must be a string (stop|merge|close|refresh)",
                )
            })?;
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
                            "node {node_id} has no PR yet; merge/close/refresh require a merge_requests row for the node"
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
                            format!("Would merge PR for {node_id}{suffix}")
                        } else {
                            crate::pr_data::actions::merge_pr_for_job(orch, &owner_id, method)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "close" => {
                        if dry_run {
                            format!("Would close PR for {node_id}")
                        } else {
                            crate::pr_data::actions::close_pr_for_job(orch, &owner_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "refresh" => {
                        if dry_run {
                            format!("Would refresh PR for {node_id}")
                        } else {
                            let cache =
                                crate::pr_data::actions::refresh_pr_for_job(orch, &owner_id)
                                    .await
                                    .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Refreshed PR #{} for {node_id} (state {}, +{} -{})",
                                cache.pr_number,
                                cache.state,
                                cache.additions.unwrap_or(0),
                                cache.deletions.unwrap_or(0)
                            )
                        }
                    }
                    other => {
                        return Err(build_failure(
                            index,
                            item,
                            format!(
                                "unknown node action '{other}'; expected stop|merge|close|refresh"
                            ),
                        ))
                    }
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
