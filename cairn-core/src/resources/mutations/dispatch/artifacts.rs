//! Artifact resource mutation dispatch, relocated from dispatch.rs.

use super::super::{build_failure, mode_name, payload_str, ResourceMutationResult};
use crate::mcp::handlers::comments_artifacts;
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
            CairnResource::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
                name,
            },
            mode @ (ChangeMode::Create | ChangeMode::Patch),
        ) => {
            let payload = item.payload.as_ref().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires an artifact payload", mode_name(mode)),
                )
            })?;
            let artifact_label = name.as_deref().unwrap_or("artifact");
            // A `patch` carrying the reserved `confirmed` key resolves the user
            // gate (confirm the latest artifact + advance the DAG) instead of
            // editing artifact data. `confirmed` is a column, never a real
            // artifact schema field, so treating it as reserved can't collide
            // with a data edit.
            if matches!(mode, ChangeMode::Patch) && payload.get("action").is_some() {
                // A `patch` carrying the reserved `action` key operates on the
                // PR that this artifact's job produced (merge/close/refresh),
                // rather than editing artifact data. PR-ness is detected at
                // runtime by a merge_requests row for the job — no new URI or
                // ChangeMode. `action` and `confirmed` are mutually exclusive.
                if payload.get("confirmed").is_some() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.action and payload.confirmed are mutually exclusive",
                    ));
                }
                let action = payload_str(payload, "action", &[]).ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.action must be a string (merge|close|refresh)",
                    )
                })?;
                let job_id = crate::resources::resolve_todos_job_id(
                    &orch.db.local,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    None,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let mr_context = crate::pr_data::actions::try_resolve_mr_context_for_job(
                    &orch.db.local,
                    &job_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let Some(mr_context) = mr_context else {
                    return Err(build_failure(
                        index,
                        item,
                        format!(
                            "artifact {node_id}/{artifact_label} has no PR yet; merge/close/refresh require a merge_requests row for the producing job"
                        ),
                    ));
                };
                // Merge/close/refresh through the PR's durable producing job id
                // (`merge_requests.job_id`). A build recipe opens the child PR via
                // a `pr` action_run, but the persisted owner is the builder job;
                // PR action-run completion resolves through parent_job_id. Shadow
                // to keep the arms below.
                let job_id = mr_context.job_id;
                match action {
                    "merge" => {
                        let method =
                            payload_str(payload, "method", &[]).map(|value| value.to_string());
                        if dry_run {
                            let suffix = method
                                .as_deref()
                                .map(|m| format!(" (method={m})"))
                                .unwrap_or_default();
                            format!("Would merge PR for {node_id}/{artifact_label}{suffix}")
                        } else {
                            crate::pr_data::actions::merge_pr_for_job(orch, &job_id, method)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "close" => {
                        if dry_run {
                            format!("Would close PR for {node_id}/{artifact_label}")
                        } else {
                            crate::pr_data::actions::close_pr_for_job(orch, &job_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "refresh" => {
                        if dry_run {
                            format!("Would refresh PR for {node_id}/{artifact_label}")
                        } else {
                            let cache = crate::pr_data::actions::refresh_pr_for_job(orch, &job_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Refreshed PR #{} for {node_id}/{artifact_label} (state {}, +{} -{})",
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
                            format!("unknown PR action '{other}'; expected merge|close|refresh"),
                        ))
                    }
                }
            } else if matches!(mode, ChangeMode::Patch) && payload.get("confirmed").is_some() {
                let confirmed = payload
                    .get("confirmed")
                    .and_then(|value| value.as_bool())
                    .ok_or_else(|| {
                        build_failure(index, item, "payload.confirmed must be a boolean")
                    })?;
                if !confirmed {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.confirmed must be true to confirm a gated artifact; there is no 'unconfirm' (omit the key to edit artifact data)",
                    ));
                }
                if dry_run {
                    format!("Would confirm artifact {node_id}/{artifact_label}")
                } else {
                    let job_id = crate::resources::resolve_todos_job_id(
                        &orch.db.local,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                        None,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    crate::execution::checkpoints::approve_job_inner(orch, &job_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    format!("Confirmed artifact {node_id}/{artifact_label}; gate resolved")
                }
            } else if dry_run {
                format!(
                    "Would {} artifact {node_id}/{artifact_label}",
                    mode_name(mode)
                )
            } else {
                comments_artifacts::write_artifact_change(
                    orch,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    None,
                    name.as_deref(),
                    payload,
                    matches!(mode, ChangeMode::Patch),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                name,
            },
            mode @ (ChangeMode::Create | ChangeMode::Patch),
        ) => {
            let payload = item.payload.as_ref().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires an artifact payload", mode_name(mode)),
                )
            })?;
            if dry_run {
                format!(
                    "Would {} artifact {}/task/{}/{}",
                    mode_name(mode),
                    node_id,
                    task_name,
                    name.as_deref().unwrap_or("artifact")
                )
            } else {
                comments_artifacts::write_artifact_change(
                    orch,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    Some(task_name),
                    name.as_deref(),
                    payload,
                    matches!(mode, ChangeMode::Patch),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
