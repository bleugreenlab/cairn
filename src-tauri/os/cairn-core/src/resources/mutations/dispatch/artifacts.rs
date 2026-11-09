//! Artifact resource mutation dispatch, relocated from dispatch.rs.

use super::super::{build_failure, mode_name, payload_str, ResourceMutationResult};
use crate::mcp::handlers::comments_artifacts;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_db::turso::params;
use cairn_uri::CairnResource;

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

/// Refuse a confirm addressed to an artifact name the node's own job never
/// wrote.
///
/// Approval resolves the node's gate whatever name the URI carries, and a
/// blocked job with no artifact is approved by minting a checkpoint for it
/// (`confirm_latest_artifact_conn`). So confirming a name the node does not
/// hold, such as a delegated task's `return` (which lives at the task's own
/// address), would advance the DAG past a node that produced nothing under that
/// name. The name matches the way read addresses are built: the stored
/// `output_name`, else the artifact type.
async fn require_own_artifact_chain(
    orch: &Orchestrator,
    job_id: &str,
    node_id: &str,
    name: &str,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let (owned_job_id, owned_name) = (job_id.to_string(), name.to_string());
    let held = db
        .read(move |conn| {
            let (job_id, name) = (owned_job_id.clone(), owned_name.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM artifacts
                          WHERE job_id = ?1
                            AND COALESCE(NULLIF(output_name, ''), artifact_type) = ?2
                          LIMIT 1",
                        params![job_id.as_str(), name.as_str()],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if held {
        Ok(())
    } else {
        Err(format!(
            "{node_id} has no `{name}` artifact of its own to confirm; a delegated task's output lives at its own `{node_id}/task/<task>` address and is never confirmed through its parent"
        ))
    }
}

/// `held_store` is set when the calling write holds its project's store lock;
/// a create-pr write then hands its pull-request sync back through
/// `deferred_pr_sync` instead of running it under the lock.
#[allow(clippy::too_many_arguments)]
pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    held_store: Option<&crate::orchestrator::JjStoreGuard>,
    deferred_pr_sync: &mut Option<crate::pr_data::actions::DeferredPrSync>,
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
            // A `patch` carrying the reserved `defer` key sets a pending gate
            // aside until a cause comes true, instead of editing artifact data.
            if matches!(mode, ChangeMode::Patch) && payload.get("defer").is_some() {
                let until = crate::execution::gates::ask_conditions::defer_until(payload)
                    .map_err(|error| build_failure(index, item, error))?;
                let job_id = crate::resources::resolve_node_or_task_job_id(
                    &orch.db.local,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    None,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let db = orch.db.for_project(project).await;
                let created_by = if request
                    .payload
                    .get("_cairn_origin")
                    .and_then(|v| v.as_str())
                    == Some("cli")
                {
                    Some("operator".to_string())
                } else {
                    crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
                        .await
                        .ok()
                };
                crate::execution::gates::ask_conditions::defer(
                    orch,
                    &db,
                    &job_id,
                    name.as_deref(),
                    until,
                    created_by,
                    dry_run,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            } else if matches!(mode, ChangeMode::Patch) && payload.get("action").is_some() {
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
                // The write preflight refuses this shape in a file batch before
                // anything applies; a caller that dispatches it directly under
                // a held store lock is refused here, before any effect.
                if held_store.is_some() {
                    return Err(build_failure(
                        index,
                        item,
                        "a pull-request action cannot run while this write holds the project's version-control store lock; send the artifact patch carrying `action` as its own write",
                    ));
                }
                let action = payload_str(payload, "action", &[]).ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.action must be a string (merge|close|refresh|publish)",
                    )
                })?;
                let precondition = crate::pr_data::actions::parse_merge_precondition(
                    payload.get("precondition"),
                    action,
                )
                .map_err(|error| build_failure(index, item, error))?;
                let job_id = crate::resources::resolve_node_or_task_job_id(
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
                            "artifact {node_id}/{artifact_label} has no PR yet; merge/close/refresh/publish require a merge_requests row for the producing job"
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
                            let gate = precondition
                                .map(|_| " (precondition=pinBump)")
                                .unwrap_or_default();
                            format!("Would merge PR for {node_id}/{artifact_label}{suffix}{gate}")
                        } else {
                            crate::pr_data::actions::merge_pr_for_job(
                                orch,
                                &job_id,
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
                            format!("Would close PR for {node_id}/{artifact_label}")
                        } else {
                            crate::pr_data::actions::close_pr_for_job(
                                orch,
                                &job_id,
                                resolution_attribution(orch, request).await,
                            )
                            .await
                            .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "refresh" => {
                        if dry_run {
                            format!("Would refresh PR for {node_id}/{artifact_label}")
                        } else {
                            let barrier =
                                crate::mcp::handlers::run::held::job_commit_barrier(orch, &job_id)
                                    .await;
                            let cache = crate::pr_data::actions::refresh_pr_for_job(orch, &job_id)
                                .await
                                .map_err(|error| {
                                    build_failure(index, item, barrier.prefix(error))
                                })?;
                            barrier.prefix(format!(
                                "Refreshed {} for {node_id}/{artifact_label}",
                                crate::pr_data::publication::refreshed_summary(&cache)
                            ))
                        }
                    }
                    "publish" => {
                        if dry_run {
                            format!("Would publish the PR branch for {node_id}/{artifact_label}")
                        } else {
                            crate::pr_data::actions::publish_pr_branch_for_job(orch, &job_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    other => {
                        return Err(build_failure(
                            index,
                            item,
                            format!(
                                "unknown PR action '{other}'; expected merge|close|refresh|publish"
                            ),
                        ))
                    }
                }
            } else if matches!(mode, ChangeMode::Patch) && payload.get("confirmed").is_some() {
                // A `patch` carrying the reserved `confirmed` key resolves the
                // user gate (confirm the latest artifact + advance the DAG)
                // instead of editing artifact data. `confirmed` is a column,
                // never a real artifact schema field, so treating it as
                // reserved can't collide with a data edit.
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
                // The confirmer is the run this request authenticated as. A
                // resource write never carries operator authority: the
                // operator confirms through the `approve_job` invoke.
                let confirmer = match request.run_id.as_deref().filter(|run| !run.is_empty()) {
                    Some(run_id) => {
                        crate::execution::checkpoints::Confirmer::AgentRun(run_id.to_string())
                    }
                    None => crate::execution::checkpoints::Confirmer::Unproven(
                        "resource write from outside a run",
                    ),
                };
                if dry_run {
                    let job_id = crate::resources::resolve_node_or_task_job_id_for_read(
                        &orch.db.local,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                        None,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    if let Some(name) = name.as_deref() {
                        require_own_artifact_chain(orch, &job_id, node_id, name)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                    }
                    crate::execution::checkpoints::check_confirmation(orch, &job_id, &confirmer)
                        .await
                        .map_err(|error| build_failure(index, item, error.to_string()))?;
                    format!("Would confirm artifact {node_id}/{artifact_label}")
                } else {
                    let job_id = crate::resources::resolve_node_or_task_job_id(
                        &orch.db.local,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                        None,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    if let Some(name) = name.as_deref() {
                        require_own_artifact_chain(orch, &job_id, node_id, name)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                    }
                    crate::execution::checkpoints::confirm_gated_artifact(
                        orch, &job_id, &confirmer,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                    format!("Confirmed artifact {node_id}/{artifact_label}; gate resolved")
                }
            } else if dry_run {
                comments_artifacts::preview_artifact_change(
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
                .map_err(|error| build_failure(index, item, error))?;
                format!(
                    "Would {} artifact {node_id}/{artifact_label}",
                    mode_name(mode)
                )
            } else {
                // Saving the PR artifact is a commit barrier on the branch it
                // presents: a commit an earlier batch sealed but could not
                // publish lands before the PR is opened or re-synced from it.
                // A write holding its store lock has already drained its
                // branch's held work under that lock and pushes the branch after
                // releasing it; the barrier's own push would need the lock.
                let barrier = if artifact_label == "create-pr" && held_store.is_none() {
                    match crate::resources::resolve_node_or_task_job_id(
                        &orch.db.local,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                        None,
                    )
                    .await
                    {
                        Ok(job_id) => Some(
                            crate::mcp::handlers::run::held::job_commit_barrier(orch, &job_id)
                                .await,
                        ),
                        Err(_) => None,
                    }
                } else {
                    None
                };
                let (summary, deferred) = comments_artifacts::write_artifact_change(
                    orch,
                    request,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    None,
                    name.as_deref(),
                    payload,
                    matches!(mode, ChangeMode::Patch),
                    held_store.is_some(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                *deferred_pr_sync = deferred;
                match barrier {
                    Some(barrier) => barrier.prefix(barrier.warn_if_held(summary)),
                    None => summary,
                }
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
                comments_artifacts::preview_artifact_change(
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
                .map_err(|error| build_failure(index, item, error))?;
                format!(
                    "Would {} artifact {}/task/{}/{}",
                    mode_name(mode),
                    node_id,
                    task_name,
                    name.as_deref().unwrap_or("artifact")
                )
            } else {
                let (summary, deferred) = comments_artifacts::write_artifact_change(
                    orch,
                    request,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    Some(task_name),
                    name.as_deref(),
                    payload,
                    matches!(mode, ChangeMode::Patch),
                    held_store.is_some(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                *deferred_pr_sync = deferred;
                summary
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
