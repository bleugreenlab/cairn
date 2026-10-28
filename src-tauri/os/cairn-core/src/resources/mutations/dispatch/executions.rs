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
    applied_data: &mut Option<serde_json::Value>,
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
            let after = parse_after_clauses(
                index,
                item,
                item.payload.as_ref().and_then(|p| p.get("after")),
                "payload.after",
            )?;
            let tenet = crate::tenets::filing::TenetFiling::from_payload(
                request.run_id.clone(),
                item.payload.as_ref().and_then(|p| p.as_object()),
                "payload",
            )
            .map_err(|error| build_failure(index, item, error))?;
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
                    "Would {} an execution for {project}/{number}{}",
                    if after.is_empty() {
                        "start"
                    } else {
                        "file a held"
                    },
                    recipe
                        .map(|r| format!(" (recipe '{r}')"))
                        .unwrap_or_default()
                )
            } else {
                let started = executions::start_execution_from_collection(
                    orch,
                    project,
                    *number,
                    recipe,
                    backend,
                    branch_target,
                    continuity,
                    overrides,
                    intent,
                    after,
                    tenet,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if let Some(tenet) = started.proposed_tenet {
                    *applied_data = Some(serde_json::json!({ "tenet": tenet }));
                }
                started.summary
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

            if let Some(patch) = parse_gate_patch(index, item, payload)? {
                let summary = executions::patch_execution_gates(
                    orch, request, project, *number, *exec_seq, patch, dry_run,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                return Ok(Some(summary));
            }

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

/// Parse an `after` array of gate clause objects, refusing prose, malformed
/// clauses, and unknown facts at write time with the corrected object. Shared by
/// both launch doors and the gate patch, so one grammar is refused one way.
pub(super) fn parse_after_clauses(
    index: usize,
    item: &ChangeItem,
    value: Option<&serde_json::Value>,
    key: &str,
) -> ResourceMutationResult<Vec<crate::routes::TriggerClause>> {
    let clauses = crate::execution::gates::authoring::parse_after(value, key)
        .map_err(|error| build_failure(index, item, error))?;
    crate::execution::gates::authoring::canonical_predicates(&clauses, key)
        .map_err(|error| build_failure(index, item, error))?;
    Ok(clauses)
}

/// The gate half of an execution patch: `{release: true}` or `{after: [...]}`,
/// one at a time, and never mixed with a snapshot edit or a re-evaluation, so a
/// write cannot half-apply.
fn parse_gate_patch(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Option<executions::GatePatch>> {
    let release = payload.get("release");
    let after = payload.get("after");
    if release.is_none() && after.is_none() {
        return Ok(None);
    }
    if ["agent", "snapshot", "reevaluate"]
        .iter()
        .any(|key| payload.get(key).is_some())
        || (release.is_some() && after.is_some())
    {
        return Err(build_failure(
            index,
            item,
            "payload.release and payload.after each stand alone: send {release:true} to start a \
             held execution now, or {after:[...]} to replace its gates, in separate changes",
        ));
    }
    if let Some(release) = release {
        return match release {
            serde_json::Value::Bool(true) => Ok(Some(executions::GatePatch::Release)),
            _ => Err(build_failure(
                index,
                item,
                "payload.release must be true; to keep an execution held, omit it",
            )),
        };
    }
    let after = match after {
        Some(serde_json::Value::Null) => Some(&serde_json::Value::Array(Vec::new())),
        other => other,
    };
    Ok(Some(executions::GatePatch::Replace(parse_after_clauses(
        index,
        item,
        after,
        "payload.after",
    )?)))
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
