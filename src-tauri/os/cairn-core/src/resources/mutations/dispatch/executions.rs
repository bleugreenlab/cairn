//! Execution collection/edit dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use crate::mcp::handlers::executions;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;

/// The refusal for an execution start that still names an execution-wide
/// provider: each node selects within its own allotment, so the way to run an
/// agent on one provider is to name that provider's model for it.
pub(super) const RETIRED_EXECUTION_BACKEND: &str = "payload.backend was retired: each node now selects a model within its allotment. To run an agent on one provider, name its model: overrides.agents.<agent>.models: [provider/model]; see docs/backends.md#model-selection";

/// `place` on a filing is gone: every filing joins the end of the run queue,
/// and the queue's own `move` is the one way to reorder. Refused by name so
/// a caller using the old key learns where ordering lives now.
pub(super) fn refuse_place(
    index: usize,
    item: &ChangeItem,
    value: Option<&serde_json::Value>,
    key: &str,
) -> ResourceMutationResult<()> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(()),
        Some(_) => Err(build_failure(
            index,
            item,
            format!(
                "{key} was retired: a filing joins the end of the run queue. Reorder it with \
                 cairn:@/queue {{move:{{execution:ID,position:N}}}}. Nothing changed."
            ),
        )),
    }
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
        (CairnResource::IssueExecutions { project, number }, ChangeMode::Append) => {
            let recipe = match item.payload.as_ref().and_then(|p| p.get("recipe")) {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.recipe must be a string")
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
            refuse_place(
                index,
                item,
                item.payload.as_ref().and_then(|p| p.get("place")),
                "payload.place",
            )?;
            // Before adoption, resolution or any write, and on a preview too:
            // the release predicate never starts an execution on a merged or
            // closed issue, so accepting one would be a false `Queued`.
            executions::refuse_filing_on_resolved_issue(orch, project, *number)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            // A same-project thread filing work on an issue that hangs from
            // nothing adopts it, so the issue's attention (its plan gates, its
            // reviews, its failure) reaches the thread that asked for it, and
            // it takes its place in that thread's slice of the run queue. Only the trusted
            // acting thread qualifies; an issue with either kind of parent,
            // and every start that is not a thread's, is left as it is.
            let parents = super::threads::issue_parents(orch, project, *number)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let adopter = match &parents {
                Some(parents) if parents.is_unparented() => {
                    super::threads::acting_thread_in_project(orch, request, project).await
                }
                _ => None,
            };
            // With no recipe named, Cairn chooses; either way the recipe is
            // resolved before anything is written, so a refusal files nothing.
            let creator =
                crate::execution::recipe_resolution::ResolutionCreator::of_request(orch, request)
                    .await;
            let mut filing = executions::resolve_filing(
                orch,
                project,
                *number,
                recipe,
                creator,
                item.payload
                    .as_ref()
                    .and_then(|p| p.get("overrides"))
                    .cloned(),
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            filing.principal = crate::execution::gates::cancellation::filing_actor(orch, request)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            // An adoption outlives the launch, so a filing that would adopt is
            // refused here, before the edge is written, by every check the
            // launch would make of its configuration.
            if adopter.is_some() {
                executions::check_filing(
                    orch,
                    project,
                    *number,
                    &filing,
                    overrides.as_ref(),
                    branch_target,
                    continuity.clone(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
            }
            if dry_run {
                let adopts = match &adopter {
                    Some((_, name)) => format!(
                        "\nWould adopt {project}/{number} under thread {name} first: it has no \
                         parent."
                    ),
                    None => String::new(),
                };
                format!(
                    "Would {} an execution for {project}/{number}{adopts}\n{}",
                    if after.is_empty() {
                        "start"
                    } else {
                        "file a held"
                    },
                    filing.resolution.line()
                )
            } else {
                // Adopt before the launch: the run queue reads the issue's
                // thread for its slice, so the edge must be durable before the
                // execution joins the queue.
                let mut adopted = None;
                let mut adoption_note = String::new();
                if let Some((thread_id, thread_name)) = &adopter {
                    match crate::mcp::handlers::issues::adopt_unparented_issue_into_thread(
                        orch, project, *number, thread_id,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?
                    {
                        crate::issues::crud::ThreadAdoption::Adopted(_) => {
                            adoption_note = format!(
                                "\nAdopted {project}/{number} under thread {thread_name}: it had no \
                                 parent, so its attention (plan gates, reviews, failures) now \
                                 reaches {thread_name}. Patch cairn://p/{project}/{number} \
                                 {{parent:...}} to move it."
                            );
                            adopted = Some(thread_name.clone());
                        }
                        // Someone gave it a parent between the read above and
                        // this write; that parent stands.
                        crate::issues::crud::ThreadAdoption::AlreadyParented(_) => {
                            adoption_note = format!(
                                "\n{project}/{number} gained a parent before {thread_name} could \
                                 adopt it; that parent stands."
                            );
                        }
                    }
                }
                let started = executions::start_execution_from_collection(
                    orch,
                    project,
                    *number,
                    filing,
                    branch_target,
                    continuity,
                    overrides,
                    intent,
                    after,
                )
                .await;
                match started {
                    Ok(summary) => format!("{summary}{adoption_note}"),
                    // The adoption records the thread's demonstrated ownership
                    // and stays: undoing it unconditionally could erase a
                    // parent someone set since, so the failure says it held.
                    Err(error) => {
                        let error = match adopted {
                            Some(thread_name) => format!(
                                "{error}\n{project}/{number} was adopted under thread \
                                 {thread_name} before the start failed and keeps that parent; \
                                 retrying adopts nothing twice."
                            ),
                            None => error,
                        };
                        return Err(build_failure(index, item, error));
                    }
                }
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

            if payload.get("cancel").is_some() {
                validate_cancel_payload(payload)
                    .map_err(|error| build_failure(index, item, error))?;
                let summary = executions::cancel_execution(
                    orch, request, project, *number, *exec_seq, dry_run,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                return Ok(Some(summary));
            }

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

/// The gate half of an execution patch: `{release: true}`
/// or `{after: [...]}`, one at a time, and never mixed with a snapshot edit or
/// a re-evaluation, so a write cannot half-apply.
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
    let gate_keys = [release, after].iter().filter(|key| key.is_some()).count();
    if ["agent", "snapshot", "reevaluate"]
        .iter()
        .any(|key| payload.get(key).is_some())
        || gate_keys > 1
    {
        return Err(build_failure(
            index,
            item,
            "payload.release and payload.after each stand alone; send {release:true} or {after:[...]} in separate changes",
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

fn validate_cancel_payload(payload: &serde_json::Value) -> Result<(), String> {
    match payload.as_object() {
        Some(fields) if fields.len() == 1 && fields.get("cancel") == Some(&serde_json::Value::Bool(true)) => Ok(()),
        _ => Err("Withdrawal takes only standalone {cancel:true}; false, malformed values and mixed actions are refused. Nothing changed.".into()),
    }
}

#[cfg(test)]
mod cancellation_payload_tests {
    #[test]
    fn cancellation_is_one_explicit_action() {
        use serde_json::json;
        assert!(super::validate_cancel_payload(&json!({"cancel": true})).is_ok());
        for payload in [
            json!({"cancel":false}),
            json!({"cancel":null}),
            json!({"cancel":"true"}),
            json!({"cancel":true,"after":[]}),
            json!({"cancel":true,"agent":"builder"}),
            json!({"cancel":true,"release":true}),
        ] {
            assert!(
                super::validate_cancel_payload(&payload).is_err(),
                "{payload}"
            );
        }
    }
}
