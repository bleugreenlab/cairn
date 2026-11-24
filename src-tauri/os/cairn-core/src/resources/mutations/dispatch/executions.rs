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

/// `place`, on a filing: `"head"` asks for the head of the issue's thread's
/// own queue; absent queues it behind what is already held.
pub(super) fn parse_place(
    index: usize,
    item: &ChangeItem,
    value: Option<&serde_json::Value>,
    key: &str,
) -> ResourceMutationResult<bool> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(value) if value.as_str() == Some("head") => Ok(true),
        Some(other) => Err(build_failure(
            index,
            item,
            format!(
                "{key} takes only \"head\" (file it at the head of its thread's own queue), not \
                 {other}; omit it to queue behind what is already held. Nothing changed."
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
            let head = parse_place(
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
            // its thread's focus cap and queue govern it. Only the trusted
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
            let effective_thread = parents
                .as_ref()
                .and_then(|parents| parents.thread.clone())
                .or_else(|| adopter.as_ref().map(|(_, name)| name.clone()));
            let mut place = file_place(
                orch,
                request,
                project,
                *number,
                head,
                effective_thread.as_deref(),
                !after.is_empty(),
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
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
                // The adoption moves the issue into the thread's focus count,
                // so the queue is asked as it will stand after it. A filing
                // with `after:` takes no place until its gates clear.
                let adopts = match &adopter {
                    Some((thread_id, name)) => {
                        let queue = if after.is_empty() {
                            executions::prospective_queue_line(orch, project, *number, thread_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        } else {
                            None
                        };
                        let queue = match queue {
                            Some(line) => format!(
                                " It would count against {name}'s focus cap, and the queue \
                                 would hold it now: {line}"
                            ),
                            None => format!(" It would count against {name}'s focus cap."),
                        };
                        format!(
                            "\nWould adopt {project}/{number} under thread {name} first: it has \
                             no parent.{queue}"
                        )
                    }
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
                // Adopt before the launch: admission reads the issue's thread
                // for its focus cap and queue, so the edge must be durable
                // before the start is decided.
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
                        // this write; that parent stands, and a head place is
                        // decided again against it.
                        crate::issues::crud::ThreadAdoption::AlreadyParented(_) => {
                            let current = super::threads::issue_parents(orch, project, *number)
                                .await
                                .map_err(|error| build_failure(index, item, error))?;
                            place = file_place(
                                orch,
                                request,
                                project,
                                *number,
                                head,
                                current.and_then(|parents| parents.thread).as_deref(),
                                !after.is_empty(),
                            )
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
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
                    place,
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

/// Where a filing queues: at the tail, or, for `place:"head"`, at the head of
/// `thread`'s own queue (the issue's parent thread, or the thread about to
/// adopt it), checked before anything is written.
async fn file_place(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    number: i32,
    head: bool,
    thread: Option<&str>,
    has_after: bool,
) -> Result<crate::execution::recipe::QueuePlace, String> {
    if !head {
        return Ok(crate::execution::recipe::QueuePlace::Tail);
    }
    let placed_by = super::threads::head_placed_by(
        orch,
        request,
        project,
        thread,
        &format!("{project}/{number}"),
        has_after,
    )
    .await?;
    Ok(crate::execution::recipe::QueuePlace::Head { placed_by })
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

/// The gate half of an execution patch: `{release: true}`, `{start_now: true}`
/// or `{after: [...]}`, one at a time, and never mixed with a snapshot edit or
/// a re-evaluation, so a write cannot half-apply.
fn parse_gate_patch(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Option<executions::GatePatch>> {
    let release = payload.get("release");
    let start_now = payload.get("start_now");
    let after = payload.get("after");
    if release.is_none() && start_now.is_none() && after.is_none() {
        return Ok(None);
    }
    let gate_keys = [release, start_now, after]
        .iter()
        .filter(|key| key.is_some())
        .count();
    if ["agent", "snapshot", "reevaluate"]
        .iter()
        .any(|key| payload.get(key).is_some())
        || gate_keys > 1
    {
        return Err(build_failure(
            index,
            item,
            "payload.release, payload.start_now and payload.after each stand alone: send \
             {release:true} to release its after: gates, {start_now:true} to start it past every \
             cap, or {after:[...]} to replace its gates, in separate changes",
        ));
    }
    if let Some(start_now) = start_now {
        return match start_now {
            serde_json::Value::Bool(true) => Ok(Some(executions::GatePatch::StartNow)),
            _ => Err(build_failure(
                index,
                item,
                "payload.start_now must be true; to keep an execution held, omit it",
            )),
        };
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
            json!({"cancel":true,"start_now":true}),
        ] {
            assert!(
                super::validate_cancel_payload(&payload).is_err(),
                "{payload}"
            );
        }
    }
}
