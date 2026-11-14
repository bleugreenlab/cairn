//! Question / permission / task-append prompt dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, payload_str, ResourceMutationResult};
use crate::mcp::handlers::permission::{
    AnswerSurface, PermissionAnswer, PermissionDecision, PermissionScope, ResolveOutcome,
};
use crate::mcp::handlers::planning;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityLifetimeKind;
use cairn_uri::CairnResource;

/// Parse a permission answer payload into the decision plus both lifetime
/// concepts.
///
/// One resource addresses three kinds of pending request — a fence crossing, a
/// legacy tool prompt, and an authority request — so the payload carries both
/// lifetime keys and the resolver applies whichever the stored request actually
/// is. They are separate keys, not one merged field, because they mean
/// genuinely different things: `scope` reuses a concrete containment exception
/// for this process, `lifetime` mints a journaled, revocable authority grant.
///
/// This resource is agent-reachable, so the answer it builds carries no
/// operator capability. `lifetime` is still parsed and still validated here — a
/// nonsense value is a payload error whoever sent it — but on an authority
/// prompt the resolver refuses the allow outright, whichever lifetime was
/// asked for. An agent approving its own escalation is the thing this whole
/// path exists to prevent. The resolver separately checks authenticated
/// ancestor lineage before accepting any non-operator answer.
///
/// `message` is a deny's reason or workaround, delivered to the requesting
/// agent with its refusal. An allow has nothing to explain to the agent the
/// way a refusal does, so a message on one is refused rather than dropped.
///
/// Returns the parsed answer alongside the raw decision word for the summary.
fn parse_permission_answer(
    payload: &serde_json::Value,
) -> Result<(PermissionAnswer, &'static str), String> {
    let (decision, decision_word) = match payload_str(payload, "decision", &[])
        .ok_or_else(|| "payload.decision is required (allow|deny)".to_string())?
    {
        "allow" => (PermissionDecision::Allow, "allow"),
        "deny" => (PermissionDecision::Deny, "deny"),
        other => {
            return Err(format!(
                "invalid decision '{other}'; expected allow or deny"
            ))
        }
    };
    let scope = match payload_str(payload, "scope", &[]).unwrap_or("once") {
        "once" => PermissionScope::Once,
        "session" => PermissionScope::Session,
        other => return Err(format!("invalid scope '{other}'; expected once or session")),
    };
    let lifetime = match payload_str(payload, "lifetime", &[]) {
        Some(raw) => Some(AuthorityLifetimeKind::parse(raw)?),
        None => None,
    };
    let expires_at = match payload.get("expiresInSeconds") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => {
            let seconds = value
                .as_i64()
                .filter(|seconds| *seconds > 0)
                .ok_or_else(|| "payload.expiresInSeconds must be a positive integer".to_string())?;
            Some(chrono::Utc::now().timestamp() + seconds)
        }
    };
    let note = match payload.get("message") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(message)) => {
            Some(message.trim()).filter(|message| !message.is_empty())
        }
        Some(_) => return Err("payload.message must be a string".to_string()),
    };
    if note.is_some() && matches!(decision, PermissionDecision::Allow) {
        return Err(
            "payload.message rides only on a deny; append to the node's messages to say \
                    something with an allow"
                .to_string(),
        );
    }
    let answer = PermissionAnswer::from_surface(decision, AnswerSurface::ResourcePatch)
        .with_containment_scope(scope)
        .with_lifetime(lifetime)
        .with_expiry(expires_at);
    Ok((
        match note {
            Some(note) => answer.with_note(note),
            None => answer,
        },
        decision_word,
    ))
}

/// What a `permissions` patch reports. An answer that arrived after another
/// had won is told what won, not that it was refused.
fn permission_receipt(segment: &str, outcome: ResolveOutcome, decision: &str) -> String {
    if let Some(notice) = outcome.withdrawn {
        notice
    } else if let Some(prior) = outcome.prior {
        prior.receipt(segment)
    } else if outcome.duplicate {
        format!("Permission {segment} was already answered")
    } else {
        format!("Answered permission {segment}: {decision}")
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
        (CairnResource::NodeTasks { .. }, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "task append requires payload"))?;
            let subagent = payload_non_empty_str(payload, "subagentType", &["subagent_type"])
                .ok_or_else(|| build_failure(index, item, "payload.subagentType is required"))?;
            if dry_run {
                format!("Would spawn task: {subagent}")
            } else {
                // Apply routes task appends through the blocking group before reaching
                // dispatch; arriving here means the caller bypassed that path.
                return Err(build_failure(
                    index,
                    item,
                    "internal: task append must run through the blocking group, not dispatch",
                ));
            }
        }
        (CairnResource::NodeCalls { .. }, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "call append requires payload"))?;
            let prompt = payload_non_empty_str(payload, "prompt", &[])
                .ok_or_else(|| build_failure(index, item, "payload.prompt is required"))?;
            if dry_run {
                let head: String = prompt.chars().take(48).collect();
                format!("Would spawn call: {head}")
            } else {
                // Apply routes call appends through the blocking group before reaching
                // dispatch; arriving here means the caller bypassed that path.
                return Err(build_failure(
                    index,
                    item,
                    "internal: call append must run through the blocking group, not dispatch",
                ));
            }
        }
        (
            CairnResource::NodeQuestion {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            },
            ChangeMode::Patch | ChangeMode::Append,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "question answer requires payload"))?;
            let withdraw = match payload.get("withdraw") {
                None | Some(serde_json::Value::Null) | Some(serde_json::Value::Bool(false)) => {
                    false
                }
                Some(serde_json::Value::Bool(true)) => true,
                Some(_) => {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.withdraw is true or omitted",
                    ))
                }
            };
            let answering = ["answer", "answers", "handled", "moot"]
                .into_iter()
                .any(|key| payload.get(key).is_some());
            if withdraw && answering {
                return Err(build_failure(
                    index,
                    item,
                    "payload.withdraw takes the question back; it does not ride with an answer, handled or moot",
                ));
            }
            if !withdraw && !answering {
                return Err(build_failure(
                    index,
                    item,
                    "payload.answer or payload.answers is required (or handled:true or moot:true; the asker may withdraw:true)",
                ));
            }
            if dry_run {
                let verb = if withdraw { "withdraw" } else { "answer" };
                format!(
                    "Would {verb} question {} for {}-{}/{}/{}",
                    segment, project, number, exec_seq, node_id
                )
            } else if withdraw {
                planning::withdraw_node_question(
                    orch, request, project, *number, *exec_seq, node_id, segment,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            } else {
                let outcome = planning::answer_node_question_as(
                    orch,
                    request.run_id.as_deref(),
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    segment,
                    payload,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if let Some(notice) = outcome.withdrawn {
                    notice
                } else if outcome.duplicate {
                    outcome.duplicate_summary(segment)
                } else {
                    outcome.answered_summary(segment)
                }
            }
        }
        (
            CairnResource::NodePermission {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "permission answer requires payload"))?;
            let (answer, decision_str) = parse_permission_answer(payload)
                .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                format!(
                    "Would answer permission {} for {}-{}/{}/{}",
                    segment, project, number, exec_seq, node_id
                )
            } else {
                let agent =
                    crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                let (actor, appearance) = agent
                    .appearance(chrono::Utc::now().timestamp())
                    .map_err(|error| build_failure(index, item, error))?;
                let answer = answer
                    .with_authenticated_appearance(actor, appearance)
                    .map_err(|error| build_failure(index, item, error))?;
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch, project, *number, *exec_seq, node_id, None, segment, answer,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                permission_receipt(segment, outcome, decision_str)
            }
        }
        (
            CairnResource::TaskPermission {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                segment,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "permission answer requires payload"))?;
            let (answer, decision_str) = parse_permission_answer(payload)
                .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                format!(
                    "Would answer permission {} for {}-{}/{}/{}/task/{}",
                    segment, project, number, exec_seq, node_id, task_name
                )
            } else {
                let agent =
                    crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                let (actor, appearance) = agent
                    .appearance(chrono::Utc::now().timestamp())
                    .map_err(|error| build_failure(index, item, error))?;
                let answer = answer
                    .with_authenticated_appearance(actor, appearance)
                    .map_err(|error| build_failure(index, item, error))?;
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    Some(task_name),
                    segment,
                    answer,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                permission_receipt(segment, outcome, decision_str)
            }
        }
        (CairnResource::NodeQuestions { .. }, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "question append requires payload"))?;
            let questions = payload
                .get("questions")
                .and_then(|value| value.as_array())
                .ok_or_else(|| build_failure(index, item, "payload.questions must be an array"))?;
            if dry_run {
                format!("Would ask {} question(s)", questions.len())
            } else {
                return Err(build_failure(
                    index,
                    item,
                    "internal: question append must run through the blocking group, not dispatch",
                ));
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn deny() -> PermissionAnswer {
        PermissionAnswer::from_surface(PermissionDecision::Deny, AnswerSurface::ResourcePatch)
    }

    #[test]
    fn a_message_rides_on_a_deny_and_is_refused_on_an_allow() {
        let (answer, word) = parse_permission_answer(
            &json!({"decision": "deny", "message": "  write it under $TMPDIR instead "}),
        )
        .unwrap();
        assert_eq!(word, "deny");
        assert_eq!(answer, deny().with_note("write it under $TMPDIR instead"));

        let (blank, _) =
            parse_permission_answer(&json!({"decision": "deny", "message": "  "})).unwrap();
        assert_eq!(blank, deny(), "a blank message is no message");

        let refusal = parse_permission_answer(&json!({"decision": "allow", "message": "go ahead"}))
            .unwrap_err();
        assert!(refusal.contains("rides only on a deny"), "{refusal}");
        assert!(parse_permission_answer(&json!({"decision": "deny", "message": 7})).is_err());
    }

    #[test]
    fn both_permission_resources_declare_the_message_key() {
        use cairn_common::contract::contract_for;
        use cairn_uri::ResourceKind;
        for kind in [ResourceKind::NodePermission, ResourceKind::TaskPermission] {
            let contract = contract_for(kind).unwrap();
            assert!(
                contract
                    .mutations
                    .iter()
                    .any(|spec| spec.mode == ChangeMode::Patch
                        && spec.optional.iter().any(|key| key.key == "message")),
                "{kind:?} must accept payload.message"
            );
        }
    }
}
