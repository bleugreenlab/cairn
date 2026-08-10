//! Question / permission / task-append prompt dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, payload_str, ResourceMutationResult};
use crate::mcp::handlers::permission::{
    AnswerSurface, PermissionAnswer, PermissionDecision, PermissionScope,
};
use crate::mcp::handlers::planning;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityLifetimeKind;
use cairn_common::uri::CairnResource;

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
/// path exists to prevent; denying and cancelling stay available.
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
    Ok((
        PermissionAnswer::from_surface(decision, AnswerSurface::ResourcePatch)
            .with_containment_scope(scope)
            .with_lifetime(lifetime)
            .with_expiry(expires_at),
        decision_word,
    ))
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
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
            if payload.get("answer").is_none() && payload.get("answers").is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload.answer or payload.answers is required",
                ));
            }
            if dry_run {
                format!(
                    "Would answer question {} for {}-{}/{}/{}",
                    segment, project, number, exec_seq, node_id
                )
            } else {
                let outcome = planning::answer_node_question(
                    orch, project, *number, *exec_seq, node_id, segment, payload,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if outcome.duplicate {
                    format!("Question {} was already answered", segment)
                } else {
                    format!("Answered question {}", segment)
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
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch, project, *number, *exec_seq, node_id, segment, answer,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if outcome.duplicate {
                    format!("Permission {} was already answered", segment)
                } else {
                    format!("Answered permission {}: {}", segment, decision_str)
                }
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
                // The permission resource keys on the OWNING job's own
                // `uri_segment`; for a sub-agent task that is the task segment,
                // so the task name addresses the request directly (issue #143).
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch, project, *number, *exec_seq, task_name, segment, answer,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if outcome.duplicate {
                    format!("Permission {} was already answered", segment)
                } else {
                    format!("Answered permission {}: {}", segment, decision_str)
                }
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
