//! Question / permission / task-append prompt dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, payload_str, ResourceMutationResult};
use crate::mcp::handlers::planning;
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
            let decision_str = payload_str(payload, "decision", &[]).ok_or_else(|| {
                build_failure(index, item, "payload.decision is required (allow|deny)")
            })?;
            let decision = match decision_str {
                "allow" => crate::mcp::handlers::permission::PermissionDecision::Allow,
                "deny" => crate::mcp::handlers::permission::PermissionDecision::Deny,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid decision '{other}'; expected allow or deny"),
                    ))
                }
            };
            let scope = match payload_str(payload, "scope", &[]).unwrap_or("once") {
                "once" => crate::mcp::handlers::permission::PermissionScope::Once,
                "session" => crate::mcp::handlers::permission::PermissionScope::Session,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid scope '{other}'; expected once or session"),
                    ))
                }
            };
            if dry_run {
                format!(
                    "Would answer permission {} for {}-{}/{}/{}",
                    segment, project, number, exec_seq, node_id
                )
            } else {
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch, project, *number, *exec_seq, node_id, segment, decision, scope,
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
            let decision_str = payload_str(payload, "decision", &[]).ok_or_else(|| {
                build_failure(index, item, "payload.decision is required (allow|deny)")
            })?;
            let decision = match decision_str {
                "allow" => crate::mcp::handlers::permission::PermissionDecision::Allow,
                "deny" => crate::mcp::handlers::permission::PermissionDecision::Deny,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid decision '{other}'; expected allow or deny"),
                    ))
                }
            };
            let scope = match payload_str(payload, "scope", &[]).unwrap_or("once") {
                "once" => crate::mcp::handlers::permission::PermissionScope::Once,
                "session" => crate::mcp::handlers::permission::PermissionScope::Session,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid scope '{other}'; expected once or session"),
                    ))
                }
            };
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
                    orch, project, *number, *exec_seq, task_name, segment, decision, scope,
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
