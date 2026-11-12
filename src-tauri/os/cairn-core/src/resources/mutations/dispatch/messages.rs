//! Direct/channel message append dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::handlers::messages;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::ProjectMessages { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars to project channel {project}",
                    content.len()
                )
            } else {
                messages::append_project_or_issue_message(
                    orch,
                    messages::MessageAuthor::Mcp(request),
                    project,
                    None,
                    content,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::IssueMessages { project, number }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars to issue channel {project}/{number}",
                    content.len()
                )
            } else {
                messages::append_project_or_issue_message(
                    orch,
                    messages::MessageAuthor::Mcp(request),
                    project,
                    Some(*number),
                    content,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: None,
                },
                dry_run,
            )
            .await?
        }
        (
            CairnResource::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            // Canonical node messaging target — identical delivery to the
            // bare-node append, which remains a backward-compatible alias.
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: None,
                },
                dry_run,
            )
            .await?
        }
        (
            CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            ChangeMode::Append,
        ) => {
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: Some(task_name),
                },
                dry_run,
            )
            .await?
        }
        (
            CairnResource::TaskMessages {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            ChangeMode::Append,
        ) => {
            // Canonical sub-task messaging target — identical delivery to the
            // bare-task append, which remains a backward-compatible alias.
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: Some(task_name),
                },
                dry_run,
            )
            .await?
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

struct DirectMessageTarget<'a> {
    project: &'a str,
    number: i32,
    exec_seq: i32,
    node_id: &'a str,
    task_name: Option<&'a str>,
}

/// The optional keys of a direct-message append, parsed and checked.
#[derive(Debug, PartialEq)]
struct DirectOptions {
    cc: Vec<String>,
    urgency: DeliveryUrgency,
}

fn direct_options(payload: &serde_json::Value) -> Result<DirectOptions, String> {
    let cc = match payload.get("cc") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(entries)) => entries
            .iter()
            .map(|entry| match entry.as_str().map(str::trim) {
                Some(uri) if !uri.is_empty() => Ok(uri.to_string()),
                _ => Err(format!(
                    "payload.cc entries must be non-empty address strings; got {entry}"
                )),
            })
            .collect::<Result<_, _>>()?,
        Some(other) => {
            return Err(format!(
                "payload.cc must be an array of addresses, e.g. cc:[\"cairn://p/PROJECT/7/1/builder\"]; got {other}"
            ))
        }
    };
    let urgency = match payload.get("urgency") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or("")
                .and_then(|text| DeliveryUrgency::parse(text).map_err(|_| ""))
                .map_err(|_| {
                    format!("payload.urgency must be one of passive, queue, steer, interrupt; got {value}")
                })?,
        ),
    };
    let escalate = match payload.get("escalate") {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(escalate)) => *escalate,
        Some(other) => {
            return Err(format!(
                "payload.escalate must be true or false; got {other}"
            ))
        }
    };
    let urgency = messages::requested_urgency(escalate, urgency)?;
    Ok(DirectOptions { cc, urgency })
}

async fn append_node_or_task_message(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    target: DirectMessageTarget<'_>,
    dry_run: bool,
) -> ResourceMutationResult<String> {
    let payload = append_payload(index, item)?;
    let content = payload_non_empty_str(payload, "content", &[])
        .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
    let options = direct_options(payload).map_err(|error| build_failure(index, item, error))?;
    let address = messages::DirectAddress::new(
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        target.task_name,
    );
    if dry_run {
        let target_uri = address.uri();
        let recipients = std::iter::once(target_uri.as_str())
            .chain(options.cc.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "Would send {} chars ({}) to {recipients}",
            content.len(),
            options.urgency.as_str()
        ))
    } else {
        messages::send_direct_message(
            orch,
            request,
            messages::DirectSend {
                target: address,
                cc: &options.cc,
                content,
                urgency: options.urgency,
                mutation_key: None,
            },
        )
        .await
        .map_err(|error| build_failure(index, item, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_direct_append_parses_cc_urgency_and_escalate() {
        assert_eq!(
            direct_options(&json!({"content": "x"})).unwrap(),
            DirectOptions {
                cc: vec![],
                urgency: DeliveryUrgency::Steer
            }
        );
        assert_eq!(
            direct_options(
                &json!({"cc": ["cairn://p/p/1/1/a", " cairn:~/x "], "urgency": "queue"})
            )
            .unwrap(),
            DirectOptions {
                cc: vec!["cairn://p/p/1/1/a".into(), "cairn:~/x".into()],
                urgency: DeliveryUrgency::Queue
            }
        );
        assert_eq!(
            direct_options(&json!({"escalate": true})).unwrap().urgency,
            DeliveryUrgency::Interrupt
        );
        assert_eq!(
            direct_options(&json!({"escalate": true, "urgency": "interrupt"}))
                .unwrap()
                .urgency,
            DeliveryUrgency::Interrupt
        );
    }

    #[test]
    fn a_malformed_direct_append_is_refused_naming_the_key() {
        for (payload, names) in [
            (
                json!({"cc": "cairn://p/p/1/1/a"}),
                "payload.cc must be an array",
            ),
            (json!({"cc": [7]}), "payload.cc entries"),
            (json!({"cc": [""]}), "payload.cc entries"),
            (json!({"urgency": "loud"}), "payload.urgency must be one of"),
            (json!({"escalate": "yes"}), "payload.escalate must be"),
            (
                json!({"escalate": true, "urgency": "queue"}),
                "escalate:true means urgency `interrupt`",
            ),
        ] {
            let error = direct_options(&payload).unwrap_err();
            assert!(error.contains(names), "{payload}: {error}");
        }
    }
}
