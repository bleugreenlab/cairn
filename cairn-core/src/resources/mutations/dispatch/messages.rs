//! Direct/channel message append dispatch, relocated from dispatch.rs.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::handlers::messages;
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
                messages::append_project_or_issue_message(orch, request, project, None, content)
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
                    "Would append {} chars to issue channel {project}-{number}",
                    content.len()
                )
            } else {
                messages::append_project_or_issue_message(
                    orch,
                    request,
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
    let escalate = payload
        .get("escalate")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let target_uri = match target.task_name {
        Some(task_name) => format!(
            "{}-{}/{}/{}/task/{}",
            target.project, target.number, target.exec_seq, target.node_id, task_name
        ),
        None => format!(
            "{}-{}/{}/{}",
            target.project, target.number, target.exec_seq, target.node_id
        ),
    };
    if dry_run {
        Ok(format!(
            "Would send {} chars to {target_uri}",
            content.len()
        ))
    } else {
        messages::append_direct_message(
            orch,
            request,
            target.project,
            target.number,
            target.exec_seq,
            target.node_id,
            target.task_name,
            content,
            escalate,
        )
        .await
        .map_err(|error| build_failure(index, item, error))
    }
}
