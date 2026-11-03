//! Terminal resource mutation dispatch, relocated from dispatch.rs.

use super::super::{
    build_failure, payload_bool, payload_str, payload_trimmed_non_empty_str, ResourceMutationResult,
};
use super::append_payload;
use crate::mcp::handlers::terminal;
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
        (
            CairnResource::NodeTerminal { slug, .. }
            | CairnResource::ProjectTerminal { slug, .. }
            | CairnResource::TaskTerminal { slug, .. },
            ChangeMode::Create,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            let command =
                payload_trimmed_non_empty_str(payload, "command", &[]).ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.command is required and must be a non-empty string",
                    )
                })?;
            let description = match payload.get("description") {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.description must be a string")
                })?),
                None => None,
            };
            let wake = match payload.get("wake") {
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            "payload.wake must be a string: \"exit\" or a literal output phrase",
                        )
                    })?;
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        return Err(build_failure(index, item, "payload.wake must not be empty; use \"exit\" or a literal output phrase"));
                    }
                    Some(if trimmed == "exit" {
                        terminal::TerminalWakeSpec::Exit
                    } else {
                        terminal::TerminalWakeSpec::Output(trimmed.to_string())
                    })
                }
                None => None,
            };
            let own_selector = match payload.get("executor") {
                Some(value) => Some(
                    crate::fleet::residency::parse_resident_selector(value)
                        .map_err(|error| build_failure(index, item, error))?,
                ),
                None => None,
            };
            // A job's terminal created without a machine of its own goes where
            // the job's placement default names, and persists that selector on
            // its row so recovery places it by the same rule. A project
            // terminal belongs to no job and has no default.
            let job_default = match (own_selector.as_ref(), resource) {
                (
                    None,
                    CairnResource::NodeTerminal {
                        project,
                        number,
                        exec_seq,
                        node_id,
                        ..
                    },
                ) => {
                    crate::fleet::placement_default::for_job_resource(
                        orch, project, *number, *exec_seq, node_id, None,
                    )
                    .await
                }
                (
                    None,
                    CairnResource::TaskTerminal {
                        project,
                        number,
                        exec_seq,
                        node_id,
                        task_name,
                        ..
                    },
                ) => {
                    crate::fleet::placement_default::for_job_resource(
                        orch,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                        Some(task_name),
                    )
                    .await
                }
                _ => None,
            };
            let (selector, node_default) =
                crate::fleet::placement_default::resident_selector(own_selector, || job_default);
            if wake.is_some()
                && matches!(resource, CairnResource::ProjectTerminal { .. })
                && request.run_id.is_none()
            {
                return Err(build_failure(
                    index,
                    item,
                    "payload.wake requires an agent caller when creating a project terminal",
                ));
            }
            if dry_run {
                let wake_suffix = wake
                    .as_ref()
                    .map_or_else(String::new, terminal::TerminalWakeSpec::dry_run_suffix);
                let placement = selector.as_ref().map_or_else(String::new, |selector| {
                    format!(" on {}", selector.describe())
                });
                let source = match node_default {
                    Some(_) => " (node default)",
                    None => "",
                };
                format!("Would start terminal {slug}{placement}{source}: {command}{wake_suffix}")
            } else {
                crate::fleet::placement_default::resident_outcome(
                    node_default.as_ref(),
                    terminal::create_terminal_from_resource(
                        orch,
                        resource,
                        command,
                        description,
                        wake,
                        request.run_id.as_deref(),
                        selector.as_ref(),
                    )
                    .await,
                )
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeTerminal { slug, .. }
            | CairnResource::ProjectTerminal { slug, .. }
            | CairnResource::TaskTerminal { slug, .. },
            ChangeMode::Append,
        ) => {
            let payload = append_payload(index, item)?;
            let content = payload_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            let submit = payload_bool(payload, "submit").unwrap_or(true);
            if dry_run {
                if submit {
                    format!("Would submit {} chars to terminal {slug}", content.len())
                } else {
                    format!("Would send {} raw chars to terminal {slug}", content.len())
                }
            } else {
                terminal::append_terminal_input(orch, resource, content, submit)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeTerminal { slug, .. }
            | CairnResource::ProjectTerminal { slug, .. }
            | CairnResource::TaskTerminal { slug, .. },
            ChangeMode::Delete,
        ) => {
            if dry_run {
                format!("Would stop terminal {slug}")
            } else {
                terminal::delete_terminal_by_resource(orch, resource)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
