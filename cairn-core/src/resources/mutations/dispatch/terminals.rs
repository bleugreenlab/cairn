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
                format!("Would start terminal {slug}: {command}{wake_suffix}")
            } else {
                terminal::create_terminal_from_resource(
                    orch,
                    resource,
                    command,
                    description,
                    wake,
                    request.run_id.as_deref(),
                )
                .await
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
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
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
