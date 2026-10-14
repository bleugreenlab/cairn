//! External MCP server registry mutation dispatch, relocated from dispatch.rs.

use super::super::mcp::{apply_mcp_create, apply_mcp_delete, apply_mcp_patch};
use super::super::{build_failure, ResourceMutationResult};
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
        // External MCP server registry (cairn://mcp write CRUD). create targets
        // the family root (cairn://mcp); patch/delete name one server
        // (cairn://mcp/<server>). Workspace-scope writes are routed through the
        // worktree fence in the change handler BEFORE this dispatch runs.
        (
            CairnResource::Mcp {
                server: None,
                resource: None,
            },
            ChangeMode::Create,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            apply_mcp_create(orch, request, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (
            CairnResource::Mcp {
                server: Some(server),
                resource: None,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            apply_mcp_patch(orch, request, payload, server, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (
            CairnResource::Mcp {
                server: Some(server),
                resource: None,
            },
            ChangeMode::Delete,
        ) => apply_mcp_delete(orch, request, item.payload.as_ref(), server, dry_run)
            .await
            .map_err(|error| build_failure(index, item, error))?,
        // Mcp shape mismatches: create must target cairn://mcp (no server);
        // patch/delete must name a server (cairn://mcp/<server>).
        (
            CairnResource::Mcp {
                server: Some(_),
                resource: None,
            },
            ChangeMode::Create,
        ) => {
            return Err(build_failure(
                index,
                item,
                "mode=create targets cairn://mcp and names the server in payload.name; it does not take a server in the URI",
            ));
        }
        (
            CairnResource::Mcp {
                server: None,
                resource: None,
            },
            ChangeMode::Patch | ChangeMode::Delete,
        ) => {
            return Err(build_failure(
                index,
                item,
                "patch/delete target one server: cairn://mcp/<server>",
            ));
        }
        (
            CairnResource::Mcp {
                resource: Some(_), ..
            },
            _,
        ) => {
            return Err(build_failure(
                index,
                item,
                "cairn://mcp/<server>/<tool-or-resource> is a tool/resource target (use run/read), not a registry write",
            ));
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
