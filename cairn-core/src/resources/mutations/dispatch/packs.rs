//! Resource-pack mutation dispatch.

use super::super::packs::{apply_pack_delete, dispatch_pack_action, PackMutationResult};
use super::super::{build_failure, ResourceMutationResult};
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
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::Pack { pack_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would install or update pack '{pack_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                record(
                    dispatch_pack_action(orch, payload, pack_id)
                        .map_err(|error| build_failure(index, item, error))?,
                    applied_data,
                )
            }
        }
        (CairnResource::Pack { pack_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would uninstall pack '{pack_id}'")
            } else {
                record(
                    apply_pack_delete(orch, pack_id)
                        .map_err(|error| build_failure(index, item, error))?,
                    applied_data,
                )
            }
        }
        // `cairn://packs` itself is read-only: installing from a URL lands
        // in CAIRN-3773, and the contract advertises no mutation for it, so
        // the gate rejects one straight from the table.
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

/// Report the same facts twice: the prose an agent reads, and the structured
/// result a UI renderer can act on. Conflicts and kept items are the whole
/// reason the structured form exists — flattened into a sentence they can only
/// be re-parsed.
fn record(result: PackMutationResult, applied_data: &mut Option<serde_json::Value>) -> String {
    let summary = result.summary.clone();
    *applied_data = serde_json::to_value(result).ok();
    summary
}
