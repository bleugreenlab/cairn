//! Authority grant revocation dispatch.
//!
//! Revocation is a patch, not a delete: the authorization journal cites grants
//! by id, and history that points at a row someone removed is not an audit
//! trail. A revoked grant stays readable and simply stops authorizing.

use super::super::{build_failure, payload_str, ResourceMutationResult};
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
        (CairnResource::Grant { id }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            // `revoke` is required and must be an explicit true. Revoking is the
            // only thing a grant can be patched into, so an ambiguous payload
            // should say what it wants rather than be guessed at.
            if payload.get("revoke").and_then(serde_json::Value::as_bool) != Some(true) {
                return Err(build_failure(
                    index,
                    item,
                    "payload.revoke must be true; revoking is the only patch a grant accepts",
                ));
            }
            if dry_run {
                format!("Would revoke authority grant '{id}'")
            } else {
                let revoked_by = payload_str(payload, "revokedBy", &["revoked_by"]);
                let revoked = crate::authorization::revoke_grant(&orch.db.local, id, revoked_by)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                if revoked {
                    format!(
                        "Revoked authority grant '{id}'; it stops authorizing on the next check"
                    )
                } else {
                    format!("Authority grant '{id}' was already revoked or does not exist")
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
