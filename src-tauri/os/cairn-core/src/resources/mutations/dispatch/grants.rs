//! Authority grant revocation dispatch.
//!
//! Revocation is a patch, not a delete: the authorization journal cites grants
//! by id, and history that points at a row someone removed is not an audit
//! trail. A revoked grant stays readable and simply stops authorizing.
//!
//! A caller here revokes only a grant it holds (`authorization::revoke`). The
//! operator revokes any grant, but not through this resource: a write here
//! cannot prove it comes from the operator, so the operator's door is the
//! `revoke_authority_grant` invoke behind Settings → Grants.

use super::super::{build_failure, ResourceMutationResult};
use crate::authorization::revoke::{check_holder, Revoker};
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
        (CairnResource::Grant { id }, ChangeMode::Patch) => {
            let stored_id = crate::resources::grants::stored_grant_id(id);
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
            let grant = crate::authorization::get_grant(&orch.db.local, &stored_id)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let Some(grant) = grant else {
                return Ok(Some(format!("Authority grant '{id}' does not exist")));
            };
            let revoker = revoker(orch, request).await;
            check_holder(id, &grant, &revoker)
                .map_err(|refusal| build_failure(index, item, refusal))?;
            if dry_run {
                format!("Would revoke authority grant '{id}'")
            } else {
                // The revoker is who the runner resolved the request to, never
                // a name the payload supplies.
                let revoked = crate::authorization::revoke_grant(
                    &orch.db.local,
                    &stored_id,
                    Some(&revoker.label()),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if revoked {
                    format!(
                        "Revoked authority grant '{id}'; it stops authorizing on the next check"
                    )
                } else {
                    format!("Authority grant '{id}' was already revoked")
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

/// Who this request revokes as: its authenticated run, or the verified actor
/// a row-write origin carries. A request with neither is a caller with no run
/// and holds nothing.
async fn revoker(orch: &Orchestrator, request: &McpCallbackRequest) -> Revoker {
    match crate::authorization::resolve_actor(orch, request).await {
        Some(actor) => Revoker {
            run_id: actor.principal.run_id,
            node_uri: actor.principal.node_uri,
            actor: actor.principal.actor,
        },
        None => Revoker::default(),
    }
}
