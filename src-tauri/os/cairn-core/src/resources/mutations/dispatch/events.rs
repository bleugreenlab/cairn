use super::super::{build_failure, ResourceMutationResult};
use crate::{
    mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest},
    orchestrator::Orchestrator,
};
use cairn_common::uri::CairnResource;

/// Mint (append on the collection) and revoke (delete on a member) ingress
/// endpoints.
pub(super) async fn dispatch(
    o: &Orchestrator,
    _r: &McpCallbackRequest,
    i: usize,
    x: &ChangeItem,
    dry: bool,
    u: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let outcome = match (u, x.mode) {
        (CairnResource::ProjectEventEndpoints { project }, ChangeMode::Append) => {
            if dry {
                return Ok(Some("Would mint an ingress endpoint".into()));
            }
            let payload = x
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(i, x, "payload required"))?;
            crate::resources::event_endpoints::mint(o, project, payload).await
        }
        (CairnResource::ProjectEventEndpoint { project, name }, ChangeMode::Delete) => {
            if dry {
                return Ok(Some(format!("Would revoke ingress endpoint {name}")));
            }
            crate::resources::event_endpoints::revoke(o, project, name).await
        }
        _ => return Ok(None),
    };
    outcome
        .map(Some)
        .map_err(|error| build_failure(i, x, error))
}
