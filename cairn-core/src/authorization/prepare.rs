//! Resolving what a `write` batch would actually do, before it does any of it.
//!
//! The authority gate used to read a batch's *syntax*: it matched
//! `cairn://settings` and `cairn://mcp` targets and derived scopes from them.
//! That makes the carrier the authorization identity, which is exactly backwards
//! -- a `file:` write to the same bytes reached the same place while naming
//! nothing, so the answer depended on which URI an agent happened to use.
//!
//! Preparation asks the other question: for each change, what would this leave
//! behind, and does that expand workspace capability? Every carrier that can
//! reach the workspace configuration document is resolved here, ahead of any
//! side effect, so a batch either crosses its boundaries before it starts or
//! does not cross them at all.
//!
//! Two outcomes are possible per item, and they are different things:
//!
//! - an [`AuthorityRequest`], which policy classifies and a grant may authorize;
//! - a [`Refusal`], which no approval can legalize, because the carrier itself
//!   cannot express what is being authorized.

use cairn_common::authorization::AuthorityRequest;

use crate::mcp::types::{ChangeItem, McpCallbackRequest};
use crate::orchestrator::Orchestrator;

/// One item's preparation outcome.
#[derive(Debug)]
pub enum Prepared {
    /// A named authority boundary to adjudicate.
    Authority(AuthorityRequest),
    /// A malformed or unresolvable boundary. Fails closed: an authority
    /// question we cannot state is not one we may answer with "allow".
    Invalid(String),
    /// Structurally refused, whatever any grant says.
    Refused(String),
}

/// Everything a batch would cross, paired with the item index that crosses it,
/// in item order.
///
/// A list rather than a first match: a settings patch can touch several
/// sections and a batch can carry several MCP mutations, so each distinct place
/// gets its own decision and approving one never implicitly approves the rest.
pub async fn prepare_batch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &[ChangeItem],
) -> Vec<(usize, Prepared)> {
    let mut prepared = Vec::new();
    for (index, item) in changes.iter().enumerate() {
        // A structured file write that reaches the workspace configuration
        // document is refused before anything else is considered: there is no
        // scope to name for it, so there is nothing to prompt about.
        if let Some(refusal) = super::protected::structured_change_refusal(
            &orch.config_dir,
            std::path::Path::new(&request.cwd),
            item,
        ) {
            prepared.push((index, Prepared::Refused(refusal)));
            continue;
        }
        if let Some(request) =
            crate::resources::mutations::workspace_mcp_authority(orch, request, item).await
        {
            prepared.push((
                index,
                match request {
                    Ok(request) => Prepared::Authority(request),
                    Err(error) => Prepared::Invalid(error),
                },
            ));
        }
        for request in crate::resources::mutations::workspace_settings_authority(item) {
            prepared.push((
                index,
                match request {
                    Ok(request) => Prepared::Authority(request),
                    Err(error) => Prepared::Invalid(error),
                },
            ));
        }
    }
    prepared
}
