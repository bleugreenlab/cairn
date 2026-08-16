//! `patch cairn:~/feed {ack}` — the one way a reading position moves.
//!
//! The payload carries a token and nothing else. A position is never named by a
//! caller: this arm hands the token to the storage layer, which advances only to
//! what the server recorded that token having shown.

use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::FeedAck;
use cairn_common::uri::CairnResource;

/// The whole of what an acknowledgement may carry.
///
/// An unknown key is refused rather than ignored: the shape that would carry a
/// caller-chosen position is exactly the shape that must never be accepted
/// quietly.
pub(super) fn ack_token<'a>(
    index: usize,
    item: &ChangeItem,
    payload: &'a serde_json::Value,
) -> ResourceMutationResult<&'a str> {
    let object = payload
        .as_object()
        .ok_or_else(|| build_failure(index, item, "payload must be an object"))?;
    if let Some(key) = object.keys().find(|key| key.as_str() != "ack") {
        return Err(build_failure(
            index,
            item,
            format!(
                "unsupported feed payload key: {key}; a feed accepts only the ack token its last \
                 read returned, and the position it advances to is server-recorded"
            ),
        ));
    }
    payload
        .get("ack")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                "payload.ack is required and must be the non-empty token this home's last feed read returned",
            )
        })
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let CairnResource::HomeFeed {
        project,
        number,
        exec_seq,
        node_id,
        task_name,
    } = resource
    else {
        return Ok(None);
    };
    if item.mode != ChangeMode::Patch {
        return Ok(None);
    }

    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
    let token = ack_token(index, item, payload)?;

    if dry_run {
        return Ok(Some("Would acknowledge this home's feed page".to_string()));
    }

    // Resolved read-only, and only for the ADDRESSED home: a token minted
    // elsewhere finds no outstanding issuance here and moves nothing.
    let routed = orch.db.for_project(project).await;
    let home = crate::resources::feed::resolve_feed_home(
        &orch.db.local,
        &routed,
        project,
        *number,
        *exec_seq,
        node_id,
        task_name.as_deref(),
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;

    // Fail closed: only a committed cursor transaction is reported as an
    // acknowledgement.
    let summary = match orch
        .db
        .local
        .acknowledge_feed(&home, token)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
    {
        FeedAck::Advanced { from, to } => {
            format!("Acknowledged feed through post {to} (was {from})")
        }
        FeedAck::AlreadyAcknowledged { at } => {
            format!("Feed already acknowledged through post {at}; nothing to do")
        }
        FeedAck::Rejected(reason) => return Err(build_failure(index, item, reason)),
    };
    Ok(Some(summary))
}
