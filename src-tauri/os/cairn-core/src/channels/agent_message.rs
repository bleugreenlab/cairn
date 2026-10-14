//! Agent-authored free text sent to a bound channel conversation.
//!
//! The write side (`write cairn://channels/conversations`, append) admits a send
//! and [`enqueue`]s it; the provider's router delivers it through the same
//! `channel_outbound` intent, claim, retry, and failure path every other outbound
//! uses. This module holds the grammar the two sides share: the ledger kind, the
//! `binding_ref` that records the sender, and the context header rendered above
//! the text.
//!
//! A `message` row is neither an ask nor a route. It carries no answerable
//! prompt, writes no route-firing history, and is never deferred for operator
//! presence: the gate is initiated by [`OutboundInitiator::Agent`], which is not
//! presence aware.
//!
//! [`OutboundInitiator::Agent`]: super::OutboundInitiator::Agent

use cairn_common::uri::{parse_uri, CairnResource};
use uuid::Uuid;

use super::{ledger, render_text_floor, ConversationAddress, OutboundAsk};
use crate::storage::LocalDb;

/// The `channel_outbound.kind` of an agent message.
pub const KIND: &str = "message";

const BINDING_PREFIX: &str = "message:";

/// `message:{intent_id}:{sender_home}`. The intent id keeps every send distinct
/// under the outbound uniqueness fence; the full canonical home records exactly
/// which agent spoke, and is what a reply routes back to.
pub fn binding_ref(intent_id: &str, sender_home: &str) -> String {
    format!("{BINDING_PREFIX}{intent_id}:{sender_home}")
}

/// The sender's canonical home URI recorded in a message `binding_ref`.
pub fn sender_home(binding_ref: &str) -> Option<&str> {
    let rest = binding_ref.strip_prefix(BINDING_PREFIX)?;
    let start = rest.find("cairn://")?;
    Some(&rest[start..])
}

/// The attribution line a provider renders above the text, derived from the
/// sender's home at delivery time so nothing beyond the `binding_ref` is stored:
/// `[Cairn · cairn/general]` for a thread, `[Cairn · cairn/4881 planner]` for an
/// issue node.
pub fn context_header(sender_home: &str) -> String {
    let speaker = match parse_uri(sender_home) {
        Some(CairnResource::Thread {
            project,
            name,
            path,
        }) => match path.as_slice() {
            [task, task_name] if task == "task" => format!("{project}/{name}/{task_name}"),
            _ => format!("{project}/{name}"),
        },
        Some(CairnResource::Node {
            project,
            number,
            node_id,
            ..
        }) => format!("{project}/{number} {node_id}"),
        Some(CairnResource::Task {
            project,
            number,
            node_id,
            task_name,
            ..
        }) => format!("{project}/{number} {node_id}/{task_name}"),
        _ => return "[Cairn]".to_string(),
    };
    format!("[Cairn · {speaker}]")
}

/// Record a pending `message` intent for an admitted send and hand it to the
/// provider's router. Returns the intent id.
///
/// The row is the durable boundary: if the router is not running to receive the
/// hand-off, the intent stays `pending` and the router's next sweep delivers it.
pub async fn enqueue(
    ledger: &LocalDb,
    address: &ConversationAddress,
    sender_home: &str,
    job_id: Option<&str>,
    content: &str,
) -> Result<String, String> {
    let id = Uuid::new_v4().to_string();
    let binding_ref = binding_ref(&id, sender_home);
    let conversation = address.to_string();
    let rendered = render_text_floor(&OutboundAsk::Notify {
        text: content.to_string(),
    });
    let provider = address.provider().id();
    let inserted = ledger::insert_intent(
        ledger,
        &ledger::NewOutbound {
            id: &id,
            channel: provider,
            kind: KIND,
            binding_ref: &binding_ref,
            conversation: &conversation,
            job_id,
            rendered_text: &rendered,
            rendering: "text",
            created_at: chrono::Utc::now().timestamp_millis(),
        },
    )
    .await?;
    if !inserted {
        return Err(format!(
            "agent message {id} collided with an existing channel_outbound intent"
        ));
    }
    super::submit_agent_message(provider, &id);
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_ref_round_trips_the_sender_home() {
        for home in [
            "cairn://p/cairn/general",
            "cairn://p/cairn/4881/1/planner",
            "cairn://p/cairn/4881/1/builder/task/review",
        ] {
            let binding = binding_ref("intent-1", home);
            assert_eq!(sender_home(&binding), Some(home));
        }
        assert_eq!(sender_home("route:x:cairn://p/cairn/general"), None);
    }

    #[test]
    fn context_header_names_the_speaker() {
        assert_eq!(
            context_header("cairn://p/cairn/general"),
            "[Cairn · cairn/general]"
        );
        assert_eq!(
            context_header("cairn://p/cairn/4881/1/planner"),
            "[Cairn · cairn/4881 planner]"
        );
        assert_eq!(
            context_header("cairn://p/cairn/4881/1/builder/task/review"),
            "[Cairn · cairn/4881 builder/review]"
        );
        assert_eq!(context_header("not a uri"), "[Cairn]");
    }
}
