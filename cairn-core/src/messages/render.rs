//! Agent-facing rendering of direct messages.
//!
//! A direct message is shown to its recipient at several delivery sites (cold
//! resume, mid-turn tool-result augmentation, flush-on-idle resume, and the
//! Claude `additionalContext` hook). Each one needs the same two things: a
//! "from" header identifying the sender, and a reply-to hint telling the
//! recipient where to send a reply. This module is the single source of that
//! rendering so every path stays consistent.
//!
//! The reply-to target is the sender's `/messages` collection — the canonical
//! messaging-append target made authoritative by CAIRN-1329. `sender_name` is
//! the sender's canonical home URI: a node, a task, or a thread. Appending
//! `/messages` yields the canonical address for every addressable sender.
//! Before CAIRN-1363 these sites echoed the bare
//! `sender_name` as the reply-to, so recipients were pointed at the raw node
//! URI even though `/messages` is the documented form.
//!
//! An external sender is the one case with no URI at all, and it gets its own
//! hint rather than an address. CAIRN-4135: this arm used to name a literal
//! `to: "external"` target, whose dispatcher was removed as unreachable code
//! once the `to`-carrying message tool gave way to the `write` carrier. The
//! instruction outlived the mechanism, so recipients followed an address that
//! nothing implemented. A rendered hint may name only affordances that exist.

use crate::models::Message;

/// The sender name minted for a message from an external session: an MCP caller
/// with no `run_id`, i.e. a CLI or driver session running outside any Cairn run.
/// Such a session is not a node, has no URI, and therefore cannot be addressed
/// as a reply target. Shared with the handler that mints it so the sending and
/// rendering halves cannot drift apart.
pub(crate) const EXTERNAL_SENDER: &str = "external";

/// The reply affordance for an external session.
///
/// An external session has no inbox. It reads replies by polling the message
/// stream of the container it wrote into, so "post a message in this
/// conversation" is the whole of the affordance — there is no address. For an
/// issue-scoped node, an ordinary issue-channel message is additionally what a
/// `cairn watch` driver surfaces as its `external_message_reply` fact.
const EXTERNAL_REPLY_HINT: &str = "(Sent by an external session outside Cairn, which has no address to reply to. It reads replies from the conversation it wrote into — post your reply as an ordinary message here; on an issue, a message in that issue's /messages collection is what an external `cairn watch` driver surfaces.)";

/// The canonical reply-to URI for a direct message, or `None` when the sender
/// is not addressable by URI (e.g. a project-level agent whose `sender_name`
/// is a bare node name rather than a `cairn://` URI, or an external session).
fn reply_to_uri(sender_name: &str) -> Option<String> {
    sender_name
        .starts_with("cairn://")
        .then(|| format!("{sender_name}/messages"))
}

/// Render a direct message for its recipient: the `[Direct message from …]`
/// header followed by the content, plus a reply hint — the sender's canonical
/// `/messages` collection when the sender is URI-addressable, or the
/// post-in-this-conversation affordance when the sender is an external session.
pub(crate) fn render_direct_message(msg: &Message) -> String {
    let head = format!("[Direct message from {}] {}", msg.sender_name, msg.content);
    if msg.sender_name == EXTERNAL_SENDER {
        return format!("{head}\n{EXTERNAL_REPLY_HINT}");
    }

    #[test]
    fn thread_senders_reply_to_their_messages_collections() {
        assert_eq!(
            reply_to_uri("cairn://p/cairn/general").unwrap(),
            "cairn://p/cairn/general/messages"
        );
        assert_eq!(
            reply_to_uri("cairn://p/cairn/general/task/explore").unwrap(),
            "cairn://p/cairn/general/task/explore/messages"
        );
    }

    match reply_to_uri(&msg.sender_name) {
        Some(reply_to) => {
            format!("{head}\nTo reply, use the message tool with to: \"{reply_to}\"")
        }
        None => head,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ChannelType, Message};

    fn direct_from(sender_name: &str, content: &str) -> Message {
        Message {
            id: "m1".to_string(),
            channel_type: ChannelType::Direct,
            channel_id: None,
            sender_run_id: Some("sender-run".to_string()),
            sender_name: sender_name.to_string(),
            recipient_run_id: Some("recipient-run".to_string()),
            content: content.to_string(),
            created_at: 1,
            urgency: None,
        }
    }

    #[test]
    fn node_sender_reply_to_targets_messages_collection() {
        let uri = reply_to_uri("cairn://p/cairn/1361/1/builder").unwrap();
        assert_eq!(uri, "cairn://p/cairn/1361/1/builder/messages");
    }

    #[test]
    fn task_sender_reply_to_targets_task_messages_collection() {
        let uri = reply_to_uri("cairn://p/cairn/1361/1/builder/task/explore").unwrap();
        assert_eq!(uri, "cairn://p/cairn/1361/1/builder/task/explore/messages");
    }

    #[test]
    fn bare_name_sender_has_no_reply_to() {
        assert!(reply_to_uri("builder").is_none());
    }

    /// An external session has no URI, so it is not a reply-to address. The
    /// retired special case returned the literal `external` here, which the
    /// renderer then stamped as an instruction no dispatcher could satisfy.
    #[test]
    fn external_sender_is_not_uri_addressable() {
        assert!(reply_to_uri(EXTERNAL_SENDER).is_none());
    }

    #[test]
    fn render_includes_header_content_and_messages_reply_to() {
        let msg = direct_from("cairn://p/cairn/1361/1/builder", "ship it");
        let rendered = render_direct_message(&msg);
        assert!(
            rendered.contains("[Direct message from cairn://p/cairn/1361/1/builder] ship it"),
            "header + content preserved: {rendered}"
        );
        assert!(
            rendered.contains(
                "To reply, use the message tool with to: \"cairn://p/cairn/1361/1/builder/messages\""
            ),
            "reply-to points at /messages: {rendered}"
        );
        // The reply target must be the /messages collection, never the bare node URI.
        assert!(
            !rendered.contains("to: \"cairn://p/cairn/1361/1/builder\""),
            "reply-to must not be the bare node URI: {rendered}"
        );
    }

    #[test]
    fn render_omits_reply_to_for_bare_name_sender() {
        let msg = direct_from("planner", "hello");
        let rendered = render_direct_message(&msg);
        assert_eq!(rendered, "[Direct message from planner] hello");
    }

    /// The external arm must name only affordances that exist: posting an
    /// ordinary message in this conversation. It must never mint an address,
    /// because no dispatcher accepts one for an external sender.
    #[test]
    fn render_external_sender_hint_names_a_real_affordance() {
        let msg = direct_from(EXTERNAL_SENDER, "please summarize");
        let rendered = render_direct_message(&msg);
        assert!(rendered.contains("[Direct message from external] please summarize"));
        assert!(
            rendered.contains("post your reply as an ordinary message here"),
            "hint names the affordance that exists: {rendered}"
        );
        assert!(
            !rendered.contains("to: \"external\""),
            "must not name a target no dispatcher implements: {rendered}"
        );
        assert!(
            !rendered.contains("message tool"),
            "must not route the external arm through the retired to:-carrying tool: {rendered}"
        );
    }
}
