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
//! the sender's bare node/task base URI (`cairn://p/PROJECT/N/EXEC/NODE` or
//! `.../task/NAME`); appending `/messages` yields the canonical address for
//! both node and task senders. Before CAIRN-1363 these sites echoed the bare
//! `sender_name` as the reply-to, so recipients were pointed at the raw node
//! URI even though `/messages` is the documented form.

use crate::models::Message;

/// The canonical reply-to URI for a direct message, or `None` when the sender
/// is not addressable by URI (e.g. a project-level agent whose `sender_name`
/// is a bare node name rather than a `cairn://` URI).
pub fn reply_to_uri(sender_name: &str) -> Option<String> {
    if sender_name == "external" {
        return Some("external".to_string());
    }

    sender_name
        .starts_with("cairn://")
        .then(|| format!("{sender_name}/messages"))
}

/// Render a direct message for its recipient: the `[Direct message from …]`
/// header followed by the content, plus a reply-to hint pointing at the
/// sender's canonical `/messages` collection when the sender is URI-addressable.
pub fn render_direct_message(msg: &Message) -> String {
    let head = format!("[Direct message from {}] {}", msg.sender_name, msg.content);
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
            delivered_at: None,
            urgency: None,
        }
    }

    #[test]
    fn node_sender_reply_to_targets_messages_collection() {
        let uri = reply_to_uri("cairn://p/CAIRN/1361/1/builder").unwrap();
        assert_eq!(uri, "cairn://p/CAIRN/1361/1/builder/messages");
    }

    #[test]
    fn task_sender_reply_to_targets_task_messages_collection() {
        let uri = reply_to_uri("cairn://p/CAIRN/1361/1/builder/task/explore").unwrap();
        assert_eq!(uri, "cairn://p/CAIRN/1361/1/builder/task/explore/messages");
    }

    #[test]
    fn bare_name_sender_has_no_reply_to() {
        assert!(reply_to_uri("builder").is_none());
    }

    #[test]
    fn external_sender_reply_to_targets_external_literal() {
        assert_eq!(reply_to_uri("external"), Some("external".to_string()));
    }

    #[test]
    fn render_includes_header_content_and_messages_reply_to() {
        let msg = direct_from("cairn://p/CAIRN/1361/1/builder", "ship it");
        let rendered = render_direct_message(&msg);
        assert!(
            rendered.contains("[Direct message from cairn://p/CAIRN/1361/1/builder] ship it"),
            "header + content preserved: {rendered}"
        );
        assert!(
            rendered.contains(
                "To reply, use the message tool with to: \"cairn://p/CAIRN/1361/1/builder/messages\""
            ),
            "reply-to points at /messages: {rendered}"
        );
        // The reply target must be the /messages collection, never the bare node URI.
        assert!(
            !rendered.contains("to: \"cairn://p/CAIRN/1361/1/builder\""),
            "reply-to must not be the bare node URI: {rendered}"
        );
    }

    #[test]
    fn render_omits_reply_to_for_bare_name_sender() {
        let msg = direct_from("planner", "hello");
        let rendered = render_direct_message(&msg);
        assert_eq!(rendered, "[Direct message from planner] hello");
    }

    #[test]
    fn render_external_sender_includes_documented_literal_reply_to() {
        let msg = direct_from("external", "please summarize");
        let rendered = render_direct_message(&msg);
        assert!(rendered.contains("[Direct message from external] please summarize"));
        assert!(rendered.contains("To reply, use the message tool with to: \"external\""));
    }
}
