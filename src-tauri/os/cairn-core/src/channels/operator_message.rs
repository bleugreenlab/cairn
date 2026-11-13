//! The `channel_outbound` row a message to the operator writes when its `via`
//! is a phone channel.
//!
//! It is the sibling of [`super::agent_message`]: the same intent, claim,
//! retry, and failure path, initiated by [`OutboundInitiator::Agent`] so it is
//! never deferred for the operator's presence (the purpose line he wrote for
//! the channel is the grant). It differs in what it is addressed to: the
//! operator himself, on the conversation configured for him on that provider,
//! rather than a conversation bound to the sender. A reply to it is attributed
//! to the message by its `binding_ref`.
//!
//! [`OutboundInitiator::Agent`]: super::OutboundInitiator::Agent

/// The `channel_outbound.kind` of a message to the operator.
pub const KIND: &str = "operator";

const BINDING_PREFIX: &str = "operator:";

/// `operator:{message_id}`.
pub fn binding_ref(message_id: &str) -> String {
    format!("{BINDING_PREFIX}{message_id}")
}

/// The message id an operator `binding_ref` names.
pub fn message_id(binding_ref: &str) -> Option<&str> {
    binding_ref
        .strip_prefix(BINDING_PREFIX)
        .filter(|id| !id.is_empty())
}

/// The body as it goes on the wire, under the sender's attribution line: the
/// claim, then the default when the message states one.
pub fn render(content: &str, default: Option<&str>) -> String {
    match default {
        Some(default) => format!("{content}\n\nIf I don't hear back: {default}"),
        None => content.to_string(),
    }
}

/// Hand a freshly written row to its provider's router. Soft, as for agent
/// messages: the `pending` row is the durable record and the next sweep
/// delivers it if no router is listening.
pub fn submit(provider_id: &str, intent_id: &str) {
    super::submit_agent_message(provider_id, intent_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_ref_round_trips_the_message_id() {
        assert_eq!(message_id(&binding_ref("m-1")), Some("m-1"));
        assert_eq!(message_id("operator:"), None);
        assert_eq!(message_id("message:x:cairn://p/a/b"), None);
    }

    #[test]
    fn the_default_follows_the_claim() {
        assert_eq!(render("Ship it?", None), "Ship it?");
        assert_eq!(
            render("Ship it?", Some("I ship Friday")),
            "Ship it?\n\nIf I don't hear back: I ship Friday"
        );
    }
}
