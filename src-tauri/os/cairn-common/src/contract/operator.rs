//! The operator as a recipient: `cairn://operator` and its messages.
use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

pub(crate) const OPERATOR_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Operator,
    uri_template: "cairn://operator",
    name: "Operator",
    description: "The operator as a recipient. His channels (app, iMessage, Telegram), each with the purpose line he wrote for it or `not offered: no purpose line`; his presence and when he was last seen on each; and the reader's own messages to him, newest first, each with its state: sent, delivered (or delivery failed), seen (by read receipt, by his headline cursor passing the linked headline, or by opening it in the app), or answered. A seen message with no reply reads `seen ... ago, no reply`: he knows, so a message with a default proceeds on it. Append {content, via} to send one: `via` is a channel listed with a purpose line, and the purpose is what the message must fit. `content` is the claim, at most 400 characters and at most the transport's own limit; an over-limit message is refused, never truncated. `default` says what you will do, on what assumption, and when, if he does not reply; its presence puts the message in his notification center until he answers. `headline` links a headline of your own thread: its row URI, or {line, ref, until} to write one in the same call. A reply on the phone or in the app reaches you at steer; being seen reaches you as a passive notice.",
    read_projections: &[ProjectionSpec { key: "limit", values: "N newest messages (default 20, max 200)" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[OPERATOR_CONTENT, OPERATOR_VIA],
        optional: &[OPERATOR_DEFAULT, OPERATOR_HEADLINE],
        label: "send a message to the operator",
        example: "write({changes:[{target:\"cairn://operator\",mode:\"append\",payload:{content:\"...\",via:\"cairn://channels/app\",default:\"Assuming you want X, I'll do Y on Friday\"}}]})",
    }],
};

pub(crate) const OPERATOR_MESSAGE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::OperatorMessage,
    uri_template: "cairn://operator/messages/{id}",
    name: "Message to the operator",
    description: "One message to the operator: its sender, channel, content, default, linked headline, state, the delivery row for a phone channel (conversation, attempts, last error, sent and read times), and the reply that answered it. Opening it in the app marks it seen; an agent's read never does. Only the operator answers it, from the app.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[CONTENT],
        optional: &[],
        label: "the operator's reply, from the app only",
        example: "write({changes:[{target:\"cairn://operator/messages/ID\",mode:\"append\",payload:{content:\"...\"}}]})",
    }],
};
