//! Human-facing channel transport, message contracts, and provider formatting.
//!
//! Providers translate messages and events at the network boundary. The caller
//! owns routing, admission, persistence, presence policy, and restart decisions.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub mod address;
pub mod commands;
pub mod discord;
pub mod state_card;
pub mod telegram;

pub use address::{
    conversation_capabilities, ConversationAddress, ConversationAddressError,
    ConversationCapabilities, ConversationDestination, ConversationProvider,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateCardMessage {
    pub conversation: String,
    pub state: state_card::AgentCardState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostedCard {
    pub provider_guid: String,
}

/// Why an outbound message exists. Direct operator responses remain conversation;
/// standing subscriptions are Cairn-initiated pushes and obey presence policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OutboundInitiator {
    OperatorInbound,
    OperatorSubscription,
    CairnPush,
    /// An agent speaking in a conversation it was granted. Not operator
    /// attention, so it is sent regardless of the operator's presence.
    Agent,
}

impl OutboundInitiator {
    pub fn is_presence_aware(self) -> bool {
        matches!(self, Self::OperatorSubscription | Self::CairnPush)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedQuestionMessage {
    pub conversation: String,
    pub provider_guid: String,
    pub caption_guid: Option<String>,
    pub sent_at: i64,
    pub original_text: String,
    pub answer: Option<String>,
    pub answering_surface: Option<String>,
    pub multi_select: bool,
    pub receipt: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OperatorPresence {
    Present,
    Away,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCapabilities {
    pub state_cards: bool,
    pub structured_asks: bool,
    pub open_options: bool,
    pub edit_in_place: bool,
    pub max_text_len: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskOption {
    pub label: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum OutboundAsk {
    Question {
        prompt_id: String,
        question_index: usize,
        text: String,
        options: Vec<AskOption>,
    },
    Permission {
        request_id: String,
        summary: String,
    },
    Notify {
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutboundMessage {
    pub intent_id: String,
    pub conversation: String,
    pub initiated_by: OutboundInitiator,
    pub ask: OutboundAsk,
    pub context_header: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentIds {
    pub primary_guid: String,
    pub caption_guid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum InboundEvent {
    Selection {
        conversation: String,
        bound_guid: String,
        sender: String,
        option_id: String,
        option_text: String,
        selected: bool,
    },
    Selections {
        conversation: String,
        bound_guid: String,
        sender: String,
        changes: Vec<PollSelectionChange>,
    },
    Reply {
        conversation: String,
        bound_guid: String,
        sender: String,
        text: String,
    },
    Bare {
        conversation: String,
        sender: String,
        text: String,
    },
    /// Recording-only input from a sender rejected at the provider boundary.
    Rejected {
        conversation: String,
        sender: String,
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollSelectionChange {
    pub option_id: String,
    pub option_text: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChannelLiveness {
    pub active: bool,
    pub started_at: Option<i64>,
    pub last_receipt_at: Option<i64>,
    pub last_receipt_rowid: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ChannelHealth {
    Ready,
    Degraded { reason: String },
    Unavailable { reason: String },
}

#[async_trait]
pub trait ChannelProvider: Send + Sync {
    fn capabilities(&self) -> ChannelCapabilities;
    async fn send(&self, message: &OutboundMessage) -> Result<SentIds, String>;
    fn subscribe(&self) -> mpsc::Receiver<InboundEvent>;
    fn health(&self) -> ChannelHealth;
    fn liveness(&self) -> ChannelLiveness {
        ChannelLiveness::default()
    }
    /// How many characters this provider would cut when delivering `body` as a
    /// notification under `context_header`; zero means it arrives whole.
    /// Providers truncate rather than split, so a caller that must not lose text
    /// refuses any nonzero answer.
    ///
    /// The default budgets the body alone against `max_text_len`, which is right
    /// for a renderer that sacrifices the header to keep the body. A provider
    /// whose header shares the budget, or whose rendering changes the visible
    /// length, overrides this with its real renderer.
    fn notify_overflow(&self, _context_header: &str, body: &str) -> usize {
        self.capabilities()
            .max_text_len
            .map_or(0, |limit| body.chars().count().saturating_sub(limit))
    }
    /// Headless fallback for a provider running on the same console as the
    /// runner. Remotely placed providers must use the default Away result:
    /// service placement is never evidence of operator presence.
    async fn operator_presence(&self) -> OperatorPresence {
        OperatorPresence::Away
    }
    async fn cleanup_question(&self, _message: &ResolvedQuestionMessage) -> Result<(), String> {
        Ok(())
    }
    async fn post_state_card(&self, _message: &StateCardMessage) -> Result<PostedCard, String> {
        Err("state cards are not supported by this provider".into())
    }
    async fn edit_state_card(
        &self,
        _provider_guid: &str,
        _message: &StateCardMessage,
    ) -> Result<(), String> {
        Err("state cards are not supported by this provider".into())
    }
    async fn find_state_card(
        &self,
        _conversation: &str,
        _marker: &str,
    ) -> Result<Option<PostedCard>, String> {
        Ok(None)
    }
    async fn pin_state_card(
        &self,
        _conversation: &str,
        _provider_guid: &str,
    ) -> Result<(), String> {
        Err("state cards are not supported by this provider".into())
    }
}

/// Renders the provider-independent plain-text floor for an outbound ask.
pub fn render_text_floor(ask: &OutboundAsk) -> String {
    match ask {
        OutboundAsk::Question { text, options, .. } => {
            if options.is_empty() {
                return format!("{text}\n\nReply to this message with your answer.");
            }

            let options = options
                .iter()
                .enumerate()
                .map(|(index, option)| match &option.description {
                    Some(description) if !description.trim().is_empty() => {
                        format!("{}. {} — {}", index + 1, option.label, description)
                    }
                    _ => format!("{}. {}", index + 1, option.label),
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "{text}\n\n{options}\n\nReply to this message with a number or your answer."
            )
        }
        OutboundAsk::Permission { summary, .. } => format!(
            "{summary}\n\n1. Approve\n2. Deny\n\nReply to this message with a number or your answer."
        ),
        OutboundAsk::Notify { text } => text.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_numbered_question_options() {
        let ask = OutboundAsk::Question {
            prompt_id: "prompt".into(),
            question_index: 0,
            text: "Which path?".into(),
            options: vec![
                AskOption {
                    label: "Legacy".into(),
                    description: Some("Preserve current behavior".into()),
                },
                AskOption {
                    label: "New".into(),
                    description: None,
                },
            ],
        };

        assert_eq!(
            render_text_floor(&ask),
            "Which path?\n\n1. Legacy — Preserve current behavior\n2. New\n\nReply to this message with a number or your answer."
        );
    }

    #[test]
    fn renders_permissions_and_free_text_questions() {
        let permission = OutboundAsk::Permission {
            request_id: "request".into(),
            summary: "Run an external command?".into(),
        };
        assert_eq!(
            render_text_floor(&permission),
            "Run an external command?\n\n1. Approve\n2. Deny\n\nReply to this message with a number or your answer."
        );

        let free_text = OutboundAsk::Question {
            prompt_id: "prompt".into(),
            question_index: 0,
            text: "What should change?".into(),
            options: vec![],
        };
        assert_eq!(
            render_text_floor(&free_text),
            "What should change?\n\nReply to this message with your answer."
        );
    }
}
