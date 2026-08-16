use crate::{
    channels::{
        bindings::{MESSAGE_CLASS_NOTIFY, MESSAGE_CLASS_PERMISSION, MESSAGE_CLASS_QUESTION},
        conversation_capabilities, ledger, runtime_status, ConversationAddress,
        ConversationCapabilities, ConversationDestination, ConversationProvider,
    },
    orchestrator::Orchestrator,
};
use cairn_common::query::QueryParam;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConversationDeliverability {
    Ready,
    Degraded,
    Stopped,
}

fn message_class_names(bits: i64) -> Vec<&'static str> {
    [
        (MESSAGE_CLASS_QUESTION, "question"),
        (MESSAGE_CLASS_PERMISSION, "permission"),
        (MESSAGE_CLASS_NOTIFY, "notify"),
    ]
    .into_iter()
    .filter_map(|(bit, name)| (bits & bit != 0).then_some(name))
    .collect()
}

impl ConversationDeliverability {
    fn id(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationRow {
    pub address: String,
    pub label: String,
    pub provider: String,
    pub deliverability: ConversationDeliverability,
    pub target: String,
    pub classes: Vec<&'static str>,
    pub capabilities: ConversationCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn row_for_status(
    address: ConversationAddress,
    target: String,
    message_classes: i64,
    state: &str,
    last_error: Option<String>,
) -> ConversationRow {
    let deliverability = if state == "bridgeUp" {
        if last_error.is_some() {
            ConversationDeliverability::Degraded
        } else {
            ConversationDeliverability::Ready
        }
    } else if state == "degraded" {
        ConversationDeliverability::Degraded
    } else {
        ConversationDeliverability::Stopped
    };
    let destination = match address.destination() {
        ConversationDestination::IMessage { handle } => handle.clone(),
        ConversationDestination::Telegram { chat_id } => format!("operator chat {chat_id}"),
        ConversationDestination::Discord {
            guild_id,
            channel_id,
        } => format!("{guild_id}/{channel_id}"),
    };
    ConversationRow {
        address: address.to_string(),
        label: format!("{} — {destination}", address.provider().display_name()),
        provider: address.provider().id().to_string(),
        deliverability,
        target,
        classes: message_class_names(message_classes),
        capabilities: conversation_capabilities(address.provider()),
        last_error,
    }
}

pub async fn configured_conversations(orch: &Orchestrator) -> Vec<ConversationRow> {
    let channels = crate::config::settings::load_settings(&orch.config_dir).channels;
    let mut rows = Vec::new();
    let bindings = match ledger::list_active_bindings(&orch.db.local).await {
        Ok(bindings) => bindings,
        Err(_) => return vec![],
    };
    for binding in bindings {
        let provider = match binding.provider.as_str() {
            "imessage" => ConversationProvider::IMessage,
            "telegram" => ConversationProvider::Telegram,
            "discord" => ConversationProvider::Discord,
            _ => continue,
        };
        let enabled = match provider {
            ConversationProvider::IMessage => channels.imessage.enabled,
            ConversationProvider::Telegram => channels.telegram.enabled,
            ConversationProvider::Discord => channels.discord.enabled,
        };
        if !enabled {
            continue;
        }
        let status = runtime_status(orch, provider.id()).await;
        match binding.conversation.parse::<ConversationAddress>() {
            Ok(address) => rows.push(row_for_status(
                address,
                binding.target_uri,
                binding.message_classes,
                status.state,
                status.last_send_error,
            )),
            Err(error) => rows.push(ConversationRow {
                address: binding.conversation,
                label: format!("{} — invalid configuration", provider.display_name()),
                provider: provider.id().to_string(),
                deliverability: ConversationDeliverability::Stopped,
                target: binding.target_uri,
                classes: message_class_names(binding.message_classes),
                capabilities: conversation_capabilities(provider),
                last_error: Some(status.detail.unwrap_or_else(|| error.to_string())),
            }),
        }
    }
    rows
}

pub async fn render_conversations(orch: &Orchestrator, params: &[QueryParam]) -> String {
    let provider = params
        .iter()
        .find(|param| param.key == "provider")
        .map(|param| param.value.as_str());
    let deliverability = params
        .iter()
        .find(|param| param.key == "deliverability")
        .map(|param| param.value.as_str());
    if let Some(value) = provider {
        if !ConversationProvider::ALL
            .iter()
            .any(|candidate| candidate.id() == value)
        {
            return format!(
                "Unknown provider '{value}' for cairn://channels/conversations; expected imessage, telegram, or discord"
            );
        }
    }
    if let Some(value) = deliverability {
        if !matches!(value, "ready" | "degraded" | "stopped") {
            return format!(
                "Unknown deliverability '{value}' for cairn://channels/conversations; expected ready, degraded, or stopped"
            );
        }
    }
    let rows = configured_conversations(orch)
        .await
        .into_iter()
        .filter(|row| provider.is_none_or(|value| row.provider == value))
        .filter(|row| deliverability.is_none_or(|value| row.deliverability.id() == value))
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&rows).expect("conversation rows serialize")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_and_outbound_failure_combine_into_deliverability() {
        let ready = ConversationAddress::telegram("123").unwrap();
        assert_eq!(
            row_for_status(ready.clone(), "target".into(), 1, "bridgeUp", None).deliverability,
            ConversationDeliverability::Ready
        );
        assert_eq!(
            row_for_status(
                ready.clone(),
                "target".into(),
                1,
                "bridgeUp",
                Some("send failed".into())
            )
            .deliverability,
            ConversationDeliverability::Degraded
        );
        assert_eq!(
            row_for_status(ready.clone(), "target".into(), 1, "degraded", None).deliverability,
            ConversationDeliverability::Degraded
        );
        assert_eq!(
            row_for_status(ready, "target".into(), 1, "stopped", None).deliverability,
            ConversationDeliverability::Stopped
        );
    }

    #[test]
    fn row_shape_uses_stable_field_names() {
        let row = row_for_status(
            ConversationAddress::discord("1", "2").unwrap(),
            "cairn://p/cairn/4026".into(),
            MESSAGE_CLASS_QUESTION | MESSAGE_CLASS_NOTIFY,
            "bridgeUp",
            None,
        );
        assert_eq!(
            serde_json::to_value(row).unwrap(),
            serde_json::json!({
                "address": "discord:1/2",
                "label": "Discord — 1/2",
                "provider": "discord",
                "deliverability": "ready",
                "target": "cairn://p/cairn/4026",
                "classes": ["question", "notify"],
                "capabilities": {
                    "append_to_conversation": true,
                    "create_into_container": true
                }
            })
        );
    }
}
