use crate::{
    channels::{
        bindings::{
            canonical_conversation, legacy_conversation, MESSAGE_CLASS_AGENT_TEXT,
            MESSAGE_CLASS_NOTIFY, MESSAGE_CLASS_PERMISSION, MESSAGE_CLASS_QUESTION,
        },
        conversation_capabilities, ledger, runtime_status, ConversationAddress,
        ConversationCapabilities, ConversationDestination, ConversationProvider,
    },
    orchestrator::Orchestrator,
};
use cairn_common::query::QueryParam;
use serde::Serialize;

/// The provider runtime state that is fully up.
const RUNTIME_UP: &str = "bridgeUp";
/// Running, with a gap its detail names. The router still delivers through it
/// (Discord's card-pin permission gap is decoration: cards keep updating
/// unpinned), so it admits agent messages as `bridgeUp` does.
const RUNTIME_DEGRADED: &str = "degraded";

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
        (MESSAGE_CLASS_AGENT_TEXT, "agent_text"),
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
    pub failed_count: i64,
    /// A copy-paste write that sends an agent message here, present exactly when
    /// [`ConversationRow::sendable`] holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send: Option<String>,
    /// Set (and serialized) only on a follow row's placeholder address, which
    /// stands for the follow stream rather than naming a destination, so nothing
    /// can ever be sent to it whatever its classes say.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub placeholder: bool,
    /// The binding's raw class bits; `classes` is their rendering.
    #[serde(skip)]
    message_classes: i64,
    /// The provider runtime's state and detail, from the same status read that
    /// computes `deliverability`.
    #[serde(skip)]
    runtime_state: &'static str,
    #[serde(skip)]
    runtime_detail: Option<String>,
}

impl ConversationRow {
    fn grants_agent_text(&self) -> bool {
        self.message_classes & MESSAGE_CLASS_AGENT_TEXT != 0
    }

    fn runtime_running(&self) -> bool {
        matches!(self.runtime_state, RUNTIME_UP | RUNTIME_DEGRADED)
    }

    /// Whether an agent may send free text here: the operator granted
    /// `agent_text`, the provider runtime is running (up or degraded), and the
    /// address is a real destination. Degradation never withholds a send:
    /// neither a provider-level gap the runtime keeps delivering through nor a
    /// prior outbound failure, which refusing on would let one failure silence
    /// the conversation. Only a stopped, dead, offline, or invalid runtime does.
    /// The one predicate drives both the `send` affordance and write admission.
    pub fn sendable(&self) -> bool {
        self.grants_agent_text() && self.runtime_running() && !self.placeholder
    }

    fn with_send_affordance(mut self) -> Self {
        self.send = self.sendable().then(|| send_example(&self.address));
        self
    }
}

fn send_example(address: &str) -> String {
    format!(
        "write({{changes:[{{target:\"cairn://channels/conversations\",mode:\"append\",payload:{{conversation:\"{address}\",content:\"...\"}}}}]}})"
    )
}

/// A follow row's conversation is the provider's placeholder address
/// (`imessage:legacy`, `telegram:0`, `discord:0/0`).
fn is_placeholder(address: &ConversationAddress) -> bool {
    legacy_conversation(address.provider().id())
        .is_ok_and(|placeholder| placeholder == address.to_string())
}

/// The operator's grant or revocation of `agent_text` on one binding.
///
/// This is the only writer of a binding's classes outside the reconciler and
/// migrations, and it is deliberately unreachable from any `cairn://` write: the
/// runner exposes it only as the `set_conversation_agent_text` invoke, which
/// refuses a caller without the desktop operator credential. An agent can
/// therefore never grant itself a conversation.
///
/// Granting on a follow placeholder is refused because the placeholder names
/// no destination, so the grant could never deliver anything. Revoking there is
/// allowed: it only removes an inert bit.
pub async fn set_agent_text(
    db: &crate::storage::LocalDb,
    conversation: &str,
    target: &str,
    granted: bool,
) -> Result<(), String> {
    let address = conversation
        .parse::<ConversationAddress>()
        .map_err(|error| format!("'{conversation}' is not a conversation address: {error}"))?;
    let provider = address.provider().id();
    if granted && is_placeholder(&address) {
        return Err(format!(
            "{conversation} is {provider}'s follow placeholder, which names no destination, so agent text granted there could never be delivered"
        ));
    }
    if !ledger::set_message_class(
        db,
        provider,
        conversation,
        target,
        MESSAGE_CLASS_AGENT_TEXT,
        granted,
    )
    .await?
    {
        return Err(format!("{conversation} is not bound to {target}"));
    }
    Ok(())
}

fn row_for_status(
    address: ConversationAddress,
    target: String,
    message_classes: i64,
    state: &'static str,
    detail: Option<String>,
    last_error: Option<String>,
    failed_count: i64,
) -> ConversationRow {
    let deliverability = if state == RUNTIME_UP {
        if last_error.is_some() {
            ConversationDeliverability::Degraded
        } else {
            ConversationDeliverability::Ready
        }
    } else if state == RUNTIME_DEGRADED {
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
        failed_count,
        send: None,
        placeholder: is_placeholder(&address),
        message_classes,
        runtime_state: state,
        runtime_detail: detail,
    }
    .with_send_affordance()
}

/// The slice of a provider's runtime status that every one of its conversation
/// rows carries.
#[derive(Debug, Clone)]
struct ProviderRowStatus {
    state: &'static str,
    detail: Option<String>,
    last_send_error: Option<String>,
    failed_count: i64,
}

pub async fn configured_conversations(orch: &Orchestrator) -> Vec<ConversationRow> {
    let channels = crate::config::settings::load_settings(&orch.config_dir).channels;
    let bindings = match ledger::list_active_bindings(&orch.db.local).await {
        Ok(bindings) => bindings,
        Err(_) => return vec![],
    };
    conversation_rows(
        bindings,
        |provider| match provider {
            ConversationProvider::IMessage => channels.imessage.enabled,
            ConversationProvider::Telegram => channels.telegram.enabled,
            ConversationProvider::Discord => channels.discord.enabled,
        },
        |provider| async move {
            let status = runtime_status(orch, provider.id()).await;
            ProviderRowStatus {
                state: status.state,
                detail: status.detail,
                last_send_error: status.last_send_error,
                failed_count: status.failed_count,
            }
        },
    )
    .await
}

/// Project active bindings onto conversation rows. A provider's status depends
/// only on the provider, and reading it costs several database queries, so it is
/// looked up once per enabled provider and shared by all of that provider's
/// rows -- Discord alone carries a structural binding per issue thread.
async fn conversation_rows<Status, StatusFuture>(
    bindings: Vec<ledger::ActiveBinding>,
    enabled: impl Fn(ConversationProvider) -> bool,
    mut status_of: Status,
) -> Vec<ConversationRow>
where
    Status: FnMut(ConversationProvider) -> StatusFuture,
    StatusFuture: std::future::Future<Output = ProviderRowStatus>,
{
    let mut statuses = std::collections::HashMap::<ConversationProvider, ProviderRowStatus>::new();
    let mut rows = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let provider = match binding.provider.as_str() {
            "imessage" => ConversationProvider::IMessage,
            "telegram" => ConversationProvider::Telegram,
            "discord" => ConversationProvider::Discord,
            _ => continue,
        };
        if !enabled(provider) {
            continue;
        }
        let status = match statuses.get(&provider) {
            Some(status) => status.clone(),
            None => {
                let status = status_of(provider).await;
                statuses.insert(provider, status.clone());
                status
            }
        };
        match binding.conversation.parse::<ConversationAddress>() {
            Ok(address) => rows.push(row_for_status(
                address,
                binding.target_uri,
                binding.message_classes,
                status.state,
                status.detail,
                status.last_send_error,
                status.failed_count,
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
                failed_count: status.failed_count,
                send: None,
                placeholder: false,
                message_classes: binding.message_classes,
                runtime_state: "invalid",
                runtime_detail: None,
            }),
        }
    }
    rows
}

/// A send admitted to a conversation: its parsed canonical address, and the
/// enumeration row that admitted it (whose `last_error`, if any, is a prior
/// outbound failure the caller reports as a warning).
#[derive(Debug, Clone)]
pub struct AdmittedSend {
    pub address: ConversationAddress,
    pub row: ConversationRow,
}

/// Admit an agent message to `conversation` against the deliverable-conversation
/// enumeration, which is the only source of "may Cairn send here".
pub async fn admit_send(orch: &Orchestrator, conversation: &str) -> Result<AdmittedSend, String> {
    admit_send_to(&configured_conversations(orch).await, conversation)
}

/// The admission decision over an already-read enumeration. Every refusal names
/// what would have been accepted.
fn admit_send_to(rows: &[ConversationRow], conversation: &str) -> Result<AdmittedSend, String> {
    let address = conversation.parse::<ConversationAddress>().map_err(|error| {
        format!(
            "payload.conversation '{conversation}' is not a conversation address ({error}); expected a canonical provider-prefixed address such as discord:<guild>/<channel>, telegram:<chat-id>, or imessage:<handle>, exactly as cairn://channels/conversations lists it"
        )
    })?;
    let canonical = canonical_conversation(address.provider().id(), conversation)
        .map_err(|error| format!("payload.conversation '{conversation}': {error}"))?;

    let bound = rows
        .iter()
        .filter(|row| row.address == canonical)
        .collect::<Vec<_>>();
    if let Some(row) = bound.iter().find(|row| row.sendable()) {
        return Ok(AdmittedSend {
            address,
            row: (*row).clone(),
        });
    }
    let Some(row) = bound
        .iter()
        .find(|row| row.grants_agent_text())
        .or_else(|| bound.first())
    else {
        let mut sendable = rows
            .iter()
            .filter(|row| row.sendable())
            .map(|row| row.address.as_str())
            .collect::<Vec<_>>();
        sendable.dedup();
        let accepted = if sendable.is_empty() {
            "no conversation is currently sendable".to_string()
        } else {
            format!("sendable conversations: {}", sendable.join(", "))
        };
        return Err(format!(
            "{canonical} has no active binding on an enabled provider, and agent messages go only to bound conversations; {accepted} (read cairn://channels/conversations)"
        ));
    };
    if row.placeholder {
        return Err(format!(
            "{canonical} is {}'s follow placeholder, which stands for the operator's follow stream rather than naming a deliverable conversation; agent messages need a real conversation address bound with agent_text (read cairn://channels/conversations for rows that carry send)",
            row.provider
        ));
    }
    if !row.grants_agent_text() {
        return Err(format!(
            "agent messages are not enabled for {canonical} (classes: {}). Discord thread and issue channels carry them by default; only the operator can enable them for this conversation, under Settings > Channels.",
            if row.classes.is_empty() {
                "none".to_string()
            } else {
                row.classes.join(", ")
            }
        ));
    }
    Err(match row.runtime_detail.as_deref() {
        Some(detail) => format!(
            "{} is {}: {detail}; agent messages are admitted only while the provider is running",
            row.provider, row.runtime_state
        ),
        None => format!(
            "{} is {}; agent messages are admitted only while the provider is running",
            row.provider, row.runtime_state
        ),
    })
}

/// Refuse content the provider would truncate, given the overflow its own
/// renderer reports for the message as it will actually be sent (attribution
/// line included). Providers cut over-long text rather than splitting it, so
/// admitting it would silently drop the tail while the send still reports
/// success.
pub fn check_send_length(
    provider: &str,
    overflow: usize,
    max_text_len: Option<usize>,
) -> Result<(), String> {
    if overflow == 0 {
        return Ok(());
    }
    let limit = max_text_len
        .map(|limit| format!(" {limit}-character"))
        .unwrap_or_default();
    Err(format!(
        "payload.content does not fit {provider}'s{limit} message limit once rendered under its attribution line; the provider would cut the last {overflow} characters, so shorten it by at least {overflow} characters"
    ))
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
    use crate::channels::bindings::MESSAGE_CLASSES_ALL;

    fn row(
        address: ConversationAddress,
        classes: i64,
        state: &'static str,
        detail: Option<&str>,
        last_error: Option<&str>,
    ) -> ConversationRow {
        row_for_status(
            address,
            "cairn://p/cairn/general".into(),
            classes,
            state,
            detail.map(str::to_string),
            last_error.map(str::to_string),
            i64::from(last_error.is_some()),
        )
    }

    #[test]
    fn runtime_and_outbound_failure_combine_into_deliverability() {
        let ready = ConversationAddress::telegram("123").unwrap();
        assert_eq!(
            row(ready.clone(), 1, "bridgeUp", None, None).deliverability,
            ConversationDeliverability::Ready
        );
        assert_eq!(
            row(ready.clone(), 1, "bridgeUp", None, Some("send failed")).deliverability,
            ConversationDeliverability::Degraded
        );
        assert_eq!(
            row(ready.clone(), 1, "degraded", None, None).deliverability,
            ConversationDeliverability::Degraded
        );
        assert_eq!(
            row(ready, 1, "stopped", None, None).deliverability,
            ConversationDeliverability::Stopped
        );
    }

    #[test]
    fn row_shape_uses_stable_field_names() {
        let row = row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASS_QUESTION | MESSAGE_CLASS_NOTIFY,
            "bridgeUp",
            None,
            None,
        );
        assert_eq!(
            serde_json::to_value(row).unwrap(),
            serde_json::json!({
                "address": "discord:1/2",
                "label": "Discord — 1/2",
                "provider": "discord",
                "deliverability": "ready",
                "target": "cairn://p/cairn/general",
                "classes": ["question", "notify"],
                "capabilities": {
                    "append_to_conversation": true,
                    "create_into_container": true
                },
                "failed_count": 0
            })
        );
    }

    #[test]
    fn a_sendable_row_advertises_the_send_write_and_its_grant() {
        let sendable = row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            "bridgeUp",
            None,
            None,
        );
        let value = serde_json::to_value(&sendable).unwrap();
        assert_eq!(
            value["classes"],
            serde_json::json!(["question", "permission", "notify", "agent_text"])
        );
        assert_eq!(
            value["send"],
            "write({changes:[{target:\"cairn://channels/conversations\",mode:\"append\",payload:{conversation:\"discord:1/2\",content:\"...\"}}]})"
        );
        // Internal admission state never leaks into the projection.
        for hidden in ["message_classes", "runtime_state", "runtime_detail"] {
            assert!(value.get(hidden).is_none(), "{hidden} must not serialize");
        }

        // Degraded only by a prior outbound failure: still sendable.
        assert!(row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASS_AGENT_TEXT,
            "bridgeUp",
            None,
            Some("send failed"),
        )
        .send
        .is_some());

        for unsendable in [
            row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASSES_ALL,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                "dead",
                Some("channel is not running"),
                None,
            ),
            row(
                ConversationAddress::telegram("0").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                "bridgeUp",
                None,
                None,
            ),
        ] {
            assert!(!unsendable.sendable());
            assert!(serde_json::to_value(&unsendable)
                .unwrap()
                .get("send")
                .is_none());
        }
    }

    #[test]
    fn a_degraded_provider_still_admits_agent_text_and_only_a_stopped_one_refuses() {
        let pin_gap = "Cards are not being pinned: Missing Permissions in Discord channel 2";
        let degraded = row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASS_AGENT_TEXT,
            "degraded",
            Some(pin_gap),
            None,
        );
        assert_eq!(
            degraded.deliverability,
            ConversationDeliverability::Degraded
        );
        assert!(degraded.send.is_some());
        let admitted = admit_send_to(std::slice::from_ref(&degraded), "discord:1/2").unwrap();
        assert_eq!(admitted.address.to_string(), "discord:1/2");

        for (state, detail) in [
            ("stopped", None),
            ("dead", Some("channel is not running")),
            ("executorOffline", Some("executor offline")),
        ] {
            let down = row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                state,
                detail,
                None,
            );
            assert!(down.send.is_none(), "{state}");
            let refusal = admit_send_to(&[down], "discord:1/2").unwrap_err();
            assert!(
                refusal.starts_with(&format!("discord is {state}"))
                    && refusal.contains("admitted only while the provider is running"),
                "{refusal}"
            );
        }
    }

    #[tokio::test]
    async fn an_ancient_failure_the_provider_has_sent_past_reads_ready() {
        let db = crate::storage::migrated_test_db("channels-ancient-failure.db").await;
        let outbound =
            |id: &'static str, binding_ref: &'static str, created_at| ledger::NewOutbound {
                id,
                channel: "discord",
                kind: "review",
                binding_ref,
                conversation: "discord:1/2",
                job_id: None,
                rendered_text: "Ready for review",
                rendering: "text",
                created_at,
            };
        // Weeks ago: a review notice into a since-locked thread failed.
        assert!(
            ledger::insert_intent(&db, &outbound("locked", "review-1", 1_000))
                .await
                .unwrap()
        );
        assert!(ledger::begin_delivery(&db, "locked", 1_100).await.unwrap());
        assert!(
            ledger::mark_failed(&db, "locked", "thread is locked", 1_200, None)
                .await
                .unwrap()
        );
        // Since then, the provider has delivered.
        for (id, binding_ref, at) in [
            ("later-1", "review-2", 10_000),
            ("later-2", "review-3", 20_000),
        ] {
            assert!(ledger::insert_intent(&db, &outbound(id, binding_ref, at))
                .await
                .unwrap());
            assert!(ledger::mark_sent(&db, id, id, None, None, at + 1)
                .await
                .unwrap());
        }

        let (last_error, undelivered) = crate::channels::outbound_status(&db, "discord").await;
        let rendered = serde_json::to_value(row_for_status(
            ConversationAddress::discord("1", "2").unwrap(),
            "cairn://p/cairn/general".into(),
            MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            RUNTIME_UP,
            None,
            last_error,
            undelivered.failed,
        ))
        .unwrap();
        assert_eq!(rendered["deliverability"], "ready", "{rendered}");
        assert_eq!(rendered["failed_count"], 0, "{rendered}");
        assert!(rendered.get("last_error").is_none(), "{rendered}");
        assert!(rendered.get("send").is_some(), "{rendered}");
    }

    fn enumeration() -> Vec<ConversationRow> {
        vec![
            row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::discord("1", "3").unwrap(),
                MESSAGE_CLASSES_ALL,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::telegram("0").unwrap(),
                MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::telegram("42").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                "dead",
                Some("channel is not running"),
                None,
            ),
        ]
    }

    #[test]
    fn admission_accepts_only_a_sendable_bound_row() {
        let admitted = admit_send_to(&enumeration(), "discord:1/2").unwrap();
        assert_eq!(admitted.address.to_string(), "discord:1/2");
        assert!(admitted.row.last_error.is_none());
    }

    #[test]
    fn every_refusal_names_what_would_be_accepted() {
        let rows = enumeration();
        let refusal = |conversation: &str| admit_send_to(&rows, conversation).unwrap_err();

        let not_canonical = refusal("Discord:1/2");
        assert!(
            not_canonical.contains("expected discord:1/2"),
            "{not_canonical}"
        );
        let unprefixed = refusal("42/7");
        assert!(
            unprefixed.contains("discord:<guild>/<channel>"),
            "{unprefixed}"
        );
        let unbound = refusal("discord:9/9");
        assert!(
            unbound.contains("no active binding") && unbound.contains("discord:1/2"),
            "{unbound}"
        );
        let placeholder = refusal("telegram:0");
        assert!(placeholder.contains("placeholder"), "{placeholder}");
        let not_granted = refusal("discord:1/3");
        assert!(
            not_granted.contains("agent messages are not enabled for discord:1/3")
                && not_granted.contains("question, permission, notify")
                && not_granted.contains("only the operator can enable them"),
            "{not_granted}"
        );
        let down = refusal("telegram:42");
        assert!(
            down.contains("telegram is dead: channel is not running"),
            "{down}"
        );
    }

    #[test]
    fn only_a_placeholder_row_serializes_the_placeholder_flag() {
        let placeholder = row(
            ConversationAddress::telegram("0").unwrap(),
            MESSAGE_CLASSES_ALL,
            "bridgeUp",
            None,
            None,
        );
        assert_eq!(
            serde_json::to_value(&placeholder).unwrap()["placeholder"],
            true
        );
        let real = row(
            ConversationAddress::telegram("42").unwrap(),
            MESSAGE_CLASSES_ALL,
            "bridgeUp",
            None,
            None,
        );
        assert!(serde_json::to_value(&real)
            .unwrap()
            .get("placeholder")
            .is_none());
    }

    #[tokio::test]
    async fn the_operator_grant_moves_agent_text_and_refuses_what_cannot_deliver() {
        use crate::channels::bindings::BindingKind;
        let db = crate::storage::migrated_test_db("channels-operator-agent-text.db").await;
        assert!(
            ledger::follow_target(&db, "telegram", "cairn://p/cairn/1", 1, 0)
                .await
                .unwrap()
        );
        assert!(ledger::bind_target(
            &db,
            "discord",
            "discord:1/2",
            "cairn://p/cairn/general",
            BindingKind::Structural,
            MESSAGE_CLASSES_ALL,
            1,
            0,
        )
        .await
        .unwrap());
        let classes = |conversation: &'static str| {
            let db = &db;
            async move {
                db.query_opt_i64(
                    "SELECT message_classes FROM channel_conversation_binding WHERE conversation = ?1",
                    (conversation,),
                )
                .await
                .unwrap()
            }
        };

        set_agent_text(&db, "discord:1/2", "cairn://p/cairn/general", true)
            .await
            .unwrap();
        assert_eq!(
            classes("discord:1/2").await,
            Some(MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT)
        );
        set_agent_text(&db, "discord:1/2", "cairn://p/cairn/general", false)
            .await
            .unwrap();
        assert_eq!(classes("discord:1/2").await, Some(MESSAGE_CLASSES_ALL));

        let placeholder = set_agent_text(&db, "telegram:0", "cairn://p/cairn/1", true)
            .await
            .unwrap_err();
        assert!(placeholder.contains("follow placeholder"), "{placeholder}");
        assert_eq!(classes("telegram:0").await, Some(MESSAGE_CLASSES_ALL));
        // Revoking on a placeholder only clears an inert bit, so it is allowed.
        set_agent_text(&db, "telegram:0", "cairn://p/cairn/1", false)
            .await
            .unwrap();

        let unbound = set_agent_text(&db, "discord:1/9", "cairn://p/cairn/general", true)
            .await
            .unwrap_err();
        assert!(unbound.contains("not bound"), "{unbound}");
        assert!(
            set_agent_text(&db, "Discord:1/2", "cairn://p/cairn/general", true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn provider_status_is_looked_up_once_per_enabled_provider_not_per_binding() {
        use crate::channels::bindings::BindingKind;
        let db = crate::storage::migrated_test_db("channels-status-per-provider.db").await;
        let bind = |provider: &'static str, conversation: String| {
            let db = &db;
            async move {
                assert!(ledger::bind_target(
                    db,
                    provider,
                    &conversation,
                    "cairn://p/cairn/general",
                    BindingKind::Structural,
                    MESSAGE_CLASSES_ALL,
                    1,
                    0,
                )
                .await
                .unwrap());
            }
        };
        for channel in 0..200 {
            bind("discord", format!("discord:1/{channel}")).await;
        }
        bind("telegram", "telegram:42".into()).await;
        bind("telegram", "telegram:43".into()).await;
        bind("imessage", "imessage:a@example.com".into()).await;
        let bindings = ledger::list_active_bindings(&db).await.unwrap();
        assert_eq!(bindings.len(), 203);

        let mut lookups = std::collections::HashMap::<ConversationProvider, usize>::new();
        let rows = conversation_rows(
            bindings,
            |provider| provider != ConversationProvider::IMessage,
            |provider| {
                *lookups.entry(provider).or_default() += 1;
                async move {
                    match provider {
                        ConversationProvider::Discord => ProviderRowStatus {
                            state: RUNTIME_DEGRADED,
                            detail: Some("cards are not being pinned".into()),
                            last_send_error: Some("thread is locked".into()),
                            failed_count: 3,
                        },
                        _ => ProviderRowStatus {
                            state: RUNTIME_UP,
                            detail: None,
                            last_send_error: None,
                            failed_count: 0,
                        },
                    }
                }
            },
        )
        .await;

        assert_eq!(
            lookups,
            std::collections::HashMap::from([
                (ConversationProvider::Discord, 1),
                (ConversationProvider::Telegram, 1),
            ])
        );
        assert_eq!(
            rows.len(),
            202,
            "the disabled provider's binding is skipped"
        );
        let discord = rows
            .iter()
            .filter(|row| row.provider == "discord")
            .collect::<Vec<_>>();
        assert_eq!(discord.len(), 200);
        for row in discord {
            assert_eq!(row.runtime_state, RUNTIME_DEGRADED);
            assert_eq!(
                row.runtime_detail.as_deref(),
                Some("cards are not being pinned")
            );
            assert_eq!(row.last_error.as_deref(), Some("thread is locked"));
            assert_eq!(row.failed_count, 3);
        }
        for row in rows.iter().filter(|row| row.provider == "telegram") {
            assert_eq!(row.deliverability, ConversationDeliverability::Ready);
            assert_eq!(row.failed_count, 0);
        }
    }

    /// The enumeration's cost at the registry's live shape: 871 active bindings
    /// (discord 863, imessage 6, telegram 2) over a ~6,300-row outbound ledger.
    /// The status lookup issues the same queries `runtime_status` does. Run with
    /// `bun run test:rust -p cairn-core --lib -E 'test(/measure_conversation_enumeration/)' --run-ignored only --no-capture`.
    #[tokio::test]
    #[ignore = "measurement: seeds a live-sized binding registry and outbound ledger"]
    async fn measure_conversation_enumeration_at_live_scale() {
        use crate::channels::{bindings::BindingKind, state_card};
        use std::time::Instant;
        const OUTBOUND: usize = 6_296;

        let db = crate::storage::migrated_test_db("channels-enumeration-scale.db").await;
        let conversation = |provider: &str, n: usize| match provider {
            "discord" => format!("discord:1/{n}"),
            "imessage" => format!("imessage:user{n}@example.com"),
            _ => format!("telegram:{}", 100 + n),
        };
        for (provider, count) in [("discord", 863), ("imessage", 6), ("telegram", 2)] {
            for n in 0..count {
                assert!(ledger::bind_target(
                    &db,
                    provider,
                    &conversation(provider, n),
                    &format!("cairn://p/cairn/{}", n + 1),
                    BindingKind::Structural,
                    MESSAGE_CLASSES_ALL,
                    1,
                    0,
                )
                .await
                .unwrap_or_else(|error| panic!("{provider} binding {n}: {error}")));
            }
        }
        for chunk in (0..OUTBOUND).collect::<Vec<_>>().chunks(500) {
            let values = chunk
                .iter()
                .map(|&n| {
                    let channel = match n % 20 {
                        0 => "imessage",
                        1 => "telegram",
                        _ => "discord",
                    };
                    let to = conversation(channel, 0);
                    if n % 25 == 0 {
                        format!("('o-{n}','{channel}','review','ref-{n}','{to}','text','text','failed',{n},NULL,NULL,'send failed',{n})")
                    } else {
                        format!("('o-{n}','{channel}','review','ref-{n}','{to}','text','text','sent',{n},'g-{n}',{n},NULL,NULL)")
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            db.execute_batch(&format!(
                "INSERT INTO channel_outbound (id, channel, kind, binding_ref, conversation, rendered_text, rendering, status, created_at, provider_guid, sent_at, last_error, last_attempt_at) VALUES {values};"
            ))
            .await
            .unwrap();
        }

        let status_of = |provider: ConversationProvider| {
            let db = &db;
            async move {
                if provider == ConversationProvider::Discord {
                    state_card::latest_pin_failure(db, "discord", 0)
                        .await
                        .unwrap();
                }
                ledger::list_inbound(db, provider.id(), 50).await.unwrap();
                let (last_send_error, undelivered) =
                    crate::channels::outbound_status(db, provider.id()).await;
                ProviderRowStatus {
                    state: RUNTIME_UP,
                    detail: None,
                    last_send_error,
                    failed_count: undelivered.failed,
                }
            }
        };
        let bindings = ledger::list_active_bindings(&db).await.unwrap();
        assert_eq!(bindings.len(), 871);

        // The per-binding lookup the enumeration used to make.
        let started = Instant::now();
        for binding in &bindings {
            let provider = match binding.provider.as_str() {
                "imessage" => ConversationProvider::IMessage,
                "telegram" => ConversationProvider::Telegram,
                _ => ConversationProvider::Discord,
            };
            status_of(provider).await;
        }
        let per_binding = started.elapsed();

        let started = Instant::now();
        let rows = conversation_rows(bindings, |_| true, status_of).await;
        let per_provider = started.elapsed();
        assert_eq!(rows.len(), 871);

        eprintln!(
            "871 bindings, {OUTBOUND} outbound rows: per-binding status {per_binding:?} ({:?}/lookup), per-provider enumeration {per_provider:?}",
            per_binding / 871
        );
    }

    #[test]
    fn content_over_the_provider_limit_is_refused_not_truncated() {
        assert!(check_send_length("discord", 0, Some(2000)).is_ok());
        let refusal = check_send_length("telegram", 25, Some(4096)).unwrap_err();
        assert!(
            refusal.contains("4096-character") && refusal.contains("cut the last 25 characters"),
            "{refusal}"
        );
        assert!(check_send_length("imessage", 0, None).is_ok());
    }
}
