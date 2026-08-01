//! Provider-neutral primitives for delivering human attention gates externally.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

static IMESSAGE_RUNTIME: OnceLock<Mutex<Option<Arc<imessage::IMessageProvider>>>> = OnceLock::new();

fn runtime_slot() -> &'static Mutex<Option<Arc<imessage::IMessageProvider>>> {
    IMESSAGE_RUNTIME.get_or_init(|| Mutex::new(None))
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRuntimeStatus {
    pub provider: &'static str,
    pub state: &'static str,
    pub detail: Option<String>,
    pub unsolicited_inbound: Vec<ChannelInboundSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelInboundSummary {
    pub id: String,
    pub sender: String,
    pub text: String,
    pub received_at: i64,
}

pub async fn runtime_status(orch: &crate::orchestrator::Orchestrator) -> ChannelRuntimeStatus {
    let health = runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned")
        .as_ref()
        .map(|provider| provider.health());
    let (state, detail) = match health {
        Some(ChannelHealth::Ready) => ("bridgeUp", None),
        Some(ChannelHealth::Degraded { reason }) => ("degraded", Some(reason)),
        Some(ChannelHealth::Unavailable { reason }) if reason.contains("offline") => {
            ("executorOffline", Some(reason))
        }
        Some(ChannelHealth::Unavailable { reason }) => ("dead", Some(reason)),
        None => ("dead", Some("channel is not running".into())),
    };
    let unsolicited_inbound = ledger::list_inbound(&orch.db.local, "imessage", 50)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|record| ChannelInboundSummary {
            id: record.id,
            sender: record.sender,
            text: record.text,
            received_at: record.received_at,
        })
        .collect();
    ChannelRuntimeStatus {
        provider: "imessage",
        state,
        detail,
        unsolicited_inbound,
    }
}

pub mod imessage;
pub mod ledger;
pub mod router;

/// Starts the configured external channel service on runner hosts.
pub fn spawn_configured(orch: crate::orchestrator::Orchestrator) {
    tokio::spawn(async move {
        loop {
            let config = crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .imessage;
            if config.enabled && !config.to.trim().is_empty() {
                spawn_imessage(orch.clone(), config).await;
            } else {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    });
}

async fn spawn_imessage(
    orch: crate::orchestrator::Orchestrator,
    config: crate::models::IMessageChannelConfig,
) {
    use cairn_common::executor_protocol::{OwnerDeathPolicy, ResidencyFootprint};

    let Some(name) = config.executor.as_deref() else {
        log::error!(
            "iMessage channel requires a named executor so Messages account and TCC identity are explicit; channel remains stopped"
        );
        return;
    };
    let executor: Arc<dyn imessage::IMessageExecutor> =
        match crate::fleet::service_placement::acquire_service_lease(
            &orch,
            "channel-imessage",
            name,
            ResidencyFootprint {
                memory_bytes: 64 * 1024 * 1024,
                disk_growth_bytes: 64 * 1024 * 1024,
            },
            OwnerDeathPolicy {
                heartbeat_timeout_ms: 5 * 60 * 1000,
                reclaim_grace_ms: 60 * 1000,
            },
        )
        .await
        {
            Ok(lease) => {
                let lease = Arc::new(lease);
                lease.spawn_renewal();
                Arc::new(imessage::PlacedProcessExecutor::new(lease))
            }
            Err(error) => {
                log::error!(
                    "iMessage channel executor `{name}` is unavailable; channel remains stopped: {error}"
                );
                return;
            }
        };
    let provider = Arc::new(imessage::IMessageProvider::new(
        executor.clone(),
        config.allow_from.clone(),
    ));
    *runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned") = Some(provider.clone());
    let health_task = provider.spawn_health_monitor();
    let watch_provider = provider.clone();
    let cursor_db = orch.db.local.clone();
    let watch_task = tokio::spawn(async move {
        loop {
            let since = ledger::get_cursor(&cursor_db, "imessage")
                .await
                .ok()
                .flatten()
                .unwrap_or(0);
            let (cursor_tx, mut cursor_rx) = tokio::sync::mpsc::channel(128);
            let mut watch = watch_provider.spawn_watch(since, cursor_tx);
            let failure = loop {
                tokio::select! {
                    rowid = cursor_rx.recv() => match rowid {
                        Some(rowid) => {
                            if let Err(error) = ledger::advance_cursor(&cursor_db, "imessage", rowid).await {
                                log::warn!("failed to persist iMessage cursor: {error}");
                            }
                        }
                        None => break watch.await,
                    },
                    result = &mut watch => break result,
                }
            };
            while let Ok(rowid) = cursor_rx.try_recv() {
                if let Err(error) = ledger::advance_cursor(&cursor_db, "imessage", rowid).await {
                    log::warn!("failed to persist iMessage cursor: {error}");
                }
            }
            match failure {
                Ok(Ok(())) => log::warn!("iMessage watch ended; restarting from durable cursor"),
                Ok(Err(error)) => {
                    log::warn!("iMessage watch failed; restarting from durable cursor: {error}")
                }
                Err(error) => log::warn!(
                    "iMessage watch task failed; restarting from durable cursor: {error}"
                ),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let mut tasks = vec![health_task, watch_task];
    tasks.extend(router::spawn(
        orch.clone(),
        provider.clone(),
        config.clone(),
    ));

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let current = crate::config::settings::load_settings(&orch.config_dir)
            .channels
            .imessage;
        if current != config {
            for task in tasks {
                task.abort();
            }
            executor.shutdown().await;
            let mut runtime = runtime_slot()
                .lock()
                .expect("channel runtime lock poisoned");
            if runtime
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &provider))
            {
                *runtime = None;
            }
            return;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCapabilities {
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
        bound_guid: String,
        sender: String,
        option_id: String,
        option_text: String,
    },
    Reply {
        bound_guid: String,
        sender: String,
        text: String,
    },
    Bare {
        sender: String,
        text: String,
    },
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
