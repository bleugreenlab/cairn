//! Opt-in live cache probe. Five calls, 64 output tokens each, no retries.
//! Run on the credential-owning host, never transfer its key to an executor:
//! cargo run -p cairn-models --example openrouter_cache_probe -- <config-dir>
//! Alternatively set OPENROUTER_API_KEY and omit the config directory.
//! Uses the same request policy and FamilyTurn as Cairn, without a desktop/DB.
use cairn_models::accounts::{store::load_credential_store, ApiProvider, ProviderAuth};
use cairn_models::complete::HttpComplete;
use cairn_models::interface::complete::{Complete, CompletionCall, CompletionWire};
use cairn_models::turn::{ChatFamily, FamilyTurn, ToolDispatch, TurnEffects};
use cairn_models::wire::openai_compat::{
    http::{ChatDialect, ChatReasoning, Endpoint},
    wire::ChatMessage,
};
use cairn_models::wire::stream::{StreamSink, StreamSummary};
use cairn_models::wire::{GenerationConfig, TurnToolCall, TurnUsage};
use serde_json::{json, Map};
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

const MODEL: &str = "anthropic/claude-haiku-5.5";

struct Sink;
impl StreamSink for Sink {
    fn append(&mut self, _: &str) -> Result<(), String> {
        Ok(())
    }
    fn append_thinking(&mut self, _: &str) -> Result<(), String> {
        Ok(())
    }
    fn finalize(&mut self, _: StreamSummary) -> Result<(), String> {
        Ok(())
    }
}
#[derive(Default)]
struct Effects {
    usage: Option<TurnUsage>,
}
impl TurnEffects for Effects {
    fn stream_sink(&mut self) -> Box<dyn StreamSink + '_> {
        Box::new(Sink)
    }
    fn execute_tool(&mut self, _: &TurnToolCall, _: bool) -> Result<ToolDispatch, String> {
        Err("probe must not call tools".into())
    }
    fn context_snapshot(&mut self, usage: &TurnUsage) {
        self.usage = Some(usage.clone());
    }
}

fn prompt(kind: &str, nonce: &str) -> String {
    let mut text = format!("Cache probe {kind} {nonce}. Reply exactly OK to every question. Reference data follows; do not summarize it.\n");
    for i in 0..400 {
        text.push_str(&format!("Reference entry {i}: The observatory records the sky at dawn, measures the river at noon, and checks the instruments before night.\n"));
    }
    text
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = match std::env::var("OPENROUTER_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            let dir = std::env::args()
                .nth(1)
                .ok_or("set OPENROUTER_API_KEY or pass the credential config directory")?;
            let store =
                load_credential_store(std::path::Path::new(&dir))?.ok_or("no credential store")?;
            store
                .accounts_for_provider(ApiProvider::OpenRouter, None)
                .into_iter()
                .find(|account| account.serves_backend("openrouter"))
                .and_then(|account| match &account.auth {
                    ProviderAuth::ApiKey { value } => Some(value.clone()),
                    _ => None,
                })
                .ok_or("no OpenRouter API key")?
        }
    };
    let family = ChatFamily {
        endpoint: Endpoint {
            provider_name: "OpenRouter".into(),
            backend_key: "openrouter".into(),
            chat_url: "https://openrouter.ai/api/v1/chat/completions".into(),
            headers: cairn_models::openrouter::headers(
                &key,
                &cairn_models::openrouter::Attribution::cairn(),
            ),
        },
        dialect: ChatDialect {
            max_tokens: Some(64),
            include_usage: false,
            reasoning: ChatReasoning::Object,
            ..ChatDialect::default()
        },
        extras: Map::from_iter([("provider".into(), json!({"require_parameters":true}))]),
    };
    let config = GenerationConfig {
        reasoning_effort: Some("none".into()),
        ..Default::default()
    };
    let nonce = uuid::Uuid::new_v4().to_string();
    let conversation = vec![
        ChatMessage::system(prompt("complete", &nonce)),
        ChatMessage::user("Reply OK.".into()),
    ];
    let mut total_cost = 0.0;
    for call in 1..=2 {
        let body = family.request_body(&family.dialect, MODEL, &conversation, &config, false);
        let reply = HttpComplete
            .complete(CompletionCall::whole(
                CompletionWire::Chat(&family.endpoint),
                body,
                Duration::from_secs(90),
            ))?
            .into_chat()?;
        let usage = reply.usage.ok_or("missing completion usage")?;
        println!(
            "{}",
            json!({"probe":"complete","call":call,"model":reply.model,"usage":usage})
        );
        total_cost += usage.cost.unwrap_or(0.0);
        if total_cost >= 0.75 {
            return Err("probe spend guard reached".into());
        }
    }
    let mut conversation = vec![ChatMessage::system(prompt("session", &nonce))];
    for turn in 1..=3 {
        conversation.push(ChatMessage::user(format!("Turn {turn}: Reply OK.")));
        let mut host = FamilyTurn {
            family: family.clone(),
            model: MODEL.into(),
            config: config.clone(),
            context_window: None,
            conversation,
            effects: Effects::default(),
            complete: &HttpComplete,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        host.run()?;
        let usage = host.effects.usage.ok_or("missing session usage")?;
        println!("{}", json!({"probe":"session","turn":turn,"usage":usage}));
        total_cost += usage.cost.unwrap_or(0.0);
        if total_cost >= 0.75 {
            return Err("probe spend guard reached".into());
        }
        conversation = host.conversation;
    }
    println!("{}", json!({"totalCostUsd":total_cost}));
    Ok(())
}
