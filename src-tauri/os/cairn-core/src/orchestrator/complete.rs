//! The `run cairn://models/complete` door: one tool-free completion with a
//! model the caller names, without a response definition.
//!
//! The door selects the named model's offer and prepares the request; the
//! rest is a named response's own path ([`crate::responses::invoke_prepared`]):
//! the launch door's verb gate (tenet 1182), the provider's account selection
//! and retry, the caller's bound and interrupt, the output-schema check with
//! its one corrective retry, and the response journal, under
//! [`COMPLETE_RESPONSE_ID`]. Response definitions stay the named, reusable
//! layer over the same verb.

use std::collections::HashMap;
use std::time::Duration;

use cairn_models::interface::{Catalog, ModelScope, Models, NoOffer, OfferQuery};
use cairn_models::types::catalog::VerbKind;
use serde_json::Value;

use super::Orchestrator;
use crate::backends::{CompletionMessage, CompletionRequest, CompletionRole};
use crate::models::{Model, Preset, PresetOptionValue};
use crate::responses::{
    InvocationBound, InvocationInterrupt, OutputContract, PreparedCompletion, ResponseCaller,
    ResponseDefinition,
};

/// The response id every door completion is journaled under, beside the
/// workspace's named responses.
pub(crate) const COMPLETE_RESPONSE_ID: &str = "builtin:models-complete";

/// Bounds each provider request: long enough for a reasoning model's answer,
/// short enough that a hung provider does not hold a run item for hours.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// The per-call options a completion takes, as a response definition's
/// `options` spells them ([`Preset::to_extras`]).
const OPTIONS: &[&str] = &["reasoningEffort", "serviceTier", "fastMode"];

/// How many offers a refusal names as the ones that do serve `complete`.
const ALTERNATIVES_NAMED: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DoorRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DoorMessage {
    pub role: DoorRole,
    pub content: String,
}

/// What `run cairn://models/complete` takes. Nothing here is chosen for the
/// caller: no workspace default, no tier fallback.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CompleteDoorRequest {
    /// Optional only so its absence is refused by name.
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub messages: Option<Vec<DoorMessage>>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub options: HashMap<String, PresetOptionValue>,
    /// A JSON Schema the answer must meet.
    #[serde(default)]
    pub output: Option<Value>,
}

/// Tokens the completion consumed, where the provider reported them.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompleteUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// What the complete door returns.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompleteOutcome {
    pub text: String,
    /// The answer parsed and checked against `output`, when one was given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured: Option<Value>,
    pub model: String,
    pub provider: String,
    pub usage: CompleteUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    pub latency_ms: u64,
    /// The response-history entry this call was journaled as.
    pub history: String,
}

/// The offers serving `complete` in scope, named `provider/model`: at most
/// [`ALTERNATIVES_NAMED`], then how many more. Empty when there are none.
fn complete_offers_named(catalog: &Catalog) -> String {
    let all: Vec<String> = catalog
        .offers_serving(VerbKind::Complete)
        .into_iter()
        .map(|offer| format!("{}/{}", offer.provider, offer.model))
        .collect();
    let mut named = all
        .iter()
        .take(ALTERNATIVES_NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if all.len() > ALTERNATIVES_NAMED {
        named.push_str(&format!(" and {} more", all.len() - ALTERNATIVES_NAMED));
    }
    named
}

/// The conversation `request` asks for, or why it cannot be sent.
fn conversation(request: &CompleteDoorRequest) -> Result<Vec<CompletionMessage>, String> {
    let messages = match (&request.prompt, &request.messages) {
        (Some(_), Some(_)) => {
            return Err("Give `prompt` or `messages`, not both.".to_string())
        }
        (None, None) => {
            return Err(
                "Give the text to complete: `prompt` (one user message) or `messages` (the conversation so far, ending with a user message)."
                    .to_string(),
            )
        }
        (Some(prompt), None) => vec![CompletionMessage {
            role: CompletionRole::User,
            content: prompt.clone(),
        }],
        (None, Some(messages)) => messages
            .iter()
            .map(|message| CompletionMessage {
                role: match message.role {
                    DoorRole::User => CompletionRole::User,
                    DoorRole::Assistant => CompletionRole::Assistant,
                },
                content: message.content.clone(),
            })
            .collect(),
    };
    if let Some(index) = messages
        .iter()
        .position(|message| message.content.trim().is_empty())
    {
        return Err(format!(
            "Message {index} is empty; every message needs content."
        ));
    }
    match messages.last() {
        Some(last) if last.role == CompletionRole::User => Ok(messages),
        Some(_) => Err(
            "`messages` must end with a user message: that is the turn the model answers."
                .to_string(),
        ),
        None => Err("`messages` is empty.".to_string()),
    }
}

impl Orchestrator {
    /// Send one completion with the model `args` names, and only that model:
    /// the door behind `run cairn://models/complete`. `args` is the item's
    /// `payload.args_json`; it is journaled as the call's arguments.
    pub(crate) async fn complete_named(
        &self,
        project_id: Option<&str>,
        caller: ResponseCaller,
        args: Value,
        bound: &InvocationBound,
        interrupt: Option<&InvocationInterrupt>,
    ) -> Result<CompleteOutcome, String> {
        let request: CompleteDoorRequest = serde_json::from_value(args.clone()).map_err(|error| {
            format!(
                "cairn://models/complete takes {{model, prompt | messages, system?, options?, output?}} in payload.args_json: {error}. Read cairn://models/complete for the shape."
            )
        })?;
        let catalog = self
            .catalog(&ModelScope {
                project_id: project_id.map(str::to_string),
            })
            .map_err(|error| format!("the model catalog is unavailable: {error}"))?;
        let Some(model) = request
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
        else {
            let lead = "Name the model to complete with (a catalog id, provider/model, or provider/tier): this door never chooses one";
            let offers = complete_offers_named(&catalog);
            return Err(if offers.is_empty() {
                let nothing = NoOffer {
                    verb: VerbKind::Complete,
                    requested: None,
                    detail: None,
                };
                format!("{lead}. {nothing}")
            } else {
                format!("{lead}; offers that serve complete: {offers}.")
            });
        };
        let offer = catalog
            .offer_for(OfferQuery {
                requested: Some(model),
                ..OfferQuery::new(VerbKind::Complete)
            })
            .map_err(|mut refusal| {
                let offers = complete_offers_named(&catalog);
                if !offers.is_empty() {
                    let suffix = format!("offers that serve complete: {offers}");
                    refusal.detail = Some(match refusal.detail.take() {
                        Some(detail) => format!("{detail}; {suffix}"),
                        None => suffix,
                    });
                }
                refusal.to_string()
            })?;
        let messages = conversation(&request)?;
        if let Some(unknown) = request
            .options
            .keys()
            .find(|key| !OPTIONS.contains(&key.as_str()))
        {
            return Err(format!(
                "Unknown option '{unknown}'; a completion takes {}.",
                OPTIONS.join(", ")
            ));
        }
        if let Some(schema) = &request.output {
            if !schema.is_object() {
                return Err("`output` must be a JSON Schema object.".to_string());
            }
            jsonschema::validator_for(schema)
                .map_err(|error| format!("`output` is not a valid JSON Schema: {error}"))?;
        }

        let timeout = bound.limit.min(REQUEST_TIMEOUT);
        let definition = ResponseDefinition {
            name: "models-complete".to_string(),
            description: String::new(),
            tier: None,
            model: Some(offer.model.clone()),
            backend: Some(offer.provider.clone()),
            options: request.options.clone(),
            variables: Vec::new(),
            output: request
                .output
                .clone()
                .map_or_else(OutputContract::default, OutputContract::Schema),
            timeout,
            examples: Vec::new(),
            template: String::new(),
        };
        let completion = CompletionRequest {
            system: request.system.clone(),
            messages,
            model: offer.model.clone(),
            project_id: project_id.map(str::to_string),
            options: Preset {
                model: Model::new(&offer.model),
                options: request.options.clone(),
            }
            .to_extras(),
            output_schema: request.output.clone(),
            timeout,
        };
        let rendered = match &request.system {
            Some(system) => format!("[system]\n{system}\n\n{}", completion.single_prompt()),
            None => completion.single_prompt(),
        };
        let outcome = crate::responses::invoke_prepared(
            self,
            &definition,
            PreparedCompletion {
                rendered,
                model: Model::new(&offer.model),
                backend: offer.provider.clone(),
                request: completion,
                output_schema: request.output.clone(),
            },
            COMPLETE_RESPONSE_ID,
            caller,
            &args,
            Some(bound),
            interrupt,
        )
        .await
        .map_err(|error| error.to_string())?;
        let history = match self.project_key(project_id).await {
            Some(key) => format!(
                "cairn://p/{key}/responses/{COMPLETE_RESPONSE_ID}/history/{}",
                outcome.seq
            ),
            None => format!(
                "cairn://responses/{COMPLETE_RESPONSE_ID}/history/{}",
                outcome.seq
            ),
        };
        Ok(CompleteOutcome {
            text: outcome.text,
            structured: outcome.parsed,
            model: outcome.model,
            provider: outcome.backend,
            usage: CompleteUsage {
                input_tokens: outcome.input_tokens,
                output_tokens: outcome.output_tokens,
            },
            cost: outcome.cost,
            latency_ms: outcome.latency_ms,
            history,
        })
    }

    /// The key of the project `project_id` names, when it names one.
    async fn project_key(&self, project_id: Option<&str>) -> Option<String> {
        self.db
            .local
            .query_opt(
                "SELECT key FROM projects WHERE id = ?1",
                (project_id?.to_string(),),
                |row| crate::storage::RowExt::text(row, 0),
            )
            .await
            .ok()
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use cairn_models::types::usage::{Charge, Measure, Meter, MeteredUsage};
    use serde_json::{json, Value};

    use super::{CompleteOutcome, COMPLETE_RESPONSE_ID};
    use crate::backends::{CompletionOutcome, CompletionRequest, CompletionRole};
    use crate::credentials::CustomProtocol;
    use crate::orchestrator::Orchestrator;
    use crate::responses::{InvocationBound, ResponseCaller};

    /// A workspace whose one provider serves the chat model `qwen3`, with its
    /// completions answered from `answers` in order and every request kept.
    async fn workspace(
        answers: Vec<&str>,
    ) -> (Orchestrator, Arc<Mutex<Vec<CompletionRequest>>>, String) {
        let (orch, _, key) = crate::orchestrator::decide_fixtures::workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            Vec::new(),
        )
        .await;
        let sent = Arc::new(Mutex::new(Vec::new()));
        let answers = Arc::new(Mutex::new(
            answers
                .into_iter()
                .map(str::to_string)
                .collect::<VecDeque<_>>(),
        ));
        let kept = sent.clone();
        crate::responses::test_completion::install(
            &orch.config_dir,
            Arc::new(move |_backend, request: CompletionRequest, _orch| {
                let model = request.model.clone();
                kept.lock().unwrap().push(request);
                Ok(CompletionOutcome {
                    text: answers
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("an unscripted request"),
                    parsed: None,
                    model,
                    usage: MeteredUsage {
                        measures: vec![
                            Measure {
                                meter: Meter::tokens(Charge::Input),
                                quantity: 12,
                            },
                            Measure {
                                meter: Meter::tokens(Charge::Output),
                                quantity: 3,
                            },
                        ],
                        cost_usd: Some(0.0002),
                    },
                    latency_ms: 40,
                    account_id: None,
                })
            }),
        );
        (orch, sent, key)
    }

    fn bound() -> InvocationBound {
        crate::mcp::handlers::run::response_item_bound(Some(30_000))
    }

    fn caller() -> ResponseCaller {
        ResponseCaller::Internal {
            label: "test".into(),
        }
    }

    async fn complete(orch: &Orchestrator, args: Value) -> Result<CompleteOutcome, String> {
        orch.complete_named(None, caller(), args, &bound(), None)
            .await
    }

    /// The door sends exactly what it was given to the named offer, answers
    /// with the tokens and cost, and journals the call where response history
    /// is read.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_prompt_goes_to_the_named_model_and_is_journaled() {
        let (orch, sent, key) = workspace(vec!["four"]).await;
        let outcome = complete(
            &orch,
            json!({"model": "qwen3", "system": "Answer in one word.", "prompt": "2+2?",
                   "options": {"reasoningEffort": "low"}}),
        )
        .await
        .unwrap();
        assert_eq!(outcome.text, "four");
        assert_eq!(
            (outcome.provider.as_str(), outcome.model.as_str()),
            (key.as_str(), "qwen3")
        );
        assert_eq!(
            (
                outcome.usage.input_tokens,
                outcome.usage.output_tokens,
                outcome.cost
            ),
            (Some(12), Some(3), Some(0.0002))
        );
        assert_eq!(outcome.structured, None);
        assert_eq!(
            outcome.history,
            format!("cairn://responses/{COMPLETE_RESPONSE_ID}/history/1")
        );
        let request = sent.lock().unwrap()[0].clone();
        assert_eq!(request.system.as_deref(), Some("Answer in one word."));
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.options.reasoning_effort.as_deref(), Some("low"));

        let entry = crate::resources::read_cairn_resource(
            &orch,
            &crate::mcp::types::McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: "/tmp".into(),
                run_id: None,
                tool: "read".into(),
                payload: json!({}),
                tool_use_id: None,
                origin: None,
            },
            &outcome.history,
        )
        .await;
        assert!(
            entry.contains("four"),
            "the journal holds the answer: {entry}"
        );
    }

    /// A conversation keeps its turns, and an answer that violates the schema
    /// is retried once naming the violation, then returned parsed.
    #[tokio::test(flavor = "multi_thread")]
    async fn messages_and_a_schema_round_trip_through_the_contract_retry() {
        let (orch, sent, _) = workspace(vec!["not json", r#"{"answer": 4}"#]).await;
        let outcome = complete(
            &orch,
            json!({"model": "qwen3",
                   "messages": [{"role": "user", "content": "2+2?"},
                                {"role": "assistant", "content": "Shall I show working?"},
                                {"role": "user", "content": "No, just the number."}],
                   "output": {"type": "object", "properties": {"answer": {"type": "integer"}},
                              "required": ["answer"]}}),
        )
        .await
        .unwrap();
        assert_eq!(outcome.structured, Some(json!({"answer": 4})));
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2, "one corrective retry");
        assert_eq!(sent[0].messages.len(), 3);
        assert_eq!(sent[0].messages[1].role, CompletionRole::Assistant);
        assert!(sent[0].output_schema.is_some());
        assert!(sent[1]
            .messages
            .last()
            .unwrap()
            .content
            .contains("violated"));
    }

    /// Every refusal comes before anything is sent and says what to change.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_door_refuses_naming_the_fix_and_sends_nothing() {
        let (orch, sent, _) = workspace(Vec::new()).await;
        // The workspace's default providers are offered too, ahead of the hub.
        let serving = "offers that serve complete: claude/";
        for (args, expected) in [
            (json!({"prompt": "hi"}), "Name the model to complete with"),
            (json!({"model": "nowhere/model", "prompt": "hi"}), "no installed provider offers"),
            (json!({"model": "qwen3", "prompt": "hi", "messages": []}), "not both"),
            (json!({"model": "qwen3"}), "Give the text to complete"),
            (
                json!({"model": "qwen3", "messages": [{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"}]}),
                "must end with a user message",
            ),
            (json!({"model": "qwen3", "prompt": "  "}), "Message 0 is empty"),
            (
                json!({"model": "qwen3", "prompt": "hi", "options": {"temperature": 0}}),
                "Unknown option 'temperature'; a completion takes reasoningEffort, serviceTier, fastMode",
            ),
            (json!({"model": "qwen3", "prompt": "hi", "output": "json"}), "must be a JSON Schema object"),
            (
                json!({"model": "qwen3", "prompt": "hi", "output": {"type": "nonsense"}}),
                "not a valid JSON Schema",
            ),
            (json!({"model": "qwen3", "text": "hi"}), "{model, prompt | messages, system?, options?, output?}"),
        ] {
            let refusal = complete(&orch, args.clone()).await.unwrap_err();
            assert!(refusal.contains(expected), "{args}: {refusal}");
            if expected.starts_with("Name") || expected.starts_with("no installed") {
                assert!(refusal.contains(serving), "{args}: {refusal}");
                assert!(refusal.contains(" more"), "{args}: {refusal}");
            }
        }
        assert!(sent.lock().unwrap().is_empty(), "nothing was sent");
    }
}
