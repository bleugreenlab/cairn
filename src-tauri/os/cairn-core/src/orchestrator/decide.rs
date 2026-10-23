//! The orchestrator's side of the cairn-models facet's `decide` verb.
//!
//! The facet measures; this module decides where. It reports what each
//! logprobs offer was seen unable to do into the catalog, resolves an authored
//! model or tier to an offer serving `decide` through the facet's one selector
//! ([`cairn_models::interface::Catalog::offer_for`]), confirms that offer can
//! measure the node's questions, and runs the facet's blocking `decide` off the
//! async runtime. Which providers serve `decide` at all, and over which wire,
//! is each backend's own declaration (`AgentBackend::serves`).
//!
//! Where an offer is reached is its backend's answer
//! (`AgentBackend::decision_route`), from the wire the offer declares: a
//! custom provider's Decisions or OpenAI-chat endpoint, or OpenRouter's
//! System One endpoint for the decision models its listing reports. The
//! subscriptions and OpenCode serve no `decide`, so a workspace with only
//! those refuses with the fixes rather than asking a chat model to report its
//! own probabilities.
//!
//! A chat provider's protocol says only that logprobs *might* be served. So a
//! logprobs offer is admitted (at save, import, validation, and launch) only
//! once it has been seen to answer: [`DecisionObservations`] holds what each
//! one did, from a one-question probe or a real decision. One seen unable
//! leaves the catalog's decision offers, so selection moves past it and every
//! surface stops offering it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::models::decision::{
    question_problems, Decision, DecisionNode, DecisionQuestion, MeasuredBy,
};
use crate::models::RecipeNode;
use cairn_models::interface::decide::{DecisionCall, DecisionRoute};
use cairn_models::interface::{
    Catalog, ModelScope, Models, NoOffer, OfferQuery, SelectedOffer, Unable,
};
use cairn_models::types::catalog::{VerbKind, Wire};

use super::Orchestrator;

/// Bounds each provider request. A native decision answers in under a second
/// and a single logprobs token barely longer; this leaves room for a cold
/// local model without letting a hung server hold the node.
const DECISION_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounds the capability probe, which can run inside a save or a launch.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);

/// How many offers a refusal names as the ones that do serve `decide`.
const ALTERNATIVES_NAMED: usize = 3;

/// How long an observation stands. A server reconfigured to serve logprobs,
/// or a model swapped behind an id, is seen afresh after this.
const OBSERVATION_TTL: Duration = Duration::from_secs(10 * 60);

/// What each logprobs offer, by provider and model, was seen to do.
#[derive(Default)]
pub(crate) struct DecisionObservations {
    seen: Mutex<HashMap<(String, String), Observation>>,
}

struct Observation {
    /// `None` when it answered; otherwise why it cannot.
    unable: Option<String>,
    at: Instant,
}

impl DecisionObservations {
    /// What `offer` was last seen to do, while that still stands: `Some(None)`
    /// when it answered, `Some(Some(why))` when it could not.
    fn get(&self, offer: &SelectedOffer) -> Option<Option<String>> {
        let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.get(&(offer.provider.clone(), offer.model.clone()))
            .filter(|observation| observation.at.elapsed() < OBSERVATION_TTL)
            .map(|observation| observation.unable.clone())
    }

    fn record(&self, offer: &SelectedOffer, unable: Option<String>) {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.insert(
            (offer.provider.clone(), offer.model.clone()),
            Observation {
                unable,
                at: Instant::now(),
            },
        );
    }

    /// The models on `provider` seen unable to decide, each with why, in
    /// model order.
    pub(crate) fn unable(&self, provider: &str) -> Vec<Unable> {
        let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let mut unable: Vec<Unable> = seen
            .iter()
            .filter(|((key, _), observation)| {
                key == provider && observation.at.elapsed() < OBSERVATION_TTL
            })
            .filter_map(|((_, model), observation)| {
                Some(Unable {
                    model: model.clone(),
                    verb: VerbKind::Decide,
                    why: observation.unable.clone()?,
                })
            })
            .collect();
        unable.sort_by(|a, b| a.model.cmp(&b.model));
        unable
    }
}

/// One decision node's standing, as a recipe read reports it.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionDiagnostic {
    pub node_id: String,
    pub node_name: String,
    /// Whether saving or launching the recipe refuses it as it stands. A
    /// non-blocking diagnostic is a note: an offer not yet seen to answer.
    pub blocking: bool,
    pub message: String,
}

/// Why a decision produced no answers, split by what a caller should do
/// about it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DecisionFailure {
    /// Nothing in scope can answer these questions as asked: no offer serves
    /// `decide`, the one selected cannot measure them, none could be
    /// confirmed, or the request itself cannot be asked (no model named,
    /// malformed questions). Only a configuration or request change fixes it,
    /// so retrying is pointless. The text names the fixes.
    Unanswerable(String),
    /// An offer that can answer was found and something went wrong reaching
    /// it: the catalog read, the route, a probe that could not tell, or the
    /// provider call itself.
    Failed(String),
}

impl std::fmt::Display for DecisionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unanswerable(text) | Self::Failed(text) => f.write_str(text),
        }
    }
}

/// What a decision node asks the catalog for: the model it names, or else the
/// workspace's decision default (`decisionModel`), or else the best offer.
fn decision_query<'a>(
    decision: &'a DecisionNode,
    workspace_default: Option<&'a str>,
) -> OfferQuery<'a> {
    let named = decision
        .model
        .as_deref()
        .filter(|model| !model.trim().is_empty());
    OfferQuery {
        requested: named.or(workspace_default),
        ..OfferQuery::new(VerbKind::Decide)
    }
}

/// Why `offer` cannot ask `questions`, when it cannot.
fn unmeasurable_by(offer: &SelectedOffer, questions: &[DecisionQuestion]) -> Option<String> {
    let Some(method) = offer.measured_by() else {
        return Some(format!(
            "{} on {} serves decisions over {}, which measures none",
            offer.model, offer.provider, offer.served.wire
        ));
    };
    let unmeasurable = cairn_models::decide::unmeasurable(questions, method);
    (!unmeasurable.is_empty()).then(|| {
        format!(
            "{} on {} measures decisions by {}, which cannot ask this: {}",
            offer.model,
            offer.provider,
            method,
            unmeasurable.join(" ")
        )
    })
}

/// The offers that could answer `questions` instead, named `provider/model`:
/// at most [`ALTERNATIVES_NAMED`], then how many more. Every offer serving
/// `decide` in scope, whatever provider the request narrowed to, less any
/// that cannot measure these questions. Empty when there are none.
fn alternatives(catalog: &Catalog, questions: &[DecisionQuestion]) -> String {
    let able: Vec<String> = catalog
        .offers_serving(VerbKind::Decide)
        .into_iter()
        .filter(|offer| unmeasurable_by(offer, questions).is_none())
        .map(|offer| format!("{}/{}", offer.provider, offer.model))
        .collect();
    let mut named = able
        .iter()
        .take(ALTERNATIVES_NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if able.len() > ALTERNATIVES_NAMED {
        named.push_str(&format!(" and {} more", able.len() - ALTERNATIVES_NAMED));
    }
    named
}

/// The offer `query` selects for `questions` from `catalog`, or the refusal.
///
/// The one gate every decision passes, before anything is probed: the
/// catalog's selector, then whether the offer can measure these questions.
/// When a model was named and refused, the refusal ends by naming the offers
/// that do serve `decide`, so the fix is in the text.
fn select(
    catalog: &Catalog,
    query: OfferQuery<'_>,
    questions: &[DecisionQuestion],
) -> Result<SelectedOffer, String> {
    let named = query
        .requested
        .is_some_and(|requested| !requested.trim().is_empty());
    let offer = catalog.offer_for(query).map_err(|mut refusal| {
        let alternatives = alternatives(catalog, questions);
        if named && !alternatives.is_empty() {
            let suffix = format!("offers that serve decide: {alternatives}");
            refusal.detail = Some(match refusal.detail.take() {
                Some(detail) => format!("{detail}; {suffix}"),
                None => suffix,
            });
        }
        refusal.to_string()
    })?;
    match unmeasurable_by(&offer, questions) {
        None => Ok(offer),
        Some(refusal) => {
            let alternatives = alternatives(catalog, questions);
            Err(if named && !alternatives.is_empty() {
                format!("{refusal} Offers that serve decide: {alternatives}.")
            } else {
                refusal
            })
        }
    }
}

/// A decision asked through `run cairn://models/decide`: the questions, the
/// state they judge, and the model that must answer them. Nothing here is
/// chosen for the caller: no workspace default, no best offer.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DecideRequest {
    /// A catalog id, native id, alias, or `provider/tier`, as any verb's
    /// selector reads it. Optional only so its absence is refused by name.
    #[serde(default)]
    pub model: Option<String>,
    /// Only this provider's offers of `model`.
    #[serde(default)]
    pub provider: Option<String>,
    pub input: String,
    pub questions: Vec<DecisionQuestion>,
}

/// The offer that answered a named decision.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OfferView {
    pub provider: String,
    /// The id the provider was sent.
    pub model: String,
    pub measured_by: MeasuredBy,
    pub wire: Wire,
}

/// What the decide door returns: the facet's [`Decision`] unchanged (its
/// answers, refusals, and usage), with the offer that answered and how long
/// the whole call took, a first-time probe included.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DecideOutcome {
    #[serde(flatten)]
    pub decision: Decision,
    pub offer: OfferView,
    pub latency_ms: u64,
}

impl Orchestrator {
    /// The offer `query` selects for `questions` in `project_id`'s scope,
    /// confirmed able to measure them, or the refusal naming the fixes. Only
    /// offers of what `query` asked for are walked: one recorded unable is
    /// refused, never replaced by another model.
    ///
    /// May ask an unconfirmed logprobs offer one probe question, so it
    /// blocks for up to [`PROBE_TIMEOUT`]; the probe runs on its own thread,
    /// so this is safe to call from an async worker.
    pub(crate) fn decision_offer(
        &self,
        project_id: Option<&str>,
        query: OfferQuery<'_>,
        questions: &[DecisionQuestion],
    ) -> Result<SelectedOffer, DecisionFailure> {
        let scope = ModelScope {
            project_id: project_id.map(str::to_string),
        };
        // Each pass either returns or records one more offer as unable, which
        // the next catalog read no longer offers, so the walk ends; the bound
        // only guards against an observation expiring mid-walk.
        for _ in 0..32 {
            let catalog = self.catalog(&scope).map_err(DecisionFailure::Failed)?;
            let offer =
                select(&catalog, query, questions).map_err(DecisionFailure::Unanswerable)?;
            if self
                .confirm_offer(project_id, &offer)
                .map_err(DecisionFailure::Failed)?
            {
                return Ok(offer);
            }
        }
        Err(DecisionFailure::Unanswerable(
            "no decision-capable offer could be confirmed".to_string(),
        ))
    }

    /// The offer a recipe's decision node would be answered by: the model it
    /// names, else the workspace's `decisionModel`, else the best offer.
    pub(crate) fn node_decision_offer(
        &self,
        project_id: Option<&str>,
        decision: &DecisionNode,
    ) -> Result<SelectedOffer, DecisionFailure> {
        let workspace_default = self.get_settings().decision_model;
        self.decision_offer(
            project_id,
            decision_query(decision, workspace_default.as_deref()),
            &decision.questions,
        )
    }

    /// Whether `offer` can answer decisions: a native offer by its protocol,
    /// a logprobs offer by what it was seen to do, probing it if nothing
    /// stands. `Ok(false)` means it was seen unable (and is now recorded so);
    /// an error means the probe could not tell.
    fn confirm_offer(
        &self,
        project_id: Option<&str>,
        offer: &SelectedOffer,
    ) -> Result<bool, String> {
        if offer.measured_by() == Some(MeasuredBy::Native) {
            return Ok(true);
        }
        if let Some(unable) = self.decision_observations.get(offer) {
            return Ok(unable.is_none());
        }
        let route = self.decision_route(project_id, offer)?;
        let complete = self.models_runtime.complete.clone();
        let model = offer.model.clone();
        let outcome = std::thread::spawn(move || {
            cairn_models::decide::probe(complete.as_ref(), &route, &model, PROBE_TIMEOUT)
        })
        .join()
        .map_err(|_| "the decision capability probe panicked".to_string())?;
        match outcome {
            Ok(()) => {
                self.decision_observations.record(offer, None);
                Ok(true)
            }
            Err(error) => match error.incapability() {
                Some(why) => {
                    self.decision_observations.record(offer, Some(why));
                    Ok(false)
                }
                None => Err(format!(
                    "{} on {} could not be confirmed to answer decisions: {error}",
                    offer.model, offer.provider
                )),
            },
        }
    }

    /// The deployment `offer` is measured on, as its backend reaches it.
    fn decision_route(
        &self,
        project_id: Option<&str>,
        offer: &SelectedOffer,
    ) -> Result<DecisionRoute, String> {
        crate::backends::backend_for_name(Some(&offer.provider))
            .decision_route(self, project_id, offer)
    }

    /// Where each decision node in `nodes` stands in `project_id`'s scope,
    /// from what is already known. Nothing is asked of any provider, so this
    /// is cheap enough for every read: it is the load-time report, while save
    /// and launch ([`Self::refuse_unanswerable_decisions`]) are the gates.
    pub fn decision_diagnostics(
        &self,
        nodes: &[RecipeNode],
        project_id: Option<&str>,
    ) -> Vec<DecisionDiagnostic> {
        let scope = ModelScope {
            project_id: project_id.map(str::to_string),
        };
        let catalog = self.catalog(&scope);
        let workspace_default = self.get_settings().decision_model;
        nodes
            .iter()
            .filter_map(|node| {
                let decision = node.decision_config.as_ref()?;
                let report = |blocking: bool, rest: String| DecisionDiagnostic {
                    node_id: node.id.clone(),
                    node_name: node.name.clone(),
                    blocking,
                    message: format!("Decision node '{}' {rest}", node.name),
                };
                let catalog = match &catalog {
                    Ok(catalog) => catalog,
                    Err(error) => {
                        return Some(report(
                            true,
                            format!("can't be checked: the model catalog could not be read: {error}"),
                        ))
                    }
                };
                let offer = match select(
                    catalog,
                    decision_query(decision, workspace_default.as_deref()),
                    &decision.questions,
                ) {
                    Ok(offer) => offer,
                    Err(refusal) => return Some(report(true, format!("can't be answered: {refusal}"))),
                };
                (offer.measured_by() == Some(MeasuredBy::Logprobs)
                    && self.decision_observations.get(&offer).is_none())
                .then(|| {
                    report(
                        false,
                        format!(
                            "will be answered by {} on {} from logprobs, which has not been seen to answer yet. Saving or launching the recipe asks it one probe question first.",
                            offer.model, offer.provider
                        ),
                    )
                })
            })
            .collect()
    }

    /// Every decision node in `nodes` that nothing in `project_id`'s scope can
    /// answer, each as a sentence naming the node and the fixes.
    pub(crate) fn decision_problems(
        &self,
        nodes: &[RecipeNode],
        project_id: Option<&str>,
    ) -> Vec<String> {
        nodes
            .iter()
            .filter_map(|node| {
                let decision = node.decision_config.as_ref()?;
                self.node_decision_offer(project_id, decision)
                    .err()
                    .map(|refusal| {
                        format!("Decision node '{}' can't be answered: {refusal}", node.name)
                    })
            })
            .collect()
    }

    /// Refuse a recipe whose decision nodes nothing can answer. Called where a
    /// recipe is saved, imported, validated, and launched, so the gap is
    /// reported before an execution exists rather than discovered as a
    /// blocked node.
    pub(crate) fn refuse_unanswerable_decisions(
        &self,
        nodes: &[RecipeNode],
        project_id: Option<&str>,
    ) -> Result<(), String> {
        match self.decision_problems(nodes, project_id).as_slice() {
            [] => Ok(()),
            problems => Err(problems.join("\n")),
        }
    }

    /// Answer `decision`'s questions about `input`, one answer per question in
    /// order. A refused question is an answer; an error means nothing
    /// answered.
    pub(crate) async fn decide(
        &self,
        project_id: Option<&str>,
        decision: &DecisionNode,
        input: String,
    ) -> Result<Decision, DecisionFailure> {
        let orch = self.clone();
        let project_id = project_id.map(str::to_string);
        let decision = decision.clone();
        tokio::task::spawn_blocking(move || {
            let workspace_default = orch.get_settings().decision_model;
            orch.decide_blocking(
                project_id.as_deref(),
                decision_query(&decision, workspace_default.as_deref()),
                &decision.questions,
                &input,
            )
            .map(|(decision, _)| decision)
        })
        .await
        .map_err(|error| DecisionFailure::Failed(format!("the decision task failed: {error}")))?
    }

    /// Answer `request`'s questions with the model it names, and only that
    /// model: the door behind `run cairn://models/decide`. Selection, the
    /// one-time probe, and what the call reveals about the offer are the
    /// recipe path's own; the answers come back raw, with no thresholds.
    ///
    /// The facet's decide is one whole request per provider call and takes no
    /// cancel token. Dropping this future releases the caller at once; the
    /// blocking worker then detaches and ends within its own bounds, a
    /// [`PROBE_TIMEOUT`] probe and a [`DECISION_TIMEOUT`] per provider request.
    pub(crate) async fn decide_named(
        &self,
        project_id: Option<&str>,
        request: DecideRequest,
    ) -> Result<DecideOutcome, DecisionFailure> {
        let started = Instant::now();
        let orch = self.clone();
        let project_id = project_id.map(str::to_string);
        let (decision, offer) = tokio::task::spawn_blocking(move || {
            orch.decide_named_blocking(project_id.as_deref(), &request)
        })
        .await
        .map_err(|error| DecisionFailure::Failed(format!("the decision task failed: {error}")))??;
        // `select` admits only an offer that measures, so this always holds.
        let measured_by = offer.measured_by().ok_or_else(|| {
            DecisionFailure::Failed(format!(
                "{} on {} answered over {}, which measures nothing",
                offer.model, offer.provider, offer.served.wire
            ))
        })?;
        Ok(DecideOutcome {
            decision,
            offer: OfferView {
                measured_by,
                wire: offer.served.wire,
                provider: offer.provider,
                model: offer.model,
            },
            latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }

    fn decide_named_blocking(
        &self,
        project_id: Option<&str>,
        request: &DecideRequest,
    ) -> Result<(Decision, SelectedOffer), DecisionFailure> {
        let named = |value: &Option<String>| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let Some(model) = named(&request.model) else {
            return Err(DecisionFailure::Unanswerable(
                self.unnamed_model_refusal(project_id, &request.questions)?,
            ));
        };
        let problems = question_problems(&request.questions);
        if !problems.is_empty() {
            return Err(DecisionFailure::Unanswerable(format!(
                "These questions can't be asked: {}.",
                problems.join("; ")
            )));
        }
        let provider = named(&request.provider);
        self.decide_blocking(
            project_id,
            OfferQuery {
                requested: Some(&model),
                provider: provider.as_deref(),
                ..OfferQuery::new(VerbKind::Decide)
            },
            &request.questions,
            &request.input,
        )
    }

    /// The refusal for a named decision that names no model, with the offers
    /// it could have named.
    fn unnamed_model_refusal(
        &self,
        project_id: Option<&str>,
        questions: &[DecisionQuestion],
    ) -> Result<String, DecisionFailure> {
        let catalog = self
            .catalog(&ModelScope {
                project_id: project_id.map(str::to_string),
            })
            .map_err(DecisionFailure::Failed)?;
        let lead = "Name the model to decide with (a catalog id, or `model` with `provider`): this door never chooses one";
        let alternatives = alternatives(&catalog, questions);
        Ok(if alternatives.is_empty() {
            let nothing = NoOffer {
                verb: VerbKind::Decide,
                requested: None,
                detail: None,
            };
            format!("{lead}. {nothing}")
        } else {
            format!("{lead}; offers that serve decide: {alternatives}.")
        })
    }

    /// Select, confirm, and ask: the one path every decision takes, so what a
    /// call reveals about its offer is recorded whoever asked.
    fn decide_blocking(
        &self,
        project_id: Option<&str>,
        query: OfferQuery<'_>,
        questions: &[DecisionQuestion],
        input: &str,
    ) -> Result<(Decision, SelectedOffer), DecisionFailure> {
        let offer = self.decision_offer(project_id, query, questions)?;
        let route = self
            .decision_route(project_id, &offer)
            .map_err(DecisionFailure::Failed)?;
        let result = cairn_models::decide::decide(
            self.models_runtime.complete.as_ref(),
            DecisionCall {
                route: &route,
                model: &offer.model,
                input,
                questions,
                timeout: DECISION_TIMEOUT,
            },
        );
        // What a real decision reveals about the model is kept, so the next
        // save or launch does not admit an offer this one just found unable.
        match &result {
            Ok(_) if offer.measured_by() == Some(MeasuredBy::Logprobs) => {
                self.decision_observations.record(&offer, None)
            }
            Err(error) => {
                if let Some(why) = error.incapability() {
                    self.decision_observations.record(&offer, Some(why));
                }
            }
            Ok(_) => {}
        }
        result
            .map(|decision| (decision, offer))
            .map_err(|error| DecisionFailure::Failed(error.to_string()))
    }
}

/// The scripted provider these tests ask, shared with the run handler's tests
/// of the decide door.
#[cfg(test)]
pub(crate) mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use cairn_models::interface::complete::{
        Complete, CompletionCall, CompletionError, CompletionReply, CompletionWire,
    };
    use cairn_models::interface::{ModelScope, Models};
    use cairn_models::types::catalog::VerbKind;
    use serde_json::{json, Value};

    pub(crate) use super::DecideOutcome;
    use super::{DecideRequest, DecisionFailure, OfferView};
    use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth};
    use crate::models::decision::{DecisionAnswer, MeasuredBy};
    use crate::models::{CreateRecipe, Recipe, RecipeFile, UpdateRecipe, UpdateSettings};
    use crate::orchestrator::models::ModelRuntime;
    use crate::orchestrator::Orchestrator;
    use cairn_models::types::catalog::Wire;

    /// A server that answers from a script and counts what it was asked.
    #[derive(Default)]
    pub(crate) struct Scripted {
        replies: Mutex<VecDeque<Value>>,
        calls: Mutex<usize>,
        /// How long each reply takes, when it should not be at once.
        delay: Mutex<Option<Duration>>,
    }

    impl Scripted {
        pub(crate) fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }

        /// Hold every reply for `delay`, as a hung provider would.
        pub(crate) fn hold_each_reply(&self, delay: Duration) {
            *self.delay.lock().unwrap() = Some(delay);
        }
    }

    impl Complete for Scripted {
        fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
            *self.calls.lock().unwrap() += 1;
            let delay = *self.delay.lock().unwrap();
            if let Some(delay) = delay {
                std::thread::sleep(delay);
            }
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("a request the script did not expect");
            Ok(match call.wire {
                CompletionWire::Decisions(_) => {
                    CompletionReply::Decisions(serde_json::from_value(reply).unwrap())
                }
                _ => CompletionReply::Chat(serde_json::from_value(reply).unwrap()),
            })
        }
    }

    /// What a chat server that serves no logprobs answers.
    fn without_logprobs() -> Value {
        json!({"model": "qwen3", "choices": [{"message": {"role": "assistant", "content": "Yes"}}]})
    }

    /// What a chat server that serves logprobs answers.
    pub(crate) fn with_logprobs() -> Value {
        json!({"model": "qwen3", "choices": [{
            "message": {"role": "assistant", "content": "Yes"},
            "logprobs": {"content": [{"token": "Yes", "logprob": -0.1, "top_logprobs": [
                {"token": "Yes", "logprob": -0.1}, {"token": "No", "logprob": -2.5}
            ]}]}
        }]})
    }

    /// A bare workspace whose one provider is a custom chat server serving
    /// `qwen3`, answering from `replies`.
    async fn workspace_with_chat_server(replies: Vec<Value>) -> (Orchestrator, Arc<Scripted>) {
        let (orch, scripted, _) = workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            replies,
        )
        .await;
        (orch, scripted)
    }

    /// A bare workspace whose one provider is a custom server speaking
    /// `protocol` and serving `model`, answering from `replies`, with that
    /// provider's backend key.
    pub(crate) async fn workspace_with_server(
        protocol: CustomProtocol,
        api_key: Option<&str>,
        model: &str,
        replies: Vec<Value>,
    ) -> (Orchestrator, Arc<Scripted>, String) {
        let mut orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let scripted = Arc::new(Scripted {
            replies: Mutex::new(replies.into()),
            ..Scripted::default()
        });
        orch.models_runtime = ModelRuntime {
            complete: scripted.clone(),
            ..orch.models_runtime.clone()
        };
        let account = orch
            .add_custom_provider(
                "Sparks".into(),
                ProviderAuth::custom_endpoint(
                    "http://sparks.invalid/v1",
                    protocol,
                    api_key,
                    &[],
                    CustomModelPolicy::Explicit {
                        ids: vec![model.into()],
                    },
                    CustomSampling::default(),
                    1,
                )
                .unwrap(),
                None,
            )
            .unwrap();
        let key = cairn_models::types::catalog::custom_key(&account.id);
        orch.refresh_provider_model_catalog_blocking(&key);
        (orch, scripted, key)
    }

    /// What a chat server that serves logprobs answers to a two-option
    /// choice, offered as `A` and `B`: mostly the second.
    pub(crate) fn choosing_b() -> Value {
        json!({"model": "qwen3", "choices": [{
            "message": {"role": "assistant", "content": "B"},
            "logprobs": {"content": [{"token": "B", "logprob": 0.86_f64.ln(), "top_logprobs": [
                {"token": "B", "logprob": 0.86_f64.ln()}, {"token": "A", "logprob": 0.14_f64.ln()}
            ]}]}
        }]})
    }

    fn named_request(model: Option<&str>, questions: Value) -> DecideRequest {
        serde_json::from_value(json!({
            "model": model,
            "input": "The checkout page times out for every customer.",
            "questions": questions,
        }))
        .unwrap()
    }

    fn scale() -> Value {
        json!([{"name": "scale", "type": "choice", "instructions": "How large is this issue?",
                "options": [{"value": "small"}, {"value": "large"}]}])
    }

    fn unanswerable(failure: DecisionFailure) -> String {
        match failure {
            DecisionFailure::Unanswerable(text) => text,
            DecisionFailure::Failed(text) => panic!("expected a refusal, got a failure: {text}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_door_returns_the_full_distribution_and_the_offer_that_answered() {
        let (orch, server, key) = workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            vec![with_logprobs(), choosing_b(), choosing_b()],
        )
        .await;

        let outcome = orch
            .decide_named(None, named_request(Some("qwen3"), scale()))
            .await
            .unwrap();
        assert_eq!(server.calls(), 2, "one probe, then the question");
        assert_eq!(
            outcome.offer,
            OfferView {
                provider: key.clone(),
                model: "qwen3".into(),
                measured_by: MeasuredBy::Logprobs,
                wire: Wire::OpenAiChatCompletions,
            }
        );
        let DecisionAnswer::Choice {
            pick,
            confidence,
            probabilities,
            measured_by,
            ..
        } = &outcome.decision.answers[0]
        else {
            panic!("expected a choice: {:?}", outcome.decision.answers);
        };
        assert_eq!(pick, "large");
        assert_eq!(*measured_by, MeasuredBy::Logprobs);
        assert!(confidence.is_some());
        assert_eq!(
            probabilities
                .iter()
                .map(|option| option.value.as_str())
                .collect::<Vec<_>>(),
            ["small", "large"]
        );
        assert!((probabilities[1].probability - 0.86).abs() < 1e-9);

        let body = serde_json::to_value(&outcome).unwrap();
        assert_eq!(body["provider"], key.as_str());
        assert_eq!(body["answers"][0]["measuredBy"], "logprobs");
        assert_eq!(body["offer"]["wire"], "openai-chat");
        assert!(body["latencyMs"].is_u64(), "{body}");

        orch.decide_named(None, named_request(Some("qwen3"), scale()))
            .await
            .unwrap();
        assert_eq!(server.calls(), 3, "what the probe saw stands");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_question_comes_back_as_a_refusal_in_its_position() {
        let (orch, server, _) = workspace_with_server(
            CustomProtocol::Decisions,
            Some("gateway-key"),
            "typesafe-ai/jev",
            vec![json!({
                "model": "typesafe-ai/jev",
                "answers": [
                    {"type": "refusal", "name": "damaged"},
                    {"type": "choice", "name": "queue", "choice": "billing"}
                ],
                "usage": {"input_tokens": 40, "output_tokens": 0}
            })],
        )
        .await;
        let request = named_request(
            Some("typesafe-ai/jev"),
            json!([
                {"name": "damaged", "type": "predicate", "instructions": "Is it damaged?"},
                {"name": "queue", "type": "choice", "instructions": "Which queue?",
                 "options": [{"value": "billing"}, {"value": "shipping"}]}
            ]),
        );

        let outcome = orch.decide_named(None, request).await.unwrap();
        assert_eq!(server.calls(), 1, "a native offer is never probed");
        let body = serde_json::to_value(&outcome).unwrap();
        assert_eq!(
            body["answers"][0],
            json!({"type": "refusal", "name": "damaged", "measuredBy": "native"})
        );
        assert_eq!(body["answers"][1]["pick"], "billing");
        assert_eq!(body["inputTokens"], 40);
        assert_eq!(body["offer"]["measuredBy"], "native");
        let back: DecideOutcome = serde_json::from_value(body).unwrap();
        assert!(back.decision.answers[0].is_refusal());
        assert_eq!(back, outcome);
    }

    /// Records where each System One request went and answers OpenRouter's
    /// documented reply.
    #[derive(Default)]
    struct OpenRouterDecisions {
        sent: Mutex<Vec<(String, Vec<(String, String)>, Value)>>,
    }

    impl Complete for OpenRouterDecisions {
        fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
            let CompletionWire::SystemOne(endpoint) = call.wire else {
                panic!("expected System One, got {}", call.wire.family());
            };
            self.sent.lock().unwrap().push((
                endpoint.url.clone(),
                endpoint.headers.clone(),
                call.body,
            ));
            Ok(CompletionReply::SystemOne(
                serde_json::from_value(json!({
                    "answers": {
                        "is_bug": {"noul": 0.96, "type": "noul"},
                        "team": {"choice": "payments", "confidence": 0.75, "type": "choice",
                                 "probabilities": {"account": 0, "frontend": 0.16, "payments": 0.84}},
                        "urgency": {"confidence": 0.99, "score": 1.99, "type": "score",
                                    "legend": {"0": "later", "1": "this week", "2": "now"},
                                    "probabilities": {"0": 0, "1": 0.01, "2": 0.99}}
                    },
                    "id": "gen-dec-1789738314-X5e5eKGQdvR9rblyX250",
                    "model": "typesafe/jev-1.14-20261014",
                    "provider": "TypeSafe",
                    "usage": {"cost": 0.000019992, "input_tokens": 476, "output_tokens": 70}
                }))
                .unwrap(),
            ))
        }
    }

    /// OpenRouter's decision models reach `decide` through what discovery
    /// declared: selected by id, sent over System One to the Decisions API on
    /// the workspace key, answered with typed answers, and listed with their
    /// price on every surface that shows decision offers.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_openrouter_decision_model_is_selected_by_id_and_answers_over_system_one() {
        let mut orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let recorder = Arc::new(OpenRouterDecisions::default());
        orch.models_runtime = ModelRuntime {
            complete: recorder.clone(),
            ..orch.models_runtime.clone()
        };
        let mut enabled = orch.enabled_providers();
        enabled.push("openrouter".to_string());
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(enabled),
            ..Default::default()
        })
        .unwrap();
        orch.add_account(
            crate::credentials::ApiProvider::OpenRouter,
            "OpenRouter".into(),
            ProviderAuth::ApiKey {
                value: "sk-or-test".into(),
            },
            None,
        )
        .unwrap();
        orch.seed_model_catalog_for_test(cairn_models::types::catalog::ProviderModelCatalog {
            backend: "openrouter".into(),
            models: crate::backends::openrouter::decode_decision_models(include_str!(
                "../backends/openrouter/testdata/decisions_models.json"
            ))
            .unwrap(),
            options: Vec::new(),
            refreshed_at: Some(1_790_000_000),
            error: None,
        });

        let request = named_request(
            Some("openrouter/typesafe/jev-1.14"),
            json!([
                {"name": "is_bug", "type": "predicate", "instructions": "Is this a defect?"},
                {"name": "team", "type": "choice", "instructions": "Which team?",
                 "options": [{"value": "account"}, {"value": "frontend"}, {"value": "payments"}]},
                {"name": "urgency", "type": "score", "instructions": "How urgent?",
                 "levels": [{"label": "later"}, {"label": "this week"}, {"label": "now"}]}
            ]),
        );
        let outcome = orch.decide_named(None, request).await.unwrap();
        assert_eq!(
            outcome.offer,
            OfferView {
                provider: "openrouter".into(),
                model: "typesafe/jev-1.14".into(),
                measured_by: MeasuredBy::Native,
                wire: Wire::SystemOne,
            }
        );
        let body = serde_json::to_value(&outcome).unwrap();
        assert_eq!(body["cost"], json!(0.000019992));
        assert_eq!(body["inputTokens"], 476);
        assert_eq!(body["model"], "typesafe/jev-1.14-20261014");
        assert_eq!(body["offer"]["wire"], "systemone");
        let picks: Vec<_> = outcome
            .decision
            .answers
            .iter()
            .map(|answer| (answer.pick().unwrap(), answer.confidence().is_some()))
            .collect();
        assert_eq!(
            picks,
            vec![("yes", true), ("payments", true), ("now", true)]
        );

        let sent = recorder.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1, "a native offer is never probed");
        let (url, headers, sent_body) = &sent[0];
        assert_eq!(url, "https://openrouter.ai/api/alpha/decisions");
        assert!(headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("authorization")
                && value == "Bearer sk-or-test"));
        assert_eq!(sent_body["model"], "typesafe/jev-1.14");
        assert_eq!(sent_body["questions"]["is_bug"]["type"], "noul");

        let request = crate::mcp::types::McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({}),
            tool_use_id: None,
        };
        let read = |uri: &'static str| {
            let orch = orch.clone();
            let request = request.clone();
            async move { crate::resources::read_cairn_resource(&orch, &request, uri).await }
        };
        let page = read("cairn://models/decide").await;
        assert!(
            page.contains("`typesafe/jev-1.14` on `openrouter` — measured by native"),
            "{page}"
        );
        let catalog = read("cairn://models/catalog?provider=openrouter&serves=decide").await;
        assert!(catalog.contains("typesafe/jev-1.14"), "{catalog}");
        assert!(catalog.contains("~typesafe/jev-latest"), "{catalog}");
        assert!(catalog.contains("in $0.042/M"), "{catalog}");
        assert!(catalog.contains("in $0.1/M"), "{catalog}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_model_that_cannot_decide_is_refused_naming_the_offers_that_can() {
        let (orch, server, key) = workspace_with_chat_server_keyed().await;
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(vec!["claude".to_string(), key.clone()]),
            ..Default::default()
        })
        .unwrap();

        let refused = unanswerable(
            orch.decide_named(None, named_request(Some("opus"), scale()))
                .await
                .unwrap_err(),
        );
        assert!(refused.contains("cannot answer decisions"), "{refused}");
        assert!(
            refused.contains(&format!("offers that serve decide: {key}/qwen3")),
            "{refused}"
        );
        assert_eq!(server.calls(), 0, "nothing was asked of any provider");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_door_never_chooses_a_model() {
        let (orch, server, key) = workspace_with_chat_server_keyed().await;
        // A workspace default exists, and the door still does not use it.
        orch.update_settings(UpdateSettings {
            decision_model: Some(Some("qwen3".into())),
            ..Default::default()
        })
        .unwrap();

        for model in [None, Some(""), Some("   ")] {
            let refused = unanswerable(
                orch.decide_named(None, named_request(model, scale()))
                    .await
                    .unwrap_err(),
            );
            assert!(refused.contains("never chooses one"), "{refused}");
            assert!(
                refused.contains(&format!("offers that serve decide: {key}/qwen3")),
                "{refused}"
            );
        }
        assert_eq!(server.calls(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_model_is_refused_and_never_answered_by_another() {
        let (orch, server, key) = workspace_with_chat_server_keyed().await;
        let refused = unanswerable(
            orch.decide_named(None, named_request(Some("not-served"), scale()))
                .await
                .unwrap_err(),
        );
        assert!(
            refused.contains("no installed provider offers 'not-served'"),
            "{refused}"
        );
        assert!(
            refused.contains(&format!("offers that serve decide: {key}/qwen3")),
            "{refused}"
        );
        assert_eq!(server.calls(), 0);

        // Narrowing to a provider that does not offer the model is the same
        // refusal, not a fallback to the one that does.
        let mut request = named_request(Some("qwen3"), scale());
        request.provider = Some("claude".into());
        let refused = unanswerable(orch.decide_named(None, request).await.unwrap_err());
        assert!(refused.contains("'qwen3'"), "{refused}");
        assert_eq!(server.calls(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_requests_are_refused_before_anything_is_asked() {
        let (orch, server, _) = workspace_with_chat_server_keyed().await;
        let refused = unanswerable(
            orch.decide_named(None, named_request(Some("qwen3"), json!([])))
                .await
                .unwrap_err(),
        );
        assert!(refused.contains("at least one question"), "{refused}");
        assert!(serde_json::from_value::<DecideRequest>(json!({
            "model": "qwen3", "input": "x", "questions": [], "thresholds": {}
        }))
        .is_err());
        assert_eq!(server.calls(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_recipe_node_naming_an_unserved_model_is_told_what_does_serve() {
        let (orch, server, key) = workspace_with_chat_server_keyed().await;
        let options = ["yes".to_string(), "no".to_string()];
        let recipe = decision_recipe_naming(&options, Some("not-served"));
        let refused = orch
            .refuse_unanswerable_decisions(&recipe.nodes, None)
            .unwrap_err();
        assert!(refused.contains("'not-served'"), "{refused}");
        assert!(
            refused.contains(&format!("offers that serve decide: {key}/qwen3")),
            "{refused}"
        );
        let [report] = orch
            .decision_diagnostics(&recipe.nodes, None)
            .try_into()
            .unwrap();
        assert!(
            report.message.contains("offers that serve decide"),
            "the load-time report and the gate say the same thing: {}",
            report.message
        );
        assert_eq!(server.calls(), 0);
    }

    async fn workspace_with_chat_server_keyed() -> (Orchestrator, Arc<Scripted>, String) {
        workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            Vec::new(),
        )
        .await
    }

    fn decision_recipe(options: &[String]) -> Recipe {
        decision_recipe_naming(options, None)
    }

    /// A one-decision recipe whose node names `model`, or none.
    fn decision_recipe_naming(options: &[String], model: Option<&str>) -> Recipe {
        let options = options
            .iter()
            .map(|value| format!("{{value: {value}}}"))
            .collect::<Vec<_>>()
            .join(", ");
        let model = model
            .map(|model| format!("\n      model: {model}"))
            .unwrap_or_default();
        let yaml = format!(
            r#"cairnVersion: 1
name: Decides
trigger: manual
nodes:
  - id: trigger-1
    type: trigger
    name: Trigger
    position: 0@0
    config:
      triggerType: manual
  - id: db
    type: decision
    name: Touches the DB?
    position: 200@0
    config:{model}
      questions:
        - name: route
          type: choice
          instructions: Does the issue touch the database?
          options: [{options}]
edges:
  - from: trigger-1@control-out
    to: db@control-in
    type: control
"#
        );
        RecipeFile::from_yaml(&yaml)
            .unwrap()
            .into_recipe(None, None)
    }

    fn yes_no() -> Recipe {
        decision_recipe(&["yes".into(), "no".into()])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_workspace_decision_default_answers_only_a_node_that_names_no_model() {
        use crate::models::UpdateSettings;
        let (orch, server) = workspace_with_chat_server(Vec::new()).await;
        let set_default = |model: Option<&str>| {
            orch.update_settings(UpdateSettings {
                decision_model: Some(model.map(str::to_string)),
                ..Default::default()
            })
            .unwrap();
        };
        let options = ["yes".to_string(), "no".to_string()];
        let naming_qwen = decision_recipe_naming(&options, Some("qwen3"));
        set_default(Some("not-served"));

        // A node naming no model asks for the workspace default.
        let [report] = orch
            .decision_diagnostics(&yes_no().nodes, None)
            .try_into()
            .unwrap();
        assert!(report.blocking, "{}", report.message);
        assert!(
            report.message.contains("'not-served'"),
            "{}",
            report.message
        );
        let refused = orch
            .refuse_unanswerable_decisions(&yes_no().nodes, None)
            .unwrap_err();
        assert!(refused.contains("'not-served'"), "{refused}");

        // A node naming its own model ignores it.
        let [note] = orch
            .decision_diagnostics(&naming_qwen.nodes, None)
            .try_into()
            .unwrap();
        assert!(!note.blocking, "{}", note.message);
        assert!(note.message.contains("qwen3 on"), "{}", note.message);

        // Cleared, the catalog chooses again.
        set_default(None);
        let [note] = orch
            .decision_diagnostics(&yes_no().nodes, None)
            .try_into()
            .unwrap();
        assert!(!note.blocking, "{}", note.message);
        assert!(note.message.contains("qwen3 on"), "{}", note.message);
        assert_eq!(server.calls(), 0, "nothing was probed");
    }

    fn create(orch: &Orchestrator, recipe: Recipe) -> Result<Recipe, String> {
        orch.create_recipe(CreateRecipe {
            name: recipe.name,
            description: None,
            trigger: None,
            workspace_id: Some("default".into()),
            project_id: None,
            is_system: None,
            nodes: Some(recipe.nodes),
            edges: Some(recipe.edges),
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_chat_server_without_logprobs_is_refused_before_any_execution_and_then_not_offered() {
        let (orch, server) = workspace_with_chat_server(vec![without_logprobs()]).await;
        let recipe = yes_no();

        // The launch gate: one probe finds the server cannot measure.
        let refusal = orch
            .refuse_unanswerable_decisions(&recipe.nodes, None)
            .unwrap_err();
        assert!(refusal.contains("returned no logprobs"), "{refusal}");
        assert!(
            refusal.contains("Decisions API key"),
            "names the fixes: {refusal}"
        );
        assert_eq!(server.calls(), 1);

        // What was seen stands: saving asks nothing more, and refuses.
        let refused = create(&orch, yes_no()).unwrap_err();
        assert!(refused.contains("can't be answered"), "{refused}");
        assert_eq!(server.calls(), 1);

        // And the catalog stops offering it for decisions.
        let catalog = orch.catalog(&ModelScope::default()).unwrap();
        assert!(catalog.offers_serving(VerbKind::Decide).is_empty());

        // A read reports it as blocking, naming why, without asking again.
        let [report] = orch
            .decision_diagnostics(&recipe.nodes, None)
            .try_into()
            .unwrap();
        assert!(report.blocking);
        assert!(
            report.message.contains("returned no logprobs"),
            "{}",
            report.message
        );
        assert_eq!(server.calls(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_chat_server_seen_to_answer_is_admitted_for_what_logprobs_can_ask() {
        let (orch, server) = workspace_with_chat_server(vec![with_logprobs()]).await;

        // Before anything has been asked, a read notes the offer is
        // unconfirmed, without asking it.
        let [note] = orch
            .decision_diagnostics(&yes_no().nodes, None)
            .try_into()
            .unwrap();
        assert!(!note.blocking);
        assert!(
            note.message.contains("not been seen to answer yet"),
            "{}",
            note.message
        );
        assert_eq!(server.calls(), 0);

        let saved = create(&orch, yes_no()).unwrap();
        assert_eq!(server.calls(), 1, "one probe");
        orch.refuse_unanswerable_decisions(&saved.nodes, None)
            .unwrap();
        assert_eq!(server.calls(), 1, "the probe's answer stands");

        // Twenty-one options are more than logprobs can see: refused at save,
        // before anything is asked.
        let wide: Vec<String> = (0..21).map(|index| format!("o{index}")).collect();
        let wide = decision_recipe(&wide);
        let refused = orch
            .update_recipe(
                &saved.id,
                UpdateRecipe {
                    name: None,
                    description: None,
                    trigger: None,
                    workspace_id: None,
                    project_id: None,
                    is_system: None,
                    nodes: Some(wide.nodes),
                    edges: Some(wide.edges),
                },
                None,
                Some("default"),
            )
            .unwrap_err();
        assert!(refused.contains("at most 20"), "{refused}");
        assert_eq!(server.calls(), 1);

        // Once seen to answer, a read has nothing to say; the wide recipe is
        // reported as blocking, still without asking anything.
        assert!(orch.decision_diagnostics(&saved.nodes, None).is_empty());
        let wide: Vec<String> = (0..21).map(|index| format!("o{index}")).collect();
        let [wide] = orch
            .decision_diagnostics(&decision_recipe(&wide).nodes, None)
            .try_into()
            .unwrap();
        assert!(
            wide.blocking && wide.message.contains("at most 20"),
            "{}",
            wide.message
        );
        assert_eq!(server.calls(), 1);

        // The saved recipe still loads, so it stays editable.
        assert!(orch.get_recipe(&saved.id, None).unwrap().is_some());
    }
}
