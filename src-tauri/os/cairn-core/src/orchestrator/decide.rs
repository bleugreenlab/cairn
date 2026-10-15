//! The orchestrator's side of the cairn-llm facet's `decide` verb.
//!
//! The facet measures; this module decides where. It reports each provider's
//! decision method into the catalog, resolves an authored model or tier to a
//! decision offer through the facet's one selector
//! ([`cairn_llm::interface::Catalog::decision_offer`]), confirms that offer
//! can measure the node's questions, and runs the facet's blocking `decide`
//! off the async runtime.
//!
//! Only custom providers can answer a decision today: one speaking the
//! Decisions protocol natively, one speaking OpenAI chat from its logprobs.
//! Every shipped provider (the subscriptions, OpenRouter, OpenCode) reports
//! none, so a workspace with only those refuses with the fixes rather than
//! asking a chat model to report its own probabilities.
//!
//! A chat provider's protocol says only that logprobs *might* be served. So a
//! logprobs offer is admitted (at save, import, validation, and launch) only
//! once it has been seen to answer: [`DecisionObservations`] holds what each
//! one did, from a one-question probe or a real decision. One seen unable
//! leaves the catalog's decision offers, so selection moves past it and every
//! surface stops offering it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::models::decision::{Decision, DecisionNode, MeasuredBy};
use crate::models::RecipeNode;
use cairn_llm::interface::decide::{DecisionCall, DecisionOffer, DecisionRoute};
use cairn_llm::interface::{Llm, LlmScope};
use cairn_llm::types::catalog;

use super::Orchestrator;
use crate::backends::custom::CustomProviderConfig;

/// Bounds each provider request. A native decision answers in under a second
/// and a single logprobs token barely longer; this leaves room for a cold
/// local model without letting a hung server hold the node.
const DECISION_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounds the capability probe, which can run inside a save or a launch.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);

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
    fn get(&self, offer: &DecisionOffer) -> Option<Option<String>> {
        let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.get(&(offer.provider.clone(), offer.model.clone()))
            .filter(|observation| observation.at.elapsed() < OBSERVATION_TTL)
            .map(|observation| observation.unable.clone())
    }

    fn record(&self, offer: &DecisionOffer, unable: Option<String>) {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.insert(
            (offer.provider.clone(), offer.model.clone()),
            Observation {
                unable,
                at: Instant::now(),
            },
        );
    }

    /// The models on `provider` seen unable to decide, each with why.
    pub(crate) fn unsupported(&self, provider: &str) -> BTreeMap<String, String> {
        let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.iter()
            .filter(|((key, _), observation)| {
                key == provider && observation.at.elapsed() < OBSERVATION_TTL
            })
            .filter_map(|((_, model), observation)| {
                Some((model.clone(), observation.unable.clone()?))
            })
            .collect()
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

/// Why `offer`'s method cannot ask `decision`'s questions, when it cannot.
fn unmeasurable_by(offer: &DecisionOffer, decision: &DecisionNode) -> Option<String> {
    let unmeasurable = cairn_llm::decide::unmeasurable(&decision.questions, offer.method);
    (!unmeasurable.is_empty()).then(|| {
        format!(
            "{} on {} measures decisions by {}, which cannot ask this: {}",
            offer.model,
            offer.provider,
            offer.method,
            unmeasurable.join(" ")
        )
    })
}

impl Orchestrator {
    /// How the provider `key` answers a decision in `project_id`'s scope, or
    /// `None` when it cannot.
    pub(crate) fn decision_method(
        &self,
        key: &str,
        project_id: Option<&str>,
    ) -> Option<MeasuredBy> {
        let account_id = catalog::custom_account_id(key)?;
        CustomProviderConfig::resolve(self, account_id, project_id)
            .ok()
            .map(|config| config.decision_method())
    }

    /// The offer `decision` would be answered by in `project_id`'s scope,
    /// confirmed able to measure its questions, or the refusal naming the
    /// fixes.
    ///
    /// May ask an unconfirmed logprobs offer one probe question, so it
    /// blocks for up to [`PROBE_TIMEOUT`]; the probe runs on its own thread,
    /// so this is safe to call from an async worker.
    pub(crate) fn decision_offer(
        &self,
        project_id: Option<&str>,
        decision: &DecisionNode,
    ) -> Result<DecisionOffer, String> {
        let scope = LlmScope {
            project_id: project_id.map(str::to_string),
        };
        // Each pass either returns or records one more offer as unable, which
        // the next catalog read no longer offers, so the walk ends; the bound
        // only guards against an observation expiring mid-walk.
        for _ in 0..32 {
            let offer = self
                .catalog(&scope)?
                .decision_offer(decision.model.as_deref())
                .map_err(|refusal| refusal.to_string())?;
            if let Some(refusal) = unmeasurable_by(&offer, decision) {
                return Err(refusal);
            }
            if self.confirm_offer(project_id, &offer)? {
                return Ok(offer);
            }
        }
        Err("no decision-capable offer could be confirmed".to_string())
    }

    /// Whether `offer` can answer decisions: a native offer by its protocol,
    /// a logprobs offer by what it was seen to do, probing it if nothing
    /// stands. `Ok(false)` means it was seen unable (and is now recorded so);
    /// an error means the probe could not tell.
    fn confirm_offer(
        &self,
        project_id: Option<&str>,
        offer: &DecisionOffer,
    ) -> Result<bool, String> {
        if offer.method == MeasuredBy::Native {
            return Ok(true);
        }
        if let Some(unable) = self.decision_observations.get(offer) {
            return Ok(unable.is_none());
        }
        let route = self.decision_route(project_id, offer)?;
        let complete = self.llm_runtime.complete.clone();
        let model = offer.model.clone();
        let outcome = std::thread::spawn(move || {
            cairn_llm::decide::probe(complete.as_ref(), &route, &model, PROBE_TIMEOUT)
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

    /// The deployment `offer` is measured on.
    fn decision_route(
        &self,
        project_id: Option<&str>,
        offer: &DecisionOffer,
    ) -> Result<DecisionRoute, String> {
        let account_id = catalog::custom_account_id(&offer.provider)
            .ok_or_else(|| format!("{} cannot answer a decision", offer.provider))?;
        Ok(CustomProviderConfig::resolve(self, account_id, project_id)?.decision_route())
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
        let scope = LlmScope {
            project_id: project_id.map(str::to_string),
        };
        let catalog = self.catalog(&scope);
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
                let offer = match catalog.decision_offer(decision.model.as_deref()) {
                    Ok(offer) => offer,
                    Err(refusal) => return Some(report(true, format!("can't be answered: {refusal}"))),
                };
                if let Some(refusal) = unmeasurable_by(&offer, decision) {
                    return Some(report(true, format!("can't be answered: {refusal}")));
                }
                (offer.method == MeasuredBy::Logprobs
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
                self.decision_offer(project_id, decision)
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
    ) -> Result<Decision, String> {
        let orch = self.clone();
        let project_id = project_id.map(str::to_string);
        let decision = decision.clone();
        tokio::task::spawn_blocking(move || {
            orch.decide_blocking(project_id.as_deref(), &decision, &input)
        })
        .await
        .map_err(|error| format!("the decision task failed: {error}"))?
    }

    fn decide_blocking(
        &self,
        project_id: Option<&str>,
        decision: &DecisionNode,
        input: &str,
    ) -> Result<Decision, String> {
        let offer = self.decision_offer(project_id, decision)?;
        let route = self.decision_route(project_id, &offer)?;
        let result = cairn_llm::decide::decide(
            self.llm_runtime.complete.as_ref(),
            DecisionCall {
                route: &route,
                model: &offer.model,
                input,
                questions: &decision.questions,
                timeout: DECISION_TIMEOUT,
            },
        );
        // What a real decision reveals about the model is kept, so the next
        // save or launch does not admit an offer this one just found unable.
        match &result {
            Ok(_) if offer.method == MeasuredBy::Logprobs => {
                self.decision_observations.record(&offer, None)
            }
            Err(error) => {
                if let Some(why) = error.incapability() {
                    self.decision_observations.record(&offer, Some(why));
                }
            }
            Ok(_) => {}
        }
        result.map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use cairn_llm::interface::complete::{
        Complete, CompletionCall, CompletionError, CompletionReply,
    };
    use cairn_llm::interface::{Llm, LlmScope};
    use serde_json::{json, Value};

    use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth};
    use crate::models::{CreateRecipe, Recipe, RecipeFile, UpdateRecipe};
    use crate::orchestrator::llm::LlmRuntime;
    use crate::orchestrator::Orchestrator;

    /// A chat server that answers from a script and counts what it was asked.
    #[derive(Default)]
    struct Scripted {
        replies: Mutex<VecDeque<Value>>,
        calls: Mutex<usize>,
    }

    impl Scripted {
        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }

    impl Complete for Scripted {
        fn complete(&self, _call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
            *self.calls.lock().unwrap() += 1;
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("a request the script did not expect");
            Ok(CompletionReply::Chat(
                serde_json::from_value(reply).unwrap(),
            ))
        }
    }

    /// What a chat server that serves no logprobs answers.
    fn without_logprobs() -> Value {
        json!({"model": "qwen3", "choices": [{"message": {"role": "assistant", "content": "Yes"}}]})
    }

    /// What a chat server that serves logprobs answers.
    fn with_logprobs() -> Value {
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
        let mut orch = crate::orchestrator::llm::tests::bare_orchestrator().await;
        let scripted = Arc::new(Scripted {
            replies: Mutex::new(replies.into()),
            ..Scripted::default()
        });
        orch.llm_runtime = LlmRuntime {
            complete: scripted.clone(),
            ..orch.llm_runtime.clone()
        };
        let account = orch
            .add_custom_provider(
                "Sparks".into(),
                ProviderAuth::custom_endpoint(
                    "http://sparks.invalid/v1",
                    CustomProtocol::OpenAiChatCompletions,
                    None,
                    &[],
                    CustomModelPolicy::Explicit {
                        ids: vec!["qwen3".into()],
                    },
                    CustomSampling::default(),
                    1,
                )
                .unwrap(),
                None,
            )
            .unwrap();
        orch.refresh_provider_model_catalog_blocking(&cairn_llm::types::catalog::custom_key(
            &account.id,
        ));
        (orch, scripted)
    }

    fn decision_recipe(options: &[String]) -> Recipe {
        let options = options
            .iter()
            .map(|value| format!("{{value: {value}}}"))
            .collect::<Vec<_>>()
            .join(", ");
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
    config:
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
        let catalog = orch.catalog(&LlmScope::default()).unwrap();
        assert!(catalog.decision_offers().is_empty());

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
