//! The route side of a decision node: the answer fields a decision makes
//! known to everything below it, and the resolution of a declared option
//! source into ordinary options before the question is asked.
//!
//! No decision type is defined here. The node, its questions, the answers and
//! the threshold rules are `cairn_domain::decision`'s; this module only turns
//! them into the flat `answers.<question>.<field>` keys the clause language
//! and argument bindings read.

use std::collections::BTreeMap;

use cairn_common::uri::{parse_uri, CairnResource};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{RouteDefinition, RouteNodeConfig, ANSWERS_PREFIX};
use crate::models::decision::{
    question_problems, Abstention, Decision, DecisionAnswer, DecisionNode, DecisionOption,
    DecisionQuestion, OptionSource, QuestionKind,
};
use crate::orchestrator::{DecisionDiagnostic, DecisionFailure, NamedDecision, Orchestrator};

/// What became of one question, the five values of `answers.<q>.outcome`.
pub const OUTCOMES: &[&str] = &[
    "admitted",
    "below_threshold",
    "refused",
    "unanswerable",
    "failed",
];

/// The values of `answers.<q>.measuredBy`.
pub const MEASURED_BY: &[&str] = &["native", "logprobs"];

/// How many digest characters an option's `revision` carries.
const REVISION_CHARS: usize = 12;

/// The values an answer field can hold, which is what an edge clause over it
/// is checked against.
#[derive(Debug, Clone, PartialEq)]
pub enum AnswerValues {
    /// A number; the only kind a comparison operator applies to.
    Number,
    Bool,
    /// One of a closed set of strings.
    Closed(Vec<String>),
    /// Any string: a sourced question's option values, known only once the
    /// source is resolved at fire time.
    Any,
}

/// One `answers.<question>.<field>` key a decision makes known.
#[derive(Debug, Clone, PartialEq)]
pub struct AnswerField {
    /// The whole key, `answers.<question>.<field>`.
    pub key: String,
    pub values: AnswerValues,
    /// Whether the field holds a list (`picks`, `addresses`).
    pub list: bool,
    /// Whether a message sink can deliver to the field's values.
    pub address: bool,
}

/// `answers.<question>.<rest>` as its question and the rest, when `key` is an
/// answer key at all.
pub fn split_answer_key(key: &str) -> Option<(&str, &str)> {
    key.strip_prefix(ANSWERS_PREFIX)?
        .split_once('.')
        .filter(|(question, rest)| !question.is_empty() && !rest.is_empty())
}

/// Whether a value is an address a message sink can deliver to.
pub(crate) fn is_message_target(value: &str) -> bool {
    matches!(
        parse_uri(value),
        Some(
            CairnResource::Project { .. }
                | CairnResource::Issue { .. }
                | CairnResource::Thread { .. }
        )
    ) && !matches!(parse_uri(value), Some(CairnResource::Thread { path, .. }) if !path.is_empty())
}

fn choice_options(question: &DecisionQuestion) -> Option<&[DecisionOption]> {
    match &question.kind {
        QuestionKind::Choice { options, .. } => Some(options),
        _ => None,
    }
}

/// Every answer field one question makes known, in a stable order. A
/// sourced choice's probabilities are listed as `probability.*`: its option
/// values are not known until the source resolves.
pub fn answer_vocabulary(question: &DecisionQuestion) -> Vec<AnswerField> {
    let key = |rest: &str| format!("{ANSWERS_PREFIX}{}.{rest}", question.name);
    let closed =
        |values: &[&str]| AnswerValues::Closed(values.iter().map(|v| v.to_string()).collect());
    let sourced = question.option_source().is_some();
    let answers = if sourced {
        AnswerValues::Any
    } else {
        closed(&question.answer_values())
    };
    let options = choice_options(question);
    let picks_are_addresses = options.is_some_and(|options| {
        !options.is_empty()
            && options
                .iter()
                .all(|option| is_message_target(&option.value))
    });
    let field = |rest: &str, values: AnswerValues, list: bool, address: bool| AnswerField {
        key: key(rest),
        values,
        list,
        address,
    };
    let mut fields = vec![
        field("outcome", closed(OUTCOMES), false, false),
        field("refused", AnswerValues::Bool, false, false),
        field("pick", answers.clone(), false, picks_are_addresses),
        field("picks", answers.clone(), true, picks_are_addresses),
        field("reason", AnswerValues::Any, false, false),
        field("citation", AnswerValues::Any, false, false),
        field("citations", AnswerValues::Any, true, false),
    ];
    if options.is_some_and(|options| sourced || options.iter().any(|o| o.address.is_some())) {
        fields.push(field("address", AnswerValues::Any, false, true));
        fields.push(field("addresses", AnswerValues::Any, true, true));
    }
    fields.push(field("confidence", AnswerValues::Number, false, false));
    fields.push(field("measuredBy", closed(MEASURED_BY), false, false));
    if sourced {
        fields.push(field("probability.*", AnswerValues::Number, false, false));
    } else {
        for value in question.answer_values() {
            fields.push(field(
                &format!("probability.{value}"),
                AnswerValues::Number,
                false,
                false,
            ));
        }
    }
    if matches!(question.kind, QuestionKind::Score { .. }) {
        fields.push(field("score", AnswerValues::Number, false, false));
    }
    fields
}

/// The answer field `rest` names on `question`, or why it names none.
pub fn answer_field(question: &DecisionQuestion, rest: &str) -> Result<AnswerField, String> {
    let vocabulary = answer_vocabulary(question);
    let prefix = format!("{ANSWERS_PREFIX}{}.", question.name);
    if let Some(field) = vocabulary
        .iter()
        .find(|field| field.key.strip_prefix(&prefix) == Some(rest))
    {
        return Ok(field.clone());
    }
    if let Some(value) = rest.strip_prefix("probability.") {
        if question.option_source().is_some() && !value.is_empty() {
            return Ok(AnswerField {
                key: format!("{ANSWERS_PREFIX}{}.{rest}", question.name),
                values: AnswerValues::Number,
                list: false,
                address: false,
            });
        }
        return Err(format!(
            "question '{}' has no answer '{value}'; it answers {}",
            question.name,
            question.answer_values().join(", ")
        ));
    }
    let known: Vec<&str> = vocabulary
        .iter()
        .filter_map(|field| field.key.strip_prefix(&prefix))
        .collect();
    Err(format!(
        "question '{}' has no answer field '{rest}'; it has {}",
        question.name,
        known.join(", ")
    ))
}

/// Whether `value` is a legal right-hand side for `field` in a clause.
/// Comparison operators apply to numbers only; a closed set admits only its
/// own values.
pub fn check_clause_value(field: &AnswerField, value: &Value) -> Result<(), String> {
    let key = &field.key;
    if let Value::Object(_) = value {
        return match field.values {
            AnswerValues::Number => Ok(()),
            _ => Err(format!(
                "'{key}' is not a number, so it cannot be compared with gt, gte, lt, or lte"
            )),
        };
    }
    let items: Vec<&Value> = match value {
        Value::Array(items) => items.iter().collect(),
        value => vec![value],
    };
    for item in items {
        match &field.values {
            AnswerValues::Number if !item.is_number() => {
                return Err(format!("'{key}' is a number, not {item}"))
            }
            AnswerValues::Bool if !item.is_boolean() => {
                return Err(format!("'{key}' is true or false, not {item}"))
            }
            AnswerValues::Closed(values)
                if !item
                    .as_str()
                    .is_some_and(|item| values.iter().any(|v| v == item)) =>
            {
                return Err(format!(
                    "'{key}' is one of {}, not {item}",
                    values.join(", ")
                ))
            }
            AnswerValues::Any if !item.is_string() => {
                return Err(format!("'{key}' is a string, not {item}"))
            }
            _ => {}
        }
    }
    Ok(())
}

/// What became of one question on one firing: the five-valued outcome and
/// everything the answer reported, before it is flattened into fields.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionOutcome {
    pub name: String,
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pick: Option<String>,
    pub picks: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citation: Option<String>,
    pub citations: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_by: Option<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub probabilities: Vec<(String, f64)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    /// Why nothing was admitted, when nothing was.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl QuestionOutcome {
    fn empty(name: &str, outcome: &'static str, reason: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            outcome,
            pick: None,
            picks: Vec::new(),
            citation: None,
            citations: Vec::new(),
            address: None,
            addresses: Vec::new(),
            confidence: None,
            measured_by: None,
            probabilities: Vec::new(),
            score: None,
            reason,
        }
    }
}

/// Each asked question's outcome, in question order. A node-level failure
/// is every question's outcome; a question the reply left out failed.
pub(crate) fn outcomes(
    asked: &DecisionNode,
    result: &Result<Decision, DecisionFailure>,
) -> Vec<QuestionOutcome> {
    asked
        .questions
        .iter()
        .map(|question| match result {
            Err(DecisionFailure::Unanswerable(why)) => {
                QuestionOutcome::empty(&question.name, "unanswerable", Some(why.clone()))
            }
            Err(DecisionFailure::Failed(why)) => {
                QuestionOutcome::empty(&question.name, "failed", Some(why.clone()))
            }
            Ok(decision) => match decision.answer(&question.name) {
                None => QuestionOutcome::empty(
                    &question.name,
                    "failed",
                    Some(format!(
                        "the reply carried no answer for '{}'",
                        question.name
                    )),
                ),
                Some(answer) => question_outcome(asked, question, answer),
            },
        })
        .collect()
}

fn citation_for(
    question: &DecisionQuestion,
    pick: &str,
    measured_by: &str,
    probabilities: &[(String, f64)],
) -> String {
    let mut components = Vec::new();
    if let Some((_, probability)) = probabilities.iter().find(|(value, _)| value == pick) {
        components.push(crate::text_template::format_number(*probability));
    }
    components.push(measured_by.to_owned());
    if let Some(revision) = choice_options(question)
        .and_then(|options| options.iter().find(|option| option.value == pick))
        .and_then(|option| option.revision.as_deref())
    {
        components.push(format!("rev {revision}"));
    }
    format!("{pick} ({})", components.join(", "))
}

fn question_outcome(
    asked: &DecisionNode,
    question: &DecisionQuestion,
    answer: &DecisionAnswer,
) -> QuestionOutcome {
    let method = answer.measured_by().as_str();
    let measured_by = Some(method);
    if let DecisionAnswer::Refusal { reason, .. } = answer {
        return QuestionOutcome {
            measured_by,
            ..QuestionOutcome::empty(
                &question.name,
                "refused",
                Some(reason.clone().unwrap_or_else(|| {
                    Abstention::Refused {
                        question: question.name.clone(),
                        reason: None,
                    }
                    .to_string()
                })),
            )
        };
    }
    let address_of = |value: &str| {
        choice_options(question)?
            .iter()
            .find(|option| option.value == value)?
            .address
            .clone()
    };
    let probabilities: Vec<(String, f64)> = match answer {
        DecisionAnswer::Choice { probabilities, .. }
        | DecisionAnswer::Score { probabilities, .. } => probabilities
            .iter()
            .map(|option| (option.value.clone(), option.probability))
            .collect(),
        DecisionAnswer::Predicate {
            probability_yes, ..
        } => vec![
            ("yes".to_string(), *probability_yes),
            ("no".to_string(), 1.0 - probability_yes),
        ],
        DecisionAnswer::Refusal { .. } => Vec::new(),
    };
    let pick = answer.pick().map(str::to_string);
    let declared = question.answer_values();
    let (outcome, picks, reason) =
        match asked
            .thresholds
            .admit_top(answer, &declared, question.top_k())
        {
            Ok(picks) => (
                "admitted",
                picks.into_iter().map(str::to_string).collect(),
                None,
            ),
            Err(abstention @ Abstention::BelowThreshold { .. }) => {
                ("below_threshold", Vec::new(), Some(abstention.to_string()))
            }
            Err(abstention @ Abstention::Refused { .. }) => {
                ("refused", Vec::new(), Some(abstention.to_string()))
            }
        };
    let addresses = picks
        .iter()
        .filter_map(|value: &String| address_of(value))
        .collect();
    let citation = pick
        .as_deref()
        .map(|value| citation_for(question, value, method, &probabilities));
    let citations = picks
        .iter()
        .map(|value| citation_for(question, value, method, &probabilities))
        .collect();
    QuestionOutcome {
        name: question.name.clone(),
        outcome,
        citation,
        citations,
        address: pick.as_deref().and_then(address_of),
        pick,
        picks,
        addresses,
        confidence: answer.confidence(),
        measured_by,
        probabilities,
        score: match answer {
            DecisionAnswer::Score { score, .. } => Some(*score),
            _ => None,
        },
        reason,
    }
}

/// The flat `answers.<question>.<field>` keys the outcomes make known to
/// every node a decision dominates.
pub fn answer_fields(outcomes: &[QuestionOutcome]) -> BTreeMap<String, Value> {
    let mut fields = BTreeMap::new();
    for outcome in outcomes {
        let key = |rest: &str| format!("{ANSWERS_PREFIX}{}.{rest}", outcome.name);
        fields.insert(key("outcome"), json!(outcome.outcome));
        fields.insert(key("refused"), json!(outcome.outcome == "refused"));
        fields.insert(key("picks"), json!(outcome.picks));
        fields.insert(key("citations"), json!(outcome.citations));
        if let Some(citation) = &outcome.citation {
            fields.insert(key("citation"), json!(citation));
        }
        if let Some(reason) = &outcome.reason {
            fields.insert(key("reason"), json!(reason));
        }
        if let Some(pick) = &outcome.pick {
            fields.insert(key("pick"), json!(pick));
        }
        if let Some(address) = &outcome.address {
            fields.insert(key("address"), json!(address));
        }
        fields.insert(key("addresses"), json!(outcome.addresses));
        if let Some(confidence) = outcome.confidence {
            fields.insert(key("confidence"), json!(confidence));
        }
        if let Some(measured_by) = outcome.measured_by {
            fields.insert(key("measuredBy"), json!(measured_by));
        }
        for (value, probability) in &outcome.probabilities {
            fields.insert(key(&format!("probability.{value}")), json!(probability));
        }
        if let Some(score) = outcome.score {
            fields.insert(key("score"), json!(score));
        }
    }
    fields
}

/// The outcomes keyed by question, as a trace or a journal row shows them.
pub fn summary(outcomes: &[QuestionOutcome]) -> Value {
    Value::Object(
        outcomes
            .iter()
            .map(|outcome| {
                (
                    outcome.name.clone(),
                    serde_json::to_value(outcome).unwrap_or(Value::Null),
                )
            })
            .collect(),
    )
}

/// The first [`REVISION_CHARS`] hex characters of the SHA-256 of `text`: a
/// version that changes exactly when the text does.
pub fn revision(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()[..REVISION_CHARS]
        .to_string()
}

/// Whether `thread` can own routed work: active, with a non-empty charter.
pub fn is_chartered_active(thread: &crate::models::Thread) -> bool {
    thread.status == crate::models::ThreadStatus::Active
        && thread
            .jurisdiction
            .as_deref()
            .is_some_and(|charter| !charter.trim().is_empty())
}

/// The chartered threads of a project as choice options: every active thread
/// with a non-empty jurisdiction, by name, each carrying its jurisdiction as
/// the description, its URI as the address, and the jurisdiction's digest as
/// the revision.
pub fn chartered_options(
    project_key: &str,
    threads: &[crate::models::Thread],
) -> Vec<DecisionOption> {
    let mut options: Vec<DecisionOption> = threads
        .iter()
        .filter(|thread| is_chartered_active(thread))
        .filter_map(|thread| {
            let charter = thread.jurisdiction.as_deref()?.trim();
            Some(DecisionOption {
                value: thread.name.clone(),
                description: Some(charter.to_string()),
                address: Some(cairn_common::uri::build_thread_uri(
                    project_key,
                    &thread.name,
                )),
                revision: Some(revision(charter)),
            })
        })
        .collect();
    options.sort_by(|a, b| a.value.cmp(&b.value));
    options
}

/// `authored` with every declared option source resolved into ordinary
/// options against `project_id`'s threads, so the facet receives a literal
/// question. A source that cannot resolve (no project, or no chartered
/// thread) is unanswerable as configured.
pub(crate) async fn resolve_sources(
    orch: &Orchestrator,
    project_id: Option<&str>,
    authored: &DecisionNode,
) -> Result<DecisionNode, DecisionFailure> {
    let mut asked = authored.clone();
    let mut chartered: Option<Vec<DecisionOption>> = None;
    for question in &mut asked.questions {
        let QuestionKind::Choice {
            options, source, ..
        } = &mut question.kind
        else {
            continue;
        };
        let Some(OptionSource::CharteredThreads) = *source else {
            continue;
        };
        let resolved = match &chartered {
            Some(resolved) => resolved.clone(),
            None => {
                let resolved = chartered_threads(orch, project_id, &question.name).await?;
                chartered = Some(resolved.clone());
                resolved
            }
        };
        if resolved.is_empty() {
            return Err(DecisionFailure::Unanswerable(format!(
                "choice question '{}' draws its options from chartered_threads, and the project has no active thread with a jurisdiction",
                question.name
            )));
        }
        *options = resolved;
        *source = None;
    }
    Ok(asked)
}

async fn chartered_threads(
    orch: &Orchestrator,
    project_id: Option<&str>,
    question: &str,
) -> Result<Vec<DecisionOption>, DecisionFailure> {
    let Some(project_id) = project_id else {
        return Err(DecisionFailure::Unanswerable(format!(
            "choice question '{question}' draws its options from chartered_threads, and this fact belongs to no project"
        )));
    };
    let key = orch
        .db
        .local
        .query_opt_text("SELECT key FROM projects WHERE id = ?1", (project_id.to_string(),))
        .await
        .map_err(|error| DecisionFailure::Failed(format!("reading the project failed: {error}")))?
        .ok_or_else(|| {
            DecisionFailure::Unanswerable(format!(
                "choice question '{question}' draws its options from chartered_threads, and project {project_id} was not found"
            ))
        })?;
    let db = orch.db.for_project(&key).await;
    let threads = crate::threads::crud::list(&db, project_id)
        .await
        .map_err(|error| DecisionFailure::Failed(format!("listing the threads failed: {error}")))?;
    Ok(chartered_options(&key, &threads))
}

/// A route's decision nodes as their capability is measured: option sources
/// resolved against `project_id` when one is in scope. A source that cannot
/// resolve here (no project yet, or no chartered thread) is measured with
/// stand-in options instead, with a note when there is a project to say why:
/// whether any offer serves `decide` and whether the node's `models` resolve
/// do not depend on the option values, so those still block the save, while
/// the real options are resolved again, and journaled, on every firing.
async fn measured_decisions(
    orch: &Orchestrator,
    definition: &RouteDefinition,
    project_id: Option<&str>,
) -> (Vec<(String, String, DecisionNode)>, Vec<DecisionDiagnostic>) {
    let mut measured = Vec::new();
    let mut notes = Vec::new();
    for node in &definition.nodes {
        let RouteNodeConfig::Decision { decision, .. } = &node.config else {
            continue;
        };
        let name = if node.name.is_empty() {
            node.id.clone()
        } else {
            node.name.clone()
        };
        let sourced = decision
            .questions
            .iter()
            .any(|question| question.option_source().is_some());
        let asked = match (sourced, project_id) {
            (false, _) => decision.clone(),
            (true, Some(_)) => match resolve_sources(orch, project_id, decision).await {
                Ok(resolved) => resolved,
                Err(failure) => {
                    notes.push(DecisionDiagnostic {
                        node_id: node.id.clone(),
                        node_name: name.clone(),
                        blocking: false,
                        message: format!("Decision node '{name}' can't be measured yet: {failure}"),
                    });
                    with_stand_in_options(decision)
                }
            },
            (true, None) => with_stand_in_options(decision),
        };
        measured.push((node.id.clone(), name, asked));
    }
    (measured, notes)
}

/// `decision` with each sourced choice given placeholder options in place of
/// its source: as many as its `topK` admits, and never fewer than two, so the
/// question stays a well-formed choice.
fn with_stand_in_options(decision: &DecisionNode) -> DecisionNode {
    let mut asked = decision.clone();
    for question in &mut asked.questions {
        let picks = question.top_k().max(2);
        let QuestionKind::Choice {
            options, source, ..
        } = &mut question.kind
        else {
            continue;
        };
        if source.take().is_none() {
            continue;
        }
        *options = (1..=picks)
            .map(|n| DecisionOption {
                value: format!("option-{n}"),
                description: None,
                address: None,
                revision: None,
            })
            .collect();
    }
    asked
}

fn named(measured: &[(String, String, DecisionNode)]) -> Vec<NamedDecision<'_>> {
    measured
        .iter()
        .map(|(id, name, decision)| NamedDecision { id, name, decision })
        .collect()
}

/// Where each of a route's decision nodes stands, from what is already known
/// about the catalog: the editor's Incomplete-panel report. No provider is
/// asked.
pub async fn route_decision_diagnostics(
    orch: &Orchestrator,
    definition: &RouteDefinition,
    project_id: Option<&str>,
) -> Vec<DecisionDiagnostic> {
    let (measured, mut notes) = measured_decisions(orch, definition, project_id).await;
    notes.extend(orch.diagnose_decisions(&named(&measured), project_id, "Saving the route"));
    notes
}

/// Refuse a route whose decision nodes nothing in scope can answer, naming
/// the node and the fixes. Called where a route is authored, never where one
/// is loaded: a shipped decision route still loads on an install with no
/// decide offer, and its firings journal `unanswerable`.
pub async fn refuse_unanswerable_decisions(
    orch: &Orchestrator,
    definition: &RouteDefinition,
    project_id: Option<&str>,
) -> Result<(), String> {
    let (measured, _) = measured_decisions(orch, definition, project_id).await;
    if measured.is_empty() {
        return Ok(());
    }
    let (orch, project_id) = (orch.clone(), project_id.map(str::to_owned));
    // Selection may probe an unconfirmed logprobs offer, which blocks.
    let problems = tokio::task::spawn_blocking(move || {
        orch.named_decision_problems(&named(&measured), project_id.as_deref())
    })
    .await
    .map_err(|error| format!("checking the route's decisions failed: {error}"))?;
    match problems.as_slice() {
        [] => Ok(()),
        problems => Err(problems.join("\n")),
    }
}

/// One decision node's run on one firing: the questions as asked (sources
/// resolved) and what came of asking them.
#[derive(Debug, Clone)]
pub(crate) struct DecisionRun {
    pub asked: DecisionNode,
    pub result: Result<Decision, DecisionFailure>,
}

/// Resolve `authored`'s sources and ask it about `input` through the
/// orchestrator's one decide path, the recipe consumer's selection, probe
/// and observation included.
pub(crate) async fn run(
    orch: &Orchestrator,
    project_id: Option<&str>,
    authored: &DecisionNode,
    input: String,
) -> DecisionRun {
    let asked = match resolve_sources(orch, project_id, authored).await {
        Ok(asked) => asked,
        Err(failure) => {
            return DecisionRun {
                asked: authored.clone(),
                result: Err(failure),
            }
        }
    };
    let problems = question_problems(&asked.questions);
    if !problems.is_empty() {
        return DecisionRun {
            asked,
            result: Err(DecisionFailure::Unanswerable(format!(
                "These questions can't be asked: {}.",
                problems.join("; ")
            ))),
        };
    }
    let result = orch.decide(project_id, &asked, input).await;
    DecisionRun { asked, result }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::decision::{Decision, MeasuredBy, OptionProbability, Thresholds};

    fn listed(name: &str, values: &[&str]) -> DecisionQuestion {
        DecisionQuestion::choice(
            name,
            "Which?",
            values
                .iter()
                .map(|value| DecisionOption::new(*value, None))
                .collect(),
        )
    }

    fn node(question: DecisionQuestion, native: Option<f64>) -> DecisionNode {
        DecisionNode {
            questions: vec![question],
            models: None,
            thresholds: Thresholds {
                native,
                logprobs: None,
            },
        }
    }

    fn answered(answer: DecisionAnswer) -> Result<Decision, DecisionFailure> {
        Ok(Decision {
            provider: "custom:x".into(),
            model: "jev".into(),
            answers: vec![answer],
            usage: Default::default(),
        })
    }

    fn presentation_choice(top_k: u8) -> DecisionQuestion {
        let mut question = listed("thread", &["execution", "devops"]);
        if let QuestionKind::Choice {
            options,
            top_k: count,
            ..
        } = &mut question.kind
        {
            *count = Some(top_k);
            options[0].revision = Some("9f8e7d6c5b4a".into());
            options[1].revision = Some("1a2b3c4d5e6f".into());
        }
        question
    }

    fn presentation_answer(
        probabilities: &[(&str, f64)],
        confidence: f64,
    ) -> Result<Decision, DecisionFailure> {
        answered(DecisionAnswer::Choice {
            name: "thread".into(),
            measured_by: MeasuredBy::Native,
            pick: "devops".into(),
            confidence: Some(confidence),
            probabilities: probabilities
                .iter()
                .map(|(value, probability)| OptionProbability {
                    value: (*value).into(),
                    probability: *probability,
                })
                .collect(),
        })
    }

    fn assert_presentation_parity(
        asked: &DecisionNode,
        result: &Result<Decision, DecisionFailure>,
    ) -> BTreeMap<String, Value> {
        let outcomes = outcomes(asked, result);
        let trace = summary(&outcomes);
        let fields = answer_fields(&outcomes);
        for outcome in &outcomes {
            for rest in ["reason", "citation", "citations"] {
                let key = format!("answers.{}.{rest}", outcome.name);
                let value = trace[&outcome.name].get(rest);
                assert_eq!(fields.get(&key), value, "{key}");
                let descriptor = answer_field(&asked.questions[0], rest).unwrap();
                assert_eq!(descriptor.values, AnswerValues::Any);
                assert_eq!(descriptor.list, rest == "citations");
                assert!(!descriptor.address);
                if let Some(value) = value {
                    check_clause_value(&descriptor, value).unwrap();
                }
            }
        }
        fields
    }

    #[test]
    fn presentation_top_k_matches_revisions_and_probabilities_by_value() {
        // A 0.61 runner-up over two options has chance-normalized confidence 0.22.
        let asked = node(presentation_choice(2), Some(0.2));
        // Option declaration, probability report and admitted rank deliberately differ.
        let result = presentation_answer(&[("execution", 0.61), ("devops", 0.82)], 0.82);
        let fields = assert_presentation_parity(&asked, &result);
        assert_eq!(
            fields["answers.thread.citation"],
            json!("devops (0.82, native, rev 1a2b3c4d5e6f)")
        );
        assert_eq!(
            fields["answers.thread.citations"],
            json!([
                "devops (0.82, native, rev 1a2b3c4d5e6f)",
                "execution (0.61, native, rev 9f8e7d6c5b4a)"
            ])
        );
        let citations = fields["answers.thread.citations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(format!("Routed to {citations}."), "Routed to devops (0.82, native, rev 1a2b3c4d5e6f), execution (0.61, native, rev 9f8e7d6c5b4a).");
        assert!(!fields.contains_key("answers.thread.reason"));
    }

    #[test]
    fn presentation_below_threshold_preserves_closest_citation() {
        let asked = node(presentation_choice(1), Some(0.5));
        let result = presentation_answer(&[("execution", 0.2), ("devops", 0.41)], 0.41);
        let fields = assert_presentation_parity(&asked, &result);
        assert_eq!(fields["answers.thread.outcome"], json!("below_threshold"));
        assert_eq!(fields["answers.thread.citations"], json!([]));
        assert_eq!(
            fields["answers.thread.citation"],
            json!("devops (0.41, native, rev 1a2b3c4d5e6f)")
        );
        assert_eq!(format!("No thread claimed this (closest: {}). Start with @thread or #channel to route it.",
            fields["answers.thread.citation"].as_str().unwrap()),
            "No thread claimed this (closest: devops (0.41, native, rev 1a2b3c4d5e6f)). Start with @thread or #channel to route it.");
        assert!(fields["answers.thread.reason"]
            .as_str()
            .unwrap()
            .contains("below the native threshold"));
    }

    #[test]
    fn presentation_unavailable_reasons_are_verbatim_with_refusal_fallback() {
        let asked = node(presentation_choice(1), Some(0.5));
        let cases = [
            (
                answered(DecisionAnswer::Refusal {
                    name: "thread".into(),
                    measured_by: MeasuredBy::Native,
                    reason: Some(" policy text ".into()),
                }),
                " policy text ",
            ),
            (
                answered(DecisionAnswer::Refusal {
                    name: "thread".into(),
                    measured_by: MeasuredBy::Native,
                    reason: None,
                }),
                "the model refused to answer 'thread'",
            ),
            (
                Err(DecisionFailure::Unanswerable("no suitable offer".into())),
                "no suitable offer",
            ),
            (
                Err(DecisionFailure::Failed("provider failed".into())),
                "provider failed",
            ),
            (
                answered(DecisionAnswer::Refusal {
                    name: "other".into(),
                    measured_by: MeasuredBy::Native,
                    reason: None,
                }),
                "the reply carried no answer for 'thread'",
            ),
        ];
        for (result, expected) in cases {
            let fields = assert_presentation_parity(&asked, &result);
            assert_eq!(fields["answers.thread.reason"], json!(expected));
            assert_eq!(fields["answers.thread.citations"], json!([]));
            assert!(!fields.contains_key("answers.thread.citation"));
            assert_eq!(format!("Routing is unavailable: {}. Start with @thread or #channel to route it.",
                fields["answers.thread.reason"].as_str().unwrap()),
                format!("Routing is unavailable: {expected}. Start with @thread or #channel to route it."));
        }
    }

    #[test]
    fn presentation_omits_unreported_probability_and_literal_revision() {
        let asked = node(listed("thread", &["execution", "devops"]), Some(0.5));
        let fields = assert_presentation_parity(&asked, &presentation_answer(&[], 0.9));
        assert_eq!(fields["answers.thread.citation"], json!("devops (native)"));
        assert_eq!(
            fields["answers.thread.citations"],
            json!(["devops (native)"])
        );
        let fields =
            assert_presentation_parity(&asked, &presentation_answer(&[("devops", 0.826)], 0.9));
        assert_eq!(
            fields["answers.thread.citation"],
            json!("devops (0.83, native)")
        );
        let asked = node(presentation_choice(1), Some(0.5));
        let fields = assert_presentation_parity(&asked, &presentation_answer(&[], 0.9));
        assert_eq!(
            fields["answers.thread.citation"],
            json!("devops (native, rev 1a2b3c4d5e6f)")
        );
    }

    #[test]
    fn presentation_predicate_and_score_share_vocabulary_and_trace_fields() {
        let predicate: DecisionQuestion = serde_json::from_value(json!({
            "name": "q", "instructions": "Yes?", "type": "predicate"
        }))
        .unwrap();
        let score: DecisionQuestion = serde_json::from_value(json!({
            "name": "q", "instructions": "Level?", "type": "score",
            "levels": [{"label":"low"}, {"label":"high"}]
        }))
        .unwrap();
        for (question, answer, expected) in [
            (
                predicate,
                DecisionAnswer::Predicate {
                    name: "q".into(),
                    measured_by: MeasuredBy::Logprobs,
                    probability_yes: 0.8,
                },
                "yes (0.8, logprobs)",
            ),
            (
                score,
                DecisionAnswer::Score {
                    name: "q".into(),
                    measured_by: MeasuredBy::Native,
                    score: 1.0,
                    pick: "high".into(),
                    confidence: Some(0.9),
                    probabilities: vec![
                        OptionProbability {
                            value: "high".into(),
                            probability: 1.0,
                        },
                        OptionProbability {
                            value: "low".into(),
                            probability: 0.0,
                        },
                    ],
                },
                "high (1, native)",
            ),
        ] {
            let fields = assert_presentation_parity(&node(question, None), &answered(answer));
            assert_eq!(fields["answers.q.citation"], json!(expected));
            assert_eq!(fields["answers.q.citations"], json!([expected]));
            assert!(!fields.contains_key("answers.q.reason"));
        }
    }

    #[test]
    fn an_admitted_choice_flattens_into_the_answer_vocabulary() {
        let mut question = listed("target", &["cairn://p/cairn/12", "cairn://p/cairn/34"]);
        if let QuestionKind::Choice { options, .. } = &mut question.kind {
            options[1].address = Some("cairn://p/cairn/34".into());
        }
        let asked = node(question, Some(0.6));
        let result = answered(DecisionAnswer::Choice {
            name: "target".into(),
            measured_by: MeasuredBy::Native,
            pick: "cairn://p/cairn/34".into(),
            confidence: Some(0.8),
            probabilities: vec![
                OptionProbability {
                    value: "cairn://p/cairn/12".into(),
                    probability: 0.1,
                },
                OptionProbability {
                    value: "cairn://p/cairn/34".into(),
                    probability: 0.9,
                },
            ],
        });
        let fields = answer_fields(&outcomes(&asked, &result));
        assert_eq!(fields["answers.target.outcome"], json!("admitted"));
        assert_eq!(fields["answers.target.refused"], json!(false));
        assert_eq!(fields["answers.target.pick"], json!("cairn://p/cairn/34"));
        assert_eq!(
            fields["answers.target.picks"],
            json!(["cairn://p/cairn/34"])
        );
        assert_eq!(
            fields["answers.target.address"],
            json!("cairn://p/cairn/34")
        );
        assert_eq!(fields["answers.target.confidence"], json!(0.8));
        assert_eq!(fields["answers.target.measuredBy"], json!("native"));
        assert_eq!(
            fields["answers.target.probability.cairn://p/cairn/12"],
            json!(0.1)
        );
        // Every key the fields carry is one the vocabulary declares.
        let vocabulary = answer_vocabulary(&asked.questions[0]);
        for key in fields.keys() {
            let (_, rest) = split_answer_key(key).unwrap();
            assert!(answer_field(&asked.questions[0], rest).is_ok(), "{key}");
        }
        assert!(vocabulary
            .iter()
            .any(|field| field.key == "answers.target.pick" && field.address));
    }

    #[test]
    fn below_threshold_refused_and_failed_are_named_outcomes_with_no_picks() {
        let asked = node(listed("q", &["a", "b"]), Some(0.9));
        let low = answered(DecisionAnswer::Choice {
            name: "q".into(),
            measured_by: MeasuredBy::Native,
            pick: "a".into(),
            confidence: Some(0.2),
            probabilities: vec![],
        });
        let [outcome] = outcomes(&asked, &low).try_into().unwrap();
        assert_eq!(
            (outcome.outcome, outcome.pick.as_deref()),
            ("below_threshold", Some("a"))
        );
        assert!(outcome.picks.is_empty());

        let refused = answered(DecisionAnswer::Refusal {
            name: "q".into(),
            measured_by: MeasuredBy::Native,
            reason: Some("policy".into()),
        });
        let fields = answer_fields(&outcomes(&asked, &refused));
        assert_eq!(fields["answers.q.outcome"], json!("refused"));
        assert_eq!(fields["answers.q.refused"], json!(true));
        assert!(!fields.contains_key("answers.q.pick"));

        let failed: Result<Decision, DecisionFailure> =
            Err(DecisionFailure::Unanswerable("no offer".into()));
        let [outcome] = outcomes(&asked, &failed).try_into().unwrap();
        assert_eq!(outcome.outcome, "unanswerable");
        assert_eq!(outcome.reason.as_deref(), Some("no offer"));
    }

    #[test]
    fn chartered_options_are_active_threads_with_a_jurisdiction_carrying_a_digest() {
        let thread = |name: &str, jurisdiction: Option<&str>, status| crate::models::Thread {
            id: format!("t-{name}"),
            project_id: "p".into(),
            name: name.into(),
            jurisdiction: jurisdiction.map(str::to_string),
            status,
            attention: "none".into(),
            definition: None,
            migrated_from_number: None,
            created_at: 1,
            updated_at: 1,
        };
        use crate::models::ThreadStatus::{Active, Closed};
        let options = chartered_options(
            "cairn",
            &[
                thread("zeta", Some("Releases."), Active),
                thread("devops", Some(" How it ships. "), Active),
                thread("blank", Some("  "), Active),
                thread("none", None, Active),
                thread("gone", Some("Old."), Closed),
            ],
        );
        assert_eq!(
            options.iter().map(|o| o.value.as_str()).collect::<Vec<_>>(),
            ["devops", "zeta"]
        );
        assert_eq!(options[0].description.as_deref(), Some("How it ships."));
        assert_eq!(
            options[0].address.as_deref(),
            Some("cairn://p/cairn/devops")
        );
        assert_eq!(
            options[0].revision.as_deref(),
            Some(revision("How it ships.").as_str())
        );
        assert_eq!(options[0].revision.as_ref().unwrap().len(), 12);
        assert_ne!(revision("How it ships."), revision("How it ships!"));
    }

    /// The operator-ruled refusal belongs at authoring time: with no offer
    /// serving `decide`, saving the route is refused and the editor's report
    /// blocks with the facet's no-offer text.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_route_decision_nothing_can_answer_is_refused_and_diagnosed() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let registry = crate::routes::FactRegistry::default();
        let route =
            crate::routes::parse_definition(crate::routes::definition::FIXTURE_A, &registry)
                .unwrap();
        let error = refuse_unanswerable_decisions(&orch, &route, None)
            .await
            .unwrap_err();
        assert!(
            error.starts_with("Decision node 'pick' can't be answered"),
            "{error}"
        );
        assert!(error.contains("decision-capable"), "{error}");
        let diagnostics = route_decision_diagnostics(&orch, &route, None).await;
        let [diagnostic] = diagnostics.as_slice() else {
            panic!("one diagnostic: {diagnostics:?}");
        };
        assert!(diagnostic.blocking);
        assert_eq!(diagnostic.node_id, "pick");
        assert!(
            diagnostic.message.contains("decision-capable"),
            "{diagnostic:?}"
        );

        // A sourced question with no project in scope resolves its options
        // per firing, but whether anything serves `decide` does not depend on
        // them: a workspace-scoped chartered-threads route is refused too.
        let sourced =
            crate::routes::parse_definition(crate::routes::definition::FIXTURE_B, &registry)
                .unwrap();
        let error = refuse_unanswerable_decisions(&orch, &sourced, None)
            .await
            .unwrap_err();
        assert!(error.contains("can't be answered"), "{error}");
        assert!(error.contains("decision-capable"), "{error}");
        let diagnostics = route_decision_diagnostics(&orch, &sourced, None).await;
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.blocking
                    && diagnostic.message.contains("decision-capable")),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn a_sourced_choice_is_measured_with_stand_in_options() {
        let registry = crate::routes::FactRegistry::default();
        let route =
            crate::routes::parse_definition(crate::routes::definition::FIXTURE_B, &registry)
                .unwrap();
        let decision = route
            .nodes
            .iter()
            .find_map(|node| match &node.config {
                RouteNodeConfig::Decision { decision, .. } => Some(decision),
                _ => None,
            })
            .unwrap();
        let measured = with_stand_in_options(decision);
        assert_eq!(measured.questions.len(), decision.questions.len());
        for (authored, asked) in decision.questions.iter().zip(&measured.questions) {
            assert!(asked.option_source().is_none());
            if authored.option_source().is_some() {
                let QuestionKind::Choice { options, .. } = &asked.kind else {
                    panic!("a sourced question stays a choice");
                };
                assert!(options.len() >= authored.top_k().max(2), "{options:?}");
            }
        }
    }

    #[test]
    fn clause_values_are_checked_against_the_field() {
        let question = listed("q", &["a", "b"]);
        let field = |rest: &str| answer_field(&question, rest).unwrap();
        assert!(check_clause_value(&field("outcome"), &json!("admitted")).is_ok());
        assert!(check_clause_value(&field("outcome"), &json!(["unanswerable", "failed"])).is_ok());
        assert!(check_clause_value(&field("outcome"), &json!("raised"))
            .unwrap_err()
            .contains("one of admitted"));
        assert!(check_clause_value(&field("pick"), &json!("c")).is_err());
        assert!(check_clause_value(&field("confidence"), &json!({"gte": 0.6})).is_ok());
        assert!(check_clause_value(&field("pick"), &json!({"gt": 1}))
            .unwrap_err()
            .contains("not a number"));
        assert!(check_clause_value(&field("probability.a"), &json!({"gt": 0.5})).is_ok());
        assert!(answer_field(&question, "probability.c").is_err());
        assert!(answer_field(&question, "score").is_err());
        assert!(
            answer_field(&question, "address").is_err(),
            "listed options carry no address"
        );
        assert_eq!(
            split_answer_key("answers.q.probability.a"),
            Some(("q", "probability.a"))
        );
        assert_eq!(split_answer_key("answers.q"), None);
    }
}
