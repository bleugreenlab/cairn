//! The `decision` node kind and the typed answers that `decide` produces.
//!
//! A decision selects among finite answers and generates nothing: typed
//! questions go in, one typed answer per question comes out, each stating how
//! its probabilities were measured. It is the cheapest of Cairn's three cost
//! orders (Agent: minutes, delegated; Response: seconds, generates text;
//! Decision: sub-second, selects).
//!
//! This module is the one definition every consumer shares. Recipes wrap
//! [`DecisionNode`] as their `decision` node, whose ports are the degenerate
//! case of one `choice` question; routes wrap the same type with edge
//! predicates over the answers. The cairn-models facet's `decide` takes these
//! questions and returns a [`Decision`]; it defines no mirror of them.
//!
//! Two rules the types exist to keep:
//!
//! - **A refusal is its own answer.** [`DecisionAnswer::Refusal`] is never an
//!   option, never a probability of 0, and never folded into a pick.
//! - **A probability states how it was measured.** Native decision models
//!   return calibrated probabilities; a chat model's next-token logprobs
//!   measure its own distribution but are not calibrated. Every answer carries
//!   [`MeasuredBy`], and a threshold is keyed by it ([`Thresholds`]) rather
//!   than shared as one number.

use std::collections::HashSet;
use std::fmt;

use cairn_models_types::usage::{BillingUnit, Charge, Measure, Meter, MeteredUsage};
use serde::{Deserialize, Serialize};

/// The most options a `choice` question may offer (the industry cap).
pub const MAX_CHOICE_OPTIONS: usize = 255;
/// The fewest and most levels a `score` question may have.
pub const MIN_SCORE_LEVELS: usize = 2;
pub const MAX_SCORE_LEVELS: usize = 10;

/// The name a recipe decision node's one routing question carries.
pub const PORT_QUESTION_NAME: &str = "route";

/// How an answer's probabilities were measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MeasuredBy {
    /// A decision model's own probabilities over the answers (the Decisions
    /// wire). Calibrated by the provider.
    Native,
    /// A chat model's next-token log-probabilities over a closed set of answer
    /// labels, renormalized. A measurement, but not calibrated.
    Logprobs,
}

impl MeasuredBy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Logprobs => "logprobs",
        }
    }
}

impl fmt::Display for MeasuredBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One option of a `choice` question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionOption {
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// One level of a `score` question, lowest first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreLevel {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// What kind of answer a question takes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum QuestionKind {
    /// Pick one of 1-255 options.
    Choice { options: Vec<DecisionOption> },
    /// Yes or no.
    Predicate,
    /// Place the input on 2-10 ordered levels.
    Score { levels: Vec<ScoreLevel> },
}

/// One authored question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionQuestion {
    /// Unique within its node; the answer is bound as `answers.<name>`.
    pub name: String,
    pub instructions: String,
    #[serde(flatten)]
    pub kind: QuestionKind,
}

impl DecisionQuestion {
    /// The question a recipe's question-and-ports pair always meant: one
    /// `choice` whose options are the port names.
    pub fn port_choice(instructions: &str, ports: &[String]) -> Self {
        Self {
            name: PORT_QUESTION_NAME.to_string(),
            instructions: instructions.to_string(),
            kind: QuestionKind::Choice {
                options: ports
                    .iter()
                    .map(|port| DecisionOption {
                        value: port.clone(),
                        description: None,
                    })
                    .collect(),
            },
        }
    }

    /// The answers this question can take, in authored order: option values,
    /// level labels, or `yes`/`no`.
    pub fn answer_values(&self) -> Vec<&str> {
        match &self.kind {
            QuestionKind::Choice { options } => {
                options.iter().map(|option| option.value.as_str()).collect()
            }
            QuestionKind::Predicate => vec!["yes", "no"],
            QuestionKind::Score { levels } => {
                levels.iter().map(|level| level.label.as_str()).collect()
            }
        }
    }
}

/// The lowest confidence an answer's pick must reach to be acted on, per
/// measuring method. An unset method imposes no floor.
///
/// Keyed by method because a number set against calibrated native
/// probabilities does not transfer to an instruct model's logprobs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Thresholds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<f64>,
}

impl Thresholds {
    pub fn is_empty(&self) -> bool {
        self.native.is_none() && self.logprobs.is_none()
    }

    pub fn for_method(&self, method: MeasuredBy) -> Option<f64> {
        match method {
            MeasuredBy::Native => self.native,
            MeasuredBy::Logprobs => self.logprobs,
        }
    }

    /// The pick an answer may be acted on, or why it may not.
    ///
    /// A refusal is never admitted. With a threshold set for the answer's
    /// method, an answer that reports no confidence is not admitted either:
    /// the floor cannot be shown to hold.
    pub fn admit<'a>(&self, answer: &'a DecisionAnswer) -> Result<&'a str, Abstention> {
        let Some(pick) = answer.pick() else {
            return Err(Abstention::Refused {
                question: answer.name().to_string(),
                reason: answer.refusal_reason().map(str::to_string),
            });
        };
        let method = answer.measured_by();
        if let Some(threshold) = self.for_method(method) {
            match answer.confidence() {
                Some(confidence) if confidence >= threshold => {}
                confidence => {
                    return Err(Abstention::BelowThreshold {
                        question: answer.name().to_string(),
                        pick: pick.to_string(),
                        confidence,
                        threshold,
                        measured_by: method,
                    })
                }
            }
        }
        Ok(pick)
    }
}

/// Why an answer was not acted on.
#[derive(Debug, Clone, PartialEq)]
pub enum Abstention {
    Refused {
        question: String,
        reason: Option<String>,
    },
    BelowThreshold {
        question: String,
        pick: String,
        confidence: Option<f64>,
        threshold: f64,
        measured_by: MeasuredBy,
    },
}

impl fmt::Display for Abstention {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { question, reason } => {
                write!(f, "the model refused to answer '{question}'")?;
                match reason {
                    Some(reason) => write!(f, ": {reason}"),
                    None => Ok(()),
                }
            }
            Self::BelowThreshold {
                question,
                pick,
                confidence: Some(confidence),
                threshold,
                measured_by,
            } => write!(
                f,
                "'{question}' picked '{pick}' with {measured_by} confidence {confidence:.2}, below the {measured_by} threshold {threshold:.2}"
            ),
            Self::BelowThreshold {
                question,
                pick,
                confidence: None,
                threshold,
                measured_by,
            } => write!(
                f,
                "'{question}' picked '{pick}' but reported no confidence, so the {measured_by} threshold {threshold:.2} cannot be shown to hold"
            ),
        }
    }
}

/// One option's probability in an answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionProbability {
    /// The option value, or the level label for a score.
    pub value: String,
    pub probability: f64,
}

/// One question's answer, in the question's position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DecisionAnswer {
    #[serde(rename_all = "camelCase")]
    Choice {
        name: String,
        measured_by: MeasuredBy,
        pick: String,
        /// [`choice_confidence`]: the provider's when it reports one, else
        /// computed from `probabilities`. Absent when neither was reported.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confidence: Option<f64>,
        /// Every option's probability, in option order, when reported.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        probabilities: Vec<OptionProbability>,
    },
    #[serde(rename_all = "camelCase")]
    Predicate {
        name: String,
        measured_by: MeasuredBy,
        /// The probability that the answer is yes.
        probability_yes: f64,
    },
    #[serde(rename_all = "camelCase")]
    Score {
        name: String,
        measured_by: MeasuredBy,
        /// The probability-weighted mean of the level indexes, from 0.
        score: f64,
        /// The most probable level's label, else the level nearest `score`.
        pick: String,
        /// [`score_confidence`]: the provider's when it reports one, else
        /// computed from `probabilities`. Absent when neither was reported.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confidence: Option<f64>,
        /// Every level's probability, lowest level first, when reported.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        probabilities: Vec<OptionProbability>,
    },
    /// The model declined this question. Never an option, never a
    /// probability of 0: no pick exists.
    #[serde(rename_all = "camelCase")]
    Refusal {
        name: String,
        measured_by: MeasuredBy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl DecisionAnswer {
    pub fn name(&self) -> &str {
        match self {
            Self::Choice { name, .. }
            | Self::Predicate { name, .. }
            | Self::Score { name, .. }
            | Self::Refusal { name, .. } => name,
        }
    }

    pub fn measured_by(&self) -> MeasuredBy {
        match self {
            Self::Choice { measured_by, .. }
            | Self::Predicate { measured_by, .. }
            | Self::Score { measured_by, .. }
            | Self::Refusal { measured_by, .. } => *measured_by,
        }
    }

    /// The selected answer: an option value, a level label, or `yes`/`no`.
    /// `None` exactly when the model refused.
    pub fn pick(&self) -> Option<&str> {
        match self {
            Self::Choice { pick, .. } | Self::Score { pick, .. } => Some(pick),
            Self::Predicate {
                probability_yes, ..
            } => Some(if *probability_yes >= 0.5 { "yes" } else { "no" }),
            Self::Refusal { .. } => None,
        }
    }

    /// How concentrated the answer's distribution is, from 0 (an even
    /// spread) to 1 (all probability on the pick), when it can be stated.
    /// See [`choice_confidence`], [`score_confidence`], and
    /// [`predicate_confidence`].
    pub fn confidence(&self) -> Option<f64> {
        match self {
            Self::Choice { confidence, .. } | Self::Score { confidence, .. } => *confidence,
            Self::Predicate {
                probability_yes, ..
            } => Some(predicate_confidence(*probability_yes)),
            Self::Refusal { .. } => None,
        }
    }

    pub fn is_refusal(&self) -> bool {
        matches!(self, Self::Refusal { .. })
    }

    fn refusal_reason(&self) -> Option<&str> {
        match self {
            Self::Refusal { reason, .. } => reason.as_deref(),
            _ => None,
        }
    }
}

// Confidence is the statistic OpenAI's Decisions API and TypeSafe's Jev both
// return (https://docs.typesafe.ai/confidence#how-confidence-is-calculated),
// computed here so an answer measured any other way lands on the same scale and
// a threshold means one thing whatever answered. Unlike the pick's raw
// probability, it does not drift with the number of options: an even split is
// 0 whether a choice has two options or ten.

/// The index of the most probable answer in declared order, the first on a
/// tie (as the providers' reference does). The canonical pick wherever Cairn
/// picks from a distribution: a score's confidence depends on which tied peak
/// is the pick, so every route must break ties the same way, over the declared
/// order rather than the order a reply listed them in.
pub fn most_probable(probabilities: &[f64]) -> Option<usize> {
    probabilities
        .iter()
        .enumerate()
        .fold(
            None,
            |best: Option<(usize, f64)>, (index, &probability)| match best {
                Some((_, peak)) if peak.total_cmp(&probability).is_ge() => best,
                _ => Some((index, probability)),
            },
        )
        .map(|(index, _)| index)
}

/// A choice's confidence: how far the pick's probability sits above an even
/// split over the `options` the question declared, rescaled so the even split
/// is 0 and certainty is 1.
///
/// `options` is the declared count, not however many probabilities a route
/// reported: an option a route left out had probability 0, and still counted
/// toward the even split.
pub fn choice_confidence(pick_probability: f64, options: usize) -> f64 {
    if options <= 1 {
        return 1.0;
    }
    let even = 1.0 / options as f64;
    ((pick_probability - even) / (1.0 - even)).clamp(0.0, 1.0)
}

/// A score's confidence: one minus the probability-weighted distance, in
/// levels, from the picked level, relative to the same distance for an even
/// spread measured from the middle level; floored at 0.
///
/// `level_probabilities` holds every declared level's probability, lowest
/// first, and `pick` is the picked level's index. Because levels are ordered,
/// probability on a neighbouring level costs less than the same probability at
/// the far end.
pub fn score_confidence(level_probabilities: &[f64], pick: usize) -> f64 {
    let levels = level_probabilities.len();
    if levels <= 1 {
        return 1.0;
    }
    let spread: f64 = level_probabilities
        .iter()
        .enumerate()
        .map(|(level, probability)| probability * level.abs_diff(pick) as f64)
        .sum();
    (1.0 - spread / even_spread(levels)).clamp(0.0, 1.0)
}

/// The mean distance from the middle level of an even spread over `levels`
/// (the formula's MAD_unif).
fn even_spread(levels: usize) -> f64 {
    let middle = (levels - 1) as f64 / 2.0;
    (0..levels)
        .map(|level| (level as f64 - middle).abs())
        .sum::<f64>()
        / levels as f64
}

/// A predicate's confidence, `|2p - 1|`: the choice formula for two options.
/// The Decisions APIs return none for a predicate, so this is what puts a
/// predicate on the scale its siblings use.
pub fn predicate_confidence(probability_yes: f64) -> f64 {
    choice_confidence(probability_yes.max(1.0 - probability_yes), 2)
}

/// What one `decide` call answered: the answers in question order, the model
/// that gave them, and what the call consumed. This is the record a consumer
/// keeps.
///
/// It serializes as `inputTokens` and `cost` beside the answers, the shape
/// consumers already read, rather than as the usage it holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "DecisionRecord", from = "DecisionRecord")]
pub struct Decision {
    /// The provider that answered, as its backend key.
    pub provider: String,
    /// The model that answered, as the provider reported it.
    pub model: String,
    pub answers: Vec<DecisionAnswer>,
    /// The input it was billed for (decisions generate no output), and what
    /// the provider said the call cost when it said (OpenRouter's
    /// `usage.cost`); never estimated.
    pub usage: MeteredUsage,
}

impl Decision {
    pub fn answer(&self, name: &str) -> Option<&DecisionAnswer> {
        self.answers.iter().find(|answer| answer.name() == name)
    }

    /// The input tokens the call was billed for, when reported.
    pub fn input_tokens(&self) -> Option<i64> {
        self.usage
            .quantity(Charge::Input, BillingUnit::Tokens)
            .map(|tokens| i64::try_from(tokens).unwrap_or(i64::MAX))
    }

    /// What the provider said the call cost, in USD, when it said.
    pub fn cost_usd(&self) -> Option<f64> {
        self.usage.cost_usd
    }
}

/// A decision's usage: input tokens on the input meter, when reported, and
/// the provider's stated cost.
pub fn input_usage(input_tokens: Option<i64>, cost_usd: Option<f64>) -> MeteredUsage {
    MeteredUsage {
        measures: input_tokens
            .and_then(|tokens| u64::try_from(tokens).ok())
            .map(|quantity| Measure {
                meter: Meter::tokens(Charge::Input),
                quantity,
            })
            .into_iter()
            .collect(),
        cost_usd,
    }
}

/// [`Decision`] as it is serialized.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecisionRecord {
    provider: String,
    model: String,
    answers: Vec<DecisionAnswer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cost: Option<f64>,
}

impl From<Decision> for DecisionRecord {
    fn from(decision: Decision) -> Self {
        Self {
            input_tokens: decision.input_tokens(),
            cost: decision.cost_usd(),
            provider: decision.provider,
            model: decision.model,
            answers: decision.answers,
        }
    }
}

impl From<DecisionRecord> for Decision {
    fn from(record: DecisionRecord) -> Self {
        Self {
            provider: record.provider,
            model: record.model,
            answers: record.answers,
            usage: input_usage(record.input_tokens, record.cost),
        }
    }
}

/// A decision node: the shared definition a recipe's `decision` node and a
/// route's decision node both wrap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionNode {
    pub questions: Vec<DecisionQuestion>,
    /// A tier reference (`sm`, or `provider/tier`) or an exact model. Unset
    /// takes the workspace's first decision-capable offer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Thresholds::is_empty")]
    pub thresholds: Thresholds,
}

impl DecisionNode {
    /// A recipe condition's question and ports as a decision node.
    pub fn port_choice(question: &str, ports: &[String], model: Option<String>) -> Self {
        Self {
            questions: vec![DecisionQuestion::port_choice(question, ports)],
            model,
            thresholds: Thresholds::default(),
        }
    }

    /// The node's one routing question and its options, when the node is the
    /// port-choice shape a recipe can route on: exactly one `choice` question.
    pub fn port_question(&self) -> Result<(&DecisionQuestion, &[DecisionOption]), String> {
        match self.questions.as_slice() {
            [question] => match &question.kind {
                QuestionKind::Choice { options } => Ok((question, options)),
                _ => Err(format!(
                    "a recipe decision node routes on one choice question, and '{}' is not a choice",
                    question.name
                )),
            },
            [] => Err("a decision node needs a question".to_string()),
            _ => Err(format!(
                "a recipe decision node routes on exactly one choice question; this one has {}",
                self.questions.len()
            )),
        }
    }

    /// The port names a recipe edge may leave this node by.
    pub fn ports(&self) -> Vec<String> {
        self.port_question()
            .map(|(_, options)| options.iter().map(|option| option.value.clone()).collect())
            .unwrap_or_default()
    }

    /// Every structural problem with this definition, as sentences naming the
    /// question at fault. Empty means valid.
    pub fn problems(&self) -> Vec<String> {
        let mut problems = question_problems(&self.questions);
        for (method, value) in [
            (MeasuredBy::Native, self.thresholds.native),
            (MeasuredBy::Logprobs, self.thresholds.logprobs),
        ] {
            if let Some(value) = value {
                if !(0.0..=1.0).contains(&value) {
                    problems.push(format!(
                        "the {method} threshold must be between 0 and 1, not {value}"
                    ));
                }
            }
        }
        problems
    }
}

/// Every structural problem with a question list, as sentences naming the
/// question at fault. Empty means the list can be asked.
pub fn question_problems(questions: &[DecisionQuestion]) -> Vec<String> {
    {
        let mut problems = Vec::new();
        if questions.is_empty() {
            problems.push("a decision needs at least one question".to_string());
        }
        let mut names = HashSet::new();
        for question in questions {
            let name = question.name.as_str();
            if name.is_empty() {
                problems.push("every decision question needs a name".to_string());
            } else if !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                problems.push(format!(
                    "question name '{name}' may use only letters, digits, '_' and '-', since answers are bound as answers.<name>"
                ));
            } else if !names.insert(name) {
                problems.push(format!("question name '{name}' is used twice"));
            }
            if question.instructions.trim().is_empty() {
                problems.push(format!("question '{name}' has no instructions"));
            }
            match &question.kind {
                QuestionKind::Choice { options } => {
                    if options.is_empty() || options.len() > MAX_CHOICE_OPTIONS {
                        problems.push(format!(
                            "choice question '{name}' needs between 1 and {MAX_CHOICE_OPTIONS} options, not {}",
                            options.len()
                        ));
                    }
                    let mut seen = HashSet::new();
                    for option in options {
                        if option.value.trim().is_empty() {
                            problems.push(format!("choice question '{name}' has an empty option"));
                        } else if !seen.insert(option.value.as_str()) {
                            problems.push(format!(
                                "choice question '{name}' offers '{}' twice",
                                option.value
                            ));
                        }
                    }
                }
                QuestionKind::Predicate => {}
                QuestionKind::Score { levels } => {
                    if levels.len() < MIN_SCORE_LEVELS || levels.len() > MAX_SCORE_LEVELS {
                        problems.push(format!(
                            "score question '{name}' needs between {MIN_SCORE_LEVELS} and {MAX_SCORE_LEVELS} levels, not {}",
                            levels.len()
                        ));
                    }
                    let mut seen = HashSet::new();
                    for level in levels {
                        if level.label.trim().is_empty() {
                            problems
                                .push(format!("score question '{name}' has an unlabelled level"));
                        } else if !seen.insert(level.label.as_str()) {
                            problems.push(format!(
                                "score question '{name}' has the level '{}' twice",
                                level.label
                            ));
                        }
                    }
                }
            }
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ports(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn a_question_serializes_with_its_kind_beside_its_name() {
        let question = DecisionQuestion::port_choice("Ship it?", &ports(&["yes", "no"]));
        assert_eq!(
            serde_json::to_value(&question).unwrap(),
            json!({
                "name": "route",
                "instructions": "Ship it?",
                "type": "choice",
                "options": [{"value": "yes"}, {"value": "no"}]
            })
        );
        let score: DecisionQuestion = serde_json::from_value(json!({
            "name": "urgency",
            "instructions": "How urgent?",
            "type": "score",
            "levels": [{"label": "low"}, {"label": "high", "description": "page someone"}]
        }))
        .unwrap();
        assert_eq!(score.answer_values(), vec!["low", "high"]);
    }

    #[test]
    fn a_refusal_round_trips_as_its_own_answer_with_no_pick() {
        let refusal = DecisionAnswer::Refusal {
            name: "route".into(),
            measured_by: MeasuredBy::Native,
            reason: None,
        };
        let value = serde_json::to_value(&refusal).unwrap();
        assert_eq!(
            value,
            json!({"type": "refusal", "name": "route", "measuredBy": "native"})
        );
        let back: DecisionAnswer = serde_json::from_value(value).unwrap();
        assert!(back.is_refusal());
        assert_eq!(back.pick(), None);
        assert_eq!(back.confidence(), None);
    }

    #[test]
    fn a_predicate_picks_by_its_probability_of_yes() {
        let answer = DecisionAnswer::Predicate {
            name: "urgent".into(),
            measured_by: MeasuredBy::Logprobs,
            probability_yes: 0.2,
        };
        assert_eq!(answer.pick(), Some("no"));
        assert!((answer.confidence().unwrap() - 0.6).abs() < 1e-12);
        assert_eq!(
            serde_json::to_value(&answer).unwrap()["probabilityYes"],
            json!(0.2)
        );
    }

    fn close(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < 1e-9
    }

    #[test]
    fn choice_confidence_is_zero_at_an_even_split_and_one_at_certainty() {
        for options in [2, 3, 4, 10] {
            let even = 1.0 / options as f64;
            assert!(close(choice_confidence(even, options), 0.0), "{options}");
            assert!(close(choice_confidence(1.0, options), 1.0), "{options}");
        }
        // The raw top probability called a four-way even split 0.25.
        assert!(close(choice_confidence(0.25, 4), 0.0));
        // TypeSafe: (0.6, 0.3, 0.1) and (0.6, 0.2, 0.2) are both 0.4.
        assert!(close(choice_confidence(0.6, 3), 0.4));
        // A lone option is certain.
        assert_eq!(choice_confidence(1.0, 1), 1.0);
    }

    #[test]
    fn score_confidence_charges_distance_from_the_picked_level() {
        // TypeSafe's worked examples: torn between neighbours, then between ends.
        assert!(close(score_confidence(&[0.0, 0.5, 0.5], 1), 0.25));
        assert!(close(score_confidence(&[0.5, 0.0, 0.5], 0), 0.0));
        assert!(close(score_confidence(&[0.0, 0.0, 1.0], 2), 1.0));
        // An even spread is floored at 0, not driven negative.
        assert_eq!(score_confidence(&[0.2; 5], 0), 0.0);
    }

    /// Which tied peak is the pick changes a score's confidence: level 0 of
    /// (0.4, 0.4, 0.2) gives 0, level 1 gives 0.1. The first, as TypeSafe's
    /// reference picks it.
    #[test]
    fn the_most_probable_answer_is_the_first_of_a_tie() {
        let tied = [0.4, 0.4, 0.2];
        assert_eq!(most_probable(&tied), Some(0));
        assert!(close(score_confidence(&tied, 0), 0.0));
        assert!(close(score_confidence(&tied, 1), 0.1));
        assert_eq!(most_probable(&[0.1, 0.2, 0.7]), Some(2));
        assert_eq!(most_probable(&[]), None);
    }

    #[test]
    fn the_even_spread_is_the_mean_distance_from_the_middle_level() {
        assert!(close(even_spread(5), 1.2));
        assert!(close(even_spread(3), 2.0 / 3.0));
        assert!(close(even_spread(2), 0.5));
    }

    #[test]
    fn predicate_confidence_is_the_distance_from_a_coin_flip() {
        assert!(close(predicate_confidence(0.5), 0.0));
        assert!(close(predicate_confidence(1.0), 1.0));
        assert!(close(predicate_confidence(0.0), 1.0));
        for p in [0.1, 0.35, 0.8, 0.97] {
            assert!(close(predicate_confidence(p), (2.0 * p - 1.0).abs()), "{p}");
        }
    }

    /// The published provider examples: OpenAI's guide (a four-option choice
    /// at 0.95 reporting 0.93; a three-level score reporting 0.55) and
    /// TypeSafe's (`bug_severity`, reporting about 0.35).
    #[test]
    fn the_formulas_reproduce_the_providers_published_values() {
        let round = |value: f64| (value * 100.0).round() / 100.0;
        assert_eq!(round(choice_confidence(0.95, 4)), 0.93);
        assert_eq!(round(score_confidence(&[0.1, 0.7, 0.2], 1)), 0.55);
        assert!((score_confidence(&[0.0, 0.57, 0.43], 1) - 0.355).abs() < 1e-9);
    }

    fn choice(pick: &str, confidence: Option<f64>, measured_by: MeasuredBy) -> DecisionAnswer {
        DecisionAnswer::Choice {
            name: "route".into(),
            measured_by,
            pick: pick.into(),
            confidence,
            probabilities: vec![],
        }
    }

    #[test]
    fn thresholds_are_keyed_by_how_the_answer_was_measured() {
        let thresholds = Thresholds {
            native: Some(0.7),
            logprobs: Some(0.9),
        };
        assert_eq!(
            thresholds.admit(&choice("yes", Some(0.8), MeasuredBy::Native)),
            Ok("yes")
        );
        let refused = thresholds
            .admit(&choice("yes", Some(0.8), MeasuredBy::Logprobs))
            .unwrap_err();
        assert_eq!(
            refused.to_string(),
            "'route' picked 'yes' with logprobs confidence 0.80, below the logprobs threshold 0.90"
        );
        // No floor for a method, no check.
        assert_eq!(
            Thresholds::default().admit(&choice("no", None, MeasuredBy::Native)),
            Ok("no")
        );
        // A floor that cannot be shown to hold does not hold.
        assert!(thresholds
            .admit(&choice("no", None, MeasuredBy::Native))
            .unwrap_err()
            .to_string()
            .contains("reported no confidence"));
    }

    #[test]
    fn a_refusal_is_never_admitted_whatever_the_thresholds() {
        let refusal = DecisionAnswer::Refusal {
            name: "route".into(),
            measured_by: MeasuredBy::Native,
            reason: Some("policy".into()),
        };
        let error = Thresholds::default().admit(&refusal).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the model refused to answer 'route': policy"
        );
    }

    #[test]
    fn the_port_choice_is_one_choice_over_the_ports() {
        let node = DecisionNode::port_choice("Risky?", &ports(&["high", "low"]), None);
        assert!(node.problems().is_empty());
        assert_eq!(node.ports(), vec!["high", "low"]);
        let (question, options) = node.port_question().unwrap();
        assert_eq!(question.name, PORT_QUESTION_NAME);
        assert_eq!(options.len(), 2);
    }

    #[test]
    fn a_multi_question_node_is_valid_but_not_a_port_choice() {
        let node = DecisionNode {
            questions: vec![
                DecisionQuestion::port_choice("Where?", &ports(&["a", "b"])),
                DecisionQuestion {
                    name: "urgent".into(),
                    instructions: "Urgent?".into(),
                    kind: QuestionKind::Predicate,
                },
            ],
            model: None,
            thresholds: Thresholds::default(),
        };
        assert!(node.problems().is_empty());
        assert!(node.port_question().unwrap_err().contains("exactly one"));
        assert!(node.ports().is_empty());
    }

    #[test]
    fn structural_problems_name_the_question_at_fault() {
        let node = DecisionNode {
            questions: vec![
                DecisionQuestion {
                    name: "a".into(),
                    instructions: " ".into(),
                    kind: QuestionKind::Choice { options: vec![] },
                },
                DecisionQuestion {
                    name: "a".into(),
                    instructions: "x".into(),
                    kind: QuestionKind::Score {
                        levels: vec![ScoreLevel {
                            label: "only".into(),
                            description: None,
                        }],
                    },
                },
                DecisionQuestion {
                    name: "has space".into(),
                    instructions: "x".into(),
                    kind: QuestionKind::Predicate,
                },
            ],
            model: None,
            thresholds: Thresholds {
                native: Some(1.5),
                logprobs: None,
            },
        };
        let problems = node.problems();
        assert!(problems.contains(&"question 'a' has no instructions".to_string()));
        assert!(problems
            .contains(&"choice question 'a' needs between 1 and 255 options, not 0".to_string()));
        assert!(problems.contains(&"question name 'a' is used twice".to_string()));
        assert!(problems
            .contains(&"score question 'a' needs between 2 and 10 levels, not 1".to_string()));
        assert!(problems.iter().any(|p| p.contains("'has space'")));
        assert!(
            problems.contains(&"the native threshold must be between 0 and 1, not 1.5".to_string())
        );
    }

    #[test]
    fn a_decision_record_serializes_for_the_consumer_row() {
        let decision = Decision {
            provider: "custom:acc_1".into(),
            model: "jev".into(),
            answers: vec![choice("yes", Some(0.9), MeasuredBy::Native)],
            usage: input_usage(Some(96), Some(0.000004)),
        };
        assert_eq!(decision.input_tokens(), Some(96));
        assert_eq!(decision.cost_usd(), Some(0.000004));
        let value = serde_json::to_value(&decision).unwrap();
        assert_eq!(value["inputTokens"], json!(96));
        assert_eq!(value["cost"], json!(0.000004));
        assert!(value.get("usage").is_none(), "{value}");
        // A record kept before cost was reported reads back with none.
        let older: Decision = serde_json::from_value(json!({
            "provider": "custom:acc_1", "model": "jev", "answers": [], "inputTokens": 40
        }))
        .unwrap();
        assert_eq!(older.input_tokens(), Some(40));
        assert_eq!(older.cost_usd(), None);
        assert_eq!(value["answers"][0]["measuredBy"], json!("native"));
        let back: Decision = serde_json::from_value(value).unwrap();
        assert_eq!(
            back.answer("route").and_then(DecisionAnswer::pick),
            Some("yes")
        );
    }
}
