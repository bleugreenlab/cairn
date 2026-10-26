//! The System One route: its named answers back onto Cairn's answer type.
//!
//! The answers are a map keyed by question name, so each question is matched
//! by name, not position. A question the gateway refused (the wire's
//! `Refused`, carrying the gateway's sentence), one with no answer under its
//! name, or one answered in a shape the wire does not recognise, is a refusal
//! with the reason stated: never a pick. An answer of
//! the wrong kind, a pick or probability outside the question's options, or
//! an answer to a question that was not asked is an invalid reply rather than
//! something to coerce.
//!
//! A provider's `confidence` is kept as reported, since TypeSafe computes the
//! statistic Cairn does; it is computed from the probabilities only when the
//! provider omits it.

use std::collections::BTreeMap;

use cairn_domain::decision::{
    choice_confidence, most_probable, score_confidence, DecisionAnswer, DecisionOption,
    DecisionQuestion, MeasuredBy, OptionProbability, QuestionKind, ScoreLevel,
};

use crate::interface::complete::CompletionError;
use crate::interface::decide::DecideError;
use crate::wire::systemone::wire::{SystemOneAnswer, SystemOneResponse};

const MEASURED_BY: MeasuredBy = MeasuredBy::Native;

fn invalid(text: String) -> DecideError {
    DecideError::Completion(CompletionError::InvalidResponse(text))
}

/// Each question's answer, in question order.
pub(super) fn answers(
    questions: &[DecisionQuestion],
    reply: SystemOneResponse,
) -> Result<Vec<DecisionAnswer>, DecideError> {
    let mut answered = reply.answers;
    let answers = questions
        .iter()
        .map(|question| answer(question, answered.remove(&question.name)))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(stray) = answered.keys().next() {
        return Err(invalid(format!(
            "System One answered '{stray}', which was not asked"
        )));
    }
    Ok(answers)
}

fn refusal(question: &DecisionQuestion, reason: String) -> DecisionAnswer {
    DecisionAnswer::Refusal {
        name: question.name.clone(),
        measured_by: MEASURED_BY,
        reason: Some(reason),
    }
}

fn answer(
    question: &DecisionQuestion,
    answer: Option<SystemOneAnswer>,
) -> Result<DecisionAnswer, DecideError> {
    let name = question.name.clone();
    let Some(answer) = answer else {
        return Ok(refusal(question, "no answer came back for it".to_string()));
    };
    match (&question.kind, answer) {
        (_, SystemOneAnswer::Refused { reason }) => Ok(refusal(question, reason)),
        (_, SystemOneAnswer::Unrecognised(value)) => Ok(refusal(
            question,
            format!("it was answered in no shape Cairn recognises: {value}"),
        )),
        (QuestionKind::Predicate, SystemOneAnswer::Noul { noul }) => {
            Ok(DecisionAnswer::Predicate {
                name,
                measured_by: MEASURED_BY,
                probability_yes: noul,
            })
        }
        (
            QuestionKind::Choice { options },
            SystemOneAnswer::Choice {
                choice,
                confidence,
                probabilities,
            },
        ) => choice_answer(name, options, choice, confidence, probabilities),
        (
            QuestionKind::Score { levels },
            SystemOneAnswer::Score {
                score,
                confidence,
                probabilities,
                ..
            },
        ) => score_answer(name, levels, score, confidence, probabilities),
        (kind, answer) => Err(invalid(format!(
            "'{name}' is a {} question but was answered as {answer:?}",
            match kind {
                QuestionKind::Choice { .. } => "choice",
                QuestionKind::Predicate => "predicate",
                QuestionKind::Score { .. } => "score",
            }
        ))),
    }
}

fn choice_answer(
    name: String,
    options: &[DecisionOption],
    pick: String,
    confidence: Option<f64>,
    probabilities: BTreeMap<String, f64>,
) -> Result<DecisionAnswer, DecideError> {
    let declared = |value: &str| options.iter().any(|option| option.value == value);
    if !declared(&pick) {
        return Err(invalid(format!(
            "'{name}' was answered '{pick}', which is not one of its options"
        )));
    }
    if let Some(stray) = probabilities.keys().find(|value| !declared(value)) {
        return Err(invalid(format!(
            "'{name}' gave a probability for '{stray}', which is not one of its options"
        )));
    }
    // The reply's map has no order; the answer lists them in option order.
    let probabilities: Vec<OptionProbability> = options
        .iter()
        .filter_map(|option| {
            probabilities
                .get(&option.value)
                .map(|&probability| OptionProbability {
                    value: option.value.clone(),
                    probability,
                })
        })
        .collect();
    let confidence = confidence.or_else(|| {
        probabilities
            .iter()
            .find(|entry| entry.value == pick)
            .map(|entry| choice_confidence(entry.probability, options.len()))
    });
    Ok(DecisionAnswer::Choice {
        name,
        measured_by: MEASURED_BY,
        pick,
        confidence,
        probabilities,
    })
}

fn score_answer(
    name: String,
    levels: &[ScoreLevel],
    score: f64,
    confidence: Option<f64>,
    probabilities: BTreeMap<String, f64>,
) -> Result<DecisionAnswer, DecideError> {
    // Each level's probability by its index, which the reply keys as a string.
    let mut by_level: Vec<Option<f64>> = vec![None; levels.len()];
    for (key, probability) in probabilities {
        let slot = key
            .parse::<usize>()
            .ok()
            .and_then(|index| by_level.get_mut(index))
            .ok_or_else(|| {
                invalid(format!(
                    "'{name}' gave a probability for level '{key}', and it has levels 0 to {}",
                    levels.len() - 1
                ))
            })?;
        *slot = Some(probability);
    }
    let dense: Vec<f64> = by_level.iter().map(|p| p.unwrap_or(0.0)).collect();
    let peak = if by_level.iter().any(Option::is_some) {
        most_probable(&dense)
    } else {
        None
    };
    let confidence = confidence.or_else(|| peak.map(|index| score_confidence(&dense, index)));
    let pick = match peak {
        Some(index) => levels[index].label.clone(),
        None => {
            let nearest = score.round().clamp(0.0, (levels.len() - 1) as f64);
            levels[nearest as usize].label.clone()
        }
    };
    let probabilities: Vec<OptionProbability> = levels
        .iter()
        .zip(by_level)
        .filter_map(|(level, probability)| {
            probability.map(|probability| OptionProbability {
                value: level.label.clone(),
                probability,
            })
        })
        .collect();
    Ok(DecisionAnswer::Score {
        name,
        measured_by: MEASURED_BY,
        score,
        pick,
        confidence,
        probabilities,
    })
}
