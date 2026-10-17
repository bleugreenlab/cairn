//! The native route: Cairn's questions onto the Decisions wire, and its
//! answers back onto Cairn's answer type.

use cairn_domain::decision::{
    choice_confidence, most_probable, score_confidence, DecisionAnswer, DecisionQuestion,
    MeasuredBy, OptionProbability, QuestionKind,
};

use crate::interface::complete::CompletionError;
use crate::interface::decide::DecideError;
use crate::wire::decisions::wire::{
    DecisionsResponse, WireAnswer, WireChoice, WireLevel, WireQuestion,
};

pub(super) fn wire_questions(questions: &[DecisionQuestion]) -> Vec<WireQuestion> {
    questions
        .iter()
        .map(|question| {
            let name = question.name.clone();
            let instructions = question.instructions.clone();
            match &question.kind {
                QuestionKind::Predicate => WireQuestion::Predicate { name, instructions },
                QuestionKind::Choice { options } => WireQuestion::Choice {
                    name,
                    instructions,
                    choices: options
                        .iter()
                        .map(|option| WireChoice {
                            value: option.value.clone(),
                            description: option.description.clone(),
                        })
                        .collect(),
                },
                QuestionKind::Score { levels } => WireQuestion::Score {
                    name,
                    instructions,
                    levels: levels
                        .iter()
                        .map(|level| WireLevel {
                            label: level.label.clone(),
                            description: level.description.clone(),
                        })
                        .collect(),
                },
            }
        })
        .collect()
}

fn invalid(text: String) -> DecideError {
    DecideError::Completion(CompletionError::InvalidResponse(text))
}

/// Pair each wire answer with the question in its position.
///
/// The answer takes the question's name, because the wire may return
/// `name: null`. A refusal stays a refusal. An answer of the wrong kind, or a
/// pick that is not one of the question's options, is an invalid reply rather
/// than something to coerce.
///
/// A provider's `confidence` is kept as reported, since the Decisions APIs
/// compute the same statistic Cairn does; it is computed from the
/// probabilities only when the provider omits it.
pub(super) fn answers(
    questions: &[DecisionQuestion],
    reply: DecisionsResponse,
) -> Result<Vec<DecisionAnswer>, DecideError> {
    if reply.answers.len() != questions.len() {
        return Err(invalid(format!(
            "asked {} questions and got {} answers",
            questions.len(),
            reply.answers.len()
        )));
    }
    let measured_by = MeasuredBy::Native;
    questions
        .iter()
        .zip(reply.answers)
        .map(|(question, answer)| {
            let name = question.name.clone();
            match (&question.kind, answer) {
                (_, WireAnswer::Refusal { .. }) => Ok(DecisionAnswer::Refusal {
                    name,
                    measured_by,
                    reason: None,
                }),
                (QuestionKind::Predicate, WireAnswer::Predicate { probability, .. }) => {
                    Ok(DecisionAnswer::Predicate {
                        name,
                        measured_by,
                        probability_yes: probability,
                    })
                }
                (
                    QuestionKind::Choice { options },
                    WireAnswer::Choice {
                        choice,
                        confidence,
                        probabilities,
                        ..
                    },
                ) => {
                    let pick = choice.as_text();
                    if !options.iter().any(|option| option.value == pick) {
                        return Err(invalid(format!(
                            "'{name}' was answered '{pick}', which is not one of its options"
                        )));
                    }
                    let probabilities: Vec<OptionProbability> = probabilities
                        .into_iter()
                        .map(|entry| OptionProbability {
                            value: entry.value.as_text(),
                            probability: entry.probability,
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
                        measured_by,
                        pick,
                        confidence,
                        probabilities,
                    })
                }
                (
                    QuestionKind::Score { levels },
                    WireAnswer::Score {
                        score,
                        confidence,
                        probabilities,
                        ..
                    },
                ) => {
                    let label_of = |index: i64| {
                        usize::try_from(index)
                            .ok()
                            .and_then(|index| levels.get(index))
                            .map(|level| level.label.clone())
                    };
                    // Each declared level's probability by its index, whatever
                    // order the wire listed them in; a level it left out is 0.
                    let mut by_level = vec![0.0_f64; levels.len()];
                    let mut placed = false;
                    for entry in &probabilities {
                        if let Some(slot) = usize::try_from(entry.value)
                            .ok()
                            .and_then(|index| by_level.get_mut(index))
                        {
                            *slot += entry.probability;
                            placed = true;
                        }
                    }
                    let peak = if placed {
                        most_probable(&by_level)
                    } else {
                        None
                    };
                    let confidence =
                        confidence.or_else(|| peak.map(|index| score_confidence(&by_level, index)));
                    let probabilities: Vec<OptionProbability> = probabilities
                        .into_iter()
                        .map(|entry| OptionProbability {
                            value: entry
                                .label
                                .clone()
                                .or_else(|| label_of(entry.value))
                                .unwrap_or_else(|| entry.value.to_string()),
                            probability: entry.probability,
                        })
                        .collect();
                    let pick = match peak {
                        Some(index) => levels[index].label.clone(),
                        None => {
                            let nearest = score.round().clamp(0.0, (levels.len() - 1) as f64);
                            levels[nearest as usize].label.clone()
                        }
                    };
                    Ok(DecisionAnswer::Score {
                        name,
                        measured_by,
                        score,
                        pick,
                        confidence,
                        probabilities,
                    })
                }
                (kind, answer) => Err(invalid(format!(
                    "'{name}' is a {} question but was answered as {answer:?}",
                    match kind {
                        QuestionKind::Choice { .. } => "choice",
                        QuestionKind::Predicate => "predicate",
                        QuestionKind::Score { .. } => "score",
                    }
                ))),
            }
        })
        .collect()
}
