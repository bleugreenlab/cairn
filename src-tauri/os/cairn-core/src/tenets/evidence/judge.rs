//! The judgement: does one concluded piece of work support, contradict, or not
//! bear on each of a few candidate tenets?
//!
//! It is asked through the models facet's `decide` verb, never as a Response:
//! one decision per event, one `choice` question per candidate tenet. A
//! generating model reporting how sure it is would not be a measurement, and
//! everything downstream does arithmetic on this number (the confidence floor,
//! the contradiction weight, the wake threshold). `decide` answers with a
//! probability measured natively or from logprobs, and refuses outright when
//! nothing configured can measure one.

use crate::models::decision::{
    Decision, DecisionAnswer, DecisionNode, DecisionOption, DecisionQuestion, MeasuredBy,
    QuestionKind, Thresholds,
};
use crate::tenets::{TenetOrigin, TenetView};

/// What every question tells the model about the rule it is applying. The
/// input is the concluded work; the question carries one tenet.
const PREAMBLE: &str = "The input is one piece of concluded work. Below is a tenet: a standing project decision the operator approved. The chosen option is the tenet, and every rejected option was explicitly ruled out. Does the concluded work support this tenet, contradict it, or neither? Work that adopted a rejected option contradicts the tenet. Work that only shares the tenet's topic or vocabulary is neither. When unsure, answer neither.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sign {
    Supports,
    Contradicts,
    Neither,
}

impl Sign {
    pub const ALL: [Sign; 3] = [Sign::Supports, Sign::Contradicts, Sign::Neither];

    pub fn as_str(self) -> &'static str {
        match self {
            Sign::Supports => "supports",
            Sign::Contradicts => "contradicts",
            Sign::Neither => "neither",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Sign::Supports => "the work followed the chosen option or relied on it",
            Sign::Contradicts => "the work did what the tenet rules out: it adopted a rejected option, reversed or bypassed the decision, or concluded the decision was wrong",
            Sign::Neither => "the work does not bear on the tenet, or only shares its topic or vocabulary",
        }
    }

    fn parse(value: &str) -> Option<Sign> {
        Sign::ALL.into_iter().find(|sign| sign.as_str() == value)
    }
}

/// The name of the question about tenet `number`, unique within one event's
/// decision because tenet numbers are unique within a project.
pub fn question_name(number: i64) -> String {
    format!("tenet-{number}")
}

/// One tenet as the judge reads it: its question when that differs from its
/// text, the chosen option, and the rejected ones.
pub fn tenet_block(view: &TenetView) -> String {
    let text = view.text.as_deref().unwrap_or(&view.row.question);
    let mut block = String::new();
    // A migrated ruling has no question distinct from its text.
    let distinct_question =
        view.row.origin != TenetOrigin::MigratedRuling && view.row.question.trim() != text.trim();
    if distinct_question {
        block.push_str(&format!("Question: {}\n", view.row.question));
    }
    block.push_str(&format!("Chosen: {text}\n"));
    if view.rejected.is_empty() {
        block.push_str("Rejected: none recorded");
    } else {
        block.push_str(&format!("Rejected: {}", view.rejected.join("; ")));
    }
    block
}

/// The question asked about one candidate tenet.
pub fn question(view: &TenetView) -> DecisionQuestion {
    DecisionQuestion {
        name: question_name(view.row.number),
        instructions: format!(
            "{PREAMBLE}\n\nTenet #{}\n{}",
            view.row.number,
            tenet_block(view)
        ),
        kind: QuestionKind::Choice {
            options: Sign::ALL
                .into_iter()
                .map(|sign| DecisionOption {
                    value: sign.as_str().to_string(),
                    description: Some(sign.description().to_string()),
                })
                .collect(),
        },
    }
}

/// The decision asked about one event: one question per candidate, in
/// candidate order, answered by the workspace's first decision-capable offer.
/// The event text is the decision's input, so it is sent once rather than
/// repeated inside every question.
pub fn decision(candidates: &[&TenetView]) -> DecisionNode {
    DecisionNode {
        questions: candidates.iter().map(|view| question(view)).collect(),
        model: None,
        // The worker applies `CONFIDENCE_FLOOR` itself, to the chosen
        // option's probability rather than to `DecisionAnswer::confidence`.
        thresholds: Thresholds::default(),
    }
}

/// One verdict about one candidate, by its 0-based candidate index.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub candidate: usize,
    pub sign: Sign,
    /// The measured probability of the chosen option. `None` when the answer
    /// reported no per-option probabilities, so no floor can be shown to hold.
    pub probability: Option<f64>,
    pub measured_by: MeasuredBy,
}

/// The verdicts in a decision, at most one per candidate.
///
/// A candidate whose question was refused, went unanswered, or picked a value
/// that is not a sign gets no verdict: a refusal is never a `neither` with a
/// probability. The probability is the chosen option's own, not the answer's
/// `confidence`, which rescales it against an even split.
pub fn parse(decision: &Decision, candidates: &[&TenetView]) -> Vec<Verdict> {
    candidates
        .iter()
        .enumerate()
        .filter_map(|(candidate, view)| {
            let DecisionAnswer::Choice {
                measured_by,
                pick,
                probabilities,
                ..
            } = decision.answer(&question_name(view.row.number))?
            else {
                return None;
            };
            let sign = Sign::parse(pick)?;
            let probability = probabilities
                .iter()
                .find(|option| option.value == *pick)
                .map(|option| option.probability.clamp(0.0, 1.0));
            Some(Verdict {
                candidate,
                sign,
                probability,
                measured_by: *measured_by,
            })
        })
        .collect()
}
