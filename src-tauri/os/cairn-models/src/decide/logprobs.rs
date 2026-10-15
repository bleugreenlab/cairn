//! The logprobs route: a decision measured from a chat model's next-token
//! distribution over a closed set of answer labels.
//!
//! Each question becomes one request for a single token, with every option
//! given a short label (`A`, `B`, ... or `Yes`/`No`). The model's top
//! alternatives for that token are read, the probability mass on each label is
//! summed across the spellings a tokenizer produces (`A`, ` A`, `a`, `A.`),
//! and the result is renormalized over the labels. That is a measurement of
//! the model's own distribution; it is not calibrated, which is why answers
//! say [`MeasuredBy::Logprobs`].
//!
//! What it never does is invent a distribution. A reply without logprobs, or
//! with the chosen token but no alternatives, is reported as missing. And the
//! label mass is only a measurement at a position where the model is
//! answering: when the token it generated there is not a label (a reasoning
//! model's "The", a channel-opening control token), the labels among its
//! alternatives are a far tail, and renormalizing them would report a
//! confident pick the model never made. That reply is refused as not
//! answering.

use cairn_domain::decision::{
    DecisionAnswer, DecisionQuestion, MeasuredBy, OptionProbability, QuestionKind,
};
use serde_json::{json, Value};

use crate::interface::decide::DecideError;
use crate::wire::openai_compat::wire::ChatResponse;

/// The most alternatives OpenAI-compatible servers return per token. It bounds
/// how many options can be measured at all.
pub(super) const TOP_LOGPROBS: usize = 20;

const SYSTEM: &str = "You answer one question about the input. Reply with exactly one of the \
offered labels and nothing else.";

/// One question prepared for the logprobs route.
pub(super) struct Ask<'a> {
    model: &'a str,
    question: &'a DecisionQuestion,
    /// The label each answer value is offered under, in answer order.
    labels: Vec<String>,
    prompt: String,
}

/// Why a reply measured nothing.
#[derive(Debug, PartialEq)]
pub(super) enum Unmeasured {
    /// The reply carried no usable logprobs.
    MissingLogprobs,
    /// The model generated something other than an answer label at the
    /// measured position, beginning with `began`.
    NotAnswering { began: String },
}

impl<'a> Ask<'a> {
    pub(super) fn new(
        model: &'a str,
        input: &str,
        question: &'a DecisionQuestion,
    ) -> Result<Self, DecideError> {
        if let Some(problem) =
            super::unmeasurable(std::slice::from_ref(question), MeasuredBy::Logprobs)
                .into_iter()
                .next()
        {
            return Err(DecideError::Unmeasurable(problem));
        }
        let values = question.answer_values();
        let labels: Vec<String> = match question.kind {
            QuestionKind::Predicate => vec!["Yes".into(), "No".into()],
            _ => (0..values.len())
                .map(|index| char::from(b'A' + index as u8).to_string())
                .collect(),
        };
        let mut prompt = format!("{input}\n\n---\nQuestion: {}\n", question.instructions);
        match &question.kind {
            QuestionKind::Predicate => prompt.push_str("\nReply with only Yes or No."),
            QuestionKind::Choice { options } => {
                prompt.push_str("\nOptions:\n");
                for (label, option) in labels.iter().zip(options) {
                    prompt.push_str(&format!("{label}. {}", option.value));
                    if let Some(description) = &option.description {
                        prompt.push_str(&format!(": {description}"));
                    }
                    prompt.push('\n');
                }
                prompt.push_str("\nReply with only the letter of the best option.");
            }
            QuestionKind::Score { levels } => {
                prompt.push_str("\nLevels, lowest first:\n");
                for (label, level) in labels.iter().zip(levels) {
                    prompt.push_str(&format!("{label}. {}", level.label));
                    if let Some(description) = &level.description {
                        prompt.push_str(&format!(": {description}"));
                    }
                    prompt.push('\n');
                }
                prompt.push_str("\nReply with only the letter of the level that fits best.");
            }
        }
        Ok(Self {
            model,
            question,
            labels,
            prompt,
        })
    }

    /// A one-token, deterministic chat request asking for the top
    /// alternatives, with reasoning switched off so the first token is the
    /// answer rather than a reasoning opener. Two spellings, because servers
    /// honour different ones: `reasoning_effort: "none"` is what Ollama obeys
    /// and what vLLM (from v0.21) maps onto the chat template, and
    /// `enable_thinking: false` reaches older vLLM templates directly. A
    /// server too old to accept `"none"` rejects the request with its own
    /// error rather than answering from its reasoning.
    pub(super) fn body(&self) -> Value {
        json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": self.prompt},
            ],
            "max_tokens": 1,
            "temperature": 0,
            "logprobs": true,
            "top_logprobs": TOP_LOGPROBS,
            "stream": false,
            "reasoning_effort": "none",
            "chat_template_kwargs": {"enable_thinking": false},
        })
    }

    /// Read the label distribution off the first generated token.
    pub(super) fn answer(&self, reply: &ChatResponse) -> Result<DecisionAnswer, Unmeasured> {
        let choice = reply.choices.first().ok_or(Unmeasured::MissingLogprobs)?;
        let first = choice
            .logprobs
            .as_ref()
            .and_then(|logprobs| logprobs.content.as_ref())
            .and_then(|content| content.first())
            .ok_or(Unmeasured::MissingLogprobs)?;
        if first.top_logprobs.is_empty() {
            return Err(Unmeasured::MissingLogprobs);
        }
        let not_answering = || Unmeasured::NotAnswering {
            began: first.token.trim().to_string(),
        };
        // A token the server filed as reasoning is not the answer, whatever
        // it spells.
        let reasoned = choice
            .message
            .reasoning
            .as_ref()
            .is_some_and(|reasoning| !reasoning.text().trim().is_empty());
        if reasoned || self.label_index(&first.token).is_none() {
            return Err(not_answering());
        }
        let mut mass = vec![0.0_f64; self.labels.len()];
        for alternative in &first.top_logprobs {
            if let Some(index) = self.label_index(&alternative.token) {
                mass[index] += alternative.logprob.exp();
            }
        }
        let name = self.question.name.clone();
        let measured_by = MeasuredBy::Logprobs;
        let total: f64 = mass.iter().sum();
        if total <= 0.0 {
            // The generated label was missing from its own alternatives.
            return Err(not_answering());
        }
        let values = self.question.answer_values();
        let probabilities: Vec<OptionProbability> = values
            .iter()
            .zip(&mass)
            .map(|(value, mass)| OptionProbability {
                value: value.to_string(),
                probability: mass / total,
            })
            .collect();
        let (best, best_probability) = probabilities
            .iter()
            .enumerate()
            .map(|(index, entry)| (index, entry.probability))
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .expect("a question has at least one answer");
        Ok(match &self.question.kind {
            QuestionKind::Predicate => DecisionAnswer::Predicate {
                name,
                measured_by,
                probability_yes: probabilities[0].probability,
            },
            QuestionKind::Choice { .. } => DecisionAnswer::Choice {
                name,
                measured_by,
                pick: probabilities[best].value.clone(),
                confidence: Some(best_probability),
                probabilities,
            },
            QuestionKind::Score { .. } => DecisionAnswer::Score {
                name,
                measured_by,
                score: probabilities
                    .iter()
                    .enumerate()
                    .map(|(index, entry)| index as f64 * entry.probability)
                    .sum(),
                pick: probabilities[best].value.clone(),
                confidence: Some(best_probability),
                probabilities,
            },
        })
    }

    /// Which label a token spells, ignoring surrounding whitespace, list
    /// punctuation, and case.
    fn label_index(&self, token: &str) -> Option<usize> {
        let spelled = token
            .trim()
            .trim_matches(|c: char| matches!(c, '(' | ')' | '.' | ':' | '*' | '"' | '\''))
            .to_lowercase();
        if spelled.is_empty() {
            return None;
        }
        self.labels
            .iter()
            .position(|label| label.to_lowercase() == spelled)
    }
}
