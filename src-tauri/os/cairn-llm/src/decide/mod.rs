//! The facet's decision implementation: one [`decide`] over either route,
//! composed from [`Complete`] so the POST passes the same seam every
//! completion does.

mod logprobs;
mod native;
#[cfg(test)]
mod tests;

use std::time::Duration;

use cairn_models::decision::{
    question_problems, Decision, DecisionQuestion, MeasuredBy, QuestionKind,
};

use crate::interface::complete::{Complete, CompletionCall, CompletionWire};
use crate::interface::decide::{DecideError, DecisionCall, DecisionRoute};
use crate::wire::decisions::http::build_body;

/// Every question `method` cannot measure, as a sentence naming it. Empty
/// means the method can ask them all. Checked before a recipe is admitted,
/// and again by the route itself.
pub fn unmeasurable(questions: &[DecisionQuestion], method: MeasuredBy) -> Vec<String> {
    match method {
        MeasuredBy::Native => Vec::new(),
        MeasuredBy::Logprobs => questions
            .iter()
            .filter(|question| question.answer_values().len() > logprobs::TOP_LOGPROBS)
            .map(|question| {
                format!(
                    "'{}' offers {} answers, and logprobs can measure at most {} (the most alternatives a server returns per token). Use a Decisions provider for larger choices.",
                    question.name,
                    question.answer_values().len(),
                    logprobs::TOP_LOGPROBS
                )
            })
            .collect(),
    }
}

/// Ask `model` one fixed yes-or-no question to learn whether it can answer
/// decisions on `route`. A decision model is taken at its word; a chat
/// model must show it returns logprobs and answers at once.
pub fn probe(
    complete: &dyn Complete,
    route: &DecisionRoute,
    model: &str,
    timeout: Duration,
) -> Result<(), DecideError> {
    if route.measured_by() == MeasuredBy::Native {
        return Ok(());
    }
    let question = DecisionQuestion {
        name: "probe".to_string(),
        instructions: "Is this text a greeting?".to_string(),
        kind: QuestionKind::Predicate,
    };
    decide(
        complete,
        DecisionCall {
            route,
            model,
            input: "Hello there!",
            questions: std::slice::from_ref(&question),
            timeout,
        },
    )
    .map(|_| ())
}

/// Answer `call.questions` about `call.input`, one answer per question in
/// order, each stating how it was measured.
pub fn decide(complete: &dyn Complete, call: DecisionCall<'_>) -> Result<Decision, DecideError> {
    let problems = question_problems(call.questions);
    if !problems.is_empty() {
        return Err(DecideError::InvalidQuestions(problems));
    }
    match call.route {
        DecisionRoute::Native(endpoint) => {
            let body = build_body(
                call.model,
                call.input,
                &native::wire_questions(call.questions),
            );
            let reply = complete
                .complete(CompletionCall::whole(
                    CompletionWire::Decisions(endpoint),
                    body,
                    call.timeout,
                ))?
                .into_decisions()?;
            let input_tokens = reply.usage.as_ref().and_then(|usage| usage.input_tokens);
            let model = reply
                .model
                .clone()
                .filter(|model| !model.is_empty())
                .unwrap_or_else(|| call.model.to_string());
            Ok(Decision {
                provider: endpoint.backend_key.clone(),
                model,
                answers: native::answers(call.questions, reply)?,
                input_tokens,
            })
        }
        DecisionRoute::Logprobs(endpoint) => {
            // One closed-vocabulary question per request: a single next token
            // can only measure one question.
            let mut answers = Vec::with_capacity(call.questions.len());
            let mut input_tokens: Option<i64> = None;
            let mut model = None;
            for question in call.questions {
                let ask = logprobs::Ask::new(call.model, call.input, question)?;
                let reply = complete
                    .complete(CompletionCall::whole(
                        CompletionWire::Chat(endpoint),
                        ask.body(),
                        call.timeout,
                    ))?
                    .into_chat()?;
                if let Some(prompt) = reply.usage.as_ref().and_then(|usage| usage.prompt_tokens) {
                    *input_tokens.get_or_insert(0) += i64::from(prompt);
                }
                if model.is_none() {
                    model = reply.model.clone().filter(|model| !model.is_empty());
                }
                let provider = endpoint.provider_name.clone();
                let model_id = call.model.to_string();
                answers.push(ask.answer(&reply).map_err(|unmeasured| match unmeasured {
                    logprobs::Unmeasured::MissingLogprobs => DecideError::NoLogprobs {
                        provider,
                        model: model_id,
                    },
                    logprobs::Unmeasured::NotAnswering { began } => DecideError::NotAnswering {
                        provider,
                        model: model_id,
                        began,
                    },
                })?);
            }
            Ok(Decision {
                provider: endpoint.backend_key.clone(),
                model: model.unwrap_or_else(|| call.model.to_string()),
                answers,
                input_tokens,
            })
        }
    }
}
