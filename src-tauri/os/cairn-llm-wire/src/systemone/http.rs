//! The System One request body and its one POST.

use std::ops::Add;
use std::time::Duration;

use serde_json::{json, Map, Value};

use super::wire::{SystemOneAnswer, SystemOneResponse, SystemOneUsage};
use crate::decisions::http::{post_whole, DecisionsEndpoint, WholeReply};
use crate::decisions::wire::WireQuestion;
use crate::{upstream_error_detail, CompletionError};

/// How many refused questions one call asks around before it gives up, so a
/// model that refuses everything fails loudly instead of costing a round trip
/// per question.
const MAX_REFUSALS: usize = 3;

/// Build a System One request body. `state` is what every question is asked
/// about: a string, or a JSON object or array of related context.
///
/// Each question's name becomes its key. A predicate is a `noul`. A choice's
/// options become `criteria` keyed by value, each with its description or
/// `null`. A score's levels become an ordered `criteria` list, lowest first;
/// System One shows the model only that list, so a level with a description
/// is sent as `label: description` and one without as its label.
pub fn build_body(model: &str, state: Value, questions: &[WireQuestion]) -> Value {
    let questions: Map<String, Value> = questions.iter().map(question).collect();
    json!({
        "model": model,
        "state": state,
        "questions": questions,
    })
}

fn question(question: &WireQuestion) -> (String, Value) {
    match question {
        WireQuestion::Predicate { name, instructions } => (
            name.clone(),
            json!({ "type": "noul", "instructions": instructions }),
        ),
        WireQuestion::Choice {
            name,
            instructions,
            choices,
        } => {
            let criteria: Map<String, Value> = choices
                .iter()
                .map(|choice| {
                    (
                        choice.value.clone(),
                        choice
                            .description
                            .clone()
                            .map_or(Value::Null, Value::String),
                    )
                })
                .collect();
            (
                name.clone(),
                json!({ "type": "choice", "instructions": instructions, "criteria": criteria }),
            )
        }
        WireQuestion::Score {
            name,
            instructions,
            levels,
        } => {
            let criteria: Vec<String> = levels
                .iter()
                .map(|level| match &level.description {
                    Some(description) => format!("{}: {description}", level.label),
                    None => level.label.clone(),
                })
                .collect();
            (
                name.clone(),
                json!({ "type": "score", "instructions": instructions, "criteria": criteria }),
            )
        }
    }
}

/// Post one System One request and read its reply.
///
/// A gateway may fail the whole request because its model refused one
/// question in it (the module's `# Refusals`). That question is then dropped
/// and the rest asked again, and the reply carries a
/// [`SystemOneAnswer::Refused`] under the dropped name, so it still answers
/// every question asked. More than [`MAX_REFUSALS`] refusals fail the call.
/// Usage is summed over every POST made.
pub fn post_systemone(
    endpoint: &DecisionsEndpoint,
    mut body: Value,
    timeout: Duration,
) -> Result<SystemOneResponse, CompletionError> {
    let mut refused: Vec<(String, String)> = Vec::new();
    let mut usage = None;
    loop {
        let reply = post_whole(endpoint, &body, timeout)?;
        let Some((name, reason)) = refused_question(&reply, &body) else {
            let mut response = read(&reply.into_body(endpoint)?)?;
            response.usage = add_usage(usage, response.usage.take());
            for (name, reason) in refused {
                response
                    .answers
                    .insert(name, SystemOneAnswer::Refused { reason });
            }
            return Ok(response);
        };
        if refused.is_empty() {
            // Whether this body carries partial answers or `error.metadata`
            // is not yet known; one logged occurrence answers it.
            log::warn!(
                "{} refused System One question \"{name}\"; asking the rest again. Its HTTP {} body: {}",
                endpoint.provider_name,
                reply.status.as_u16(),
                reply.text
            );
        }
        usage = add_usage(usage, error_usage(&reply.text));
        let remaining = body
            .get_mut("questions")
            .and_then(Value::as_object_mut)
            .map_or(0, |questions| {
                questions.remove(&name);
                questions.len()
            });
        refused.push((name, reason));
        if refused.len() > MAX_REFUSALS {
            let names: Vec<String> = refused
                .iter()
                .map(|(name, _)| format!("\"{name}\""))
                .collect();
            return Err(CompletionError::Upstream(format!(
                "{} refused {} questions in one decision, more than the {MAX_REFUSALS} a call asks around: {}",
                endpoint.provider_name,
                refused.len(),
                names.join(", ")
            )));
        }
        if remaining == 0 {
            return Ok(SystemOneResponse {
                model: None,
                answers: refused
                    .into_iter()
                    .map(|(name, reason)| (name, SystemOneAnswer::Refused { reason }))
                    .collect(),
                usage,
            });
        }
    }
}

/// The question a failed POST says its model refused, with the gateway's
/// sentence saying so, when that question was asked in `body`. The sentence
/// is matched from `refused to answer question "<name>"` on, since each
/// upstream puts its own name in front.
fn refused_question(reply: &WholeReply, body: &Value) -> Option<(String, String)> {
    if reply.status.is_success() {
        return None;
    }
    let sentence = upstream_error_detail(&reply.text);
    let (_, rest) = sentence.split_once("refused to answer question \"")?;
    let (name, _) = rest.split_once('"')?;
    body.get("questions")?
        .as_object()?
        .contains_key(name)
        .then(|| (name.to_string(), sentence.clone()))
}

/// A failure body's own `usage`, if it reports one.
fn error_usage(text: &str) -> Option<SystemOneUsage> {
    let usage = serde_json::from_str::<Value>(text)
        .ok()?
        .get("usage")?
        .clone();
    serde_json::from_value(usage).ok()
}

fn add_usage(a: Option<SystemOneUsage>, b: Option<SystemOneUsage>) -> Option<SystemOneUsage> {
    fn sum<T: Add<Output = T>>(a: Option<T>, b: Option<T>) -> Option<T> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a + b),
            (a, b) => a.or(b),
        }
    }
    match (a, b) {
        (Some(a), Some(b)) => Some(SystemOneUsage {
            input_tokens: sum(a.input_tokens, b.input_tokens),
            output_tokens: sum(a.output_tokens, b.output_tokens),
            cost: sum(a.cost, b.cost),
        }),
        (a, b) => a.or(b),
    }
}

/// Decode a System One reply body. Only an unreadable envelope is an error;
/// an answer of a shape Cairn does not recognise is kept as it came
/// ([`super::wire::SystemOneAnswer::Unrecognised`]).
pub fn read(text: &str) -> Result<SystemOneResponse, CompletionError> {
    serde_json::from_str(text).map_err(|error| {
        CompletionError::InvalidResponse(format!("unreadable System One reply: {error}: {text}"))
    })
}
