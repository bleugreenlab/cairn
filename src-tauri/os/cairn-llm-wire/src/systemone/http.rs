//! The System One request body and its one POST.

use std::time::Duration;

use serde_json::{json, Map, Value};

use super::wire::SystemOneResponse;
use crate::decisions::http::{post_whole, DecisionsEndpoint};
use crate::decisions::wire::WireQuestion;
use crate::CompletionError;

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
pub fn post_systemone(
    endpoint: &DecisionsEndpoint,
    body: Value,
    timeout: Duration,
) -> Result<SystemOneResponse, CompletionError> {
    read(&post_whole(endpoint, body, timeout)?)
}

/// Decode a System One reply body. Only an unreadable envelope is an error;
/// an answer of a shape Cairn does not recognise is kept as it came
/// ([`super::wire::SystemOneAnswer::Unrecognised`]).
pub fn read(text: &str) -> Result<SystemOneResponse, CompletionError> {
    serde_json::from_str(text).map_err(|error| {
        CompletionError::InvalidResponse(format!("unreadable System One reply: {error}: {text}"))
    })
}
