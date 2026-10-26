//! The facet's embedding implementation: one [`embed`] over either embedding
//! wire, composed from [`Complete`] so every POST passes the same seam a
//! completion does. The contract it keeps is stated on
//! [`crate::interface::embed`].

pub(crate) mod admit;
pub mod families;
pub mod gateway;
pub mod openai;
mod split;
#[cfg(test)]
mod tests;

use std::time::Duration;

use cairn_models_types::embedding::{BatchLimits, PurposeHandling};
use cairn_models_types::usage::{Charge, Measure, Meter, MeteredUsage};

use crate::interface::complete::{Complete, CompletionCall, CompletionError};
use crate::interface::embed::{EmbedCall, EmbedError, EmbedRoute, Embeddings, Purpose};
use crate::wire::embeddings::{gateway as gateway_wire, openai as openai_wire};

/// The limits declared for an OpenAI-compatible server that states none of
/// its own (Ollama, llama.cpp, vLLM, a custom hub): small enough that one
/// request never holds a local server for long, large enough that a sweep
/// is a handful of requests. `max_input_tokens` is the model's context, when
/// the server reports one.
pub fn local_server_limits(max_input_tokens: Option<u32>) -> BatchLimits {
    BatchLimits {
        max_texts: Some(32),
        max_chars: Some(32_000),
        max_input_tokens,
    }
}

/// Attempts one sub-batch gets, the first included.
const ATTEMPTS: u32 = 3;
/// The wait before the second attempt; it doubles for each one after.
const FIRST_RETRY_DELAY: Duration = Duration::from_secs(2);
/// The longest `Retry-After` slept inside a call. A provider asking for more
/// gets its error back at once, carrying the wait, so the caller's own pause
/// honours it instead of a blocking thread sitting on it.
const MAX_IN_CALL_WAIT: Duration = Duration::from_secs(30);

/// Run `attempt` until it succeeds, fails for good, or has been tried
/// [`ATTEMPTS`] times. Only a transient failure is retried, after the doubling
/// delay or the provider's `Retry-After`, whichever is longer.
fn with_retry<T>(
    sleep: &dyn Fn(Duration),
    mut attempt: impl FnMut() -> Result<T, CompletionError>,
) -> Result<T, CompletionError> {
    let mut tried = 1;
    loop {
        let error = match attempt() {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let asked = error.retry_after().unwrap_or_default();
        if !error.is_transient() || tried >= ATTEMPTS || asked > MAX_IN_CALL_WAIT {
            return Err(error);
        }
        sleep((FIRST_RETRY_DELAY * 2u32.pow(tried - 1)).max(asked));
        tried += 1;
    }
}

/// Embed `call.request` on one offer: one vector per text, in input order, at
/// the admitted width. A transient provider failure is retried within the
/// call; see [`with_retry`].
pub fn embed(complete: &dyn Complete, call: EmbedCall<'_>) -> Result<Embeddings, EmbedError> {
    embed_with(complete, call, &std::thread::sleep)
}

fn embed_with(
    complete: &dyn Complete,
    call: EmbedCall<'_>,
    sleep: &dyn Fn(Duration),
) -> Result<Embeddings, EmbedError> {
    let EmbedCall {
        route,
        model,
        capabilities,
        request,
        timeout,
    } = call;
    if request.texts.is_empty() {
        return Err(EmbedError::InvalidRequest(
            "there are no texts to embed".into(),
        ));
    }
    if let Some(index) = request.texts.iter().position(String::is_empty) {
        return Err(EmbedError::InvalidRequest(format!(
            "text {index} is empty; every text must have content"
        )));
    }
    let dimensions = admit::admit(capabilities, request.width)?;
    let texts = with_purpose(&request.texts, request.purpose, &capabilities.purpose);
    let batches = split::batches(&texts, &capabilities.limits)?;

    // The width every vector must have: the one asked for, else the native
    // one, else (native unknown) whatever the first vector measures.
    let mut width = dimensions.or(capabilities.native_dims);
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(texts.len());
    let mut tokens = Tally::default();
    let mut cost = Tally::default();
    for range in batches {
        let first_text = range.start;
        let batch = &texts[range];
        let wire = route.completion_wire();
        let reply = match route {
            EmbedRoute::OpenAi(_) => {
                let input_type = matches!(capabilities.purpose, PurposeHandling::Native)
                    .then(|| request.purpose.input_type());
                let body = openai_wire::build_body(model, batch, dimensions, input_type);
                let reply = with_retry(sleep, || {
                    complete.complete(CompletionCall::whole(wire, body.clone(), timeout))
                })?
                .into_embeddings()?;
                openai::vectors(reply, batch.len(), route.provider_name())?
            }
            EmbedRoute::Gateway(_) => {
                let body = gateway_wire::build_body(batch, request.purpose.input_type());
                let reply = with_retry(sleep, || {
                    complete.complete(CompletionCall::whole(wire, body.clone(), timeout))
                })?
                .into_gateway_embed()?;
                gateway::vectors(reply, batch.len())?
            }
        };
        // An empty vector is no embedding at any width, known or measured.
        if let Some(position) = reply.vectors.iter().position(Vec::is_empty) {
            return Err(EmbedError::Completion(CompletionError::InvalidResponse(
                format!(
                    "{} returned an empty vector for text {}",
                    route.provider_name(),
                    first_text + position
                ),
            )));
        }
        let expected = *width.get_or_insert_with(|| {
            reply
                .vectors
                .first()
                .map_or(0, |vector| vector.len() as u32)
        });
        if let Some(wrong) = reply
            .vectors
            .iter()
            .find(|vector| vector.len() as u32 != expected)
        {
            return Err(EmbedError::WidthMismatch {
                admitted: dimensions.or(capabilities.native_dims),
                got: wrong.len() as u32,
            });
        }
        tokens.add(reply.input_tokens);
        cost.add(reply.cost_usd);
        vectors.extend(reply.vectors);
    }

    let mut usage = MeteredUsage::default();
    if let Some(quantity) = tokens.total() {
        usage.measures.push(Measure {
            meter: Meter::tokens(Charge::Input),
            quantity,
        });
    }
    usage.cost_usd = cost.total();
    Ok(Embeddings {
        vectors,
        provider: route.backend_key().to_string(),
        model: model.to_string(),
        dims: width.unwrap_or_default(),
        usage,
    })
}

/// One sub-batch's vectors in input order, and what it reported.
struct Reply {
    vectors: Vec<Vec<f32>>,
    input_tokens: Option<u64>,
    cost_usd: Option<f64>,
}

/// A sum that exists only when every part reported: a total missing one
/// sub-batch would read as the whole call's usage and under-report it.
struct Tally<T> {
    sum: Option<T>,
    complete: bool,
}

impl<T> Default for Tally<T> {
    fn default() -> Self {
        Self {
            sum: None,
            complete: true,
        }
    }
}

impl<T: std::ops::Add<Output = T> + Copy> Tally<T> {
    fn add(&mut self, part: Option<T>) {
        match part {
            Some(part) => self.sum = Some(self.sum.map_or(part, |sum| sum + part)),
            None => self.complete = false,
        }
    }

    fn total(&self) -> Option<T> {
        self.sum.filter(|_| self.complete)
    }
}

/// The texts as the model wants them for `purpose`.
fn with_purpose(texts: &[String], purpose: Purpose, handling: &PurposeHandling) -> Vec<String> {
    match handling {
        PurposeHandling::Prefix { document, query } => {
            let prefix = match purpose {
                Purpose::Document => document,
                Purpose::Query => query,
            };
            texts.iter().map(|text| format!("{prefix}{text}")).collect()
        }
        PurposeHandling::Native | PurposeHandling::Ignored => texts.to_vec(),
    }
}
