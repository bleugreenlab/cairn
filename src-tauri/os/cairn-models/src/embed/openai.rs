//! OpenAI's `embeddings` as an embed route: the route a host builds for an
//! OpenAI-compatible deployment, and the reply read back into input order.

use super::Reply;
use crate::interface::complete::CompletionError;
use crate::interface::embed::EmbedRoute;
use crate::wire::embeddings::openai::{EmbeddingsEndpoint, EmbeddingsResponse};

/// The route to `{base_url}/embeddings`, with a bearer `api_key` when the
/// deployment takes one, plus any headers the provider adds.
pub fn route_for(
    provider_key: &str,
    provider_name: &str,
    base_url: &str,
    api_key: Option<&str>,
    headers: &[(String, String)],
) -> EmbedRoute {
    let mut all: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("authorization") || api_key.is_none())
        .cloned()
        .collect();
    if let Some(key) = api_key.filter(|key| !key.is_empty()) {
        all.push(("authorization".to_string(), format!("Bearer {key}")));
    }
    EmbedRoute::OpenAi(EmbeddingsEndpoint {
        provider_name: provider_name.to_string(),
        backend_key: provider_key.to_string(),
        url: format!("{}/embeddings", base_url.trim_end_matches('/')),
        headers: all,
    })
}

/// The reply's vectors in input order. A server may answer out of order, so
/// each is placed by its `index`; a reply that does not name exactly the
/// positions `0..count` once each is refused rather than guessed at.
pub(super) fn vectors(
    reply: EmbeddingsResponse,
    count: usize,
    provider_name: &str,
) -> Result<Reply, CompletionError> {
    let mut placed: Vec<Option<Vec<f32>>> = vec![None; count];
    let returned = reply.data.len();
    for item in reply.data {
        let index = item.index as usize;
        match placed.get_mut(index) {
            Some(slot @ None) => *slot = Some(item.embedding),
            Some(Some(_)) => {
                return Err(CompletionError::InvalidResponse(format!(
                    "{provider_name} returned two embeddings for text {index}"
                )))
            }
            None => {
                return Err(CompletionError::InvalidResponse(format!(
                    "{provider_name} returned an embedding for text {index} of {count}"
                )))
            }
        }
    }
    let vectors: Option<Vec<Vec<f32>>> = placed.into_iter().collect();
    let vectors = vectors.ok_or_else(|| {
        CompletionError::InvalidResponse(format!(
            "{provider_name} returned {returned} embeddings for {count} texts"
        ))
    })?;
    let usage = reply.usage;
    Ok(Reply {
        vectors,
        input_tokens: usage.as_ref().and_then(|usage| usage.prompt_tokens),
        cost_usd: usage.and_then(|usage| usage.cost),
    })
}
