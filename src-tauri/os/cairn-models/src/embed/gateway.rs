//! Cairn's gateway `/embed` as an embed route.

use cairn_models_types::embedding::{CAIRN_GATEWAY_EMBED_MODEL, CAIRN_GATEWAY_KEY};

use super::Reply;
use crate::interface::complete::CompletionError;
use crate::interface::embed::EmbedRoute;
use crate::wire::embeddings::gateway::{GatewayEmbedEndpoint, GatewayEmbedResponse};

/// The route to `{api_base}/embed`, on this device's Cairn token.
pub fn route_for(api_base: &str, device_token: &str) -> EmbedRoute {
    EmbedRoute::Gateway(GatewayEmbedEndpoint {
        provider_name: "Cairn gateway".to_string(),
        backend_key: CAIRN_GATEWAY_KEY.to_string(),
        url: format!("{}/embed", api_base.trim_end_matches('/')),
        headers: vec![(
            "authorization".to_string(),
            format!("Bearer {device_token}"),
        )],
    })
}

/// The reply's vectors, checked against what the gateway says of them.
///
/// The gateway is the one provider whose `model` is authoritative, since
/// Cairn runs it: a reply naming any model other than the one its offer is
/// keyed by would put vectors from another space under that key.
pub(super) fn vectors(reply: GatewayEmbedResponse, count: usize) -> Result<Reply, CompletionError> {
    if reply.model != CAIRN_GATEWAY_EMBED_MODEL {
        return Err(CompletionError::InvalidResponse(format!(
            "the gateway now serves {}, not {CAIRN_GATEWAY_EMBED_MODEL}; refusing to mix vector spaces",
            reply.model
        )));
    }
    if reply.embeddings.len() != count {
        return Err(CompletionError::InvalidResponse(format!(
            "the gateway returned {} embeddings for {count} texts",
            reply.embeddings.len()
        )));
    }
    if let Some(vector) = reply
        .embeddings
        .iter()
        .find(|vector| vector.len() as u32 != reply.dims)
    {
        return Err(CompletionError::InvalidResponse(format!(
            "the gateway said its vectors are {} wide but returned one {} wide",
            reply.dims,
            vector.len()
        )));
    }
    Ok(Reply {
        vectors: reply.embeddings,
        // The gateway's own characters-over-four estimate: Bedrock reports
        // no token count for Cohere.
        input_tokens: reply.input_tokens,
        cost_usd: None,
    })
}
