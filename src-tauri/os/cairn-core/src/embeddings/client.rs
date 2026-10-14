//! HTTP client for the cloud `/embed` gateway (Bedrock Cohere Embed v4).
//!
//! cairn-core stays Tauri-free and AWS-free: embeddings are produced by the
//! gateway, authenticated with the device JWT. The token is supplied by a
//! provider closure so the host wires it to its account manager.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::api::ApiConfig;

/// Cohere Embed v4 model name (as returned by the gateway) and vector width.
pub const COHERE_MODEL: &str = "cohere-embed-v4";
pub const COHERE_DIMS: u32 = 1536;

/// Asymmetric input type. Documents/corpus embed as `SearchDocument`; queries
/// embed as `SearchQuery`. This asymmetry is the validated retrieval win.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    SearchDocument,
    SearchQuery,
}

impl InputType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::SearchDocument => "search_document",
            Self::SearchQuery => "search_query",
        }
    }
}

/// Supplies the device JWT for gateway auth. Returns `None` when no token is
/// available at all — callers treat that as "skip embedding" (colors stay
/// neutral). An anonymous device token satisfies this, so a `Some` here means
/// the gateway is reachable, not that an account is connected.
pub type TokenProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

#[derive(Serialize)]
struct EmbedRequest<'a> {
    texts: &'a [String],
    input_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dims: Option<u32>,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

/// Client for the `/embed` gateway.
#[derive(Clone)]
pub struct EmbeddingClient {
    api: ApiConfig,
    token: TokenProvider,
    http: reqwest::Client,
}

impl EmbeddingClient {
    pub fn new(api: ApiConfig, token: TokenProvider) -> Self {
        Self {
            api,
            token,
            http: reqwest::Client::new(),
        }
    }

    /// Whether a device token is available right now.
    ///
    /// Lets a caller skip expensive preparation for a call that would be
    /// declined — the sweep would otherwise load and reconstruct a page of
    /// turns on every pass just to be told there is no token.
    pub fn has_token(&self) -> bool {
        (self.token)().is_some()
    }

    /// Embed `texts` through the gateway.
    ///
    /// Returns `Ok(None)` when there is no gateway token at all, so the caller
    /// can skip silently. That is NOT the same as "no account connected": every
    /// install registers an anonymous device token, so a token says nothing
    /// about whether anyone signed in. Callers that need consent, rather than
    /// mere reachability, must ask the account manager — see
    /// `embeddings::turns::Scope`. Returns embeddings in input order otherwise.
    pub async fn embed(
        &self,
        texts: Vec<String>,
        input_type: InputType,
        dims: Option<u32>,
    ) -> Result<Option<Vec<Vec<f32>>>, String> {
        if texts.is_empty() {
            return Ok(Some(vec![]));
        }
        let Some(jwt) = (self.token)() else {
            return Ok(None);
        };

        let body = EmbedRequest {
            texts: &texts,
            input_type: input_type.as_str(),
            dims,
        };

        let resp = self
            .http
            .post(self.api.embed_url())
            .bearer_auth(jwt)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("embed request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("embed gateway returned {status}: {text}"));
        }

        let parsed: EmbedResponse = resp
            .json()
            .await
            .map_err(|e| format!("embed response parse failed: {e}"))?;

        if parsed.embeddings.len() != texts.len() {
            return Err(format!(
                "embed gateway returned {} vectors for {} texts",
                parsed.embeddings.len(),
                texts.len()
            ));
        }
        Ok(Some(parsed.embeddings))
    }
}
