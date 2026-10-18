//! The facet's embedding verb: texts in, one vector per text out, at a width
//! the offer declared and the reply is checked against.
//!
//! `embed` shares `complete`'s substrate, as `decide` does: the same
//! [`Complete`] carries its POST and the same [`Catalog`] chooses its offer
//! ([`Catalog::embedding_offer`]). What it adds is what makes a vector safe to
//! store beside another:
//!
//! - **Identity is the offer's.** The model a result names is the catalog id it
//!   was asked for, never the reply's `model` field: hubs answer with their
//!   backend's served name. Cairn's own gateway is the one exception, because
//!   Cairn runs it, and a reply naming another model is refused.
//! - **Widths are verified.** Every vector must have the width that was
//!   admitted. A server that ignores `dimensions` returns its native width
//!   with a 200; that is a [`EmbedError::WidthMismatch`], never truncated.
//! - **Purpose is applied by the facet**, from what the offer declares
//!   (a request field, an instruction prefix, or nothing), so a consumer only
//!   ever says whether a text is a document or a query.
//! - **Large requests are split** into consecutive requests within the offer's
//!   limits and joined in input order; any failure fails the whole call.
//!
//! Usage is returned, not recorded: the caller owns persistence.
//!
//! [`Complete`]: super::complete::Complete

use std::fmt;
use std::time::Duration;

use cairn_models_types::catalog::{Verb, VerbKind, Wire};
use cairn_models_types::embedding::EmbeddingCapabilities;
use cairn_models_types::usage::MeteredUsage;

use super::complete::{CompletionError, CompletionWire};
use super::{Catalog, NoOffer, OfferQuery, SelectedOffer};
use crate::embed::admit::admit;
use crate::wire::embeddings::gateway::GatewayEmbedEndpoint;
use crate::wire::embeddings::openai::EmbeddingsEndpoint;

/// Bounds each provider request (each sub-batch of a split request).
pub const EMBED_TIMEOUT: Duration = Duration::from_secs(60);

/// The fixes every no-offer refusal of `embed` names.
pub const EMBED_FIXES: &str = "To fix it, add an embedding provider in Settings → Providers: an \
OpenRouter API key, an Ollama host with an embedding model pulled (for example `ollama pull \
nomic-embed-text`), or a custom OpenAI-compatible provider with its embedding models and their \
width declared. Cairn's own gateway (Cohere Embed v4) is offered whenever this device holds a \
Cairn token; when it is missing, no token resolved, so check that Cairn is signed in or that this \
device can reach Cairn's API to register.";

/// Whether the texts are what is searched or what searches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Purpose {
    Document,
    Query,
}

impl Purpose {
    /// The name a request field carries it under (Cohere's, which the gateway
    /// and OpenRouter take).
    pub fn input_type(self) -> &'static str {
        match self {
            Self::Document => "search_document",
            Self::Query => "search_query",
        }
    }
}

/// What to embed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedRequest {
    pub texts: Vec<String>,
    pub purpose: Purpose,
    /// The width wanted. Unset takes the model's native width.
    pub dims: Option<u32>,
}

/// One vector per text, in input order, and what produced them.
#[derive(Debug, Clone, PartialEq)]
pub struct Embeddings {
    pub vectors: Vec<Vec<f32>>,
    /// The offer's provider key.
    pub provider: String,
    /// The offer's model id: what stored vectors are keyed by.
    pub model: String,
    /// The width measured on the reply, equal for every vector.
    pub dims: u32,
    /// Input tokens, and cost where the provider reported it. Only what was
    /// reported is measured.
    pub usage: MeteredUsage,
}

/// Which embedding wire a call is sent over, and the deployment it goes to.
#[derive(Debug, Clone)]
pub enum EmbedRoute {
    /// OpenAI's `embeddings` (OpenRouter, Ollama, custom providers).
    OpenAi(EmbeddingsEndpoint),
    /// Cairn's gateway `/embed`.
    Gateway(GatewayEmbedEndpoint),
}

impl EmbedRoute {
    /// The completion route an embedding on this deployment is sent over.
    pub fn completion_wire(&self) -> CompletionWire<'_> {
        match self {
            Self::OpenAi(endpoint) => CompletionWire::Embeddings(endpoint),
            Self::Gateway(endpoint) => CompletionWire::GatewayEmbed(endpoint),
        }
    }

    /// The catalog's name for this route's protocol, through the one
    /// route-to-wire mapping.
    pub fn wire(&self) -> Wire {
        self.completion_wire().wire()
    }

    pub fn backend_key(&self) -> &str {
        match self {
            Self::OpenAi(endpoint) => &endpoint.backend_key,
            Self::Gateway(endpoint) => &endpoint.backend_key,
        }
    }

    pub fn provider_name(&self) -> &str {
        match self {
            Self::OpenAi(endpoint) => &endpoint.provider_name,
            Self::Gateway(endpoint) => &endpoint.provider_name,
        }
    }
}

/// One embedding request against one offer.
pub struct EmbedCall<'a> {
    pub route: &'a EmbedRoute,
    /// The id the provider is sent, and the id the result is keyed by.
    pub model: &'a str,
    /// What the offer declares.
    pub capabilities: &'a EmbeddingCapabilities,
    pub request: &'a EmbedRequest,
    /// Bounds each provider request.
    pub timeout: Duration,
}

/// Why an embedding produced no vectors.
#[derive(Debug, PartialEq)]
pub enum EmbedError {
    NoOffer(NoOffer),
    /// The offer cannot do what was asked (a width it does not return).
    Unsupported(String),
    /// The request cannot be sent as written (no texts, an empty text, a text
    /// longer than one request may carry).
    InvalidRequest(String),
    /// The reply's vectors are not the width that was admitted. They are
    /// discarded: a vector of the wrong width cannot be stored beside the
    /// others, and cutting it down is only sound for a Matryoshka model.
    WidthMismatch {
        admitted: Option<u32>,
        got: u32,
    },
    /// The provider failed, timed out, or answered something unreadable.
    Completion(CompletionError),
}

impl fmt::Display for EmbedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOffer(refusal) => refusal.fmt(f),
            Self::Unsupported(text) => f.write_str(text),
            Self::InvalidRequest(text) => write!(f, "the embedding request is invalid: {text}"),
            Self::WidthMismatch {
                admitted: Some(admitted),
                got,
            } => write!(
                f,
                "the reply's vectors are {got} wide where {admitted} was admitted, so none were used. A server that ignores `dimensions` answers at its native width; declare this model native-only, at the width it actually returns"
            ),
            Self::WidthMismatch {
                admitted: None,
                got,
            } => write!(
                f,
                "the reply's vectors differ in width (one is {got} wide), so none were used"
            ),
            Self::Completion(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EmbedError {}

impl From<CompletionError> for EmbedError {
    fn from(error: CompletionError) -> Self {
        Self::Completion(error)
    }
}

impl From<NoOffer> for EmbedError {
    fn from(refusal: NoOffer) -> Self {
        Self::NoOffer(refusal)
    }
}

impl Catalog {
    /// The offer that embeds `request`, with what it declares: the best offer
    /// serving `embed` for `requested` (unset takes the first) that can return
    /// the width asked for.
    pub fn embedding_offer(
        &self,
        requested: Option<&str>,
        request: &EmbedRequest,
    ) -> Result<(SelectedOffer, EmbeddingCapabilities), EmbedError> {
        self.admitting_offer(
            OfferQuery {
                requested,
                ..OfferQuery::new(VerbKind::Embed)
            },
            |offer| match &offer.served.verb {
                Verb::Embed(capabilities) => {
                    admit(capabilities, request.dims).map(|_| capabilities.clone())
                }
                _ => Err(EmbedError::Unsupported("it does not embed".to_string())),
            },
        )
        .map_err(EmbedError::NoOffer)
    }
}
