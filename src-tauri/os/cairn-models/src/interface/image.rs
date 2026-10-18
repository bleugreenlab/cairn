//! The facet's image verb: a prompt, optional labelled references and an
//! edit, and a shape, answered with image bytes, the provenance of how the
//! request was carried out, and what it consumed.
//!
//! `image` is its own verb rather than a kind of completion because neither
//! side fits [`super::complete::Complete`]: an edit is a multipart body, and a
//! reply carries megabytes of binary. It has its own transport seam instead,
//! [`ImageTransport`], with two real implementations
//! ([`crate::image::http::HttpImages`] for the HTTP wires, and
//! [`crate::image::codex::CodexImages`], which runs Codex image workers and
//! hands every other body to its HTTP transport), and shares the rest of the facet: the
//! same [`super::Catalog`] chooses its offer ([`super::Catalog::image_offer`])
//! and the same metered usage describes what it cost.
//!
//! What an offer can draw is declared on the catalog
//! ([`ImageCapabilities`], carried by `Verb::Image`). A request is admitted
//! against that declaration before anything is sent: a feature the offer
//! honours natively passes, one it declares a [`Lowering`] for is rewritten
//! and the rewrite recorded in [`Provenance`], and anything else is refused by
//! name. Nothing is degraded quietly.

use std::fmt;
use std::time::Duration;

use cairn_models_types::catalog::{VerbKind, Wire};
use cairn_models_types::image::{Lowering, Orientation, Pixels, Ratio};
use cairn_models_types::usage::MeteredUsage;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{NoOffer, OfferQuery, SelectedOffer};
use crate::image_wire::openai::wire::ImagesResponse as OpenAiImagesResponse;
use crate::image_wire::openai::EditsForm;
use crate::image_wire::openrouter::wire::ImagesResponse as OpenRouterImagesResponse;
use crate::image_wire::{ImageWireError, ImagesEndpoint};
use crate::wire::codex_app_server::ImageGenerationItem;
use crate::wire::TurnUsage;

pub use cairn_models_types::image::ImageCapabilities;

/// How long one image call may take. Generation is slow: OpenRouter's own
/// documented example took 94 s.
pub const IMAGE_TIMEOUT: Duration = Duration::from_secs(180);

/// The fixes every no-offer refusal of an image names.
pub const IMAGE_FIXES: &str = "Add an OpenRouter API key in Settings → Providers (its image \
models serve this verb), sign in to Codex with a ChatGPT account on a paid plan (Codex draws \
with its own image tool), or add a custom provider with protocol OpenAI Images pointing at an \
OpenAI-compatible /v1/images server: OpenAI at https://api.openai.com/v1, or a local server.";

/// An image sent with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageInput {
    pub bytes: Vec<u8>,
    pub media_type: String,
}

/// A reference image, optionally named so the prompt can refer to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub image: ImageInput,
    pub label: Option<String>,
}

/// An image to change rather than draw from scratch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    pub base: ImageInput,
    /// A PNG the size of `base`; fully transparent pixels mark where to edit.
    pub mask: Option<ImageInput>,
}

/// The shape asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "mode", content = "value")]
pub enum ImageShape {
    /// Whatever the provider picks.
    #[default]
    Auto,
    Pixels(Pixels),
    Aspect(Ratio),
    Orientation(Orientation),
}

/// Provider-only knobs (`quality`, `seed`, `provider.only`), applied only when
/// `wire` carries the call. They may add fields but never set one the typed
/// request owns.
#[derive(Debug, Clone, PartialEq)]
pub struct WireExtras {
    pub wire: Wire,
    pub fields: Map<String, Value>,
}

/// One image request.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageRequest {
    pub prompt: String,
    pub references: Vec<Reference>,
    pub edit: Option<Edit>,
    pub count: u32,
    pub shape: ImageShape,
    pub extras: Vec<WireExtras>,
}

impl ImageRequest {
    /// One image of `prompt`, shaped by the provider.
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            references: Vec::new(),
            edit: None,
            count: 1,
            shape: ImageShape::Auto,
            extras: Vec::new(),
        }
    }

    /// The extras for `wire`, merged in the order given.
    pub fn extras_for(&self, wire: Wire) -> Map<String, Value> {
        let mut fields = Map::new();
        for extras in self.extras.iter().filter(|extras| extras.wire == wire) {
            fields.extend(extras.fields.clone());
        }
        fields
    }
}

/// One image the provider returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedImage {
    pub bytes: Vec<u8>,
    pub media_type: String,
    /// What the image's own header says; `None` for SVG or an unreadable one.
    pub size: Option<Pixels>,
    /// The prompt the model actually drew, for models that rewrite it.
    pub revised_prompt: Option<String>,
}

/// How a request was carried out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    pub provider: String,
    pub model: String,
    pub wire: Wire,
    /// The prompt as sent, after every lowering composed into it.
    pub prompt_sent: String,
    /// Exactly the lowerings applied, in the order they were.
    pub lowerings: Vec<Lowering>,
    pub requested: ImageShape,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImageResult {
    pub images: Vec<GeneratedImage>,
    pub provenance: Provenance,
    pub usage: MeteredUsage,
}

/// A request feature an offer may not honour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ImageFeature {
    Count,
    Pixels,
    Aspect,
    Orientation,
    References,
    Labels,
    Edit,
    Mask,
}

impl ImageFeature {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "image count",
            Self::Pixels => "exact size",
            Self::Aspect => "aspect ratio",
            Self::Orientation => "orientation",
            Self::References => "reference images",
            Self::Labels => "reference labels",
            Self::Edit => "edit",
            Self::Mask => "mask",
        }
    }
}

impl fmt::Display for ImageFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why an image request produced no images.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageError {
    NoOffer(NoOffer),
    /// The offer cannot honour `feature`, natively or by a declared lowering.
    Unsupported {
        feature: ImageFeature,
        detail: String,
    },
    /// The provider's moderation or content policy declined it.
    Refused {
        message: String,
    },
    /// Out of credits or rate limited.
    Quota {
        message: String,
    },
    /// The request cannot be sent as written.
    InvalidRequest(String),
    Timeout,
    Upstream(String),
    InvalidResponse(String),
}

impl fmt::Display for ImageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOffer(refusal) => refusal.fmt(f),
            Self::Unsupported { feature, detail } => {
                write!(
                    f,
                    "the image request's {feature} cannot be honoured: {detail}"
                )
            }
            Self::Refused { message } => write!(f, "the image was refused: {message}"),
            Self::Quota { message } => write!(f, "the image provider is out of quota: {message}"),
            Self::InvalidRequest(text) => write!(f, "the image request is invalid: {text}"),
            Self::Timeout => write!(
                f,
                "the image request timed out after {} s",
                IMAGE_TIMEOUT.as_secs()
            ),
            Self::Upstream(text) => write!(f, "the image provider failed: {text}"),
            Self::InvalidResponse(text) => write!(f, "the image reply was invalid: {text}"),
        }
    }
}

impl std::error::Error for ImageError {}

impl From<NoOffer> for ImageError {
    fn from(refusal: NoOffer) -> Self {
        Self::NoOffer(refusal)
    }
}

/// The provider codes a moderation or content-policy refusal arrives with.
pub(crate) const REFUSAL_CODES: &[&str] = &[
    "moderation_blocked",
    "content_policy_violation",
    "content_filter",
];

impl ImageError {
    /// A wire failure as the verb reports it, naming `provider`.
    pub fn from_wire(provider: &str, error: ImageWireError) -> Self {
        match error {
            ImageWireError::Timeout => Self::Timeout,
            ImageWireError::InvalidRequest(text) => Self::InvalidRequest(text),
            ImageWireError::Transport(text) => Self::Upstream(text),
            ImageWireError::InvalidResponse(text) => {
                Self::InvalidResponse(format!("{provider}: {text}"))
            }
            ImageWireError::Http {
                status,
                code,
                message,
            } => {
                if code
                    .as_deref()
                    .is_some_and(|code| REFUSAL_CODES.contains(&code))
                {
                    Self::Refused {
                        message: format!("{provider}: {message}"),
                    }
                } else if status == 402 || status == 429 {
                    Self::Quota {
                        message: format!("{provider} (HTTP {status}): {message}"),
                    }
                } else if (400..500).contains(&status) {
                    Self::InvalidRequest(format!(
                        "{provider} refused it (HTTP {status}): {message}"
                    ))
                } else {
                    Self::Upstream(format!("{provider} returned HTTP {status}: {message}"))
                }
            }
        }
    }
}

/// The deployment an image call goes to, and the protocol it speaks.
#[derive(Debug, Clone)]
pub enum ImageRoute {
    /// OpenRouter's Image API.
    OpenRouter(ImagesEndpoint),
    /// The OpenAI Images API, on OpenAI or any compatible server.
    OpenAi(ImagesEndpoint),
    /// Codex's own image tool, called by a worker thread on a Codex
    /// app-server signed in to ChatGPT.
    Codex(CodexImageRoute),
}

impl ImageRoute {
    /// The catalog's name for this route's protocol: the one place a route
    /// meets the wire an offer declares.
    pub fn wire(&self) -> Wire {
        match self {
            Self::OpenRouter(_) => Wire::OpenRouterImages,
            Self::OpenAi(_) => Wire::OpenAiImages,
            Self::Codex(_) => Wire::CodexAppServer,
        }
    }

    /// What to call the provider in an error a user will read.
    pub fn provider_name(&self) -> &str {
        match self {
            Self::OpenRouter(endpoint) | Self::OpenAi(endpoint) => &endpoint.provider_name,
            Self::Codex(route) => &route.provider_name,
        }
    }

    /// The backend key provenance and usage are attributed to.
    pub fn backend_key(&self) -> &str {
        match self {
            Self::OpenRouter(endpoint) | Self::OpenAi(endpoint) => &endpoint.backend_key,
            Self::Codex(route) => &route.backend_key,
        }
    }
}

/// Where a Codex image call goes. The app-server itself is the transport's
/// ([`crate::image::codex::CodexImages`]); the route names the provider and
/// the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexImageRoute {
    pub provider_name: String,
    pub backend_key: String,
    /// The text model that calls the image tool, from the host's Codex
    /// model list; `None` leaves it to Codex's default. It is not what draws:
    /// the image tool's own model does.
    pub worker_model: Option<String>,
}

impl CodexImageRoute {
    /// Codex under a ChatGPT sign-in, attributed to the `codex` backend.
    pub fn chatgpt(worker_model: Option<String>) -> Self {
        Self {
            provider_name: "Codex (ChatGPT)".to_string(),
            backend_key: "codex".to_string(),
            worker_model,
        }
    }
}

/// One body on its way to a provider.
#[derive(Debug, Clone)]
pub enum ImageBody {
    OpenRouter(Value),
    OpenAiGenerations(Value),
    OpenAiEdits(EditsForm),
    Codex(CodexImageTurn),
}

/// What each Codex image worker is given: pure data, built by
/// [`crate::image::image`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexImageTurn {
    /// The worker thread's base instructions.
    pub instructions: String,
    /// The images the turn attaches, in order: an edit's base first, then
    /// the references.
    pub images: Vec<ImageInput>,
    /// The turn's text: the composed prompt, wrapped for the worker.
    pub text: String,
    /// How many images, each drawn by a worker thread of its own.
    pub count: u32,
    /// The worker's reasoning effort.
    pub effort: String,
}

/// One POST: where, what, and how long it may take.
#[derive(Debug)]
pub struct ImagePost<'a> {
    pub route: &'a ImageRoute,
    pub body: ImageBody,
    pub timeout: Duration,
}

/// A provider's reply, in its own protocol's shape.
#[derive(Debug, Clone, PartialEq)]
pub enum ImageReply {
    OpenRouter(OpenRouterImagesResponse),
    OpenAi(OpenAiImagesResponse),
    Codex(CodexImageReply),
}

/// What the Codex workers produced: one outcome per requested image.
#[derive(Debug, Clone, PartialEq)]
pub struct CodexImageReply {
    pub draws: Vec<CodexDrawOutcome>,
}

/// How one worker's image went.
#[derive(Debug, Clone, PartialEq)]
pub struct CodexDrawOutcome {
    /// The image tool's item, when the worker called it.
    pub image: Option<ImageGenerationItem>,
    /// The worker's final text (`DONE`, or `FAILED: ` and the tool's error),
    /// or what ended its turn when that failed.
    pub text: String,
    /// Each worker turn's usage, a retried draw's included: the worker
    /// model's text tokens, not the image model's.
    pub usage: Vec<TurnUsage>,
}

/// How image calls reach providers. The seam sits at the POST, like
/// [`super::complete::Complete`]'s: where a proxy, capture, or a test's
/// scripted replies hook in.
pub trait ImageTransport: Send + Sync {
    fn post(&self, post: ImagePost<'_>) -> Result<ImageReply, ImageWireError>;
}

/// One image call.
pub struct ImageCall<'a> {
    pub route: &'a ImageRoute,
    /// The id the provider is sent.
    pub model: &'a str,
    /// What the chosen offer declares it can do.
    pub capabilities: &'a ImageCapabilities,
    pub request: &'a ImageRequest,
    pub timeout: Duration,
}

impl super::Catalog {
    /// The image offer for `request`, with what it can do: the best offer
    /// for `requested` that admits the request over its own wire
    /// ([`super::Catalog::admitting_offer`]). A refusal names each offer that
    /// declined and why, and for a pinned model or tier, offers that would
    /// admit it.
    pub fn image_offer(
        &self,
        requested: Option<&str>,
        request: &ImageRequest,
    ) -> Result<(SelectedOffer, ImageCapabilities), ImageError> {
        // What no offer could fix is reported as itself, not as a refusal
        // by every offer.
        crate::image::request_problems(request)?;
        self.admitting_offer(
            OfferQuery {
                requested,
                ..OfferQuery::new(VerbKind::Image)
            },
            |offer| {
                let capabilities = offer
                    .served
                    .verb
                    .image()
                    .cloned()
                    .expect("an offer selected for image serves Verb::Image");
                crate::image::admit(&capabilities, offer.served.wire, request).map(|_| capabilities)
            },
        )
        .map_err(ImageError::NoOffer)
    }
}
