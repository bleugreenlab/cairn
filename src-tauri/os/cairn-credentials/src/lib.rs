//! Provider accounts, credential selection, and durable credential storage.
//!
//! Runtime attribution and session binding are assembled by the host.

pub mod crypto;
mod handle;
pub mod lease;
mod plan;
pub mod store;

pub use cairn_models_types::ApiProvider;
pub use handle::CredentialStoreHandle;
pub use plan::{jwt_claims, subscription_plan};

use cairn_models_types::image::ImageCapabilities;
use serde::{Deserialize, Serialize};

/// Authentication selected for the provider runtimes in a session.
#[derive(Debug, Clone)]
pub struct ProviderCredentials {
    pub claude_auth: Option<ClaudeAuth>,
    pub codex_auth: Option<CodexAuth>,
    pub github_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RoutedAccountDiagnostic {
    pub account_id: String,
    pub auth_type: String,
    pub project_id: Option<String>,
    pub compatible_backends: Vec<String>,
    pub blocked_until: Option<i64>,
    pub live_windows: Vec<cairn_models_types::usage::ProviderUsageWindow>,
    pub rejection: Option<String>,
}

/// Sampling controls understood by OpenAI-compatible inference servers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CustomSampling {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repetition_penalty: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<i64>,
}

impl CustomSampling {
    pub fn is_empty(&self) -> bool {
        self.repetition_penalty.is_none()
            && self.temperature.is_none()
            && self.top_p.is_none()
            && self.top_k.is_none()
    }
}

/// Claude authentication method.
///
/// Subscription auth is a Cairn-managed profile and nothing else. Every
/// claude-backend session runs against an explicit `CLAUDE_CONFIG_DIR`, so no
/// session can land on whichever account happens to be signed in at the user
/// level — that ambient account is invisible to usage routing and cannot be
/// switched away from when it runs out. An API key stays a first-class
/// alternative because it is a credential Cairn holds and can attribute.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ClaudeAuth {
    /// API key (personal or org-provided)
    ApiKey(String),
    /// A Cairn-managed Claude account. Its tokens, and where its CLI home
    /// lives, are subscription mechanics, asked of
    /// `Subscriptions::claude_session_credential` at spawn.
    ManagedProfile { account_id: String },
}

impl std::fmt::Debug for ClaudeAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClaudeAuth::ApiKey(_) => f.debug_tuple("ApiKey").field(&REDACTED).finish(),
            ClaudeAuth::ManagedProfile { account_id } => f
                .debug_struct("ManagedProfile")
                .field("account_id", account_id)
                .finish(),
        }
    }
}

impl ClaudeAuth {
    /// Get the raw token/key value.
    pub fn value(&self) -> &str {
        match self {
            ClaudeAuth::ApiKey(v) => v,
            ClaudeAuth::ManagedProfile { account_id } => account_id,
        }
    }
}

/// Codex/OpenAI authentication method.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CodexAuth {
    /// ChatGPT OAuth — stores full auth.json contents (JSON string)
    OAuthToken(String),
    /// OpenAI API key
    ApiKey(String),
}

impl std::fmt::Debug for CodexAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CodexAuth::OAuthToken(_) => "OAuthToken",
            CodexAuth::ApiKey(_) => "ApiKey",
        };
        f.debug_tuple(name).field(&REDACTED).finish()
    }
}

impl CodexAuth {
    /// Get the raw token/key/json value.
    pub fn value(&self) -> &str {
        match self {
            CodexAuth::OAuthToken(v) | CodexAuth::ApiKey(v) => v,
        }
    }
}

// === Multi-account provider model (v2) ===

/// How an account was discovered/configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountSource {
    /// User explicitly configured via settings UI
    Configured,
    /// Provided by cairn-server (future — team credentials)
    Server,
}

/// Which of Actual's two ways of reaching the same clusters a target uses.
///
/// This is stored rather than inferred from the presence of a credential. A
/// relay target whose key has been cleared is a *misconfigured relay target*,
/// which needs to be reported as exactly that; inferring the kind from the key
/// would let it masquerade as a local instance that happens to point at
/// `api.actual.inc`, and the readiness message the user got would be wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActualTargetKind {
    /// An Actual instance on a machine reachable directly. Loopback needs no
    /// per-request credential.
    Local,
    /// Actual's end-to-end Private Relay, which needs an `ac_` inference
    /// credential and optionally pins one cluster.
    Relay,
}

impl ActualTargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActualTargetKind::Local => "local",
            ActualTargetKind::Relay => "relay",
        }
    }
}

/// Authentication credential for a provider account.
///
/// `Debug` is written by hand and redacts every secret, because accounts are
/// formatted into panics, test failures and log lines that outlive the moment
/// a credential is used.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProviderAuth {
    /// API key — works with both CLI and Native backends
    #[serde(rename = "api_key")]
    ApiKey { value: String },
    /// OAuth token — CLI-specific (Claude OAuth, ChatGPT OAuth)
    #[serde(rename = "oauth_token", alias = "o_auth_token")]
    OAuthToken { value: String },
    /// Provider host URL. This is connection metadata, not a secret.
    #[serde(rename = "base_url")]
    BaseUrl { url: String },
    /// One Actual target: where to reach it, the credential it needs, and the
    /// cluster it pins — held together as one fact.
    ///
    /// These three are inseparable in practice. A relay key is meaningless
    /// against a loopback endpoint, and a cluster id only means anything paired
    /// with the credential whose account can see that cluster. Splitting the
    /// cluster id out into global settings (the shape this deliberately avoids)
    /// would let a second target silently inherit a pin belonging to the first.
    #[serde(rename = "actual_target", rename_all = "camelCase")]
    ActualTarget {
        kind: ActualTargetKind,
        base_url: String,
        /// The `ac_` inference credential. Required for relay targets, absent
        /// for loopback ones, which take no per-request auth.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
        /// Optional `X-Cluster-ID` pin. Operator-supplied: Actual's documented
        /// cluster-listing endpoint is not served by the public relay, so Cairn
        /// cannot enumerate clusters to choose from.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cluster_id: Option<String>,
    },
    /// Cairn-managed Claude subscription account. Its CLI home's path is
    /// derived from the account id; its OAuth tokens live here, not there.
    ///
    /// `oauth` is the CLI's `claudeAiOauth` object, held verbatim as JSON plus
    /// Cairn's own rotation fields (see `cairn_models::claude::tokens`). `None`
    /// means the account has not been harvested from its home yet, or has been
    /// signed out. The CLI itself only ever receives the access token, so the
    /// refresh token is spent by Cairn alone.
    #[serde(rename = "claude_profile")]
    ClaudeProfile {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        oauth: Option<String>,
    },
    /// One named custom provider: an HTTP inference service the operator already
    /// runs, reached at an address they supply.
    ///
    /// Everything that decides how to talk to that service is held together
    /// here, for the same reason an Actual target is: a bearer key means nothing
    /// apart from the endpoint it authenticates to, and a protocol family means
    /// nothing apart from the endpoint that speaks it. Splitting any of them out
    /// would let a second provider inherit the first one's answer.
    #[serde(rename = "custom_endpoint", rename_all = "camelCase")]
    CustomEndpoint {
        /// The root of the OpenAI-compatible API surface — the URL `/models`
        /// and `/chat/completions` hang off, canonicalized by
        /// [`ProviderAuth::custom_endpoint`].
        base_url: String,
        /// What this endpoint actually speaks. Explicit and stored, never
        /// inferred from the shape of the URL: an address that answers on the
        /// OpenAI paths is evidence about routing, not about semantics.
        protocol: CustomProtocol,
        /// Optional bearer credential, sent as `Authorization: Bearer`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
        /// Operator-supplied request headers. Treated as secret at rest,
        /// because the reason to add one is almost always to authenticate.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        headers: Vec<CustomHeader>,
        /// How this provider's models become known.
        models: CustomModelPolicy,
        /// Optional endpoint-specific sampling controls, sent on every
        /// chat-completions generation. Every absent field stays absent on the
        /// wire so the server's configured defaults remain authoritative.
        #[serde(default, skip_serializing_if = "CustomSampling::is_empty")]
        sampling: CustomSampling,
        /// Maximum tokens the endpoint may generate for one agent-loop turn.
        #[serde(default = "default_custom_max_output_tokens")]
        max_output_tokens: i64,
        /// The embedding models this endpoint serves beside its chat models,
        /// each with its width. A chat hub serves both on one key, so they are
        /// declared here rather than as a second provider; a declared id serves
        /// `embed` and nothing else. Set through
        /// [`ProviderAuth::with_embedding_models`].
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        embeddings: Vec<CustomEmbeddingModel>,
        /// What an OpenAI Images endpoint can draw, as its operator or host
        /// declared it. Absent means [`ImageCapabilities::openai_images_minimal`].
        /// Set through [`ProviderAuth::with_image_capabilities`]. Boxed: it is
        /// the largest field any account carries, and most accounts have none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        images: Option<Box<ImageCapabilities>>,
    },
}

/// One embedding model a custom provider declares, and the width its vectors
/// come back at.
///
/// The width is the operator's word, because no OpenAI-compatible listing
/// reports it. Shorter widths are declared only by a host that knows its
/// server honours `dimensions`: many ignore the field and answer at full
/// width, which the facet then refuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomEmbeddingModel {
    pub id: String,
    pub dims: u32,
    #[serde(default)]
    pub widths: cairn_models_types::embedding::EmbeddingWidths,
}

const REDACTED: &str = "<redacted>";

impl std::fmt::Debug for ProviderAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = |secret: &Option<String>| secret.as_ref().map(|_| REDACTED);
        match self {
            ProviderAuth::ApiKey { .. } => {
                f.debug_struct("ApiKey").field("value", &REDACTED).finish()
            }
            ProviderAuth::OAuthToken { .. } => f
                .debug_struct("OAuthToken")
                .field("value", &REDACTED)
                .finish(),
            ProviderAuth::BaseUrl { url } => f.debug_struct("BaseUrl").field("url", url).finish(),
            ProviderAuth::ActualTarget {
                kind,
                base_url,
                api_key,
                cluster_id,
            } => f
                .debug_struct("ActualTarget")
                .field("kind", kind)
                .field("base_url", base_url)
                .field("api_key", &redacted(api_key))
                .field("cluster_id", cluster_id)
                .finish(),
            ProviderAuth::ClaudeProfile { oauth } => f
                .debug_struct("ClaudeProfile")
                .field("oauth", &redacted(oauth))
                .finish(),
            ProviderAuth::CustomEndpoint {
                base_url,
                protocol,
                api_key,
                headers,
                models,
                sampling,
                max_output_tokens,
                embeddings,
                images,
            } => f
                .debug_struct("CustomEndpoint")
                .field("base_url", base_url)
                .field("protocol", protocol)
                .field("api_key", &redacted(api_key))
                .field(
                    "header_names",
                    &headers.iter().map(|h| h.name.as_str()).collect::<Vec<_>>(),
                )
                .field("models", models)
                .field("sampling", sampling)
                .field("max_output_tokens", max_output_tokens)
                .field("embeddings", embeddings)
                .field("images", images)
                .finish(),
        }
    }
}

pub const DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS: i64 = 32_768;

fn default_custom_max_output_tokens() -> i64 {
    DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS
}

/// The wire protocol a custom endpoint speaks.
///
/// The field exists so the protocol stays a *statement* the operator made
/// rather than an assumption Cairn drew. An endpoint routing other paths as
/// well is evidence those routes exist, not that their semantics have been
/// verified, so a variant is earned by a verified adapter and by fixtures, not
/// by an address that answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CustomProtocol {
    /// OpenAI Chat Completions: agent turns and responses, and decisions
    /// measured from logprobs when the server returns them.
    #[serde(rename = "openai-chat")]
    OpenAiChatCompletions,
    /// OpenAI's Decisions wire (`POST {base}/decisions`): OpenAI's Decisions
    /// API, and the Vercel AI Gateway's decisions route, which documents the
    /// same shape (whether the other vendors' models it serves there answer
    /// it faithfully is unverified). TypeSafe speaks System One instead, a
    /// different wire. It answers decisions and nothing else, so it never
    /// serves an agent turn or a response.
    #[serde(rename = "openai-decisions")]
    Decisions,
    /// The OpenAI Images API (`{base}/images/generations` and
    /// `{base}/images/edits`): OpenAI itself, or an OpenAI-compatible image
    /// server. It draws images and nothing else.
    #[serde(rename = "openai-images")]
    OpenAiImages,
    /// OpenAI-compatible speech (`POST {base}/audio/speech`): OpenAI itself,
    /// or a local TTS server such as Kokoro-FastAPI. It speaks and does
    /// nothing else. Its speech models are discovered from `{base}/models`,
    /// and an explicit list adds ids discovery does not recognise.
    #[serde(rename = "openai-speech")]
    OpenAiSpeech,
    /// ElevenLabs' text-to-speech WebSocket, reached from its REST root
    /// (`https://api.elevenlabs.io/v1` or a regional host). It speaks and does
    /// nothing else. Its speech models are discovered from `{base}/models`.
    #[serde(rename = "elevenlabs")]
    ElevenLabs,
}

impl CustomProtocol {
    /// Every protocol, in the order a refusal lists them.
    pub const ALL: [CustomProtocol; 5] = [
        Self::OpenAiChatCompletions,
        Self::Decisions,
        Self::OpenAiImages,
        Self::OpenAiSpeech,
        Self::ElevenLabs,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CustomProtocol::OpenAiChatCompletions => "openai-chat",
            CustomProtocol::Decisions => "openai-decisions",
            CustomProtocol::OpenAiImages => "openai-images",
            CustomProtocol::OpenAiSpeech => "openai-speech",
            CustomProtocol::ElevenLabs => "elevenlabs",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|protocol| protocol.as_str() == value)
    }

    /// Whether this protocol only speaks.
    pub fn is_speech(self) -> bool {
        matches!(self, Self::OpenAiSpeech | Self::ElevenLabs)
    }
}

/// One operator-supplied request header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomHeader {
    pub name: String,
    pub value: String,
}

/// Where a custom provider's model inventory comes from.
///
/// Both shapes preserve model ids byte for byte. A served id is the string the
/// endpoint will accept back on the wire; normalizing it — casing it, stripping
/// a prefix, humanizing a separator — would produce a name that looks tidier and
/// no longer resolves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum CustomModelPolicy {
    /// Ask the endpoint, via `GET {base_url}/models`.
    Discover,
    /// Take the operator's word for it. For a server that serves inference but
    /// does not enumerate, which is a normal shape rather than a broken one.
    Explicit { ids: Vec<String> },
}

impl CustomModelPolicy {
    pub fn mode_str(&self) -> &'static str {
        match self {
            CustomModelPolicy::Discover => "discover",
            CustomModelPolicy::Explicit { .. } => "explicit",
        }
    }
}

/// Headers Cairn sets itself on every request, which an operator override would
/// silently break rather than customize.
const RESERVED_CUSTOM_HEADERS: &[&str] =
    &["authorization", "content-type", "content-length", "host"];

impl ProviderAuth {
    /// Parse and canonicalize provider connection metadata at its input boundary.
    pub fn base_url(value: &str) -> Result<Self, String> {
        let value = value.trim();
        let parsed = url::Url::parse(value)
            .map_err(|error| format!("invalid base URL '{value}': {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(format!(
                "invalid base URL '{value}': expected http or https scheme"
            ));
        }
        Ok(Self::BaseUrl {
            url: parsed.to_string().trim_end_matches('/').to_string(),
        })
    }

    /// Build and canonicalize an Actual target at its input boundary.
    ///
    /// A relay target without a credential is rejected here rather than at the
    /// first inference request: the relay answers every unauthenticated call
    /// with the same opaque `missing_or_malformed_credential`, so a target saved
    /// without a key would only ever fail later with a message that does not say
    /// which of its fields is missing.
    pub fn actual_target(
        kind: ActualTargetKind,
        base_url: &str,
        api_key: Option<&str>,
        cluster_id: Option<&str>,
    ) -> Result<Self, String> {
        let ProviderAuth::BaseUrl { url } = Self::base_url(base_url)? else {
            unreachable!("base_url constructs BaseUrl");
        };
        let api_key = api_key.map(str::trim).filter(|value| !value.is_empty());
        let cluster_id = cluster_id.map(str::trim).filter(|value| !value.is_empty());
        match kind {
            ActualTargetKind::Relay if api_key.is_none() => {
                return Err(
                    "an Actual Private Relay target needs an ac_ inference credential".to_string(),
                )
            }
            // Loopback takes no per-request auth, so a key here would be stored
            // and never sent — a credential the user believes is in use.
            ActualTargetKind::Local if api_key.is_some() => {
                return Err(
                    "a local Actual instance takes no per-request credential; leave the key empty"
                        .to_string(),
                )
            }
            _ => {}
        }
        if kind == ActualTargetKind::Local && cluster_id.is_some() {
            return Err(
                "cluster pinning applies to Private Relay targets, not a local instance"
                    .to_string(),
            );
        }
        Ok(Self::ActualTarget {
            kind,
            base_url: url,
            api_key: api_key.map(str::to_string),
            cluster_id: cluster_id.map(str::to_string),
        })
    }

    /// Build and canonicalize one named custom provider at its input boundary.
    ///
    /// Everything refused here is refused because it would otherwise fail later
    /// with a message that does not name the field responsible. A URL carrying
    /// its own credentials, a query string, or a fragment is ambiguous about
    /// what Cairn is supposed to send; a header Cairn already sets is an
    /// override that would be silently discarded or would break the request
    /// body; an empty explicit model list is a provider that can never serve
    /// anything.
    ///
    /// The one thing this *infers* is the API root. An address with no path is
    /// taken to mean the default OpenAI layout and resolves to `/v1`, because
    /// that is what `http://host:8000` universally means for a server of this
    /// kind; any other path is taken literally. The resolved URL is what gets
    /// stored and what diagnostics show, so the inference is visible rather than
    /// re-derived at request time.
    pub fn custom_endpoint(
        base_url: &str,
        protocol: CustomProtocol,
        api_key: Option<&str>,
        headers: &[(String, String)],
        models: CustomModelPolicy,
        sampling: CustomSampling,
        max_output_tokens: i64,
    ) -> Result<Self, String> {
        if max_output_tokens <= 0 {
            return Err("maximum output tokens must be greater than zero".to_string());
        }
        if protocol == CustomProtocol::Decisions && matches!(models, CustomModelPolicy::Discover) {
            // A Decisions deployment's `/models` lists every model it routes,
            // nearly all of which are chat models that cannot answer a
            // decision. Offering them would be a list of refusals.
            return Err(
                "list the decision model ids for a Decisions provider (for example typesafe-ai/jev); its model listing names chat models that cannot answer decisions"
                    .to_string(),
            );
        }
        if protocol == CustomProtocol::OpenAiImages && matches!(models, CustomModelPolicy::Discover)
        {
            // An Images server's `/models`, where it has one, does not say
            // which models draw (OpenAI's lists every model it serves).
            return Err(
                "list the image model ids for an OpenAI Images provider (for example gpt-image-2); its model listing does not say which models draw images"
                    .to_string(),
            );
        }
        let raw = base_url.trim();
        let parsed = url::Url::parse(raw)
            .map_err(|error| format!("invalid endpoint URL '{raw}': {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(format!(
                "invalid endpoint URL '{raw}': expected an http or https address"
            ));
        }
        if parsed.host_str().is_none_or(str::is_empty) {
            return Err(format!("invalid endpoint URL '{raw}': no host"));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(format!(
                "invalid endpoint URL '{raw}': put credentials in the API key field, not the URL"
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(format!(
                "invalid endpoint URL '{raw}': give the API root, without a query string or fragment"
            ));
        }
        let path = parsed.path().trim_end_matches('/');
        let resolved = format!(
            "{}://{}{}",
            parsed.scheme(),
            parsed
                .host_str()
                .map(|host| match parsed.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host.to_string(),
                })
                .unwrap_or_default(),
            if path.is_empty() { "/v1" } else { path },
        );

        let api_key = api_key
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let mut seen: Vec<String> = Vec::new();
        let mut resolved_headers = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let name = name.trim();
            let value = value.trim();
            if name.is_empty() {
                return Err("a custom header needs a name".to_string());
            }
            let lowered = name.to_ascii_lowercase();
            if RESERVED_CUSTOM_HEADERS.contains(&lowered.as_str()) {
                return Err(format!(
                    "'{name}' is set by Cairn on every request and cannot be overridden\
                     {}",
                    if lowered == "authorization" {
                        "; use the API key field instead"
                    } else {
                        ""
                    }
                ));
            }
            if seen.contains(&lowered) {
                return Err(format!("header '{name}' is set twice"));
            }
            http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| format!("'{name}' is not a valid header name"))?;
            http::header::HeaderValue::from_str(value).map_err(|_| {
                format!("the value for header '{name}' is not a valid header value")
            })?;
            seen.push(lowered);
            resolved_headers.push(CustomHeader {
                name: name.to_string(),
                value: value.to_string(),
            });
        }

        let models = match models {
            CustomModelPolicy::Discover => CustomModelPolicy::Discover,
            CustomModelPolicy::Explicit { ids } => {
                let mut resolved_ids: Vec<String> = Vec::with_capacity(ids.len());
                for id in ids {
                    // Only surrounding whitespace is stripped. The id itself is
                    // what goes back on the wire as `model`, so it is preserved
                    // exactly as the operator gave it.
                    let id = id.trim().to_string();
                    if id.is_empty() {
                        continue;
                    }
                    if !resolved_ids.contains(&id) {
                        resolved_ids.push(id);
                    }
                }
                if resolved_ids.is_empty() {
                    return Err(
                        "list at least one model id, or let Cairn discover them from the endpoint"
                            .to_string(),
                    );
                }
                CustomModelPolicy::Explicit { ids: resolved_ids }
            }
        };

        Ok(Self::CustomEndpoint {
            base_url: resolved,
            protocol,
            api_key,
            headers: resolved_headers,
            models,
            sampling,
            max_output_tokens,
            embeddings: Vec::new(),
            images: None,
        })
    }

    /// This custom endpoint with `models` as the embedding models it serves,
    /// replacing any it declared. Anything but a custom endpoint is returned
    /// unchanged.
    ///
    /// Refused: an id that is blank or declared twice, a zero width, a width
    /// set that contradicts the native width, and any declaration on a
    /// Decisions provider, which answers decisions and nothing else.
    pub fn with_embedding_models(
        mut self,
        models: Vec<CustomEmbeddingModel>,
    ) -> Result<Self, String> {
        let mut resolved: Vec<CustomEmbeddingModel> = Vec::with_capacity(models.len());
        for model in models {
            let id = model.id.trim().to_string();
            if id.is_empty() {
                return Err("an embedding model needs an id".to_string());
            }
            if resolved.iter().any(|known| known.id == id) {
                return Err(format!("embedding model '{id}' is declared twice"));
            }
            if model.dims == 0 {
                return Err(format!(
                    "embedding model '{id}' needs its width (the length of the vectors it returns)"
                ));
            }
            cairn_models_types::embedding::EmbeddingCapabilities {
                native_dims: Some(model.dims),
                widths: model.widths.clone(),
                purpose: cairn_models_types::embedding::PurposeHandling::Ignored,
                limits: Default::default(),
            }
            .validate()
            .map_err(|error| format!("embedding model '{id}': {error}"))?;
            resolved.push(CustomEmbeddingModel { id, ..model });
        }
        if let Self::CustomEndpoint {
            protocol,
            embeddings,
            ..
        } = &mut self
        {
            if *protocol != CustomProtocol::OpenAiChatCompletions && !resolved.is_empty() {
                return Err(format!(
                    "embedding models are declared on an OpenAI chat provider, not an {} one",
                    protocol.as_str()
                ));
            }
            *embeddings = resolved;
        }
        Ok(self)
    }

    /// This custom endpoint, declaring what its images can do. A host that
    /// knows its image server (OpenAI's GPT Image models, a local front)
    /// declares it here; anything but a custom endpoint is returned unchanged.
    pub fn with_image_capabilities(mut self, capabilities: ImageCapabilities) -> Self {
        if let Self::CustomEndpoint { images, .. } = &mut self {
            *images = Some(Box::new(capabilities));
        }
        self
    }

    /// What an OpenAI Images endpoint can draw: its declaration, or the
    /// minimal default when it made none. `None` for anything that is not an
    /// OpenAI Images endpoint.
    pub fn image_capabilities(&self) -> Option<ImageCapabilities> {
        match self {
            Self::CustomEndpoint {
                protocol: CustomProtocol::OpenAiImages,
                images,
                ..
            } => Some(
                images
                    .as_deref()
                    .cloned()
                    .unwrap_or_else(ImageCapabilities::openai_images_minimal),
            ),
            _ => None,
        }
    }

    /// Get the raw credential value, if any.
    fn credential_value(&self) -> Option<&str> {
        match self {
            ProviderAuth::ApiKey { value } | ProviderAuth::OAuthToken { value } => Some(value),
            // The relay key is a secret and must be reachable here, or every
            // caller that redacts by asking for the credential would leak it.
            ProviderAuth::ActualTarget { api_key, .. } => api_key.as_deref(),
            ProviderAuth::CustomEndpoint { api_key, .. } => api_key.as_deref(),
            ProviderAuth::BaseUrl { .. } | ProviderAuth::ClaudeProfile { .. } => None,
        }
    }

    /// Short description of auth type for UI display.
    fn auth_type_label(&self) -> &'static str {
        match self {
            ProviderAuth::ApiKey { .. } => "api_key",
            ProviderAuth::OAuthToken { .. } => "oauth_token",
            ProviderAuth::BaseUrl { .. } => "base_url",
            ProviderAuth::ActualTarget { .. } => "actual_target",
            ProviderAuth::ClaudeProfile { .. } => "claude_profile",
            ProviderAuth::CustomEndpoint { .. } => "custom_endpoint",
        }
    }
}

/// Provider-reported credit exhaustion for one account.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccountDepletion {
    /// Unix timestamp when the provider reported the depletion.
    pub since: i64,
    /// Non-secret provider detail suitable for operator-facing diagnostics.
    pub detail: String,
}

/// Persisted availability and subscription usage for one provider account.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccountHealth {
    #[serde(default)]
    pub windows: Vec<cairn_models_types::usage::ProviderUsageWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_until: Option<i64>,
    /// Whether `blocked_until` is a reset the provider declared, rather than
    /// Cairn's own re-probe guess. Absent reads as a guess, so a block recorded
    /// before provenance existed never acquires a timer anyone can wait on.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub blocked_until_declared: bool,
    /// Credit exhaustion cannot be inferred to have healed with time. Only an
    /// explicit operator retry clears this marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depleted: Option<AccountDepletion>,
    pub captured_at: i64,
}

/// Until when a blocked account cannot serve, and on whose word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountBlock {
    /// A reset the provider declared.
    Declared(i64),
    /// Cairn's bounded re-probe after a refusal that declared no reset.
    Reprobe(i64),
}

impl AccountBlock {
    pub fn until(self) -> i64 {
        match self {
            AccountBlock::Declared(at) | AccountBlock::Reprobe(at) => at,
        }
    }

    pub fn is_declared(self) -> bool {
        matches!(self, AccountBlock::Declared(_))
    }
}

/// When the accounts a routing policy may select can next serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutedAvailability {
    /// This account can serve now.
    Now(String),
    /// None can serve now; this account is the first whose declared resets
    /// have all passed at this instant.
    At(i64, String),
    /// Every candidate is blocked with no declared end: credits, a re-probe
    /// guess, or an exhausted window with no reset.
    Unknown,
    /// The policy selects no routed account at all.
    NoCandidates,
}

impl ProviderAccountHealth {
    /// When this account can serve `requested_model` again, judged only on
    /// what the provider declared: `Some(now)` when nothing blocks it, the
    /// latest declared end of everything that does, or `None` when something
    /// blocking it has no declared end.
    fn available_at(&self, requested_model: Option<&str>, now: i64) -> Option<i64> {
        if self.depleted.is_some() {
            return None;
        }
        let mut at = now;
        if let Some(until) = self.blocked_until.filter(|until| *until > now) {
            if !self.blocked_until_declared {
                return None;
            }
            at = at.max(until);
        }
        for window in self
            .windows
            .iter()
            .filter(|window| window.is_live_at(self.captured_at, now))
            .filter(|window| window.matches_model(requested_model))
            .filter(|window| window.remaining_percent <= 0.0)
        {
            at = at.max(window.resets_at?);
        }
        Some(at)
    }

    /// Whether the provider has told Cairn this account cannot run yet.
    fn is_blocked(&self, now: i64) -> bool {
        self.depleted.is_some() || self.blocked_until.is_some_and(|until| until > now)
    }

    /// The tightest remaining headroom this snapshot still speaks for at `now`,
    /// or `None` when it says nothing measurable.
    ///
    /// A window whose reset time has passed has rolled over since it was
    /// measured, so its number is history rather than evidence of exhaustion.
    /// Without that, one measured-empty five-hour window would strand an
    /// account out of rotation until somebody re-probed it by hand — snapshots
    /// arrive on sign-in, on a manual refresh, and from live session events,
    /// none of which fire for an account nothing is routing sessions to.
    ///
    /// Liveness and model scope are asked of the window itself, so the
    /// pooled-capacity projection the settings surface renders and the
    /// eligibility test routing applies are one implementation rather than two
    /// that can drift apart.
    fn live_headroom(&self, requested_model: Option<&str>, now: i64) -> Option<f64> {
        let tightest = self
            .windows
            .iter()
            .filter(|window| window.is_live_at(self.captured_at, now))
            .filter(|window| window.matches_model(requested_model))
            .map(|window| window.remaining_percent)
            .fold(f64::INFINITY, f64::min);
        tightest.is_finite().then_some(tightest)
    }
}

/// A named credential for an API provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccount {
    pub id: String,
    pub label: String,
    pub api_provider: ApiProvider,
    pub source: AccountSource,
    pub auth: ProviderAuth,
    /// None = shared account; Some(project_id) = private to that project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Position = priority (lower = higher priority)
    pub sort_order: i32,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    /// Which of its candidate plans an account whose provider cannot say is
    /// on (ChatGPT `pro`, a Team seat), as the operator chose it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_choice: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<ProviderAccountHealth>,
}

impl ProviderAccount {
    fn is_available_for_model(&self, requested_model: Option<&str>, now: i64) -> bool {
        self.health.as_ref().is_none_or(|health| {
            !health.is_blocked(now)
                && health
                    .live_headroom(requested_model, now)
                    .is_none_or(|headroom| headroom > 0.0)
        })
    }

    /// Which agent backends can consume this credential.
    ///
    /// Owned rather than `&'static str` because a custom provider's backend key
    /// is derived from the account itself: the account IS the provider, so it
    /// serves exactly one backend and no other account can serve that one.
    pub fn compatible_backends(&self) -> Vec<String> {
        let keys: Vec<&str> = match (&self.api_provider, &self.auth) {
            (ApiProvider::Custom, ProviderAuth::CustomEndpoint { .. }) => {
                return vec![cairn_models_types::catalog::custom_key(&self.id)]
            }
            // API keys work with both CLI and Native
            (ApiProvider::Anthropic, ProviderAuth::ApiKey { .. }) => vec!["claude", "native"],
            (ApiProvider::OpenAI, ProviderAuth::ApiKey { .. }) => vec!["codex", "native"],
            (ApiProvider::Google, ProviderAuth::ApiKey { .. }) => vec!["native"],
            (ApiProvider::OpenRouter, ProviderAuth::ApiKey { .. }) => vec!["openrouter"],
            (ApiProvider::OpenCode, ProviderAuth::ApiKey { .. }) => vec!["opencode-go"],
            (ApiProvider::Ollama, ProviderAuth::BaseUrl { .. }) => vec!["ollama"],
            (ApiProvider::Actual, ProviderAuth::ActualTarget { .. }) => vec!["actual"],
            (ApiProvider::GitHub, ProviderAuth::ApiKey { .. }) => vec![],
            (ApiProvider::Bginf, _) => vec![],
            // OAuth is CLI-specific
            (ApiProvider::OpenAI, ProviderAuth::OAuthToken { .. }) => vec!["codex"],
            (ApiProvider::Google, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::OpenRouter, ProviderAuth::OAuthToken { .. }) => vec![],
            // OpenCode Zen issues API keys from its console; there is no OAuth.
            (ApiProvider::OpenCode, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::GitHub, ProviderAuth::OAuthToken { .. }) => vec![],
            // A Cairn-managed profile is the only subscription credential the
            // Claude backend accepts. A pasted `claude setup-token` value
            // (`OAuthToken`) authenticates a session Cairn can neither route by
            // remaining usage nor sign out of, so it is not a usable Anthropic
            // credential; stored ones are retired when the store loads.
            (ApiProvider::Anthropic, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::Anthropic, ProviderAuth::ClaudeProfile { .. }) => vec!["claude"],
            _ => vec![],
        };
        keys.into_iter().map(str::to_string).collect()
    }

    /// Whether this account's credential can serve `backend`.
    pub fn serves_backend(&self, backend: &str) -> bool {
        self.compatible_backends().iter().any(|key| key == backend)
    }
}

use cairn_domain::GitIdentity;

/// An Anthropic login that the profiles-only model no longer accepts, kept
/// until the user has been told why it disappeared.
///
/// Dropping a credential the user deliberately added is not something to do
/// silently: the account simply vanishing from settings reads as a bug, and the
/// only recovery — signing in again as a managed profile — is not guessable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetiredLogin {
    pub label: String,
    /// The auth shape it used to have (`local_cli` or `oauth_token`).
    pub auth_type: String,
    pub retired_at: i64,
}

/// Headroom assumed for a managed account Cairn has no usage snapshot for.
/// Full, so an account that just signed in competes normally instead of
/// being sorted below every account with a measured window.
const UNKNOWN_HEADROOM: f64 = 100.0;
/// Lowest-priority score for an account whose measured windows have rolled over.
/// It remains eligible as a bounded re-probe, but any current positive capacity wins.
const STALE_REPROBE_HEADROOM: f64 = 0.0;
/// A provider whose subscription logins Cairn routes sessions across by
/// remaining usage.
///
/// Subscriptions are inventory — interchangeable token faucets picked by
/// headroom — so what identifies one here is the provider plus the credential
/// shape that carries a metered window. An API key bills per token and has no
/// window to route around, so it is never a routing candidate; it remains the
/// deliberate fallback a caller reaches for when no subscription is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutedProvider {
    Claude,
    Codex,
}

impl RoutedProvider {
    /// The routed provider a backend draws its subscription credential from,
    /// or `None` for a backend Cairn does not hold subscriptions for.
    pub fn for_backend(backend: &str) -> Option<Self> {
        match backend {
            "claude" => Some(Self::Claude),
            "codex" => Some(Self::Codex),
            _ => None,
        }
    }

    pub fn api_provider(self) -> ApiProvider {
        match self {
            Self::Claude => ApiProvider::Anthropic,
            Self::Codex => ApiProvider::OpenAI,
        }
    }

    pub fn backend(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }

    /// Whether this credential is the metered subscription shape for the
    /// provider: a Cairn-managed Claude profile, or a ChatGPT OAuth login whose
    /// `auth.json` Cairn owns and refreshes.
    fn routes(self, auth: &ProviderAuth) -> bool {
        match self {
            Self::Claude => matches!(auth, ProviderAuth::ClaudeProfile { .. }),
            Self::Codex => matches!(auth, ProviderAuth::OAuthToken { .. }),
        }
    }
}

/// A completed OAuth login, as [`CredentialStore::upsert_oauth_account`]
/// stores it: the credential value and what the caller read out of it.
#[derive(Debug, Clone)]
pub struct OAuthLogin {
    pub value: String,
    pub email: Option<String>,
    pub plan: Option<String>,
}

/// The full credential store — multi-account model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialStore {
    pub user_id: String,
    /// Sorted by (api_provider, sort_order)
    pub accounts: Vec<ProviderAccount>,
    /// Sorted by sort_order
    pub git_identities: Vec<GitIdentity>,
    /// Per-project account overrides, keyed by project ID.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub project_overrides: std::collections::HashMap<String, AccountOverrides>,
    /// Anthropic logins dropped by the profiles-only migration, pending the
    /// user acknowledging them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired_logins: Vec<RetiredLogin>,
    /// The configuration root this store was loaded from, and therefore the one
    /// its managed Claude profiles live under. Not content, so not persisted —
    /// but carried, because a store that cannot say where it lives cannot
    /// resolve a profile path, and inferring one is how sign-in and session
    /// resolution came to disagree.
    #[serde(skip)]
    pub config_dir: std::path::PathBuf,
}

/// Whether `account` is a configured row in `project_id`'s scope holding a
/// login `same_login` recognizes.
fn holds_oauth_login(
    account: &ProviderAccount,
    api_provider: ApiProvider,
    project_id: Option<&str>,
    same_login: &dyn Fn(&str) -> bool,
) -> bool {
    account.api_provider == api_provider
        && account.source == AccountSource::Configured
        && account.project_id.as_deref() == project_id
        && matches!(&account.auth, ProviderAuth::OAuthToken { value } if same_login(value))
}

impl CredentialStore {
    /// Explain routing eligibility without exposing any credential material.
    pub fn routed_account_diagnostics(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        excluded_id: Option<&str>,
        requested_model: Option<&str>,
        now: i64,
    ) -> Vec<RoutedAccountDiagnostic> {
        let backend = provider.backend();
        self.accounts
            .iter()
            .filter(|account| account.api_provider == provider.api_provider())
            .map(|account| {
                let in_scope = account.project_id.is_none()
                    || project_id.is_some_and(|id| account.project_id.as_deref() == Some(id));
                let rejection = if !in_scope {
                    Some("project scope does not include this project".to_string())
                } else if override_id.is_some_and(|id| id != account.id) {
                    Some("a different account is explicitly selected for this project".to_string())
                } else if excluded_id == Some(account.id.as_str()) {
                    Some("account was excluded from this selection attempt".to_string())
                } else if override_id.is_none() && !provider.routes(&account.auth) {
                    Some(format!(
                        "auth type '{}' is not a routable subscription profile",
                        account.auth.auth_type_label()
                    ))
                } else if !account.serves_backend(backend) {
                    Some(format!(
                        "account is not compatible with backend '{backend}'"
                    ))
                } else if let Some(depletion) = account
                    .health
                    .as_ref()
                    .and_then(|health| health.depleted.as_ref())
                {
                    Some(format!(
                        "usage credits are exhausted since {}; Cairn cannot detect a top-up automatically ({}). Retry this account after adding credits",
                        depletion.since, depletion.detail
                    ))
                } else if account
                    .health
                    .as_ref()
                    .is_some_and(|health| health.is_blocked(now))
                {
                    Some(format!(
                        "provider block remains active until {}",
                        account
                            .health
                            .as_ref()
                            .and_then(|health| health.blocked_until)
                            .unwrap_or(now)
                    ))
                } else if account
                    .health
                    .as_ref()
                    .and_then(|health| health.live_headroom(requested_model, now))
                    .is_some_and(|headroom| headroom <= 0.0)
                {
                    Some(format!(
                        "no current headroom for {}",
                        requested_model.unwrap_or("the requested model")
                    ))
                } else {
                    None
                };
                let live_windows = account
                    .health
                    .as_ref()
                    .map(|health| {
                        health
                            .windows
                            .iter()
                            .filter(|window| window.resets_at.is_none_or(|reset| reset > now))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                RoutedAccountDiagnostic {
                    account_id: account.id.clone(),
                    auth_type: account.auth.auth_type_label().to_string(),
                    project_id: account.project_id.clone(),
                    compatible_backends: account.compatible_backends(),
                    blocked_until: account
                        .health
                        .as_ref()
                        .and_then(|health| health.blocked_until),
                    live_windows,
                    rejection,
                }
            })
            .collect()
    }

    /// The next free `sort_order` for one provider within one scope.
    fn next_sort_order(&self, api_provider: ApiProvider, project_id: Option<&str>) -> i32 {
        self.accounts
            .iter()
            .filter(|account| {
                account.api_provider == api_provider && account.project_id.as_deref() == project_id
            })
            .map(|account| account.sort_order)
            .max()
            .unwrap_or(-1)
            + 1
    }

    /// Append a configured account, last in its provider's order within its
    /// scope, with a fresh id. The caller persists the store.
    pub fn add_account(
        &mut self,
        api_provider: ApiProvider,
        label: String,
        auth: ProviderAuth,
        project_id: Option<String>,
        now: i64,
    ) -> AccountInfo {
        let account = ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label,
            api_provider,
            source: AccountSource::Configured,
            auth,
            sort_order: self.next_sort_order(api_provider, project_id.as_deref()),
            project_id,
            created_at: now,
            last_used_at: None,
            email: None,
            plan: None,
            plan_choice: None,
            health: None,
        };
        let info = AccountInfo::from(&account);
        self.accounts.push(account);
        info
    }

    /// The configured account in `project_id`'s scope that already holds a
    /// login `same_login` recognizes as the same provider account: the row
    /// [`Self::upsert_oauth_account`] would replace.
    pub fn held_oauth_account(
        &self,
        api_provider: ApiProvider,
        project_id: Option<&str>,
        same_login: impl Fn(&str) -> bool,
    ) -> Option<&ProviderAccount> {
        self.accounts
            .iter()
            .find(|account| holds_oauth_login(account, api_provider, project_id, &same_login))
    }

    /// Store a completed OAuth login, replacing in place the configured
    /// account that already holds a login for the same provider account.
    ///
    /// A rotating refresh token is single-use, so one provider account must
    /// have exactly one stored credential: a second row would hold a token the
    /// provider has already retired. `same_login` answers whether an existing
    /// row's stored value belongs to the same provider account as `login`;
    /// the caller decides, because only it can read the credential format
    /// (Cairn matches Codex logins on their ChatGPT account id, never on the
    /// bytes, which every refresh rewrites). The label becomes the login's
    /// email when it has one. The caller persists the store.
    pub fn upsert_oauth_account(
        &mut self,
        api_provider: ApiProvider,
        label: String,
        login: OAuthLogin,
        project_id: Option<String>,
        now: i64,
        same_login: impl Fn(&str) -> bool,
    ) -> AccountInfo {
        let label = login.email.clone().unwrap_or(label);
        let existing = self.accounts.iter_mut().find(|account| {
            holds_oauth_login(account, api_provider, project_id.as_deref(), &same_login)
        });
        if let Some(account) = existing {
            account.label = label;
            account.auth = ProviderAuth::OAuthToken { value: login.value };
            account.email = login.email;
            account.plan = login.plan;
            account.last_used_at = Some(now);
            return AccountInfo::from(&*account);
        }
        let account = ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label,
            api_provider,
            source: AccountSource::Configured,
            auth: ProviderAuth::OAuthToken { value: login.value },
            sort_order: self.next_sort_order(api_provider, project_id.as_deref()),
            project_id,
            created_at: now,
            last_used_at: Some(now),
            email: login.email,
            plan: login.plan,
            plan_choice: None,
            health: None,
        };
        let info = AccountInfo::from(&account);
        self.accounts.push(account);
        info
    }

    /// Record what the provider reported after a sign-in into an existing
    /// account. The email, when known, becomes the label. Health is cleared:
    /// the sign-in wrote a new credential generation, and usage measured on
    /// the previous one no longer describes it. The caller persists the store.
    pub fn set_account_metadata(
        &mut self,
        id: &str,
        email: Option<String>,
        plan: Option<String>,
    ) -> Result<AccountInfo, String> {
        let account = self
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        account.email = email.clone();
        account.plan = plan;
        account.health = None;
        if let Some(email) = email {
            account.label = email;
        }
        Ok(AccountInfo::from(&*account))
    }

    /// Remove an account, and every project override that selected it.
    /// Answers the removed row. The caller persists the store, and releases
    /// whatever the account held outside it first (a managed Claude home).
    pub fn remove_account(&mut self, id: &str) -> Result<ProviderAccount, String> {
        let index = self
            .accounts
            .iter()
            .position(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        let removed = self.accounts.remove(index);
        for overrides in self.project_overrides.values_mut() {
            if overrides.anthropic_account_id.as_deref() == Some(id) {
                overrides.anthropic_account_id = None;
            }
            if overrides.openai_account_id.as_deref() == Some(id) {
                overrides.openai_account_id = None;
            }
            if overrides.github_account_id.as_deref() == Some(id) {
                overrides.github_account_id = None;
            }
        }
        Ok(removed)
    }

    pub fn has_routed_accounts(&self, provider: RoutedProvider, project_id: Option<&str>) -> bool {
        self.accounts_for_provider(provider.api_provider(), project_id)
            .into_iter()
            .any(|account| provider.routes(&account.auth))
    }

    /// Get accounts for a specific provider, sorted by priority.
    pub fn accounts_for_provider(
        &self,
        provider: ApiProvider,
        project_id: Option<&str>,
    ) -> Vec<&ProviderAccount> {
        let mut private_accounts: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| {
                a.api_provider == provider
                    && project_id.is_some()
                    && a.project_id.as_deref() == project_id
            })
            .collect();
        private_accounts.sort_by_key(|a| a.sort_order);

        let mut shared_accounts: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| a.api_provider == provider && a.project_id.is_none())
            .collect();
        shared_accounts.sort_by_key(|a| a.sort_order);

        let mut accounts = private_accounts;
        accounts.extend(shared_accounts);
        accounts
    }

    /// The accounts whose capacity pools together for one backend.
    ///
    /// For a routed provider the pool is its subscription credentials only: an
    /// API key bills per token and reports no metered window, so it is a
    /// credential without capacity rather than a pool member whose capacity is
    /// unknown. Every other backend pools whatever accounts serve it, in the
    /// order settings lists them, which is also the order routing falls back
    /// through.
    pub fn pool_roster(&self, backend: &str, project_id: Option<&str>) -> Vec<&ProviderAccount> {
        if let Some(provider) = RoutedProvider::for_backend(backend) {
            return self
                .accounts_for_provider(provider.api_provider(), project_id)
                .into_iter()
                .filter(|account| provider.routes(&account.auth) && account.serves_backend(backend))
                .collect();
        }
        let mut private: Vec<_> = self
            .accounts
            .iter()
            .filter(|account| {
                project_id.is_some()
                    && account.project_id.as_deref() == project_id
                    && account.serves_backend(backend)
            })
            .collect();
        private.sort_by_key(|account| account.sort_order);
        let mut shared: Vec<_> = self
            .accounts
            .iter()
            .filter(|account| account.project_id.is_none() && account.serves_backend(backend))
            .collect();
        shared.sort_by_key(|account| account.sort_order);
        private.extend(shared);
        private
    }

    /// Find the highest-priority account for a provider that's compatible with a backend.
    fn best_account_for(
        &self,
        provider: ApiProvider,
        backend: &str,
        override_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Option<&ProviderAccount> {
        let accounts = self.accounts_for_provider(provider, project_id);

        // If there's an explicit override, use only that account — no fallback
        if let Some(id) = override_id {
            return accounts
                .iter()
                .find(|a| a.id == id && a.serves_backend(backend))
                .copied();
        }

        // No override — first compatible account wins
        accounts.into_iter().find(|a| a.serves_backend(backend))
    }

    /// Whether a session's pinned subscription account can still run.
    ///
    /// An account with no snapshot reads as available: health is captured after
    /// the fact, so its absence is ignorance, not exhaustion.
    pub fn routed_account_is_available(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        account_id: &str,
        requested_model: Option<&str>,
        now: i64,
    ) -> bool {
        self.accounts_for_provider(provider.api_provider(), project_id)
            .into_iter()
            .any(|account| {
                account.id == account_id
                    && provider.routes(&account.auth)
                    && account.serves_backend(provider.backend())
                    && account.is_available_for_model(requested_model, now)
            })
    }

    /// Select an available subscription account by tightest-window headroom.
    /// Assignments since each account's snapshot break equal-headroom bursts.
    ///
    /// An account Cairn has no usage snapshot for is a candidate at full
    /// headroom rather than a skipped one. Snapshots only ever arrive after a
    /// probe or a rate-limit event, so demanding one would make a login that
    /// just completed permanently unselectable and push every session onto
    /// whatever else happened to resolve.
    // These are orthogonal routing facts, not one reusable domain object: grouping
    // them solely to shorten this boundary would make callers construct a bag of
    // unrelated evidence and obscure which constraints participate in selection.
    #[allow(clippy::too_many_arguments)]
    pub fn select_routed_account(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        excluded_id: Option<&str>,
        requested_model: Option<&str>,
        assignments: &[(String, i64)],
        now: i64,
    ) -> Option<&ProviderAccount> {
        let backend = provider.backend();
        let accounts = self.accounts_for_provider(provider.api_provider(), project_id);
        if let Some(id) = override_id {
            return accounts.into_iter().find(|account| {
                account.id == id
                    && excluded_id != Some(account.id.as_str())
                    && account.serves_backend(backend)
                    && account.is_available_for_model(requested_model, now)
            });
        }

        self.routed_candidates(provider, project_id)
            .filter(|account| excluded_id != Some(account.id.as_str()))
            .filter_map(|account| {
                let (headroom, since) = match account.health.as_ref() {
                    Some(health) => {
                        if health.is_blocked(now) {
                            return None;
                        }
                        match health.live_headroom(requested_model, now) {
                            // No live window means the prior measurement has rolled
                            // over. Keep the account eligible for a bounded re-probe,
                            // but rank current measured capacity ahead of stale evidence.
                            None => (STALE_REPROBE_HEADROOM, health.captured_at),
                            Some(tightest) if tightest <= 0.0 => return None,
                            Some(tightest) => (tightest, health.captured_at),
                        }
                    }
                    None => (UNKNOWN_HEADROOM, account.created_at),
                };
                let burst = assignments
                    .iter()
                    .filter(|(id, created_at)| id == &account.id && *created_at >= since)
                    .count();
                Some((account, headroom, burst))
            })
            .max_by(
                |(left, left_headroom, left_burst), (right, right_headroom, right_burst)| {
                    left_headroom
                        .total_cmp(right_headroom)
                        .then_with(|| right_burst.cmp(left_burst))
                        .then_with(|| right.sort_order.cmp(&left.sort_order))
                },
            )
            .map(|(account, _, _)| account)
    }

    /// The subscription accounts Automatic routing chooses among.
    fn routed_candidates<'a>(
        &'a self,
        provider: RoutedProvider,
        project_id: Option<&str>,
    ) -> impl Iterator<Item = &'a ProviderAccount> + 'a {
        let backend = provider.backend();
        self.accounts_for_provider(provider.api_provider(), project_id)
            .into_iter()
            .filter(move |account| {
                provider.routes(&account.auth) && account.serves_backend(backend)
            })
    }

    /// When the accounts a routing policy may select can next serve: the
    /// explicit account alone, or every Automatic candidate. Each account is
    /// available once the latest of its declared blocks has passed; across
    /// accounts the earliest wins. Credit exhaustion and re-probe guesses
    /// declare nothing, so they never supply a time.
    pub fn routed_availability(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        explicit_account: Option<&str>,
        requested_model: Option<&str>,
        now: i64,
    ) -> RoutedAvailability {
        let mut candidates = 0usize;
        let earliest = self
            .routed_candidates(provider, project_id)
            .filter(|account| explicit_account.is_none_or(|id| account.id == id))
            .inspect(|_| candidates += 1)
            .filter_map(|account| {
                let at = match account.health.as_ref() {
                    Some(health) => health.available_at(requested_model, now)?,
                    None => now,
                };
                Some((at, account))
            })
            .min_by(|(left_at, left), (right_at, right)| {
                left_at
                    .cmp(right_at)
                    .then_with(|| left.sort_order.cmp(&right.sort_order))
            });
        match earliest {
            Some((at, account)) if at <= now => RoutedAvailability::Now(account.id.clone()),
            Some((at, account)) => RoutedAvailability::At(at, account.id.clone()),
            None if candidates == 0 => RoutedAvailability::NoCandidates,
            None => RoutedAvailability::Unknown,
        }
    }

    /// The overrides that pin one named account of a backend, or `None` when no
    /// such account serves it. Exact: never falls back to another account.
    pub fn provider_account_overrides(
        &self,
        backend: &str,
        account_id: &str,
    ) -> Option<AccountOverrides> {
        let (provider, overrides) = match backend {
            "claude" => (
                ApiProvider::Anthropic,
                AccountOverrides {
                    anthropic_account_id: Some(account_id.to_string()),
                    ..Default::default()
                },
            ),
            "codex" => (
                ApiProvider::OpenAI,
                AccountOverrides {
                    openai_account_id: Some(account_id.to_string()),
                    ..Default::default()
                },
            ),
            _ => return None,
        };
        self.accounts_for_provider(provider, None)
            .into_iter()
            .find(|account| account.id == account_id && account.serves_backend(backend))?;
        Some(overrides)
    }

    /// Resolve authentication independently of user and Git attribution.
    pub fn resolve_auth(
        &self,
        project_id: Option<&str>,
        overrides: Option<&AccountOverrides>,
    ) -> ProviderCredentials {
        let anthropic_override = overrides.and_then(|o| o.anthropic_account_id.as_deref());
        let openai_override = overrides.and_then(|o| o.openai_account_id.as_deref());
        let github_override = overrides.and_then(|o| o.github_account_id.as_deref());

        // Resolve Claude auth from best Anthropic account
        let claude_auth = self
            .best_account_for(
                ApiProvider::Anthropic,
                "claude",
                anthropic_override,
                project_id,
            )
            .and_then(|a| match &a.auth {
                ProviderAuth::ApiKey { value } => Some(ClaudeAuth::ApiKey(value.clone())),
                ProviderAuth::ClaudeProfile { .. } => Some(ClaudeAuth::ManagedProfile {
                    account_id: a.id.clone(),
                }),
                // A retired setup token is not Claude auth, and neither is a
                // host URL, an Actual target, or a custom endpoint.
                // `best_account_for` already filters on `compatible_backends`,
                // so none reach here.
                ProviderAuth::OAuthToken { .. }
                | ProviderAuth::BaseUrl { .. }
                | ProviderAuth::ActualTarget { .. }
                | ProviderAuth::CustomEndpoint { .. } => None,
            });

        // Resolve Codex auth from best OpenAI account
        let codex_auth = self
            .best_account_for(ApiProvider::OpenAI, "codex", openai_override, project_id)
            .and_then(|a| match &a.auth {
                ProviderAuth::ApiKey { value } => Some(CodexAuth::ApiKey(value.clone())),
                ProviderAuth::OAuthToken { value } => Some(CodexAuth::OAuthToken(value.clone())),
                ProviderAuth::BaseUrl { .. }
                | ProviderAuth::ActualTarget { .. }
                | ProviderAuth::CustomEndpoint { .. }
                | ProviderAuth::ClaudeProfile { .. } => None,
            });

        // Resolve GitHub token
        let github_token = self
            .accounts_for_provider(ApiProvider::GitHub, project_id)
            .into_iter()
            .find(|a| {
                if let Some(id) = github_override {
                    a.id == id
                } else {
                    true
                }
            })
            .and_then(|a| a.auth.credential_value().map(|v| v.to_string()));

        ProviderCredentials {
            claude_auth,
            codex_auth,
            github_token,
        }
    }
}

/// Per-project overrides for account selection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anthropic_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openai_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_identity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_email: Option<String>,
}

/// Frontend-safe account info (no credential values exposed).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    /// Public because callers outside this crate thread the id of a returned
    /// account back into the orchestrator's account APIs. The remaining fields
    /// stay crate-private and reach consumers by serialization.
    pub id: String,
    pub label: String,
    pub api_provider: ApiProvider,
    pub source: AccountSource,
    pub auth_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_url: Option<String>,
    /// Which way an Actual target reaches its cluster (`local` or `relay`).
    /// Absent for every other provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_kind: Option<String>,
    /// The operator-supplied `X-Cluster-ID` pin, if this target has one. Not a
    /// secret — it names a cluster, it does not authenticate to one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster_id: Option<String>,
    /// The wire protocol a custom endpoint speaks. Absent for every other
    /// provider, whose protocol is a property of the shipped adapter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protocol: Option<String>,
    /// How a custom provider's models become known: `discover`, or `explicit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_discovery: Option<String>,
    /// The model ids an explicitly-listed custom provider serves, exactly as
    /// the operator gave them. Empty for a discovering one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    model_ids: Vec<String>,
    /// Maximum tokens generated by one custom-provider agent turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<i64>,
    /// The NAMES of the operator's extra request headers — never their values,
    /// which are secrets. Enough to show that headers are configured and which
    /// ones, without ever rendering what they carry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    header_names: Vec<String>,
    /// Whether a bearer credential is stored. The key itself never leaves the
    /// credential store.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    has_api_key: bool,
    /// Non-secret generation settings for a custom provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sampling: Option<CustomSampling>,
    /// The embedding models a custom provider declares, with their widths.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    embedding_models: Vec<CustomEmbeddingModel>,
    compatible_backends: Vec<String>,
    project_id: Option<String>,
    sort_order: i32,
    last_used_at: Option<i64>,
    email: Option<String>,
    plan: Option<String>,
    /// The subscription plan this account is on and what it costs, with the
    /// operator's choice applied. Absent for an account billed per token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subscription: Option<cairn_models_types::subscription::AccountPlan>,
    health: Option<ProviderAccountHealth>,
}

impl AccountInfo {
    /// The subscription plan this account is on, when it is a subscription.
    pub fn subscription(&self) -> Option<&cairn_models_types::subscription::AccountPlan> {
        self.subscription.as_ref()
    }
}

impl From<&ProviderAccount> for AccountInfo {
    fn from(account: &ProviderAccount) -> Self {
        Self {
            id: account.id.clone(),
            label: account.label.clone(),
            api_provider: account.api_provider,
            source: account.source.clone(),
            auth_type: account.auth.auth_type_label().to_string(),
            base_url: match &account.auth {
                ProviderAuth::BaseUrl { url } => Some(url.clone()),
                ProviderAuth::ActualTarget { base_url, .. } => Some(base_url.clone()),
                ProviderAuth::CustomEndpoint { base_url, .. } => Some(base_url.clone()),
                _ => None,
            },
            protocol: match &account.auth {
                ProviderAuth::CustomEndpoint { protocol, .. } => {
                    Some(protocol.as_str().to_string())
                }
                _ => None,
            },
            model_discovery: match &account.auth {
                ProviderAuth::CustomEndpoint { models, .. } => Some(models.mode_str().to_string()),
                _ => None,
            },
            model_ids: match &account.auth {
                ProviderAuth::CustomEndpoint {
                    models: CustomModelPolicy::Explicit { ids },
                    ..
                } => ids.clone(),
                _ => Vec::new(),
            },
            max_output_tokens: match &account.auth {
                ProviderAuth::CustomEndpoint {
                    max_output_tokens, ..
                } => Some(*max_output_tokens),
                _ => None,
            },
            header_names: match &account.auth {
                ProviderAuth::CustomEndpoint { headers, .. } => {
                    headers.iter().map(|header| header.name.clone()).collect()
                }
                _ => Vec::new(),
            },
            has_api_key: matches!(
                &account.auth,
                ProviderAuth::CustomEndpoint {
                    api_key: Some(_),
                    ..
                }
            ),
            sampling: match &account.auth {
                ProviderAuth::CustomEndpoint { sampling, .. } if !sampling.is_empty() => {
                    Some(sampling.clone())
                }
                _ => None,
            },
            embedding_models: match &account.auth {
                ProviderAuth::CustomEndpoint { embeddings, .. } => embeddings.clone(),
                _ => Vec::new(),
            },
            target_kind: match &account.auth {
                ProviderAuth::ActualTarget { kind, .. } => Some(kind.as_str().to_string()),
                _ => None,
            },
            cluster_id: match &account.auth {
                ProviderAuth::ActualTarget { cluster_id, .. } => cluster_id.clone(),
                _ => None,
            },
            compatible_backends: account
                .compatible_backends()
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            project_id: account.project_id.clone(),
            sort_order: account.sort_order,
            last_used_at: account.last_used_at,
            email: account.email.clone(),
            plan: account.plan.clone(),
            subscription: account.account_plan(),
            health: account.health.clone(),
        }
    }
}

#[cfg(test)]
mod account_operation_tests {
    use super::*;

    fn empty() -> CredentialStore {
        CredentialStore {
            user_id: "user".to_string(),
            accounts: Vec::new(),
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::new(),
        }
    }

    fn login(value: &str, email: &str) -> OAuthLogin {
        OAuthLogin {
            value: value.to_string(),
            email: Some(email.to_string()),
            plan: Some("pro".to_string()),
        }
    }

    /// Logins whose values share a prefix before ':' belong to one account.
    fn same_account(new: &str) -> impl Fn(&str) -> bool + '_ {
        move |existing: &str| existing.split(':').next() == new.split(':').next()
    }

    #[test]
    fn added_accounts_queue_after_their_provider_within_their_scope() {
        let mut store = empty();
        let first = store.add_account(
            ApiProvider::Anthropic,
            "a".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
            10,
        );
        let second = store.add_account(
            ApiProvider::Anthropic,
            "b".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
            11,
        );
        let scoped = store.add_account(
            ApiProvider::Anthropic,
            "c".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            Some("p".into()),
            12,
        );
        let other = store.add_account(
            ApiProvider::OpenAI,
            "d".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
            13,
        );
        let order = |id: &str| {
            store
                .accounts
                .iter()
                .find(|account| account.id == id)
                .map(|account| (account.sort_order, account.created_at))
                .unwrap()
        };
        assert_eq!(order(&first.id), (0, 10));
        assert_eq!(order(&second.id), (1, 11));
        assert_eq!(order(&scoped.id), (0, 12));
        assert_eq!(order(&other.id), (0, 13));
        assert_ne!(first.id, second.id);
        assert!(first.id.starts_with("acc_"));
    }

    #[test]
    fn a_repeat_login_replaces_its_account_in_place_and_another_is_added() {
        let mut store = empty();
        let first = store.upsert_oauth_account(
            ApiProvider::OpenAI,
            "Codex OAuth".into(),
            login("one:r0", "one@example.com"),
            None,
            1,
            same_account("one:r0"),
        );
        assert_eq!(first.label, "one@example.com");
        let again = store.upsert_oauth_account(
            ApiProvider::OpenAI,
            "Codex OAuth".into(),
            login("one:r1", "one@example.com"),
            None,
            2,
            same_account("one:r1"),
        );
        assert_eq!(again.id, first.id);
        assert_eq!(store.accounts.len(), 1);
        assert!(matches!(
            &store.accounts[0].auth,
            ProviderAuth::OAuthToken { value } if value == "one:r1"
        ));
        assert_eq!(store.accounts[0].last_used_at, Some(2));

        let other = store.upsert_oauth_account(
            ApiProvider::OpenAI,
            "Codex OAuth".into(),
            login("two:r0", "two@example.com"),
            None,
            3,
            same_account("two:r0"),
        );
        assert_ne!(other.id, first.id);
        assert_eq!(store.accounts.len(), 2);
        assert_eq!(store.accounts[1].sort_order, 1);
    }

    #[test]
    fn sign_in_metadata_relabels_and_clears_stale_health() {
        let mut store = empty();
        let account = store.add_account(
            ApiProvider::Anthropic,
            "Claude".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
            1,
        );
        store.accounts[0].health = Some(ProviderAccountHealth {
            windows: Vec::new(),
            blocked_until: Some(99),
            blocked_until_declared: false,
            depleted: None,
            captured_at: 1,
        });
        let updated = store
            .set_account_metadata(
                &account.id,
                Some("me@example.com".into()),
                Some("max".into()),
            )
            .unwrap();
        assert_eq!(updated.label, "me@example.com");
        assert!(store.accounts[0].health.is_none());
        assert_eq!(store.accounts[0].plan.as_deref(), Some("max"));
        assert!(store.set_account_metadata("missing", None, None).is_err());
    }

    #[test]
    fn removing_an_account_clears_the_overrides_that_chose_it() {
        let mut store = empty();
        let account = store.add_account(
            ApiProvider::OpenAI,
            "key".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
            1,
        );
        store.project_overrides.insert(
            "p".into(),
            AccountOverrides {
                openai_account_id: Some(account.id.clone()),
                ..Default::default()
            },
        );
        let removed = store.remove_account(&account.id).unwrap();
        assert_eq!(removed.id, account.id);
        assert!(store.accounts.is_empty());
        assert!(store.project_overrides["p"].openai_account_id.is_none());
        assert!(store.remove_account(&account.id).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_auth_value_oauth() {
        let auth = CodexAuth::OAuthToken("oauth-json".to_string());
        assert_eq!(auth.value(), "oauth-json");
    }

    #[test]
    fn codex_auth_value_api_key() {
        let auth = CodexAuth::ApiKey("sk-key".to_string());
        assert_eq!(auth.value(), "sk-key");
    }

    #[test]
    fn codex_auth_serde_oauth_roundtrip() {
        let auth = CodexAuth::OAuthToken("token-data".to_string());
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains(r#""type":"o_auth_token""#), "got: {json}");
        assert!(json.contains(r#""value":"token-data""#), "got: {json}");
        let deserialized: CodexAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.value(), "token-data");
    }

    #[test]
    fn codex_auth_serde_api_key_roundtrip() {
        let auth = CodexAuth::ApiKey("sk-test".to_string());
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains(r#""type":"api_key""#), "got: {json}");
        assert!(json.contains(r#""value":"sk-test""#), "got: {json}");
        let deserialized: CodexAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.value(), "sk-test");
    }

    // === Multi-account provider tests ===

    fn test_account(provider: ApiProvider, auth: ProviderAuth) -> ProviderAccount {
        ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label: "Test".to_string(),
            api_provider: provider,
            source: AccountSource::Configured,
            auth,
            project_id: None,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
            email: None,
            plan: None,
            plan_choice: None,
            health: None,
        }
    }

    fn api_key_auth(value: &str) -> ProviderAuth {
        ProviderAuth::ApiKey {
            value: value.to_string(),
        }
    }

    fn oauth_auth(value: &str) -> ProviderAuth {
        ProviderAuth::OAuthToken {
            value: value.to_string(),
        }
    }

    fn test_store() -> CredentialStore {
        CredentialStore {
            user_id: "local-test".to_string(),
            accounts: vec![],
            git_identities: vec![GitIdentity {
                id: "gi_1".to_string(),
                label: "Test".to_string(),
                name: "Test User".to_string(),
                email: "test@example.com".to_string(),
                sort_order: 0,
            }],
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::from("/test-config-root"),
        }
    }

    fn healthy_claude_account(id: &str, remaining: f64, captured_at: i64) -> ProviderAccount {
        let mut account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        account.id = id.to_string();
        account.health = Some(ProviderAccountHealth {
            windows: vec![cairn_models_types::usage::ProviderUsageWindow {
                id: "five-hour".to_string(),
                label: "5h".to_string(),
                scope: cairn_models_types::usage::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: Some(300),
            }],
            blocked_until: None,
            blocked_until_declared: false,
            depleted: None,
            captured_at,
        });
        account
    }

    /// `id` with its five-hour window exhausted until `window_reset` and, when
    /// given, a block until `block.0`, declared by the provider when `block.1`.
    fn limited(id: &str, window_reset: Option<i64>, block: Option<(i64, bool)>) -> ProviderAccount {
        let mut account = healthy_claude_account(id, 0.0, 100);
        let health = account.health.as_mut().unwrap();
        health.windows[0].resets_at = window_reset;
        if let Some((until, declared)) = block {
            health.blocked_until = Some(until);
            health.blocked_until_declared = declared;
        }
        account
    }

    fn availability(
        store: &CredentialStore,
        explicit: Option<&str>,
        model: Option<&str>,
    ) -> RoutedAvailability {
        store.routed_availability(RoutedProvider::Claude, None, explicit, model, 200)
    }

    #[test]
    fn earliest_availability_takes_the_min_across_accounts_of_each_accounts_max() {
        let mut store = test_store();
        // A is declared blocked until 7_400; its exhausted window resets
        // sooner, so A serves at 7_400. B's block ends at 3_800 but its window
        // stays exhausted until 5_000, so B serves at 5_000.
        store.accounts = vec![
            limited("a", Some(300), Some((7_400, true))),
            limited("b", Some(5_000), Some((3_800, true))),
        ];
        assert_eq!(
            availability(&store, None, None),
            RoutedAvailability::At(5_000, "b".to_string())
        );
        // An explicit policy waits on its own account alone.
        assert_eq!(
            availability(&store, Some("a"), None),
            RoutedAvailability::At(7_400, "a".to_string())
        );
        // Any account that can serve now wins outright.
        store.accounts.push(healthy_claude_account("c", 50.0, 100));
        assert_eq!(
            availability(&store, None, None),
            RoutedAvailability::Now("c".to_string())
        );
    }

    #[test]
    fn credits_and_reprobe_guesses_have_no_availability() {
        let mut store = test_store();
        assert_eq!(
            availability(&store, None, None),
            RoutedAvailability::NoCandidates
        );

        let mut credits = limited("credits", Some(300), None);
        credits.health.as_mut().unwrap().depleted = Some(AccountDepletion {
            since: 150,
            detail: "credits_required".to_string(),
        });
        let guess = limited("guess", Some(300), Some((500, false)));
        let no_reset = limited("no-reset", None, None);
        store.accounts = vec![credits, guess, no_reset];
        assert_eq!(
            availability(&store, None, None),
            RoutedAvailability::Unknown
        );

        // A block persisted before provenance existed reads as a guess.
        let legacy: ProviderAccountHealth = serde_json::from_value(serde_json::json!({
            "windows": [], "blockedUntil": 500, "capturedAt": 100
        }))
        .unwrap();
        assert!(!legacy.blocked_until_declared);
        assert_eq!(legacy.available_at(None, 200), None);
    }

    #[test]
    fn a_model_scoped_exhausted_window_counts_only_for_its_model() {
        let mut store = test_store();
        let mut account = healthy_claude_account("a", 80.0, 100);
        let mut fable = account.health.as_ref().unwrap().windows[0].clone();
        fable.id = "weekly_scoped-fable".to_string();
        fable.scope_target = Some("Fable".to_string());
        fable.remaining_percent = 0.0;
        fable.resets_at = Some(9_000);
        account.health.as_mut().unwrap().windows.push(fable);
        store.accounts = vec![account];
        assert_eq!(
            availability(&store, None, Some("claude-fable-5-1")),
            RoutedAvailability::At(9_000, "a".to_string())
        );
        assert_eq!(
            availability(&store, None, Some("claude-opus-4-1")),
            RoutedAvailability::Now("a".to_string())
        );
    }

    #[test]
    fn stale_reprobe_remains_eligible_but_current_headroom_wins() {
        let mut store = test_store();
        let mut stale = healthy_claude_account("stale", 0.0, 100);
        stale.health.as_mut().unwrap().windows[0].resets_at = Some(150);
        store.accounts = vec![stale, healthy_claude_account("healthy", 20.0, 100)];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .map(|account| account.id.as_str()),
            Some("healthy")
        );
        assert_eq!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    Some("stale"),
                    None,
                    None,
                    &[],
                    200,
                )
                .map(|account| account.id.as_str()),
            Some("stale"),
            "an explicit project/account override retains its re-probe path"
        );
    }

    #[test]
    fn a_legacy_resetless_terminal_429_allows_a_bounded_reprobe() {
        let mut store = test_store();
        let mut spent = healthy_claude_account("spent", 0.0, 100);
        let health = spent.health.as_mut().unwrap();
        health.windows[0].id = "claude".to_string();
        health.windows[0].resets_at = None;
        store.accounts = vec![spent];

        assert!(store
            .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 399)
            .is_none());
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 400)
                .map(|account| account.id.as_str()),
            Some("spent")
        );
    }

    #[test]
    fn resetless_authoritative_provider_windows_do_not_become_fabricated_capacity() {
        let mut store = test_store();
        let mut spent = healthy_claude_account("spent", 0.0, 100);
        spent.health.as_mut().unwrap().windows[0].resets_at = None;
        store.accounts = vec![spent];

        assert!(store
            .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 10_000)
            .is_none());
    }

    #[test]
    fn claude_scoped_exhaustion_only_excludes_the_matching_model() {
        let mut store = test_store();
        let mut terminal_429 = healthy_claude_account("quarantined", 0.0, 100);
        terminal_429.health.as_mut().unwrap().windows[0].resets_at = Some(500);

        let mut model_scoped = healthy_claude_account("fable-spent", 24.0, 100);
        model_scoped.health.as_mut().unwrap().windows.push(
            cairn_models_types::usage::ProviderUsageWindow {
                id: "weekly_scoped-fable".to_string(),
                label: "Current week (Fable)".to_string(),
                scope: cairn_models_types::usage::ProviderUsageScope::Weekly,
                scope_target: Some("Fable".to_string()),
                used_percent: 100.0,
                remaining_percent: 0.0,
                resets_at: Some(800),
                reset_at_text: None,
                window_duration_mins: None,
            },
        );
        store.accounts = vec![terminal_429, model_scoped];

        let opus = store
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                None,
                None,
                Some("opus"),
                &[],
                200,
            )
            .expect("the profile with global headroom can serve Opus");
        assert_eq!(opus.id, "fable-spent");
        assert!(store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "fable-spent",
            Some("claude-opus-4-1"),
            200,
        ));

        assert!(store
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                None,
                None,
                Some("fable"),
                &[],
                200,
            )
            .is_none());
        assert!(!store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "fable-spent",
            Some("claude-fable-1"),
            200,
        ));
    }

    #[test]
    fn unavailable_explicit_override_does_not_fall_back() {
        let mut store = test_store();
        store.accounts = vec![
            healthy_claude_account("explicit", 0.0, 100),
            healthy_claude_account("fallback", 90.0, 100),
        ];

        assert!(store
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                Some("explicit"),
                None,
                Some("opus"),
                &[],
                200,
            )
            .is_none());
    }

    #[test]
    fn explicit_api_key_override_remains_selectable() {
        let mut store = test_store();
        let mut key = test_account(ApiProvider::Anthropic, api_key_auth("sk-ant-project"));
        key.id = "project-key".to_string();
        store.accounts = vec![key, healthy_claude_account("subscription", 90.0, 100)];

        assert_eq!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    Some("project-key"),
                    None,
                    Some("opus"),
                    &[],
                    200,
                )
                .expect("an explicit API-key override is not subscription routing")
                .id,
            "project-key"
        );
    }

    #[test]
    fn pinned_private_profile_is_available_only_inside_its_project() {
        let mut store = test_store();
        let mut private = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        private.id = "private-profile".to_string();
        private.project_id = Some("project-a".to_string());
        store.accounts = vec![private];

        assert!(store.routed_account_is_available(
            RoutedProvider::Claude,
            Some("project-a"),
            "private-profile",
            Some("opus"),
            200,
        ));
        assert!(!store.routed_account_is_available(
            RoutedProvider::Claude,
            Some("project-b"),
            "private-profile",
            Some("opus"),
            200,
        ));
    }

    #[test]
    fn claude_selection_prefers_headroom_and_skips_unavailable() {
        let mut store = test_store();
        store.accounts = vec![
            healthy_claude_account("busy", 20.0, 100),
            healthy_claude_account("free", 80.0, 100),
        ];
        let selected = store
            .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
            .unwrap();
        assert_eq!(selected.id, "free");

        store.accounts[1].health.as_mut().unwrap().blocked_until = Some(300);
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "busy"
        );
    }

    /// The pooled display and routing agree on what a spent model-scoped
    /// window means: it stops its own model and nothing else. The projection
    /// asserts the display half of this in `models::provider_pool`; this is the
    /// routing half, on the same shape.
    #[test]
    fn pooled_fable_exhaustion_leaves_opus_routable_on_every_account() {
        fn account(id: &str, session: f64, weekly: f64) -> ProviderAccount {
            let mut account = healthy_claude_account(id, session, 100);
            account.health.as_mut().unwrap().windows[0].resets_at = Some(10_000);
            let mut weekly_all = account.health.as_ref().unwrap().windows[0].clone();
            weekly_all.id = "weekly_all".to_string();
            weekly_all.scope = cairn_models_types::usage::ProviderUsageScope::Weekly;
            weekly_all.remaining_percent = weekly;
            let mut fable = weekly_all.clone();
            fable.id = "weekly_scoped-fable".to_string();
            fable.scope_target = Some("Fable".to_string());
            fable.remaining_percent = 0.0;
            let windows = &mut account.health.as_mut().unwrap().windows;
            windows.push(weekly_all);
            windows.push(fable);
            account
        }

        let mut store = test_store();
        store.accounts = vec![account("a", 60.0, 90.0), account("b", 40.0, 30.0)];

        assert_eq!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    None,
                    None,
                    Some("claude-opus-4-1"),
                    &[],
                    200,
                )
                .map(|account| account.id.as_str()),
            Some("a"),
            "an exhausted Fable window is not global exhaustion"
        );
        assert!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    None,
                    None,
                    Some("claude-fable-1"),
                    &[],
                    200,
                )
                .is_none(),
            "and every account really is out of Fable"
        );
    }

    #[test]
    fn claude_selection_prefers_headroom_over_list_order() {
        let mut store = test_store();
        let mut first = healthy_claude_account("first", 20.0, 100);
        first.sort_order = 0;
        let mut second = healthy_claude_account("second", 80.0, 100);
        second.sort_order = 1;
        store.accounts = vec![first, second];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .map(|account| account.id.as_str()),
            Some("second"),
            "order is a tie-break, not a preference: the emptier account is not the active one"
        );
    }

    /// A running session stays on its account for provider continuity and cache
    /// reuse, so availability — not "would selection pick it again" — is what
    /// decides whether a pin is kept.
    #[test]
    fn a_healthy_pin_remains_available_while_another_account_has_more_headroom() {
        let mut store = test_store();
        store.accounts = vec![
            healthy_claude_account("pinned", 20.0, 100),
            healthy_claude_account("roomier", 90.0, 100),
        ];

        assert!(store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "pinned",
            None,
            200
        ));
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .map(|account| account.id.as_str()),
            Some("roomier"),
            "a fresh selection still rebalances"
        );
    }

    #[test]
    fn claude_selection_spreads_equal_headroom_bursts() {
        let mut store = test_store();
        store.accounts = vec![
            healthy_claude_account("first", 80.0, 100),
            healthy_claude_account("second", 80.0, 100),
        ];
        let assignments = vec![("first".to_string(), 101)];
        assert_eq!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    None,
                    None,
                    None,
                    &assignments,
                    200
                )
                .unwrap()
                .id,
            "second"
        );
    }

    #[test]
    fn explicit_claude_override_wins_even_without_health() {
        let mut store = test_store();
        let mut explicit = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        explicit.id = "explicit".to_string();
        store.accounts = vec![explicit, healthy_claude_account("healthy", 90.0, 100)];
        assert_eq!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    Some("explicit"),
                    None,
                    None,
                    &[],
                    200
                )
                .unwrap()
                .id,
            "explicit"
        );
    }

    #[test]
    fn compatible_backends_claude_profile() {
        let account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        assert_eq!(account.compatible_backends(), vec!["claude"]);
    }

    #[test]
    fn compatible_backends_anthropic_api_key() {
        let account = test_account(ApiProvider::Anthropic, api_key_auth("sk-ant-test"));
        assert_eq!(account.compatible_backends(), vec!["claude", "native"]);
    }

    #[test]
    fn a_profile_with_no_snapshot_yet_is_selectable() {
        // The acceptance case for a fresh sign-in: a profile connected moments
        // ago has no usage snapshot, and must still be able to run a session.
        let mut store = test_store();
        let mut fresh = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        fresh.id = "fresh".to_string();
        store.accounts = vec![healthy_claude_account("measured", 40.0, 100), fresh];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "fresh",
            "an unmeasured profile competes at full headroom"
        );
        assert!(store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "fresh",
            None,
            200
        ));
    }

    #[test]
    fn a_snapshot_with_no_windows_reads_as_unknown_not_exhausted() {
        let mut store = test_store();
        let mut blank = healthy_claude_account("blank", 90.0, 100);
        blank.health.as_mut().unwrap().windows.clear();
        store.accounts = vec![blank];

        assert!(store
            .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
            .is_some());
        assert!(store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "blank",
            None,
            200
        ));
    }

    #[test]
    fn a_blocked_profile_is_unavailable_however_stale_its_snapshot() {
        let mut store = test_store();
        let mut blocked = healthy_claude_account("blocked", 90.0, 100);
        blocked.health.as_mut().unwrap().blocked_until = Some(300);
        store.accounts = vec![blocked];

        assert!(store
            .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
            .is_none());
        assert!(!store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "blocked",
            None,
            200
        ));
    }

    #[test]
    fn credit_depletion_stays_unavailable_without_a_time_expiry() {
        let mut store = test_store();
        let mut depleted = healthy_claude_account("depleted", 90.0, 100);
        depleted.health.as_mut().unwrap().depleted = Some(AccountDepletion {
            since: 150,
            detail: "credits_required".to_string(),
        });
        store.accounts = vec![depleted];

        assert!(store
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                None,
                None,
                None,
                &[],
                i64::MAX,
            )
            .is_none());
        let diagnostics = store.routed_account_diagnostics(
            RoutedProvider::Claude,
            None,
            Some("depleted"),
            None,
            None,
            i64::MAX,
        );
        let reason = diagnostics[0].rejection.as_deref().unwrap();
        assert!(reason.contains("credits are exhausted"));
        assert!(reason.contains("cannot detect a top-up"));
    }

    #[test]
    fn legacy_health_without_depletion_deserializes_as_available() {
        let health: ProviderAccountHealth = serde_json::from_value(serde_json::json!({
            "windows": [],
            "capturedAt": 100
        }))
        .unwrap();

        assert_eq!(health.depleted, None);
        assert!(!health.is_blocked(i64::MAX));
    }

    /// A subscription account measured empty is out of rotation only while
    /// that window stands. Once its reset time passes the number is history,
    /// not exhaustion — otherwise nothing would ever route to the account
    /// again, because every source of a fresh snapshot (sign-in, a manual
    /// refresh, a live session event) needs the account to be in use.
    #[test]
    fn an_exhausted_window_stops_excluding_once_it_has_reset() {
        let mut store = test_store();
        let mut spent = healthy_claude_account("spent", 0.0, 100);
        spent.health.as_mut().unwrap().windows[0].resets_at = Some(500);
        store.accounts = vec![spent];

        assert!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .is_none(),
            "an empty window still standing excludes the account"
        );
        assert!(!store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "spent",
            None,
            200
        ));

        assert!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 600)
                .is_some(),
            "after the window resets the account competes at unknown headroom again"
        );
        assert!(store.routed_account_is_available(
            RoutedProvider::Claude,
            None,
            "spent",
            None,
            600
        ));
    }

    // === Codex subscription routing ===

    fn codex_account(id: &str, remaining: Option<f64>, captured_at: i64) -> ProviderAccount {
        let mut account = test_account(ApiProvider::OpenAI, oauth_auth(&format!("{id}-auth-json")));
        account.id = id.to_string();
        account.health = remaining.map(|remaining| ProviderAccountHealth {
            windows: vec![cairn_models_types::usage::ProviderUsageWindow {
                id: "primary".to_string(),
                label: "5-hour window".to_string(),
                scope: cairn_models_types::usage::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: Some(300),
            }],
            blocked_until: None,
            blocked_until_declared: false,
            depleted: None,
            captured_at,
        });
        account
    }

    #[test]
    fn codex_selection_prefers_headroom_and_skips_blocked() {
        let mut store = test_store();
        store.accounts = vec![
            codex_account("busy", Some(15.0), 100),
            codex_account("free", Some(70.0), 100),
        ];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "free"
        );

        store.accounts[1].health.as_mut().unwrap().blocked_until = Some(300);
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "busy",
            "a rate-limited account is passed over while its block stands"
        );
        assert!(!store.routed_account_is_available(RoutedProvider::Codex, None, "free", None, 200));
        assert!(
            store.routed_account_is_available(RoutedProvider::Codex, None, "free", None, 400),
            "the block lifts on its own once it expires"
        );
    }

    #[test]
    fn a_fresh_codex_account_is_selectable_before_any_usage_snapshot() {
        // The acceptance case for a sign-in: the account connected moments ago
        // has no snapshot, and must still be able to run a session.
        let mut store = test_store();
        store.accounts = vec![
            codex_account("measured", Some(40.0), 100),
            codex_account("fresh", None, 100),
        ];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "fresh",
            "an unmeasured account competes at full headroom"
        );
        assert!(store.routed_account_is_available(RoutedProvider::Codex, None, "fresh", None, 200));
    }

    #[test]
    fn an_exhausted_codex_account_is_passed_over() {
        let mut store = test_store();
        store.accounts = vec![
            codex_account("spent", Some(0.0), 100),
            codex_account("left", Some(5.0), 100),
        ];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "left"
        );
        assert!(!store.routed_account_is_available(
            RoutedProvider::Codex,
            None,
            "spent",
            None,
            200
        ));
    }

    /// An OpenAI API key bills per token, so it has no window to route around
    /// and is never a routing candidate — it stays the deliberate fallback a
    /// session reaches for when no subscription is available.
    #[test]
    fn an_openai_api_key_is_not_routed_by_usage() {
        let mut store = test_store();
        let mut key = test_account(ApiProvider::OpenAI, api_key_auth("sk-test"));
        key.id = "key".to_string();
        store.accounts = vec![key];

        assert!(store
            .select_routed_account(RoutedProvider::Codex, None, None, None, None, &[], 200)
            .is_none());
        assert!(!store.routed_account_is_available(RoutedProvider::Codex, None, "key", None, 200));
    }

    /// Selection stays inside its own provider: a Codex session must never
    /// resolve onto an Anthropic profile, nor the reverse.
    #[test]
    fn routing_never_crosses_providers() {
        let mut store = test_store();
        let mut profile = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        profile.id = "claude".to_string();
        store.accounts = vec![profile, codex_account("codex", Some(50.0), 100)];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "codex"
        );
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, None, &[], 200)
                .unwrap()
                .id,
            "claude"
        );
    }

    /// Resolving onto a chosen Codex account pins that account's credential and
    /// leaves the other provider's resolution alone.

    #[test]
    fn compatible_backends_openai_api_key() {
        let account = test_account(ApiProvider::OpenAI, api_key_auth("sk-test"));
        assert_eq!(account.compatible_backends(), vec!["codex", "native"]);
    }

    #[test]
    fn compatible_backends_google_api_key() {
        let account = test_account(ApiProvider::Google, api_key_auth("goog-key"));
        assert_eq!(account.compatible_backends(), vec!["native"]);
    }

    #[test]
    fn api_provider_serde_roundtrip() {
        let provider = ApiProvider::Anthropic;
        let json = serde_json::to_string(&provider).unwrap();
        assert_eq!(json, r#""anthropic""#);
        let deserialized: ApiProvider = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, provider);
    }

    #[test]
    fn account_info_from_provider_account() {
        let account = test_account(ApiProvider::Anthropic, api_key_auth("secret"));
        let info = AccountInfo::from(&account);
        assert_eq!(info.api_provider, ApiProvider::Anthropic);
        assert_eq!(info.auth_type, "api_key");
        assert_eq!(info.compatible_backends, vec!["claude", "native"]);
    }

    #[test]
    fn accounts_for_provider_sorted() {
        let mut store = test_store();
        let mut acc_b = test_account(ApiProvider::Anthropic, api_key_auth("b"));
        acc_b.sort_order = 1;
        acc_b.label = "B".to_string();
        let mut acc_a = test_account(ApiProvider::Anthropic, api_key_auth("a"));
        acc_a.sort_order = 0;
        acc_a.label = "A".to_string();

        store.accounts.push(acc_b);
        store.accounts.push(acc_a);

        let accounts = store.accounts_for_provider(ApiProvider::Anthropic, None);
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].label, "A");
        assert_eq!(accounts[1].label, "B");
    }

    // === Additional coverage for unverified claims ===

    #[test]
    fn compatible_backends_openai_oauth() {
        let account = test_account(ApiProvider::OpenAI, oauth_auth("chatgpt-oauth"));
        assert_eq!(account.compatible_backends(), vec!["codex"]);
    }

    #[test]
    fn compatible_backends_github_api_key_empty() {
        let account = test_account(ApiProvider::GitHub, api_key_auth("ghp_test"));
        // GitHub API keys don't map to any agent backend
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn compatible_backends_google_oauth_empty() {
        let account = test_account(ApiProvider::Google, oauth_auth("goog-oauth"));
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn compatible_backends_github_oauth_empty() {
        let account = test_account(ApiProvider::GitHub, oauth_auth("gh-oauth"));
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn provider_auth_credential_value() {
        let api_key = api_key_auth("key-val");
        assert_eq!(api_key.credential_value(), Some("key-val"));

        let oauth = oauth_auth("oauth-val");
        assert_eq!(oauth.credential_value(), Some("oauth-val"));

        assert_eq!(
            ProviderAuth::ClaudeProfile { oauth: None }.credential_value(),
            None
        );
    }

    fn custom(
        base_url: &str,
        api_key: Option<&str>,
        headers: &[(&str, &str)],
        models: CustomModelPolicy,
    ) -> Result<ProviderAuth, String> {
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        ProviderAuth::custom_endpoint(
            base_url,
            CustomProtocol::OpenAiChatCompletions,
            api_key,
            &headers,
            models,
            CustomSampling::default(),
            DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS,
        )
    }

    #[test]
    fn an_image_provider_lists_its_models_and_defaults_its_capabilities() {
        let images = |models| {
            ProviderAuth::custom_endpoint(
                "http://spark:8080",
                CustomProtocol::OpenAiImages,
                None,
                &[],
                models,
                CustomSampling::default(),
                DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS,
            )
        };
        let refused = images(CustomModelPolicy::Discover).unwrap_err();
        assert!(refused.contains("list the image model ids"), "{refused}");
        let auth = images(CustomModelPolicy::Explicit {
            ids: vec!["qwen-image".into()],
        })
        .unwrap();
        assert_eq!(
            auth.image_capabilities(),
            Some(ImageCapabilities::openai_images_minimal())
        );
        let declared = auth.with_image_capabilities(ImageCapabilities::openai_gpt_image());
        assert_eq!(
            declared.image_capabilities(),
            Some(ImageCapabilities::openai_gpt_image())
        );
        assert_eq!(
            CustomProtocol::parse("openai-images"),
            Some(CustomProtocol::OpenAiImages)
        );
        assert_eq!(CustomProtocol::OpenAiImages.as_str(), "openai-images");
        // A chat endpoint draws nothing, declared or not.
        let chat = custom("http://host:8000", None, &[], CustomModelPolicy::Discover).unwrap();
        assert_eq!(
            chat.with_image_capabilities(ImageCapabilities::openai_gpt_image())
                .image_capabilities(),
            None
        );
    }

    fn base_url_of(auth: &ProviderAuth) -> &str {
        match auth {
            ProviderAuth::CustomEndpoint { base_url, .. } => base_url,
            _ => panic!("expected a custom endpoint"),
        }
    }

    #[test]
    fn a_bare_address_resolves_to_the_default_openai_layout() {
        // `http://host:8000` universally means `/v1` for a server of this kind.
        // Resolving it once and storing the answer is what keeps the URL that
        // diagnostics show the same one requests go to.
        let auth = custom(
            "http://10.10.10.83:8000",
            None,
            &[],
            CustomModelPolicy::Discover,
        )
        .unwrap();
        assert_eq!(base_url_of(&auth), "http://10.10.10.83:8000/v1");

        let trailing = custom(
            "http://10.10.10.83:8000/",
            None,
            &[],
            CustomModelPolicy::Discover,
        )
        .unwrap();
        assert_eq!(base_url_of(&trailing), "http://10.10.10.83:8000/v1");
    }

    #[test]
    fn an_explicit_path_is_taken_literally() {
        // A server behind a prefix, or one that does not use `/v1` at all, is a
        // normal deployment. Only an ABSENT path is inferred.
        for (given, expected) in [
            ("http://host:8000/v1", "http://host:8000/v1"),
            (
                "https://gw.example.com/inference/v2/",
                "https://gw.example.com/inference/v2",
            ),
            (
                "https://gw.example.com/openai",
                "https://gw.example.com/openai",
            ),
        ] {
            let auth = custom(given, None, &[], CustomModelPolicy::Discover).unwrap();
            assert_eq!(base_url_of(&auth), expected, "for {given}");
        }
    }

    #[test]
    fn an_ambiguous_or_unsafe_address_is_refused_at_the_boundary() {
        let cases = [
            ("ftp://host/v1", "http or https"),
            ("not a url", "invalid endpoint URL"),
            // Credentials in the URL are a second, invisible source of auth.
            ("http://user:pass@host:8000/v1", "API key field"),
            ("http://host:8000/v1?key=abc", "query string"),
            ("http://host:8000/v1#frag", "query string or fragment"),
        ];
        for (given, expected) in cases {
            let error = custom(given, None, &[], CustomModelPolicy::Discover)
                .expect_err(&format!("{given} should be refused"));
            assert!(error.contains(expected), "for {given}: {error}");
        }
    }

    #[test]
    fn a_header_cairn_already_sets_cannot_be_overridden() {
        // Silently discarding one, or letting it through to break the request
        // body, are both worse than saying no.
        let error = custom(
            "http://host:8000/v1",
            None,
            &[("Authorization", "Bearer x")],
            CustomModelPolicy::Discover,
        )
        .expect_err("authorization is Cairn's");
        assert!(error.contains("API key field"), "{error}");

        for reserved in ["content-type", "Content-Length", "host"] {
            assert!(custom(
                "http://host:8000/v1",
                None,
                &[(reserved, "x")],
                CustomModelPolicy::Discover
            )
            .is_err());
        }
    }

    #[test]
    fn a_header_set_twice_or_malformed_is_refused() {
        let duplicate = custom(
            "http://host:8000/v1",
            None,
            &[("x-tenant", "a"), ("X-Tenant", "b")],
            CustomModelPolicy::Discover,
        )
        .expect_err("one header, one value");
        assert!(duplicate.contains("set twice"), "{duplicate}");

        let malformed = custom(
            "http://host:8000/v1",
            None,
            &[("x tenant", "a")],
            CustomModelPolicy::Discover,
        )
        .expect_err("a space is not a header name");
        assert!(malformed.contains("valid header name"), "{malformed}");
    }

    #[test]
    fn an_explicit_model_list_keeps_its_ids_exactly() {
        let auth = custom(
            "http://host:8000/v1",
            None,
            &[],
            CustomModelPolicy::Explicit {
                ids: vec![
                    "  qwen3.8-27b  ".into(),
                    "Meta-Llama-3.1-8B-Instruct".into(),
                    // A repeat is the same model named twice, not two models.
                    "qwen3.8-27b".into(),
                    "   ".into(),
                ],
            },
        )
        .unwrap();
        let ProviderAuth::CustomEndpoint {
            models: CustomModelPolicy::Explicit { ids },
            ..
        } = &auth
        else {
            panic!("expected an explicit list");
        };
        assert_eq!(ids, &["qwen3.8-27b", "Meta-Llama-3.1-8B-Instruct"]);
    }

    fn embedding(id: &str, dims: u32) -> CustomEmbeddingModel {
        CustomEmbeddingModel {
            id: id.into(),
            dims,
            widths: Default::default(),
        }
    }

    #[test]
    fn embedding_models_are_declared_beside_chat_models_and_validated() {
        use cairn_models_types::embedding::EmbeddingWidths;
        let chat = custom(
            "http://hub/v1",
            Some("sk"),
            &[],
            CustomModelPolicy::Discover,
        )
        .unwrap();
        let auth = chat
            .clone()
            .with_embedding_models(vec![embedding("  qwen/qwen3-embedding-4b ", 2560)])
            .unwrap();
        let ProviderAuth::CustomEndpoint { embeddings, .. } = &auth else {
            panic!("a custom endpoint")
        };
        assert_eq!(embeddings, &[embedding("qwen/qwen3-embedding-4b", 2560)]);
        assert!(format!("{auth:?}").contains("qwen/qwen3-embedding-4b"));

        for (models, refusal) in [
            (vec![embedding(" ", 768)], "needs an id"),
            (
                vec![embedding("a", 768), embedding("a", 768)],
                "declared twice",
            ),
            (vec![embedding("a", 0)], "needs its width"),
            (
                vec![CustomEmbeddingModel {
                    widths: EmbeddingWidths::UpTo { min: 1024 },
                    ..embedding("a", 768)
                }],
                "floor of 1024",
            ),
        ] {
            let error = chat.clone().with_embedding_models(models).unwrap_err();
            assert!(error.contains(refusal), "{error}");
        }

        let decisions = ProviderAuth::custom_endpoint(
            "http://gateway/v1",
            CustomProtocol::Decisions,
            None,
            &[],
            CustomModelPolicy::Explicit {
                ids: vec!["typesafe-ai/jev".into()],
            },
            CustomSampling::default(),
            DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS,
        )
        .unwrap();
        assert!(decisions
            .clone()
            .with_embedding_models(vec![embedding("e", 768)])
            .unwrap_err()
            .contains("not an openai-decisions one"));
        assert!(decisions.with_embedding_models(Vec::new()).is_ok());
    }

    #[test]
    fn a_stored_endpoint_reads_with_or_without_embedding_models() {
        let without = custom("http://hub/v1", None, &[], CustomModelPolicy::Discover).unwrap();
        let round_trip = |auth: &ProviderAuth| {
            let json = serde_json::to_value(auth).unwrap();
            let read: ProviderAuth = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(serde_json::to_value(&read).unwrap(), json);
            json
        };
        let json = round_trip(&without);
        assert!(json.get("embeddings").is_none(), "{json}");

        let with = without
            .with_embedding_models(vec![embedding("nomic-embed-text", 768)])
            .unwrap();
        assert_eq!(
            round_trip(&with)["embeddings"],
            serde_json::json!([{"id": "nomic-embed-text", "dims": 768, "widths": "nativeOnly"}])
        );
    }

    #[test]
    fn an_empty_explicit_list_is_a_provider_that_can_never_serve() {
        let error = custom(
            "http://host:8000/v1",
            None,
            &[],
            CustomModelPolicy::Explicit { ids: vec![] },
        )
        .expect_err("nothing to serve");
        assert!(error.contains("at least one model id"), "{error}");
    }

    #[test]
    fn a_blank_api_key_is_no_key_rather_than_an_empty_bearer() {
        let auth = custom(
            "http://host:8000/v1",
            Some("   "),
            &[],
            CustomModelPolicy::Discover,
        )
        .unwrap();
        assert_eq!(auth.credential_value(), None);
        assert_eq!(auth.auth_type_label(), "custom_endpoint");
    }

    #[test]
    fn a_custom_account_serves_exactly_its_own_backend() {
        let auth = custom(
            "http://host:8000/v1",
            None,
            &[],
            CustomModelPolicy::Discover,
        )
        .unwrap();
        let mut account = test_account(ApiProvider::Custom, auth);
        account.id = "acc_spark".to_string();
        assert_eq!(account.compatible_backends(), vec!["custom:acc_spark"]);
        assert!(account.serves_backend("custom:acc_spark"));
        assert!(!account.serves_backend("custom:acc_other"));
        assert!(!account.serves_backend("ollama"));
    }

    #[test]
    fn provider_auth_type_labels() {
        assert_eq!(api_key_auth("x").auth_type_label(), "api_key");
        assert_eq!(oauth_auth("x").auth_type_label(), "oauth_token");
        assert_eq!(
            ProviderAuth::ClaudeProfile { oauth: None }.auth_type_label(),
            "claude_profile"
        );
    }

    #[test]
    fn api_provider_as_str() {
        assert_eq!(ApiProvider::Anthropic.as_str(), "anthropic");
        assert_eq!(ApiProvider::OpenAI.as_str(), "openai");
        assert_eq!(ApiProvider::Google.as_str(), "google");
        assert_eq!(ApiProvider::GitHub.as_str(), "github");
    }

    #[test]
    fn api_provider_display() {
        assert_eq!(format!("{}", ApiProvider::Anthropic), "Anthropic");
        assert_eq!(format!("{}", ApiProvider::OpenAI), "OpenAI");
        assert_eq!(format!("{}", ApiProvider::Google), "Google");
        assert_eq!(format!("{}", ApiProvider::GitHub), "GitHub");
    }

    #[test]
    fn api_provider_all_returns_every_provider() {
        assert_eq!(
            ApiProvider::all(),
            &[
                ApiProvider::Anthropic,
                ApiProvider::OpenAI,
                ApiProvider::Google,
                ApiProvider::OpenRouter,
                ApiProvider::OpenCode,
                ApiProvider::Ollama,
                ApiProvider::Actual,
                ApiProvider::GitHub,
                ApiProvider::Bginf,
                ApiProvider::Custom,
            ]
        );
    }

    // === Serde alias tests (critical for disk compatibility) ===

    #[test]
    fn api_provider_serde_aliases() {
        // Canonical forms
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""anthropic""#).unwrap(),
            ApiProvider::Anthropic
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""opencode""#).unwrap(),
            ApiProvider::OpenCode
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""openai""#).unwrap(),
            ApiProvider::OpenAI
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""github""#).unwrap(),
            ApiProvider::GitHub
        );

        // Aliases from serde's automatic mangling
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""open_ai""#).unwrap(),
            ApiProvider::OpenAI,
            "open_ai alias should work"
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""open_a_i""#).unwrap(),
            ApiProvider::OpenAI,
            "open_a_i alias should work (serde rename_all mangling)"
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""git_hub""#).unwrap(),
            ApiProvider::GitHub,
            "git_hub alias should work"
        );
    }

    #[test]
    fn api_provider_serializes_canonical() {
        // Serialization should always use canonical form
        assert_eq!(
            serde_json::to_string(&ApiProvider::OpenAI).unwrap(),
            r#""openai""#
        );
        assert_eq!(
            serde_json::to_string(&ApiProvider::GitHub).unwrap(),
            r#""github""#
        );
    }

    #[test]
    fn provider_auth_serde_oauth_alias() {
        // ProviderAuth::OAuthToken can be deserialized from alias "o_auth_token"
        let json = r#"{"type": "o_auth_token", "value": "tok"}"#;
        let auth: ProviderAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.credential_value(), Some("tok"));
        assert_eq!(auth.auth_type_label(), "oauth_token");
    }

    #[test]
    fn provider_auth_serde_roundtrip() {
        let api_key = api_key_auth("key");
        let json = serde_json::to_string(&api_key).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), Some("key"));

        let oauth = oauth_auth("tok");
        let json = serde_json::to_string(&oauth).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), Some("tok"));

        let base_url = ProviderAuth::BaseUrl {
            url: "http://localhost:11434".to_string(),
        };
        let json = serde_json::to_string(&base_url).unwrap();
        assert_eq!(
            json,
            r#"{"type":"base_url","url":"http://localhost:11434"}"#
        );
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            parsed,
            ProviderAuth::BaseUrl { url } if url == "http://localhost:11434"
        ));
        assert_eq!(base_url.credential_value(), None);
        assert_eq!(base_url.auth_type_label(), "base_url");

        let profile = ProviderAuth::ClaudeProfile { oauth: None };
        let json = serde_json::to_string(&profile).unwrap();
        assert_eq!(json, r#"{"type":"claude_profile"}"#);
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            parsed,
            ProviderAuth::ClaudeProfile { oauth: None }
        ));

        let harvested = ProviderAuth::ClaudeProfile {
            oauth: Some(r#"{"accessToken":"at"}"#.to_string()),
        };
        let parsed: ProviderAuth =
            serde_json::from_str(&serde_json::to_string(&harvested).unwrap()).unwrap();
        assert!(matches!(
            parsed,
            ProviderAuth::ClaudeProfile { oauth: Some(ref json) } if json.contains("\"at\"")
        ));
    }

    #[test]
    fn debug_never_prints_a_secret() {
        let secret = "SENTINEL-SECRET";
        let auths = [
            api_key_auth(secret),
            oauth_auth(secret),
            ProviderAuth::ClaudeProfile {
                oauth: Some(format!(r#"{{"refreshToken":"{secret}"}}"#)),
            },
            relay_target(secret, None),
        ];
        for auth in &auths {
            let debug = format!("{auth:?}");
            assert!(!debug.contains(secret), "{debug}");
            assert!(debug.contains(REDACTED), "{debug}");
        }
        let account = test_account(ApiProvider::Anthropic, auths[2].clone());
        assert!(!format!("{account:?}").contains(secret));
        assert!(!format!("{:?}", CodexAuth::OAuthToken(secret.into())).contains(secret));
        assert!(!format!("{:?}", ClaudeAuth::ApiKey(secret.into())).contains(secret));
    }

    #[test]
    fn account_source_serde_roundtrip() {
        for source in [AccountSource::Configured, AccountSource::Server] {
            let json = serde_json::to_string(&source).unwrap();
            let parsed: AccountSource = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, source);
        }
    }

    #[test]
    fn account_info_does_not_expose_credentials() {
        let account = test_account(ApiProvider::Anthropic, api_key_auth("super-secret-key"));
        let info = AccountInfo::from(&account);
        let json = serde_json::to_string(&info).unwrap();
        assert!(
            !json.contains("super-secret-key"),
            "AccountInfo should not contain credential values"
        );
    }

    // === Actual targets ===

    fn relay_target(api_key: &str, cluster: Option<&str>) -> ProviderAuth {
        ProviderAuth::actual_target(
            ActualTargetKind::Relay,
            "https://api.actual.inc",
            Some(api_key),
            cluster,
        )
        .expect("a relay target with a credential is valid")
    }

    #[test]
    fn a_relay_target_without_a_credential_is_refused_at_the_boundary() {
        // The relay answers every unauthenticated call with the same opaque
        // token, so a target saved without a key would only fail later with a
        // message that cannot say which field is missing.
        for missing in [None, Some(""), Some("   ")] {
            let error = ProviderAuth::actual_target(
                ActualTargetKind::Relay,
                "https://api.actual.inc",
                missing,
                None,
            )
            .unwrap_err();
            assert!(error.contains("ac_ inference credential"), "{error}");
        }
    }

    #[test]
    fn a_local_target_refuses_a_credential_and_a_cluster_pin() {
        // Loopback sends neither, so storing either would leave the user
        // believing a credential is in use that never goes on the wire.
        let error = ProviderAuth::actual_target(
            ActualTargetKind::Local,
            "http://127.0.0.1:8080",
            Some("ac_key"),
            None,
        )
        .unwrap_err();
        assert!(error.contains("takes no per-request credential"), "{error}");

        let error = ProviderAuth::actual_target(
            ActualTargetKind::Local,
            "http://127.0.0.1:8080",
            None,
            Some("cluster-1"),
        )
        .unwrap_err();
        assert!(error.contains("Private Relay targets"), "{error}");
    }

    #[test]
    fn an_actual_target_canonicalizes_its_endpoint_and_trims_blank_fields() {
        let auth = ProviderAuth::actual_target(
            ActualTargetKind::Relay,
            "  https://api.actual.inc/  ",
            Some("  ac_key  "),
            Some("   "),
        )
        .unwrap();
        let ProviderAuth::ActualTarget {
            base_url,
            api_key,
            cluster_id,
            kind,
        } = auth
        else {
            panic!("expected an Actual target");
        };
        assert_eq!(base_url, "https://api.actual.inc");
        assert_eq!(api_key.as_deref(), Some("ac_key"));
        // A blank cluster box is not a pin.
        assert_eq!(cluster_id, None);
        assert_eq!(kind, ActualTargetKind::Relay);

        assert!(
            ProviderAuth::actual_target(ActualTargetKind::Local, "not-a-url", None, None).is_err()
        );
    }

    #[test]
    fn an_actual_target_serves_only_the_actual_backend() {
        let account = test_account(ApiProvider::Actual, relay_target("ac_key", None));
        assert_eq!(account.compatible_backends(), vec!["actual"]);

        // A bare base URL is an Ollama host shape, not an Actual target, and
        // must not quietly become one.
        let mismatched = test_account(
            ApiProvider::Actual,
            ProviderAuth::base_url("http://127.0.0.1:8080").unwrap(),
        );
        assert!(mismatched.compatible_backends().is_empty());
    }

    #[test]
    fn account_info_shows_the_target_but_never_the_relay_credential() {
        let account = test_account(
            ApiProvider::Actual,
            relay_target("ac_live_supersecret", Some("cluster-7")),
        );
        let info = AccountInfo::from(&account);
        let json = serde_json::to_string(&info).unwrap();
        assert!(
            !json.contains("ac_live_supersecret"),
            "the relay credential must not reach the frontend: {json}"
        );
        // Endpoint, kind, and cluster are connection metadata the panel needs.
        assert_eq!(info.base_url.as_deref(), Some("https://api.actual.inc"));
        assert_eq!(info.target_kind.as_deref(), Some("relay"));
        assert_eq!(info.cluster_id.as_deref(), Some("cluster-7"));
        assert_eq!(info.auth_type, "actual_target");
    }

    #[test]
    fn a_local_target_reports_no_kind_confusion_and_no_cluster() {
        let account = test_account(
            ApiProvider::Actual,
            ProviderAuth::actual_target(
                ActualTargetKind::Local,
                "http://127.0.0.1:8080",
                None,
                None,
            )
            .unwrap(),
        );
        let info = AccountInfo::from(&account);
        assert_eq!(info.target_kind.as_deref(), Some("local"));
        assert_eq!(info.cluster_id, None);
        // Providers that are not Actual carry no target fields at all.
        let other = AccountInfo::from(&test_account(
            ApiProvider::Ollama,
            ProviderAuth::base_url("http://localhost:11434").unwrap(),
        ));
        assert_eq!(other.target_kind, None);
        assert_eq!(other.cluster_id, None);
    }

    #[test]
    fn an_actual_target_round_trips_through_serde_with_its_pin_intact() {
        let auth = relay_target("ac_key", Some("cluster-7"));
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains(r#""type":"actual_target""#), "{json}");
        let decoded: ProviderAuth = serde_json::from_str(&json).unwrap();
        let ProviderAuth::ActualTarget {
            kind,
            base_url,
            api_key,
            cluster_id,
        } = decoded
        else {
            panic!("expected an Actual target");
        };
        assert_eq!(kind, ActualTargetKind::Relay);
        assert_eq!(base_url, "https://api.actual.inc");
        assert_eq!(api_key.as_deref(), Some("ac_key"));
        assert_eq!(cluster_id.as_deref(), Some("cluster-7"));
    }

    #[test]
    fn the_actual_provider_key_is_stable() {
        // Persisted settings and account rows are keyed by this string; changing
        // it would orphan every configured target.
        assert_eq!(ApiProvider::Actual.as_str(), "actual");
        assert_eq!(
            serde_json::to_string(&ApiProvider::Actual).unwrap(),
            r#""actual""#
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""actual""#).unwrap(),
            ApiProvider::Actual
        );
        assert!(ApiProvider::all().contains(&ApiProvider::Actual));
    }
}
