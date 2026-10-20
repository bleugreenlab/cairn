//! The canonical catalog: the providers Cairn ships, the models they serve,
//! and the context window each model is read with.
//!
//! Four questions get asked about a provider, and they are not the same
//! question:
//!
//! - **supported** — Cairn ships an adapter for it. That is this catalog, fixed
//!   at compile time. Historical data is interpreted against it, so a job that
//!   ran on a provider stays readable forever regardless of configuration.
//! - **enabled** — this workspace installed it. That is
//!   `Settings::enabled_providers` in cairn-config, durable configuration the
//!   user owns. Only enabled providers get tabs, model discovery, picker
//!   presence, warnings, and routing participation.
//! - **configured** — a credential or host exists for it
//!   (`ProviderAccount`).
//! - **runnable** — it can serve a request right now
//!   (`AgentBackend::is_available` and the response-availability
//!   checks).
//!
//! An enabled provider with no credential is a normal, visible state: the
//! workspace depends on it and it needs setup. A supported provider that is not
//! enabled is absent from the product surface entirely.
//!
//! This is the one inventory. The model-catalog refresh, the responses surface,
//! the settings UI's tab strip and Add-provider catalog, and the enablement
//! migration all read it rather than carrying a copy, so adding a provider
//! cannot leave one surface quietly behind.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::embedding::{EmbeddingCapabilities, CAIRN_GATEWAY_KEY};
use crate::image::ImageCapabilities;
use crate::provider_id::ApiProvider;
use crate::speech::SpeechCapabilities;
use crate::usage::{BillingUnit, Charge, Meter};

/// How a provider is paid for and reached, which is what a user is actually
/// choosing between when installing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderCategory {
    /// A seat-priced plan reached through the vendor's own CLI: fixed monthly
    /// cost, rolling usage windows, one login per account.
    Subscription,
    /// Metered HTTP APIs billed per token against a key.
    Api,
    /// Models served from hardware the user controls, so there is no per-token
    /// bill and no provider-side usage window.
    ///
    /// This does not mean "no vendor account". Owning the hardware and owning
    /// the software that runs on it are different things: Ollama needs no
    /// account at all, while Actual requires one before an install works,
    /// because a device must be authorized against it. What the category
    /// actually promises is where inference *happens* and who pays for it, not
    /// that nobody had to register.
    Local,
}

/// One shipped provider, with everything a surface needs to name, group, set
/// up, and explain it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderDescriptor {
    /// Stable backend key. This is what `settings.yaml`, `jobs.model`
    /// resolution, tier defaults, and the catalog map are all keyed by; it
    /// never changes once shipped.
    ///
    /// Owned rather than `&'static str` because of the one provider kind whose
    /// key is not compile-time: a custom provider's key is `custom:<account
    /// id>`, minted when the operator adds it. Its display name is likewise the
    /// name they typed. Those two facts are the entire difference between a
    /// shipped descriptor and a workspace-defined one, which is why they are
    /// the only two fields that carry a `Cow`.
    pub key: Cow<'static, str>,
    /// Product name for tabs, pickers, and prose.
    pub display_name: Cow<'static, str>,
    pub category: ProviderCategory,
    /// The credential provider this backend authenticates through. Not the same
    /// as the backend key wherever one account spans more than one way of being
    /// served: an OpenCode Zen key serves the Go subscription.
    pub api_provider: ApiProvider,
    /// One line explaining what installing this provider gets you.
    pub summary: &'static str,
    /// Verb for the setup control on the provider's own panel.
    pub setup_action: &'static str,
    /// What an enabled provider still needs before it can run anything. Shown
    /// verbatim when a provider is enabled but unconfigured.
    pub setup_requirement: &'static str,
    /// Whether this provider serves a large, priced, third-party model catalog.
    /// Such a provider can return hundreds of models with per-token pricing, so
    /// its surfaces search the catalog, show cost beside each model, and offer a
    /// refresh. A provider with a small or self-hosted inventory (a shipped list
    /// of Claude tiers, the models on an Ollama host) is listed plainly instead.
    pub catalog_driven: bool,
    /// Whether Cairn's MCP server can be installed into this provider's CLI.
    pub supports_mcp_install: bool,
    /// Extra search terms for the Add-provider catalog, beyond the display name
    /// and key (which are always searched).
    pub keywords: &'static [&'static str],
}

/// Every provider Cairn ships, in product order.
pub const PROVIDER_CATALOG: &[ProviderDescriptor] = &[
    ProviderDescriptor {
        key: Cow::Borrowed("claude"),
        display_name: Cow::Borrowed("Anthropic"),
        category: ProviderCategory::Subscription,
        api_provider: ApiProvider::Anthropic,
        summary: "Claude models through the Claude CLI, on a Cairn-managed sign-in or an API key.",
        setup_action: "Sign in",
        setup_requirement: "Sign in to a Claude account or add an Anthropic API key.",
        catalog_driven: false,
        supports_mcp_install: true,
        keywords: &["claude", "anthropic", "opus", "sonnet", "haiku"],
    },
    ProviderDescriptor {
        key: Cow::Borrowed("codex"),
        display_name: Cow::Borrowed("OpenAI"),
        category: ProviderCategory::Subscription,
        api_provider: ApiProvider::OpenAI,
        summary: "GPT and Codex models through the Codex CLI, on a ChatGPT sign-in or an API key.",
        setup_action: "Sign in",
        setup_requirement: "Sign in to a ChatGPT account or add an OpenAI API key.",
        catalog_driven: false,
        supports_mcp_install: true,
        keywords: &["codex", "openai", "chatgpt", "gpt"],
    },
    ProviderDescriptor {
        key: Cow::Borrowed("openrouter"),
        display_name: Cow::Borrowed("OpenRouter"),
        category: ProviderCategory::Api,
        api_provider: ApiProvider::OpenRouter,
        summary: "Hundreds of models from every major lab behind one metered API key.",
        setup_action: "Add API key",
        setup_requirement: "Add an OpenRouter API key.",
        catalog_driven: true,
        supports_mcp_install: false,
        keywords: &["openrouter", "router", "metered"],
    },
    ProviderDescriptor {
        key: Cow::Borrowed(OPENCODE_BACKEND_KEY),
        display_name: Cow::Borrowed("OpenCode Go"),
        category: ProviderCategory::Api,
        api_provider: ApiProvider::OpenCode,
        summary: "Set-price rolling-window access to strong open models, on an OpenCode Zen key.",
        setup_action: "Add API key",
        setup_requirement: "Add an OpenCode Zen API key.",
        catalog_driven: true,
        supports_mcp_install: false,
        keywords: &["opencode", "zen", "go", "deepseek", "kimi", "glm"],
    },
    ProviderDescriptor {
        key: Cow::Borrowed("ollama"),
        display_name: Cow::Borrowed("Ollama"),
        category: ProviderCategory::Local,
        api_provider: ApiProvider::Ollama,
        summary: "Models served from an Ollama host you run, on your own hardware.",
        setup_action: "Add host",
        setup_requirement: "Add the base URL of an Ollama host.",
        catalog_driven: false,
        supports_mcp_install: false,
        keywords: &["ollama", "local", "self-hosted", "private"],
    },
    ProviderDescriptor {
        key: Cow::Borrowed(ACTUAL_BACKEND_KEY),
        display_name: Cow::Borrowed("Actual"),
        category: ProviderCategory::Local,
        api_provider: ApiProvider::Actual,
        summary: "Your own devices as an inference cluster, reached locally or through Actual's end-to-end Private Relay. Requires a free Actual account to authorize each device.",
        setup_action: "Add target",
        setup_requirement: "Install Actual and authorize the device against an Actual account, then add its local address or a Private Relay credential.",
        catalog_driven: false,
        supports_mcp_install: false,
        keywords: &[
            "actual",
            "local",
            "private",
            "relay",
            "cluster",
            "self-hosted",
        ],
    },
];

/// The prefix every custom provider's backend key carries.
pub const CUSTOM_KEY_PREFIX: &str = "custom:";

/// A custom provider reduced to what the catalog needs to describe it: the
/// account that IS the provider, and the name the operator gave it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomProvider {
    pub account_id: String,
    pub display_name: String,
}

/// The backend key for the custom provider held by `account_id`.
///
/// The account id is the provider's identity — not its address and not its
/// name. That is the whole point of keying on it: an operator can rename the
/// provider or move it to a new host without invalidating a tier default, a
/// job's recorded model, or a historical session that ran on it.
pub fn custom_key(account_id: &str) -> String {
    format!("{CUSTOM_KEY_PREFIX}{account_id}")
}

/// The account id inside a custom provider's backend key, if `key` is one.
///
/// Well-formedness is structural and does not consult the identity store. A key
/// naming an account that has since been removed is still a readable key, and
/// the work that ran on it has to stay interpretable.
pub fn custom_account_id(key: &str) -> Option<&str> {
    let id = key.strip_prefix(CUSTOM_KEY_PREFIX)?;
    let well_formed = !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    well_formed.then_some(id)
}

/// The descriptor for a backend key, if Cairn ships that provider.
///
/// A custom provider has no shipped descriptor — its name lives in the identity
/// store — so this answers `None` for one. Use [`workspace_catalog`] where the
/// workspace's own providers need describing.
pub fn descriptor(key: &str) -> Option<&'static ProviderDescriptor> {
    PROVIDER_CATALOG.iter().find(|entry| entry.key == key)
}

/// Whether Cairn ships an adapter for this backend key.
///
/// This is *supported*, not *enabled*: it answers questions about persisted
/// data and code capability, never about what a workspace has installed.
///
/// Every well-formed custom key is supported, because what Cairn ships for a
/// custom provider is the adapter, not the provider. Whether any particular one
/// is still configured is the identity store's question, and asking it here
/// would make a removed provider's history unreadable.
pub fn is_supported(key: &str) -> bool {
    descriptor(key).is_some() || custom_account_id(key).is_some()
}

/// Every shipped backend key, in product order.
///
/// Custom keys are absent by construction: this enumerates what Cairn ships,
/// and is used to tell a user which providers exist in this build.
pub fn supported_keys() -> impl Iterator<Item = &'static str> {
    PROVIDER_CATALOG.iter().map(|entry| entry.key.as_ref())
}

/// The descriptor for one configured custom provider.
///
/// The operator's name is the display name everywhere a first-class provider
/// would show its product name — tabs, pickers, routing, diagnostics — so a
/// custom provider reads as the thing they named rather than as "Custom".
pub fn custom_descriptor(provider: &CustomProvider) -> ProviderDescriptor {
    ProviderDescriptor {
        key: Cow::Owned(custom_key(&provider.account_id)),
        display_name: Cow::Owned(provider.display_name.clone()),
        // Inference runs on hardware the operator owns and pays for, which is
        // exactly what this category promises. It is why no per-token cost is
        // shown and no provider-side usage window is expected.
        category: ProviderCategory::Local,
        api_provider: ApiProvider::Custom,
        summary: "An OpenAI-compatible endpoint you run.",
        setup_action: "Edit endpoint",
        setup_requirement: "Give this provider a reachable endpoint URL.",
        catalog_driven: false,
        supports_mcp_install: false,
        keywords: &[],
    }
}

/// The descriptor for Cairn's own cloud embedding gateway.
///
/// Not in [`PROVIDER_CATALOG`]: nobody installs it. It is present wherever this
/// device holds a Cairn token (every install registers an anonymous one), so it
/// never appears in Add-provider, and its key is not [`is_supported`] as a
/// backend: it runs no sessions or completions, only `embed`.
pub fn cairn_gateway_descriptor() -> ProviderDescriptor {
    ProviderDescriptor {
        key: Cow::Borrowed(CAIRN_GATEWAY_KEY),
        display_name: Cow::Borrowed("Cairn"),
        category: ProviderCategory::Api,
        api_provider: ApiProvider::Custom,
        summary: "Cairn's cloud embedding gateway (Cohere Embed v4), on this device's Cairn token.",
        setup_action: "Sign in",
        setup_requirement: "This device has no Cairn token yet. Sign in to Cairn, or let it reach Cairn's API once to register.",
        catalog_driven: false,
        supports_mcp_install: false,
        keywords: &[],
    }
}

/// The descriptor for an installed key this build cannot place: a provider a
/// newer or older Cairn ships, or a custom provider whose account is gone.
///
/// The workspace still names it, so every provider surface keeps showing it and
/// says what is wrong, rather than hiding configuration the operator has to
/// clean up.
pub fn unknown_descriptor(key: &str) -> ProviderDescriptor {
    ProviderDescriptor {
        key: Cow::Owned(key.to_string()),
        display_name: Cow::Owned(key.to_string()),
        category: ProviderCategory::Api,
        api_provider: ApiProvider::Custom,
        summary: "This version of Cairn does not ship this provider.",
        setup_action: "Connect",
        setup_requirement: "This version of Cairn does not ship this provider. Update Cairn, or remove it from this workspace.",
        catalog_driven: false,
        supports_mcp_install: false,
        keywords: &[],
    }
}

/// The provider inventory for one workspace: everything Cairn ships, then the
/// custom providers this workspace has defined.
///
/// This is the inventory every provider surface reads. A shipped provider is
/// the same everywhere Cairn runs; a custom one exists only here, which is why
/// the workspace's own providers have to be passed in rather than looked up.
pub fn workspace_catalog(customs: &[CustomProvider]) -> Vec<ProviderDescriptor> {
    PROVIDER_CATALOG
        .iter()
        .cloned()
        .chain(customs.iter().map(custom_descriptor))
        .collect()
}

/// Order `keys` by the catalog's product order: shipped providers first in
/// product order, then this workspace's custom providers in the order it holds
/// them, then anything the catalog cannot place, alphabetically.
///
/// Custom providers keep their given order rather than sorting, because that
/// order is the operator's — the sequence they added them in — and an opaque id
/// has no meaningful alphabetical sort to fall back on.
///
/// Unplaceable keys are kept rather than dropped: a workspace may name a
/// provider a newer or older build ships, and silently discarding it would turn
/// a version skew into lost configuration.
pub fn in_catalog_order<I, S>(keys: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut seen: Vec<String> = Vec::new();
    for key in keys {
        let key = key.as_ref().to_string();
        if !seen.contains(&key) {
            seen.push(key);
        }
    }
    let mut ordered: Vec<String> = supported_keys()
        .filter(|key| seen.iter().any(|entry| entry == key))
        .map(|key| key.to_string())
        .collect();
    ordered.extend(
        seen.iter()
            .filter(|key| custom_account_id(key).is_some())
            .cloned(),
    );
    let mut unplaceable: Vec<String> = seen.into_iter().filter(|key| !is_supported(key)).collect();
    unplaceable.sort();
    ordered.extend(unplaceable);
    ordered
}

pub const OPENCODE_BACKEND_KEY: &str = "opencode-go";
pub const ACTUAL_BACKEND_KEY: &str = "actual";
pub const CLAUDE_FAMILY_BACKEND: &str = "claude";
/// The persisted backend key of a Codex session.
pub const CODEX_BACKEND: &str = "codex";
/// Known Codex model prefixes — prefix matching covers versioned variants
/// (e.g. "gpt-5" matches "gpt-5.4", "gpt-5.3-codex", etc.).
const CODEX_MODEL_PREFIXES: &[&str] = &["codex-mini", "gpt-5", "gpt-6"];

/// Infer backend from a model identifier.
/// Returns "codex" for known Codex/OpenAI models, None (= Claude) otherwise.
///
/// **Deprecated path:** The primary path now is preset resolution via
/// `config::presets::resolve_preset()`. This function remains as a fallback
/// for legacy data (snapshots, concrete model strings not yet migrated to tiers).
pub fn backend_for_model(model: &str) -> Option<&'static str> {
    let lower = model.to_lowercase();
    // Legacy inference cannot distinguish slash-containing Ollama tags (for
    // example hf.co/user/model). Atomic { backend, model } selections bypass it.
    if lower == "openrouter/auto" || lower.starts_with('~') || lower.contains('/') {
        return Some("openrouter");
    }
    for prefix in CODEX_MODEL_PREFIXES {
        if lower.starts_with(prefix) {
            return Some("codex");
        }
    }
    None
}

/// The backend a model will actually run on, spelled.
///
/// [`backend_for_model`] answers `None` for every Claude model, and
/// `backend_for_name` then totalizes that `None` to Claude when the process is
/// spawned. A *comparison* site cannot use the partial answer: `None` reads as
/// "no opinion" there, so comparing against it can only ever detect a move
/// toward an inferable provider (Claude -> Codex) and never the move back
/// (Codex -> Claude), leaving the session recorded on a provider the process is
/// not running. Anything deciding whether a session still matches its model must
/// ask this, so the decision and the spawn resolve the same question one way.
pub fn resolved_backend_for_model(model: &str) -> &'static str {
    backend_for_model(model).unwrap_or(CLAUDE_FAMILY_BACKEND)
}

// ============================================================================
// Models
// ============================================================================
//
// What a provider serves, as discovered at runtime (or shipped, for Claude),
// and the context window each model is read with. Resolution, the
// orchestrator's window lookup, the settings UI and `cairn://models/catalog` all
// read these facts from here.

/// What a model takes in or gives out. Values a provider reports that Cairn
/// doesn't recognise are kept verbatim, never guessed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum Modality {
    Text,
    Image,
    Audio,
    Video,
    File,
    /// Vectors, as OpenRouter names an embedding model's output.
    Embeddings,
    Other(String),
}

impl Modality {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::File => "file",
            Self::Embeddings => "embeddings",
            Self::Other(value) => value,
        }
    }
}

impl From<&str> for Modality {
    fn from(value: &str) -> Self {
        match value {
            "text" => Self::Text,
            "image" => Self::Image,
            "audio" => Self::Audio,
            "video" => Self::Video,
            "file" => Self::File,
            "embeddings" => Self::Embeddings,
            other => Self::Other(other.to_string()),
        }
    }
}

impl From<String> for Modality {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<Modality> for String {
    fn from(value: Modality) -> Self {
        match value {
            Modality::Other(value) => value,
            known => known.as_str().to_string(),
        }
    }
}

impl fmt::Display for Modality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a model takes in and gives out, as its provider reported them.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Modalities {
    #[serde(default)]
    pub input: Vec<Modality>,
    #[serde(default)]
    pub output: Vec<Modality>,
}

impl Modalities {
    pub fn outputs(&self, modality: &Modality) -> bool {
        self.output.contains(modality)
    }
}

impl fmt::Display for Modalities {
    /// `text+image → text`, the shape a model picker or catalog line shows.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let join = |side: &[Modality]| {
            if side.is_empty() {
                "?".to_string()
            } else {
                side.iter()
                    .map(Modality::as_str)
                    .collect::<Vec<_>>()
                    .join("+")
            }
        };
        write!(f, "{} → {}", join(&self.input), join(&self.output))
    }
}

/// A protocol that carries a verb. This is a name, not the implementation: LLM
/// families and the embedding wires are implemented in cairn-llm-wire, and
/// media wires in its sibling crates, cairn-image-wire and cairn-speech-wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Wire {
    /// OpenAI-compatible `chat/completions`.
    #[serde(rename = "openai-chat")]
    OpenAiChatCompletions,
    /// Anthropic-style `messages`.
    #[serde(rename = "anthropic-messages")]
    AnthropicMessages,
    /// OpenAI `responses`.
    #[serde(rename = "openai-responses")]
    OpenAiResponses,
    /// A native decisions endpoint that answers a choice with its own
    /// probabilities.
    #[serde(rename = "openai-decisions")]
    OpenAiDecisions,
    /// The Claude CLI's stream-json protocol.
    #[serde(rename = "claude-stream-json")]
    ClaudeStreamJson,
    /// The Codex CLI's app-server protocol.
    #[serde(rename = "codex-app-server")]
    CodexAppServer,
    /// OpenAI-compatible `embeddings`.
    #[serde(rename = "openai-embeddings")]
    OpenAiEmbeddings,
    /// Cairn's cloud gateway `/embed`.
    #[serde(rename = "cairn-embed")]
    CairnEmbed,
    /// OpenRouter's Image API: `POST /api/v1/images`, JSON in and out.
    #[serde(rename = "openrouter-images")]
    OpenRouterImages,
    /// The OpenAI Images API: `/images/generations` (JSON) and
    /// `/images/edits` (multipart), as OpenAI and every OpenAI-compatible
    /// image server answers it.
    #[serde(rename = "openai-images")]
    OpenAiImages,
    /// ElevenLabs' text-to-speech WebSocket, which takes text as it arrives.
    #[serde(rename = "elevenlabs-stream-input")]
    ElevenLabsStreamInput,
    /// OpenAI-compatible `audio/speech`: whole text in, audio streamed out.
    #[serde(rename = "openai-speech")]
    OpenAiSpeech,
}

impl Wire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiChatCompletions => "openai-chat",
            Self::AnthropicMessages => "anthropic-messages",
            Self::OpenAiResponses => "openai-responses",
            Self::OpenAiDecisions => "openai-decisions",
            Self::ClaudeStreamJson => "claude-stream-json",
            Self::CodexAppServer => "codex-app-server",
            Self::OpenAiEmbeddings => "openai-embeddings",
            Self::CairnEmbed => "cairn-embed",
            Self::OpenRouterImages => "openrouter-images",
            Self::OpenAiImages => "openai-images",
            Self::ElevenLabsStreamInput => "elevenlabs-stream-input",
            Self::OpenAiSpeech => "openai-speech",
        }
    }
}

impl fmt::Display for Wire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A facet verb an offer serves. Each variant carries that verb's capability
/// type, which is where a verb with real capability differences (image sizes,
/// speech formats, embedding widths) says what it can do. The conversational
/// verbs and `decide` carry none; `image`, `speak` and `embed` carry what the
/// offer can do, flattened beside the tag (`{"verb": "image", "maxCount": 4, ...}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verb", rename_all = "lowercase")]
pub enum Verb {
    Complete,
    Session,
    Decide,
    /// Texts in, one vector per text out.
    Embed(EmbeddingCapabilities),
    Image(ImageCapabilities),
    /// Text in as a stream, audio out as a stream.
    Speak(SpeechCapabilities),
}

impl Verb {
    pub fn kind(&self) -> VerbKind {
        match self {
            Self::Complete => VerbKind::Complete,
            Self::Session => VerbKind::Session,
            Self::Decide => VerbKind::Decide,
            Self::Embed(_) => VerbKind::Embed,
            Self::Image(_) => VerbKind::Image,
            Self::Speak(_) => VerbKind::Speak,
        }
    }

    /// What this verb can draw, when it is `image`.
    pub fn image(&self) -> Option<&ImageCapabilities> {
        match self {
            Self::Image(capabilities) => Some(capabilities),
            _ => None,
        }
    }
}

/// Which verb, without its capabilities: what a query asks for and what an
/// observation records a model as unable to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VerbKind {
    Complete,
    Session,
    Decide,
    Image,
    Speak,
    Embed,
}

impl VerbKind {
    /// Every verb, in the order hosts present them.
    pub const ALL: [VerbKind; 6] = [
        Self::Complete,
        Self::Session,
        Self::Decide,
        Self::Embed,
        Self::Image,
        Self::Speak,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Session => "session",
            Self::Decide => "decide",
            Self::Embed => "embed",
            Self::Image => "image",
            Self::Speak => "speak",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

impl fmt::Display for VerbKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One verb an offer serves, and the wire that carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServedVerb {
    #[serde(flatten)]
    pub verb: Verb,
    pub wire: Wire,
}

impl ServedVerb {
    /// Complete and Session over `wire`: what every conversational model
    /// serves.
    pub fn conversation(wire: Wire) -> Vec<ServedVerb> {
        vec![
            ServedVerb {
                verb: Verb::Complete,
                wire,
            },
            ServedVerb {
                verb: Verb::Session,
                wire,
            },
        ]
    }
}

/// The USD price of one unit on one meter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Price {
    pub meter: Meter,
    /// USD per one unit, exactly as the provider reported it. Kept as the
    /// provider's decimal string so no precision is lost before display.
    pub usd: String,
}

/// What a model costs, meter by meter.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelPricing {
    #[serde(default)]
    pub prices: Vec<Price>,
    /// Provider price keys Cairn can't place on a meter, kept verbatim rather
    /// than guessed.
    #[serde(default)]
    pub unplaced: BTreeMap<String, String>,
}

impl ModelPricing {
    /// The price on exactly this meter, if the provider set one.
    pub fn usd_per(
        &self,
        charge: Charge,
        modality: Option<&Modality>,
        unit: BillingUnit,
    ) -> Option<&str> {
        self.prices
            .iter()
            .find(|price| {
                price.meter.charge == charge
                    && price.meter.modality.as_ref() == modality
                    && price.meter.unit == unit
            })
            .map(|price| price.usd.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredReasoningEffort {
    pub reasoning_effort: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredModel {
    pub id: String,
    pub model: String,
    pub display_name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub is_default: bool,
    pub default_reasoning_effort: Option<String>,
    #[serde(default)]
    pub supported_reasoning_efforts: Vec<DiscoveredReasoningEffort>,
    #[serde(default)]
    pub context_window: Option<i64>,
    #[serde(default)]
    pub canonical_slug: Option<String>,
    /// Identity of every configured account whose host serves this model, in
    /// account priority order. Providers that serve every model from a single
    /// endpoint leave this empty; Ollama populates it so routing and the
    /// settings inventory agree on which hosts have which models.
    #[serde(default)]
    pub serving_account_ids: Vec<String>,
    #[serde(default)]
    pub pricing: Option<ModelPricing>,
    #[serde(default)]
    pub supported_parameters: Vec<String>,
    #[serde(default)]
    pub router: bool,
    /// What the model takes in and gives out. `None` means the provider didn't
    /// say.
    #[serde(default)]
    pub modalities: Option<Modalities>,
    /// The verbs this model serves and the wire each rides, as discovery found
    /// them.
    ///
    /// `None` means discovery made no per-model claim, so the provider-wide
    /// declaration applies. `Some(vec![])` means discovery says this model
    /// serves nothing Cairn has. A backend fronting several wires (OpenCode Go)
    /// declares no provider-wide default, so for it an absent claim is a
    /// refusal, never a guess at a family.
    #[serde(default)]
    pub serves: Option<Vec<ServedVerb>>,
    /// The exact tokenizer this model uses, when discovery could select one
    /// of Cairn's pins (`tokenizers::tokenizer_for`). `None` means token
    /// budgets for it use the backend's estimating meter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<crate::tokenizers::TokenizerRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOptionDescriptor {
    /// Runtime-supported option key. Add variants here only when preset
    /// resolution and backend launch paths also carry the option end-to-end.
    pub key: ProviderOptionKey,
    pub label: String,
    pub kind: OptionKind,
    #[serde(default)]
    pub choices: Vec<OptionChoice>,
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OptionChoice {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProviderOptionKey {
    ReasoningEffort,
    FastMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OptionKind {
    Enum,
    Boolean,
    String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelCatalog {
    pub backend: String,
    pub models: Vec<DiscoveredModel>,
    #[serde(default)]
    pub options: Vec<ProviderOptionDescriptor>,
    pub refreshed_at: Option<i64>,
    pub error: Option<String>,
}

const CLAUDE_LONG_WINDOW: i64 = 1_000_000;
const CLAUDE_DEFAULT_WINDOW: i64 = 200_000;

/// The context window assumed for a Claude model the CLI has not yet reported a
/// window for. Once it has, [`claude_reported_context_window`] is the authority.
///
/// A Claude window is a fact of the CLI as much as of the model: the CLI sizes
/// every request, and a CLI that predates a model treats it as a 200k model.
/// What the CLI reports (`result.modelUsage[model].contextWindow`, OAuth
/// subscription auth):
///
/// - CLI 2.1.302 gives the 5.x generation 1M with no opt-in, and every 4.x ID
///   (4.5 and 4.6 alike) 200k.
/// - CLI 2.1.220 gives every ID 200k, `claude-haiku-5-5` included, and resolves
///   the bare aliases to the generation before 5.5 (`sonnet` to
///   `claude-sonnet-5`, `haiku` to Haiku 4.5, `fable` to `claude-fable-5`).
/// - Both give 1M to a model named with the CLI's `[1m]` suffix.
///
/// So this answers 1M only where every observed CLI agrees, or where the name
/// itself proves a CLI new enough: a `[1m]` name; the `opus` alias, which Cairn
/// always launches as `opus[1m]`; and an ID from fable 5.1 / the 5.5
/// generation onward, which only a CLI that knows that generation resolves an
/// alias to. Every other name reads 200k: the other bare aliases, 5.0 and 4.x
/// IDs, and unrecognized names. A window that is too small only compacts
/// early; one that is too large lets a session overrun its real context before
/// compaction fires.
pub fn claude_context_window(model: &str) -> i64 {
    let model = model.to_ascii_lowercase();
    if model.ends_with("[1m]") || model == "opus" {
        return CLAUDE_LONG_WINDOW;
    }
    match claude_generation(&model) {
        Some(generation) if generation >= (5, 1) => CLAUDE_LONG_WINDOW,
        _ => CLAUDE_DEFAULT_WINDOW,
    }
}

/// The `(major, minor)` version of a Claude model ID named family-first
/// (`claude-haiku-5-5`, `claude-opus-5`, `claude-sonnet-4-5-20250929`). A date
/// stamp is not a minor version, so `claude-opus-4-20250514` is `(4, 0)`. A bare
/// alias or a version-first name (`claude-3-5-sonnet`) has none.
pub fn claude_generation(model: &str) -> Option<(u32, u32)> {
    let model = model.to_ascii_lowercase();
    let rest = ["opus", "sonnet", "haiku", "fable"]
        .iter()
        .find_map(|family| model.split_once(&format!("{family}-")))
        .map(|(_, rest)| rest)?;
    // A version part is one or two digits; anything longer is a date stamp.
    let version = |part: &str| {
        (1..=2)
            .contains(&part.len())
            .then(|| part.parse::<u32>().ok())
            .flatten()
    };
    let mut parts = rest.split(['-', '.', '[']);
    let major = version(parts.next()?)?;
    let minor = parts.next().and_then(version).unwrap_or(0);
    Some((major, minor))
}

/// The context window the Claude CLI reported in a `result` event's
/// `modelUsage` object, for `resolved_model` (the model its `system:init` event
/// named, which is the key it reports usage under). With no such entry, a
/// `modelUsage` naming exactly one model is that model's report; one naming
/// several, none of them `resolved_model`, answers nothing rather than guess.
pub fn claude_reported_context_window(
    model_usage: &serde_json::Value,
    resolved_model: Option<&str>,
) -> Option<i64> {
    let usage = model_usage.as_object()?;
    let entry = resolved_model
        .and_then(|model| usage.get(model))
        .or_else(|| {
            let mut entries = usage.values();
            entries.next().filter(|_| entries.next().is_none())
        })?;
    entry
        .get("contextWindow")
        .and_then(serde_json::Value::as_i64)
        .filter(|window| *window > 0)
}

/// The window a Claude session runs with: the one its CLI reported, else
/// [`claude_context_window`] for the model the CLI resolved at init, else for
/// the model selected. The resolved ID outranks the selected alias because it
/// says which generation this CLI runs (`haiku` is Haiku 5.5 on one CLI and
/// Haiku 4.5 on another). No model at all means no known window.
pub fn claude_session_window(
    reported_window: Option<i64>,
    resolved_model: Option<&str>,
    selected_model: Option<&str>,
) -> Option<i64> {
    reported_window.or_else(|| resolved_model.or(selected_model).map(claude_context_window))
}

/// The Claude models Cairn ships: the Claude CLI's own aliases.
///
/// Claude has no discovery endpoint Cairn can ask, so this list is the
/// inventory. The aliases are deliberately not mapped to versioned slugs: the
/// CLI decides which version `opus` means, and naming one here would be a
/// guess that drifts the moment the CLI moves.
pub fn claude_models() -> Vec<DiscoveredModel> {
    [
        ("haiku", "Fast Claude alias", false),
        ("sonnet", "Balanced Claude alias", true),
        ("opus", "High-capability Claude alias", false),
        ("fable", "Most capable Claude alias", false),
    ]
    .into_iter()
    .map(|(alias, description, is_default)| DiscoveredModel {
        id: alias.to_string(),
        model: alias.to_string(),
        display_name: alias.to_string(),
        description: Some(description.to_string()),
        hidden: false,
        is_default,
        default_reasoning_effort: None,
        supported_reasoning_efforts: vec![],
        context_window: Some(claude_context_window(alias)),
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: None,
        supported_parameters: Vec::new(),
        router: false,
        modalities: None,
        serves: None,
        tokenizer: None,
    })
    .collect()
}

/// The context window `model` is read with on `provider`.
///
/// Claude has no discovery, so its answer is [`claude_context_window`]'s table;
/// a Claude *session* reads [`claude_session_window`] instead, which prefers
/// what its CLI reported. A Claude model that is not named has no known
/// window, since windows differ by model. Every other provider reports
/// its windows through discovery, so the answer comes from `discovered`, that
/// provider's discovered models; a model the catalog does not carry has no known window. The
/// provider only needs to be *supported*, not enabled: this also reads windows
/// for sessions that already ran, and disabling a provider must not make its
/// transcripts unreadable.
pub fn context_window(
    provider: &str,
    model: Option<&str>,
    discovered: &[DiscoveredModel],
) -> Option<i64> {
    if provider.eq_ignore_ascii_case(CLAUDE_FAMILY_BACKEND) {
        return model.map(claude_context_window);
    }
    if !is_supported(&provider.to_ascii_lowercase()) {
        return None;
    }
    let model = model?;
    discovered
        .iter()
        .find(|entry| entry.model == model || entry.id == model)
        .and_then(|entry| entry.context_window)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_modality_is_its_plain_string_and_an_unknown_one_is_kept_verbatim() {
        let modalities: Modalities = serde_json::from_value(serde_json::json!({
            "input": ["text", "image", "hologram"],
            "output": ["text"]
        }))
        .unwrap();
        assert_eq!(
            modalities.input,
            vec![
                Modality::Text,
                Modality::Image,
                Modality::Other("hologram".into())
            ]
        );
        assert_eq!(
            serde_json::to_value(&modalities).unwrap(),
            serde_json::json!({"input": ["text", "image", "hologram"], "output": ["text"]})
        );
        assert_eq!(modalities.to_string(), "text+image+hologram → text");
    }

    #[test]
    fn a_served_verb_is_one_flat_object_naming_its_verb_and_wire() {
        let served = ServedVerb::conversation(Wire::AnthropicMessages);
        let json = serde_json::to_value(&served).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"verb": "complete", "wire": "anthropic-messages"},
                {"verb": "session", "wire": "anthropic-messages"}
            ])
        );
        assert_eq!(
            serde_json::from_value::<Vec<ServedVerb>>(json).unwrap(),
            served
        );
        for wire in [
            Wire::OpenAiChatCompletions,
            Wire::AnthropicMessages,
            Wire::OpenAiResponses,
            Wire::OpenAiDecisions,
            Wire::ClaudeStreamJson,
            Wire::CodexAppServer,
            Wire::OpenAiEmbeddings,
            Wire::CairnEmbed,
            Wire::OpenRouterImages,
            Wire::OpenAiImages,
            Wire::ElevenLabsStreamInput,
            Wire::OpenAiSpeech,
        ] {
            assert_eq!(serde_json::to_value(wire).unwrap(), wire.as_str());
        }
        for kind in VerbKind::ALL {
            assert_eq!(VerbKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(VerbKind::parse("speech"), None);
    }

    #[test]
    fn an_image_verb_flattens_its_capabilities_beside_its_tag_and_wire() {
        let served = ServedVerb {
            verb: Verb::Image(ImageCapabilities::openai_images_minimal()),
            wire: Wire::OpenAiImages,
        };
        let json = serde_json::to_value(&served).unwrap();
        assert_eq!(json["verb"], "image");
        assert_eq!(json["wire"], "openai-images");
        assert_eq!(json["maxCount"], 1);
        assert_eq!(json["shape"]["auto"], true);
        assert_eq!(serde_json::from_value::<ServedVerb>(json).unwrap(), served);
        assert_eq!(served.verb.kind(), VerbKind::Image);
        assert_eq!(
            served.verb.image(),
            Some(&ImageCapabilities::openai_images_minimal())
        );
        assert_eq!(Verb::Decide.image(), None);
    }

    #[test]
    fn an_embed_verb_flattens_its_capabilities_beside_verb_and_wire() {
        let served = ServedVerb {
            verb: Verb::Embed(EmbeddingCapabilities::cairn_gateway()),
            wire: Wire::CairnEmbed,
        };
        let json = serde_json::to_value(&served).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "verb": "embed",
                "nativeDims": 1536,
                "widths": "nativeOnly",
                "purpose": "native",
                "limits": {"maxTexts": 96},
                "wire": "cairn-embed"
            })
        );
        assert_eq!(serde_json::from_value::<ServedVerb>(json).unwrap(), served);
        assert_eq!(served.verb.kind(), VerbKind::Embed);
        assert_eq!(
            Modality::from("embeddings"),
            Modality::Embeddings,
            "OpenRouter's embedding output is a known modality"
        );
    }

    #[test]
    fn the_gateway_descriptor_is_never_a_shipped_backend() {
        let gateway = cairn_gateway_descriptor();
        assert_eq!(gateway.key, CAIRN_GATEWAY_KEY);
        assert!(descriptor(CAIRN_GATEWAY_KEY).is_none());
        assert!(!is_supported(CAIRN_GATEWAY_KEY));
    }

    #[test]
    fn a_speech_verb_flattens_its_capabilities_beside_its_wire() {
        use crate::speech::{AudioFormat, InputStreaming, StyleControl};
        let served = ServedVerb {
            verb: Verb::Speak(SpeechCapabilities {
                input: InputStreaming::Native,
                formats: vec![AudioFormat::pcm(16_000)],
                speed: None,
                style: StyleControl::None,
            }),
            wire: Wire::ElevenLabsStreamInput,
        };
        let json = serde_json::to_value(&served).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "verb": "speak",
                "input": "native",
                "formats": [{"encoding": "pcm-s16le", "sampleRate": 16000}],
                "style": "none",
                "wire": "elevenlabs-stream-input"
            })
        );
        assert_eq!(serde_json::from_value::<ServedVerb>(json).unwrap(), served);
        assert_eq!(served.verb.kind(), VerbKind::Speak);
    }

    #[test]
    fn a_price_is_found_on_exactly_its_meter() {
        let pricing = ModelPricing {
            prices: vec![
                Price {
                    meter: Meter::tokens(Charge::Output),
                    usd: "0.000012".into(),
                },
                Price {
                    meter: Meter {
                        charge: Charge::Output,
                        modality: Some(Modality::Image),
                        unit: BillingUnit::Tokens,
                    },
                    usd: "0.00003".into(),
                },
            ],
            unplaced: BTreeMap::new(),
        };
        assert_eq!(
            pricing.usd_per(Charge::Output, None, BillingUnit::Tokens),
            Some("0.000012")
        );
        assert_eq!(
            pricing.usd_per(Charge::Output, Some(&Modality::Image), BillingUnit::Tokens),
            Some("0.00003")
        );
        assert_eq!(
            pricing.usd_per(Charge::Output, Some(&Modality::Image), BillingUnit::Images),
            None
        );
        assert_eq!(
            pricing.usd_per(Charge::Input, None, BillingUnit::Tokens),
            None
        );
    }

    fn provider(account_id: &str, name: &str) -> CustomProvider {
        CustomProvider {
            account_id: account_id.to_string(),
            display_name: name.to_string(),
        }
    }

    #[test]
    fn a_custom_key_round_trips_through_its_account_id() {
        let key = custom_key("acc_7f3a-9c21");
        assert_eq!(key, "custom:acc_7f3a-9c21");
        assert_eq!(custom_account_id(&key), Some("acc_7f3a-9c21"));
    }

    #[test]
    fn a_malformed_custom_key_is_not_a_custom_provider() {
        // Structural, so nothing that merely starts with the prefix is treated
        // as naming an account.
        for key in [
            "custom:",
            "custom",
            "custom:acc 1",
            "custom:acc/1",
            "custom:acc:1",
            "claude",
        ] {
            assert_eq!(custom_account_id(key), None, "for {key}");
        }
    }

    #[test]
    fn every_well_formed_custom_key_is_supported_even_once_removed() {
        // What Cairn ships for a custom provider is the ADAPTER. Making support
        // depend on the account still existing would make a removed provider's
        // finished work unreadable.
        assert!(is_supported("custom:acc_never_configured"));
        assert!(!is_supported("custom:"));
        assert!(!is_supported("gemini"));
    }

    #[test]
    fn shipped_keys_never_include_a_workspaces_own_providers() {
        // `supported_keys` answers "what does this build ship", which is what
        // an unknown-provider refusal lists back to the user.
        assert!(supported_keys().all(|key| custom_account_id(key).is_none()));
    }

    #[test]
    fn a_custom_descriptor_is_named_by_its_operator() {
        let descriptor = custom_descriptor(&provider("acc_1", "DGX Spark"));
        assert_eq!(descriptor.key, "custom:acc_1");
        assert_eq!(descriptor.display_name, "DGX Spark");
        assert_eq!(descriptor.api_provider, ApiProvider::Custom);
        // Inference on the operator's own hardware: no per-token bill, no
        // provider-side usage window.
        assert_eq!(descriptor.category, ProviderCategory::Local);
    }

    #[test]
    fn a_workspace_catalog_is_the_shipped_list_plus_its_own_providers() {
        let catalog = workspace_catalog(&[provider("acc_1", "DGX Spark")]);
        assert_eq!(catalog.len(), PROVIDER_CATALOG.len() + 1);
        assert_eq!(catalog[0].key, PROVIDER_CATALOG[0].key);
        let last = catalog.last().expect("custom entry");
        assert_eq!(last.display_name, "DGX Spark");
    }

    #[test]
    fn ordering_puts_custom_providers_after_shipped_ones_in_the_order_given() {
        // An opaque account id has no meaningful alphabetical sort, so the
        // operator's own order — the sequence they added them in — is kept.
        let ordered = in_catalog_order([
            "custom:acc_zeta",
            "ollama",
            "custom:acc_alpha",
            "claude",
            "gemini",
        ]);
        assert_eq!(
            ordered,
            vec![
                "claude".to_string(),
                "ollama".to_string(),
                "custom:acc_zeta".to_string(),
                "custom:acc_alpha".to_string(),
                // A key this build cannot place is kept, not dropped: a version
                // skew must not become lost configuration.
                "gemini".to_string(),
            ]
        );
    }

    #[test]
    fn every_descriptor_has_a_unique_key_and_setup_story() {
        let mut keys: Vec<&str> = supported_keys().collect();
        let count = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), count, "provider keys must be unique");
        for entry in PROVIDER_CATALOG {
            assert!(!entry.display_name.is_empty());
            assert!(!entry.summary.is_empty());
            assert!(!entry.setup_action.is_empty());
            assert!(!entry.setup_requirement.is_empty());
        }
    }

    #[test]
    fn catalog_order_survives_arbitrary_input_order_and_keeps_unknowns() {
        let ordered = in_catalog_order(["ollama", "zeta", "claude", "claude", "openrouter"]);
        assert_eq!(ordered, vec!["claude", "openrouter", "ollama", "zeta"]);
    }

    #[test]
    fn supported_is_not_a_claim_about_enablement() {
        assert!(is_supported("ollama"));
        assert!(!is_supported("not-a-provider"));
    }

    /// Each expectation is what CLI 2.1.302 and 2.1.220 both reported for the
    /// model (`result.modelUsage[..].contextWindow`), or the smaller of the two
    /// where they disagree, except where the ID itself proves the newer CLI.
    #[test]
    fn claude_context_window_is_the_window_every_observed_cli_grants() {
        // The 5.5 generation and Fable 5.1: 1M on the CLI that resolves to them.
        assert_eq!(claude_context_window("claude-haiku-5-5"), 1_000_000);
        assert_eq!(claude_context_window("claude-sonnet-5-5"), 1_000_000);
        assert_eq!(claude_context_window("claude-opus-5-5"), 1_000_000);
        assert_eq!(claude_context_window("claude-fable-5-1"), 1_000_000);
        assert_eq!(claude_context_window("claude-sonnet-6"), 1_000_000);
        // 4.x is 200k on both CLIs, 4.6 included.
        assert_eq!(claude_context_window("claude-haiku-4-5"), 200_000);
        assert_eq!(claude_context_window("claude-haiku-4-5-20251001"), 200_000);
        assert_eq!(claude_context_window("claude-sonnet-4-5-20250929"), 200_000);
        assert_eq!(claude_context_window("claude-opus-4-6"), 200_000);
        // 5.0 is 1M on 2.1.302 but 200k on 2.1.220, which is the CLI that
        // resolves the bare aliases to it.
        assert_eq!(claude_context_window("claude-opus-5"), 200_000);
        assert_eq!(claude_context_window("claude-sonnet-5"), 200_000);
        // The `[1m]` suffix is 1M on both CLIs, whatever the model.
        assert_eq!(claude_context_window("claude-opus-5[1m]"), 1_000_000);
        assert_eq!(claude_context_window("claude-opus-5-5[1m]"), 1_000_000);
        assert_eq!(claude_context_window("sonnet[1m]"), 1_000_000);
        // Bare aliases mean whatever the executor's CLI resolves them to: 1M
        // on 2.1.302, 200k on 2.1.220. Only `opus` is launched as `opus[1m]`,
        // which is 1M on both.
        assert_eq!(claude_context_window("opus"), 1_000_000);
        assert_eq!(claude_context_window("sonnet"), 200_000);
        assert_eq!(claude_context_window("haiku"), 200_000);
        assert_eq!(claude_context_window("fable"), 200_000);
        assert_eq!(claude_context_window("mystery"), 200_000);
    }

    #[test]
    fn claude_generation_reads_the_version_after_the_family() {
        assert_eq!(claude_generation("claude-haiku-5-5"), Some((5, 5)));
        assert_eq!(claude_generation("claude-opus-5-5[1m]"), Some((5, 5)));
        assert_eq!(claude_generation("claude-fable-5-1"), Some((5, 1)));
        assert_eq!(claude_generation("claude-opus-5"), Some((5, 0)));
        assert_eq!(
            claude_generation("Claude-Sonnet-4-5-20250929"),
            Some((4, 5))
        );
        // A date stamp is not a minor version.
        assert_eq!(claude_generation("claude-opus-4-20250514"), Some((4, 0)));
        assert_eq!(claude_generation("claude-opus-4.1"), Some((4, 1)));
        assert_eq!(claude_generation("opus"), None);
        assert_eq!(claude_generation("claude-3-5-sonnet-20241022"), None);
    }

    #[test]
    fn the_reported_window_is_the_resolved_models_entry() {
        // Recorded from CLI 2.1.302: the init event named `claude-opus-5-5[1m]`
        // and the result reports usage under that same key.
        let usage = serde_json::json!({
            "claude-opus-5-5[1m]": {"contextWindow": 1_000_000, "maxOutputTokens": 128_000},
            "claude-haiku-4-5-20251001": {"contextWindow": 200_000}
        });
        assert_eq!(
            claude_reported_context_window(&usage, Some("claude-opus-5-5[1m]")),
            Some(1_000_000)
        );
        assert_eq!(
            claude_reported_context_window(&usage, Some("claude-haiku-4-5-20251001")),
            Some(200_000)
        );
        // Several models, none the resolved one: no guess.
        assert_eq!(claude_reported_context_window(&usage, Some("other")), None);
        assert_eq!(claude_reported_context_window(&usage, None), None);

        // One model is that model's report, whatever it is keyed under.
        let single = serde_json::json!({"claude-haiku-5-5": {"contextWindow": 1_000_000}});
        assert_eq!(
            claude_reported_context_window(&single, Some("haiku")),
            Some(1_000_000)
        );
        assert_eq!(
            claude_reported_context_window(&single, None),
            Some(1_000_000)
        );

        // An older CLI's result without the figure, or a nonsense one.
        let no_window = serde_json::json!({"claude-sonnet-5": {"inputTokens": 1}});
        assert_eq!(claude_reported_context_window(&no_window, None), None);
        let zero = serde_json::json!({"claude-sonnet-5": {"contextWindow": 0}});
        assert_eq!(claude_reported_context_window(&zero, None), None);
        assert_eq!(
            claude_reported_context_window(&serde_json::Value::Null, None),
            None
        );
    }

    #[test]
    fn a_claude_session_window_prefers_the_cli_report_then_the_resolved_model() {
        // What the CLI reported wins over every table answer.
        assert_eq!(
            claude_session_window(Some(200_000), Some("claude-haiku-5-5"), Some("haiku")),
            Some(200_000)
        );
        // Before any report, the resolved ID says which generation `haiku` is.
        assert_eq!(
            claude_session_window(None, Some("claude-haiku-5-5"), Some("haiku")),
            Some(1_000_000)
        );
        assert_eq!(
            claude_session_window(None, Some("claude-haiku-4-5-20251001"), Some("haiku")),
            Some(200_000)
        );
        // Before init, the selected model is all there is.
        assert_eq!(
            claude_session_window(None, None, Some("sonnet")),
            Some(200_000)
        );
        assert_eq!(claude_session_window(None, None, None), None);
    }

    #[test]
    fn shipped_claude_models_carry_their_windows_and_one_default() {
        let models = claude_models();
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["haiku", "sonnet", "opus", "fable"]);
        assert_eq!(models.iter().filter(|m| m.is_default).count(), 1);
        for model in &models {
            assert_eq!(model.context_window, Some(claude_context_window(&model.id)));
            // The CLI decides which version an alias means.
            assert_eq!(model.canonical_slug, None);
        }
    }

    fn discovered(id: &str, window: Option<i64>) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.to_string();
        model.model = id.to_string();
        model.context_window = window;
        model
    }

    #[test]
    fn context_window_reads_claude_from_its_plans_and_others_from_discovery() {
        assert_eq!(context_window("Claude", Some("opus"), &[]), Some(1_000_000));
        assert_eq!(context_window("claude", None, &[]), None);
        let found = [discovered("qwen3:32b", Some(40_960))];
        assert_eq!(
            context_window("ollama", Some("qwen3:32b"), &found),
            Some(40_960)
        );
        assert_eq!(context_window("ollama", Some("missing"), &found), None);
        assert_eq!(context_window("ollama", None, &found), None);
        // A provider this build does not ship has no windows at all.
        assert_eq!(context_window("gemini", Some("qwen3:32b"), &found), None);
        // A removed custom provider's history stays readable.
        assert_eq!(
            context_window("custom:acc_1", Some("qwen3:32b"), &found),
            Some(40_960)
        );
    }
}
