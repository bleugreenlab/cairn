use serde::{Deserialize, Serialize};
/// API provider that a credential authenticates with.
///
/// Credentials belong to providers, not backends. A single Anthropic API key
/// works across both the Claude CLI backend and the Native HTTP backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ApiProvider {
    /// Anthropic APIs (Claude)
    #[serde(rename = "anthropic")]
    Anthropic,
    /// OpenAI APIs (GPT, Codex)
    #[serde(rename = "openai", alias = "open_ai", alias = "open_a_i")]
    OpenAI,
    /// Google APIs (Gemini)
    #[serde(rename = "google")]
    Google,
    /// OpenRouter APIs
    #[serde(rename = "openrouter", alias = "open_router")]
    OpenRouter,
    /// OpenCode Zen APIs. One key spans the Zen surfaces; the Go subscription is
    /// the one Cairn serves today, which is why the credential is named for the
    /// account and the backend is named for the subscription.
    #[serde(rename = "opencode", alias = "open_code")]
    OpenCode,
    /// TypeSafe's API (Jev). Its key reaches System One, which answers
    /// decisions only, so the one backend it serves has no agents or responses.
    #[serde(rename = "typesafe")]
    TypeSafe,
    /// Ollama APIs
    #[serde(rename = "ollama")]
    Ollama,
    /// Actual Computer. One account authorizes many devices, and the same
    /// clusters are reachable both from the device itself and through Actual's
    /// Private Relay, so a credential here names the account rather than any
    /// single endpoint.
    #[serde(rename = "actual")]
    Actual,
    /// GitHub APIs
    #[serde(rename = "github", alias = "git_hub")]
    GitHub,
    /// The token for bginf's control service, which Cairn mirrors read-only
    /// into its inference records. Like GitHub, it serves no model backend.
    /// This variant retires with bginf, when Cairn's own inference control
    /// replaces it; it is not a lasting provider.
    #[serde(rename = "bginf")]
    Bginf,
    /// An HTTP inference service the operator runs and names themselves.
    ///
    /// Unlike every other variant, this one does not identify a vendor. It
    /// identifies a *kind* of account: one account here is one named custom
    /// provider, and its `ProviderAuth::CustomEndpoint` carries where to reach
    /// it, what protocol it speaks, and how its models are known. The account id
    /// is that provider's stable identity, which is why renaming it or moving it
    /// to a new address does not disturb anything that already selected it.
    #[serde(rename = "custom")]
    Custom,
}

impl ApiProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiProvider::Anthropic => "anthropic",
            ApiProvider::OpenAI => "openai",
            ApiProvider::Google => "google",
            ApiProvider::OpenRouter => "openrouter",
            ApiProvider::OpenCode => "opencode",
            ApiProvider::TypeSafe => "typesafe",
            ApiProvider::Ollama => "ollama",
            ApiProvider::Actual => "actual",
            ApiProvider::GitHub => "github",
            ApiProvider::Bginf => "bginf",
            ApiProvider::Custom => "custom",
        }
    }

    /// All providers in display order.
    pub fn all() -> &'static [ApiProvider] {
        &[
            ApiProvider::Anthropic,
            ApiProvider::OpenAI,
            ApiProvider::Google,
            ApiProvider::OpenRouter,
            ApiProvider::OpenCode,
            ApiProvider::TypeSafe,
            ApiProvider::Ollama,
            ApiProvider::Actual,
            ApiProvider::GitHub,
            ApiProvider::Bginf,
            ApiProvider::Custom,
        ]
    }
}

impl std::fmt::Display for ApiProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiProvider::Anthropic => write!(f, "Anthropic"),
            ApiProvider::OpenAI => write!(f, "OpenAI"),
            ApiProvider::Google => write!(f, "Google"),
            ApiProvider::OpenRouter => write!(f, "OpenRouter"),
            ApiProvider::OpenCode => write!(f, "OpenCode"),
            ApiProvider::TypeSafe => write!(f, "TypeSafe"),
            ApiProvider::Ollama => write!(f, "Ollama"),
            ApiProvider::Actual => write!(f, "Actual"),
            ApiProvider::GitHub => write!(f, "GitHub"),
            ApiProvider::Bginf => write!(f, "bginf"),
            ApiProvider::Custom => write!(f, "Custom"),
        }
    }
}
