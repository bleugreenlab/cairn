//! The canonical catalog of providers Cairn ships.
//!
//! Four questions get asked about a provider, and they are not the same
//! question:
//!
//! - **supported** — Cairn ships an adapter for it. That is this catalog, fixed
//!   at compile time. Historical data is interpreted against it, so a job that
//!   ran on a provider stays readable forever regardless of configuration.
//! - **enabled** — this workspace installed it. That is
//!   [`crate::models::Settings::enabled_providers`], durable configuration the
//!   user owns. Only enabled providers get tabs, model discovery, picker
//!   presence, warnings, and routing participation.
//! - **configured** — a credential or host exists for it
//!   ([`crate::identity::ProviderAccount`]).
//! - **runnable** — it can serve a request right now
//!   ([`super::AgentBackend::is_available`] and the response-availability
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

use serde::Serialize;

use crate::identity::ApiProvider;

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
    /// Models served from hardware the user controls; no vendor account.
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
    pub key: &'static str,
    /// Product name for tabs, pickers, and prose.
    pub display_name: &'static str,
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
        key: "claude",
        display_name: "Anthropic",
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
        key: "codex",
        display_name: "OpenAI",
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
        key: "openrouter",
        display_name: "OpenRouter",
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
        key: super::opencode::OPENCODE_BACKEND_KEY,
        display_name: "OpenCode Go",
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
        key: "ollama",
        display_name: "Ollama",
        category: ProviderCategory::Local,
        api_provider: ApiProvider::Ollama,
        summary: "Models served from an Ollama host you run, on your own hardware.",
        setup_action: "Add host",
        setup_requirement: "Add the base URL of an Ollama host.",
        catalog_driven: false,
        supports_mcp_install: false,
        keywords: &["ollama", "local", "self-hosted", "private"],
    },
];

/// The descriptor for a backend key, if Cairn ships that provider.
pub fn descriptor(key: &str) -> Option<&'static ProviderDescriptor> {
    PROVIDER_CATALOG.iter().find(|entry| entry.key == key)
}

/// Whether Cairn ships an adapter for this backend key.
///
/// This is *supported*, not *enabled*: it answers questions about persisted
/// data and code capability, never about what a workspace has installed.
pub fn is_supported(key: &str) -> bool {
    descriptor(key).is_some()
}

/// Every shipped backend key, in product order.
pub fn supported_keys() -> impl Iterator<Item = &'static str> {
    PROVIDER_CATALOG.iter().map(|entry| entry.key)
}

/// Order `keys` by the catalog's product order, appending anything the catalog
/// does not know alphabetically.
///
/// Unknown keys are kept rather than dropped: a workspace may name a provider a
/// newer or older build ships, and silently discarding it would turn a version
/// skew into lost configuration.
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
    let mut known: Vec<String> = supported_keys()
        .filter(|key| seen.iter().any(|entry| entry == key))
        .map(|key| key.to_string())
        .collect();
    let mut unknown: Vec<String> = seen.into_iter().filter(|key| !is_supported(key)).collect();
    unknown.sort();
    known.extend(unknown);
    known
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_backend_exists_for_every_catalog_entry() {
        // `backend_for_name` totalizes an unknown name to Claude, so a
        // mistyped key would silently resolve to the wrong provider rather
        // than fail. Only the Claude entry may land on that default.
        let default_backend = super::super::backend_for_name(None);
        for entry in PROVIDER_CATALOG {
            let backend = super::super::backend_for_name(Some(entry.key));
            if entry.key == super::super::CLAUDE_FAMILY_BACKEND {
                continue;
            }
            assert_ne!(
                backend.name(),
                default_backend.name(),
                "catalog entry {} has no backend of its own",
                entry.key
            );
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
}
