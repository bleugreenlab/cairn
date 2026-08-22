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

use std::borrow::Cow;

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
        key: Cow::Borrowed(super::opencode::OPENCODE_BACKEND_KEY),
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
        key: Cow::Borrowed(super::actual::ACTUAL_BACKEND_KEY),
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_backend_exists_for_every_catalog_entry() {
        // `backend_for_name` totalizes an unknown name to Claude, so a
        // mistyped key would silently resolve to the wrong provider rather
        // than fail. Only the Claude entry may land on that default.
        let default_backend = super::super::backend_for_name(None);
        for entry in PROVIDER_CATALOG {
            let backend = super::super::backend_for_name(Some(entry.key.as_ref()));
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
