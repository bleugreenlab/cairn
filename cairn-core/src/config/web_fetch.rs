//! Typed web-fetch provider catalog.
//!
//! `read http(s)://…` routes to a *typed* per-provider adapter (see
//! [`crate::mcp::handlers::fetch_web`]). Like web **search**
//! ([`super::web_search`]), web fetch is a closed catalog of providers Cairn
//! ships a real adapter for — not a freeform url/method/headers form. Adding a
//! provider means writing an adapter, not filling a form.
//!
//! Selection is a single workspace scalar `activeWebFetch`. Per-provider options
//! live under `webFetch.<provider>` in `settings.yaml`, validated against a
//! Rust-side descriptor at save time. The built-in **Regular** plain-HTTP fetch
//! is the default (absent / empty / `regular`), depending on nothing.
//!
//! ## Auth kinds
//!
//! Unlike search (every provider authenticates with one API key), fetch
//! providers differ in how they authenticate ([`FetchAuth`]):
//!
//! - **Regular** — no auth; the built-in default.
//! - **Jina / Firecrawl** — an API key in the OS keychain keyed by provider id.
//! - **bmd** — no pasteable key: it reuses the configured `bmd` MCP server's
//!   OAuth connection, calling bmd's `fetch` tool through the host gateway.
//!
//! PDF extraction is a separate service ([`super::pdf`]); `.pdf` targets route
//! there rather than through a fetch provider.

use super::provider_options::{validate_options, ProviderOption};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// The built-in plain-HTTP fetch provider id (also the default when
/// `activeWebFetch` is absent or empty).
pub const REGULAR: &str = "regular";

/// A shipped, typed web-fetch provider (beyond the built-in `regular`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FetchProviderId {
    Bmd,
    Jina,
    Firecrawl,
}

impl FetchProviderId {
    /// Every shipped provider, in display order.
    pub const ALL: [FetchProviderId; 3] = [
        FetchProviderId::Bmd,
        FetchProviderId::Jina,
        FetchProviderId::Firecrawl,
    ];

    /// The stable lowercase id used in `settings.yaml`, the keychain, and the UI.
    pub fn as_str(self) -> &'static str {
        match self {
            FetchProviderId::Bmd => "bmd",
            FetchProviderId::Jina => "jina",
            FetchProviderId::Firecrawl => "firecrawl",
        }
    }

    /// Parse an id from its lowercase string form (case-insensitive).
    pub fn from_id(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "bmd" => Some(FetchProviderId::Bmd),
            "jina" => Some(FetchProviderId::Jina),
            "firecrawl" => Some(FetchProviderId::Firecrawl),
            _ => None,
        }
    }

    /// Human label for the settings UI.
    pub fn label(self) -> &'static str {
        match self {
            FetchProviderId::Bmd => "bmd",
            FetchProviderId::Jina => "Jina",
            FetchProviderId::Firecrawl => "Firecrawl",
        }
    }

    /// How this provider authenticates.
    pub fn auth(self) -> FetchAuth {
        match self {
            FetchProviderId::Bmd => FetchAuth::Mcp { server: "bmd" },
            FetchProviderId::Jina => FetchAuth::ApiKey {
                secret_var: "JINA_API_KEY",
            },
            FetchProviderId::Firecrawl => FetchAuth::ApiKey {
                secret_var: "FIRECRAWL_API_KEY",
            },
        }
    }

    /// The per-provider option descriptors the settings UI renders generically.
    pub fn options(self) -> Vec<ProviderOption> {
        match self {
            FetchProviderId::Bmd | FetchProviderId::Jina => Vec::new(),
            FetchProviderId::Firecrawl => vec![ProviderOption::bool(
                "onlyMainContent",
                "Main content only",
                true,
            )],
        }
    }
}

/// How a fetch provider authenticates. The key new concept relative to web
/// search (which has only an API key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchAuth {
    /// No authentication (the built-in regular fetch).
    None,
    /// An API key in the OS keychain under `secret_var`, keyed by provider id.
    ApiKey { secret_var: &'static str },
    /// Reuses the named MCP server's connection (OAuth) via the host gateway.
    Mcp { server: &'static str },
}

impl FetchAuth {
    /// The stable string tag the settings UI switches on.
    pub fn kind(&self) -> &'static str {
        match self {
            FetchAuth::None => "none",
            FetchAuth::ApiKey { .. } => "apiKey",
            FetchAuth::Mcp { .. } => "mcp",
        }
    }

    /// The keychain var name for an `ApiKey` provider.
    pub fn secret_var(&self) -> Option<&'static str> {
        match self {
            FetchAuth::ApiKey { secret_var } => Some(secret_var),
            _ => None,
        }
    }

    /// The backing MCP server name for an `Mcp` provider.
    pub fn mcp_server(&self) -> Option<&'static str> {
        match self {
            FetchAuth::Mcp { server } => Some(server),
            _ => None,
        }
    }
}

/// The resolved provider that backs a fetch request.
#[derive(Debug, Clone, PartialEq)]
pub enum ActiveFetch {
    /// The built-in plain-HTTP fetch (the default).
    Regular,
    /// A configured, known typed provider with its stored options.
    Provider {
        id: FetchProviderId,
        options: HashMap<String, serde_yaml::Value>,
    },
}

/// The configured `activeWebFetch` provider name, if any.
pub fn active_web_fetch_name(config_dir: &Path) -> Option<String> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.active_web_fetch)
}

/// The stored options for one provider (empty when none are saved).
pub fn load_web_fetch_options(
    config_dir: &Path,
    id: FetchProviderId,
) -> HashMap<String, serde_yaml::Value> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.web_fetch)
        .and_then(|mut m| m.remove(id.as_str()))
        .unwrap_or_default()
}

/// Resolve which provider backs fetch. Absent / empty / `regular` ⇒ the built-in
/// regular fetch. An unknown name warns and falls back to regular (matching the
/// former registry's no-fail behaviour).
pub(crate) fn resolve_active_fetch(config_dir: &Path) -> ActiveFetch {
    let name = match active_web_fetch_name(config_dir) {
        Some(n) if !n.trim().is_empty() && n != REGULAR => n,
        _ => return ActiveFetch::Regular,
    };
    match FetchProviderId::from_id(&name) {
        Some(id) => ActiveFetch::Provider {
            id,
            options: load_web_fetch_options(config_dir, id),
        },
        None => {
            log::warn!("activeWebFetch '{name}' is not a known web-fetch provider; using regular");
            ActiveFetch::Regular
        }
    }
}

/// Validate a provider's submitted options against its descriptor.
fn validate_fetch_options(
    id: FetchProviderId,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<(), String> {
    validate_options(&id.options(), options, &format!("{} web fetch", id.label()))
}

/// Set (or clear) the active fetch provider. `None`, an empty string, or
/// `regular` clears the scalar so the built-in regular fetch is the default.
pub fn set_active_web_fetch(config_dir: &Path, name: Option<&str>) -> Result<(), String> {
    super::settings::mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let key = serde_yaml::Value::String("activeWebFetch".to_string());
        match name {
            Some(name) if !name.trim().is_empty() && name != REGULAR => {
                root.insert(key, serde_yaml::Value::String(name.to_string()));
            }
            _ => {
                root.remove(&key);
            }
        }
        Ok(())
    })
}

/// Insert or replace the stored options for one provider, after validating them.
/// Writes surgically through `serde_yaml::Value` so unrelated settings survive.
pub fn upsert_web_fetch_options(
    config_dir: &Path,
    id: FetchProviderId,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<(), String> {
    validate_fetch_options(id, options)?;
    super::settings::mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let fetch = root
            .entry(serde_yaml::Value::String("webFetch".to_string()))
            .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
        let fetch = fetch
            .as_mapping_mut()
            .ok_or_else(|| "`webFetch` in config is not a mapping".to_string())?;
        fetch.insert(
            serde_yaml::Value::String(id.as_str().to_string()),
            serde_yaml::to_value(options)
                .map_err(|error| format!("Failed to serialize options: {error}"))?,
        );
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn opts(pairs: &[(&str, serde_yaml::Value)]) -> HashMap<String, serde_yaml::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn id_string_roundtrip() {
        for id in FetchProviderId::ALL {
            assert_eq!(FetchProviderId::from_id(id.as_str()), Some(id));
        }
        assert_eq!(
            FetchProviderId::from_id("JINA"),
            Some(FetchProviderId::Jina)
        );
        assert_eq!(FetchProviderId::from_id("ghost"), None);
    }

    #[test]
    fn auth_kinds_match_providers() {
        assert_eq!(
            FetchProviderId::Bmd.auth(),
            FetchAuth::Mcp { server: "bmd" }
        );
        assert_eq!(
            FetchProviderId::Jina.auth().secret_var(),
            Some("JINA_API_KEY")
        );
        assert_eq!(FetchProviderId::Bmd.auth().mcp_server(), Some("bmd"));
        assert_eq!(FetchProviderId::Jina.auth().kind(), "apiKey");
        assert_eq!(FetchAuth::None.kind(), "none");
    }

    #[test]
    fn resolve_defaults_to_regular_when_unset_or_unknown() {
        let ws = TempDir::new().unwrap();
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);
        set_active_web_fetch(ws.path(), Some("ghost")).unwrap();
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);
        set_active_web_fetch(ws.path(), Some("regular")).unwrap();
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);
    }

    #[test]
    fn resolve_picks_named_provider_with_options() {
        let ws = TempDir::new().unwrap();
        upsert_web_fetch_options(
            ws.path(),
            FetchProviderId::Firecrawl,
            &opts(&[("onlyMainContent", serde_yaml::Value::from(false))]),
        )
        .unwrap();
        set_active_web_fetch(ws.path(), Some("firecrawl")).unwrap();
        match resolve_active_fetch(ws.path()) {
            ActiveFetch::Provider { id, options } => {
                assert_eq!(id, FetchProviderId::Firecrawl);
                assert_eq!(
                    options.get("onlyMainContent").and_then(|v| v.as_bool()),
                    Some(false)
                );
            }
            other => panic!("expected provider, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_unknown_key() {
        assert!(validate_fetch_options(
            FetchProviderId::Firecrawl,
            &opts(&[("nope", serde_yaml::Value::from(1))])
        )
        .is_err());
        assert!(validate_fetch_options(
            FetchProviderId::Firecrawl,
            &opts(&[("onlyMainContent", serde_yaml::Value::from(true))])
        )
        .is_ok());
    }

    #[test]
    fn upsert_preserves_unrelated_keys() {
        let ws = TempDir::new().unwrap();
        std::fs::write(
            super::super::settings::get_settings_path(ws.path()),
            "branchPrefix: custom\n",
        )
        .unwrap();
        upsert_web_fetch_options(
            ws.path(),
            FetchProviderId::Firecrawl,
            &opts(&[("onlyMainContent", serde_yaml::Value::from(true))]),
        )
        .unwrap();
        set_active_web_fetch(ws.path(), Some("firecrawl")).unwrap();
        let raw =
            std::fs::read_to_string(super::super::settings::get_settings_path(ws.path())).unwrap();
        assert!(raw.contains("branchPrefix: custom"), "{raw}");
        assert!(raw.contains("webFetch"), "{raw}");
        assert!(raw.contains("activeWebFetch: firecrawl"), "{raw}");
    }

    #[test]
    fn existing_bmd_selection_still_resolves() {
        // The migration story: a current user's `activeWebFetch: bmd` keeps working.
        let ws = TempDir::new().unwrap();
        set_active_web_fetch(ws.path(), Some("bmd")).unwrap();
        match resolve_active_fetch(ws.path()) {
            ActiveFetch::Provider { id, .. } => assert_eq!(id, FetchProviderId::Bmd),
            other => panic!("expected bmd provider, got {other:?}"),
        }
    }
}
