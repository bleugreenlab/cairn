//! Typed web-search provider catalog.
//!
//! `read cairn://websearch?q=<query>` routes to a *typed* per-provider adapter
//! (see [`crate::mcp::handlers::search_web`]). Like web **fetch**
//! ([`super::web_fetch`]), web search is a closed catalog of providers Cairn
//! ships a real adapter for. Each provider knows its own request and response
//! shape; adding one means writing an adapter, not filling a form. The option
//! descriptors and validation are shared with fetch/PDF in
//! [`super::provider_options`].
//!
//! Selection is a single workspace scalar `activeWebSearch`. Per-provider
//! options live under `webSearch.<provider>` in `settings.yaml`, validated
//! against a Rust-side descriptor at save time. The API key lives in the OS
//! keychain keyed by provider id (never in `settings.yaml`). There is no
//! built-in default: an unconfigured search returns a setup message rather than
//! silently falling back.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A shipped, typed web-search provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchProviderId {
    Tavily,
    Exa,
    Brave,
    Jina,
}

impl SearchProviderId {
    /// Every shipped provider, in display order.
    pub const ALL: [SearchProviderId; 4] = [
        SearchProviderId::Tavily,
        SearchProviderId::Exa,
        SearchProviderId::Brave,
        SearchProviderId::Jina,
    ];

    /// The stable lowercase id used in `settings.yaml`, the keychain, and the UI.
    pub fn as_str(self) -> &'static str {
        match self {
            SearchProviderId::Tavily => "tavily",
            SearchProviderId::Exa => "exa",
            SearchProviderId::Brave => "brave",
            SearchProviderId::Jina => "jina",
        }
    }

    /// Parse an id from its lowercase string form (case-insensitive).
    pub fn from_id(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "tavily" => Some(SearchProviderId::Tavily),
            "exa" => Some(SearchProviderId::Exa),
            "brave" => Some(SearchProviderId::Brave),
            "jina" => Some(SearchProviderId::Jina),
            _ => None,
        }
    }

    /// Human label for the settings UI.
    pub fn label(self) -> &'static str {
        match self {
            SearchProviderId::Tavily => "Tavily",
            SearchProviderId::Exa => "Exa",
            SearchProviderId::Brave => "Brave",
            SearchProviderId::Jina => "Jina",
        }
    }

    /// Fixed env-var-style name the provider's API key is stored under in the
    /// keychain (one key per provider; no `${VAR}` editing in the UI).
    pub fn secret_var(self) -> &'static str {
        match self {
            SearchProviderId::Tavily => "TAVILY_API_KEY",
            SearchProviderId::Exa => "EXA_API_KEY",
            SearchProviderId::Brave => "BRAVE_API_KEY",
            SearchProviderId::Jina => "JINA_API_KEY",
        }
    }

    /// The per-provider option descriptors the settings UI renders generically.
    pub fn options(self) -> Vec<ProviderOption> {
        match self {
            SearchProviderId::Tavily => vec![
                ProviderOption::select(
                    "searchDepth",
                    "Search depth",
                    &[("basic", "Basic"), ("advanced", "Advanced")],
                    "basic",
                ),
                ProviderOption::number("maxResults", "Max results", 1.0, 20.0, 5.0),
                ProviderOption::select(
                    "topic",
                    "Topic",
                    &[("general", "General"), ("news", "News")],
                    "general",
                ),
            ],
            SearchProviderId::Exa => vec![
                ProviderOption::number("numResults", "Number of results", 1.0, 25.0, 10.0),
                ProviderOption::select(
                    "type",
                    "Search type",
                    &[
                        ("auto", "Auto"),
                        ("keyword", "Keyword"),
                        ("neural", "Neural"),
                    ],
                    "auto",
                ),
            ],
            SearchProviderId::Brave => vec![
                ProviderOption::number("count", "Result count", 1.0, 20.0, 10.0),
                ProviderOption::select(
                    "safesearch",
                    "Safe search",
                    &[
                        ("off", "Off"),
                        ("moderate", "Moderate"),
                        ("strict", "Strict"),
                    ],
                    "moderate",
                ),
            ],
            SearchProviderId::Jina => {
                vec![ProviderOption::number(
                    "count",
                    "Result count",
                    1.0,
                    20.0,
                    10.0,
                )]
            }
        }
    }

    /// The full descriptor a Tauri command returns for the settings UI.
    pub fn info(self) -> SearchProviderInfo {
        SearchProviderInfo {
            id: self,
            label: self.label().to_string(),
            secret_var: self.secret_var().to_string(),
            options: self.options(),
        }
    }
}

pub use super::provider_options::{Choice, OptionControl, ProviderOption};

/// The descriptor a Tauri command returns so the settings UI can render a
/// provider's key field + options without hardcoding them.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchProviderInfo {
    pub id: SearchProviderId,
    pub label: String,
    pub secret_var: String,
    pub options: Vec<ProviderOption>,
}

/// The resolved provider that backs a web search.
#[derive(Debug, Clone, PartialEq)]
pub enum ActiveSearch {
    /// No provider configured (or the named one is unknown). The read returns a
    /// setup message rather than failing.
    Unconfigured,
    /// A configured, known provider with its stored options.
    Provider {
        id: SearchProviderId,
        options: HashMap<String, serde_yaml::Value>,
    },
}

/// The configured `activeWebSearch` provider name, if any.
pub fn active_web_search_name(config_dir: &Path) -> Option<String> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.active_web_search)
}

/// The stored options for one provider (empty when none are saved).
pub fn load_web_search_options(
    config_dir: &Path,
    id: SearchProviderId,
) -> HashMap<String, serde_yaml::Value> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.web_search)
        .and_then(|mut m| m.remove(id.as_str()))
        .unwrap_or_default()
}

/// Resolve which provider backs web search. Absent / empty / unknown name ⇒
/// [`ActiveSearch::Unconfigured`] (no silent fallback).
pub fn resolve_active_search(config_dir: &Path) -> ActiveSearch {
    let name = match active_web_search_name(config_dir) {
        Some(n) if !n.trim().is_empty() => n,
        _ => return ActiveSearch::Unconfigured,
    };
    match SearchProviderId::from_id(&name) {
        Some(id) => ActiveSearch::Provider {
            id,
            options: load_web_search_options(config_dir, id),
        },
        None => {
            log::warn!("activeWebSearch '{name}' is not a known web-search provider");
            ActiveSearch::Unconfigured
        }
    }
}

/// Validate a provider's submitted options against its descriptor. Unknown keys
/// and type/range/choice mismatches are rejected so only well-formed options
/// reach `settings.yaml`.
pub fn validate_options(
    id: SearchProviderId,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<(), String> {
    super::provider_options::validate_options(
        &id.options(),
        options,
        &format!("{} web search", id.label()),
    )
}

/// Set (or clear) the active web-search provider scalar. `None` / empty clears
/// it back to unconfigured.
pub fn set_active_web_search(config_dir: &Path, name: Option<&str>) -> Result<(), String> {
    super::settings::mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let key = serde_yaml::Value::String("activeWebSearch".to_string());
        match name {
            Some(name) if !name.trim().is_empty() => {
                root.insert(key, serde_yaml::Value::String(name.to_string()));
            }
            _ => {
                root.remove(&key);
            }
        }
        Ok(())
    })
}

/// Insert or replace the stored options for one provider, after validating them
/// against the descriptor. Writes surgically through `serde_yaml::Value` so
/// unrelated settings survive.
pub fn upsert_web_search_options(
    config_dir: &Path,
    id: SearchProviderId,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<(), String> {
    validate_options(id, options)?;
    super::settings::mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let search = root
            .entry(serde_yaml::Value::String("webSearch".to_string()))
            .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
        let search = search
            .as_mapping_mut()
            .ok_or_else(|| "`webSearch` in config is not a mapping".to_string())?;
        search.insert(
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
        for id in SearchProviderId::ALL {
            assert_eq!(SearchProviderId::from_id(id.as_str()), Some(id));
        }
        assert_eq!(
            SearchProviderId::from_id("TAVILY"),
            Some(SearchProviderId::Tavily)
        );
        assert_eq!(SearchProviderId::from_id("ghost"), None);
    }

    #[test]
    fn resolve_unconfigured_when_unset_or_unknown() {
        let ws = TempDir::new().unwrap();
        assert_eq!(resolve_active_search(ws.path()), ActiveSearch::Unconfigured);
        set_active_web_search(ws.path(), Some("ghost")).unwrap();
        assert_eq!(resolve_active_search(ws.path()), ActiveSearch::Unconfigured);
    }

    #[test]
    fn resolve_picks_named_provider_with_options() {
        let ws = TempDir::new().unwrap();
        upsert_web_search_options(
            ws.path(),
            SearchProviderId::Tavily,
            &opts(&[
                ("searchDepth", serde_yaml::Value::from("advanced")),
                ("maxResults", serde_yaml::Value::from(8)),
            ]),
        )
        .unwrap();
        set_active_web_search(ws.path(), Some("tavily")).unwrap();
        match resolve_active_search(ws.path()) {
            ActiveSearch::Provider { id, options } => {
                assert_eq!(id, SearchProviderId::Tavily);
                assert_eq!(
                    options.get("searchDepth").and_then(|v| v.as_str()),
                    Some("advanced")
                );
                assert_eq!(options.get("maxResults").and_then(|v| v.as_i64()), Some(8));
            }
            other => panic!("expected provider, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_unknown_key_and_bad_values() {
        let id = SearchProviderId::Tavily;
        assert!(validate_options(id, &opts(&[("nope", serde_yaml::Value::from(1))])).is_err());
        assert!(validate_options(
            id,
            &opts(&[("searchDepth", serde_yaml::Value::from("deep"))])
        )
        .is_err());
        assert!(
            validate_options(id, &opts(&[("maxResults", serde_yaml::Value::from(99))])).is_err()
        );
        assert!(validate_options(id, &opts(&[("maxResults", serde_yaml::Value::from(5))])).is_ok());
        assert!(validate_options(
            id,
            &opts(&[("searchDepth", serde_yaml::Value::from("advanced"))])
        )
        .is_ok());
    }

    #[test]
    fn provider_selection_and_options_survive_general_settings_save() {
        let ws = TempDir::new().unwrap();
        upsert_web_search_options(
            ws.path(),
            SearchProviderId::Brave,
            &opts(&[("count", serde_yaml::Value::from(7))]),
        )
        .unwrap();
        set_active_web_search(ws.path(), Some("brave")).unwrap();
        super::super::pdf::upsert_pdf_options(
            ws.path(),
            super::super::pdf::PdfProviderId::Bmd,
            &HashMap::new(),
        )
        .unwrap();
        super::super::pdf::set_active_pdf(ws.path(), Some("bmd")).unwrap();

        let settings = super::super::settings::load_settings(ws.path());
        super::super::settings::save_settings(ws.path(), &settings).unwrap();

        assert_eq!(active_web_search_name(ws.path()).as_deref(), Some("brave"));
        assert_eq!(
            load_web_search_options(ws.path(), SearchProviderId::Brave)
                .get("count")
                .and_then(serde_yaml::Value::as_i64),
            Some(7)
        );
        assert_eq!(
            super::super::pdf::active_pdf_name(ws.path()).as_deref(),
            Some("bmd")
        );
        assert!(super::super::pdf::load_pdf_options(
            ws.path(),
            super::super::pdf::PdfProviderId::Bmd
        )
        .is_empty());
    }

    #[test]
    fn upsert_preserves_unrelated_keys() {
        let ws = TempDir::new().unwrap();
        std::fs::write(
            super::super::settings::get_settings_path(ws.path()),
            "branchPrefix: custom\n",
        )
        .unwrap();
        upsert_web_search_options(
            ws.path(),
            SearchProviderId::Brave,
            &opts(&[("count", serde_yaml::Value::from(7))]),
        )
        .unwrap();
        set_active_web_search(ws.path(), Some("brave")).unwrap();
        let raw =
            std::fs::read_to_string(super::super::settings::get_settings_path(ws.path())).unwrap();
        assert!(raw.contains("branchPrefix: custom"), "{raw}");
        assert!(raw.contains("webSearch"), "{raw}");
        assert!(raw.contains("activeWebSearch: brave"), "{raw}");
        assert_eq!(
            load_web_search_options(ws.path(), SearchProviderId::Brave)
                .get("count")
                .and_then(|v| v.as_i64()),
            Some(7)
        );
    }

    #[test]
    fn every_provider_has_options_and_secret_var() {
        for id in SearchProviderId::ALL {
            assert!(!id.secret_var().is_empty());
            assert!(!id.options().is_empty());
            // Select defaults must be a valid choice.
            for opt in id.options() {
                if let OptionControl::Select { choices, default } = &opt.control {
                    assert!(
                        choices.iter().any(|c| &c.value == default),
                        "{}.{} default `{default}` not a choice",
                        id.as_str(),
                        opt.key
                    );
                }
            }
        }
    }
}
