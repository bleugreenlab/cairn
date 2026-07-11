//! Typed PDF-extraction service catalog.
//!
//! PDF reads (`read` of a local `.pdf` file or a remote `.pdf` URL) are their
//! own typed service, parallel to web fetch ([`super::web_fetch`]) and web
//! search ([`super::web_search`]). Selection is a single workspace scalar
//! `activePdf`; per-provider options live under `pdf.<provider>`.
//!
//! - **Local** (the default) — a pure-Rust extractor that works on local files
//!   and remote PDF URLs with no auth.
//! - **bmd** — routes remote PDF URLs through the configured `bmd` MCP server's
//!   `fetch` tool. bmd's HTTP fetch cannot read local bytes, so local PDF files
//!   fall back to the local extractor.

use super::provider_options::{validate_options, ProviderOption};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// The default PDF provider id (also the value when `activePdf` is absent).
pub const LOCAL: &str = "local";

/// A shipped, typed PDF-extraction provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PdfProviderId {
    Local,
    Bmd,
}

impl PdfProviderId {
    /// Every shipped provider, in display order (Local first / default).
    pub const ALL: [PdfProviderId; 2] = [PdfProviderId::Local, PdfProviderId::Bmd];

    /// The stable lowercase id used in `settings.yaml` and the UI.
    pub fn as_str(self) -> &'static str {
        match self {
            PdfProviderId::Local => "local",
            PdfProviderId::Bmd => "bmd",
        }
    }

    /// Parse an id from its lowercase string form (case-insensitive).
    pub fn from_id(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "local" => Some(PdfProviderId::Local),
            "bmd" => Some(PdfProviderId::Bmd),
            _ => None,
        }
    }

    /// Human label for the settings UI.
    pub fn label(self) -> &'static str {
        match self {
            PdfProviderId::Local => "Local",
            PdfProviderId::Bmd => "bmd",
        }
    }

    /// The per-provider option descriptors the settings UI renders generically.
    pub fn options(self) -> Vec<ProviderOption> {
        Vec::new()
    }
}

/// The configured `activePdf` provider name, if any.
pub fn active_pdf_name(config_dir: &Path) -> Option<String> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.active_pdf)
}

/// The stored options for one provider (empty when none are saved).
pub fn load_pdf_options(
    config_dir: &Path,
    id: PdfProviderId,
) -> HashMap<String, serde_yaml::Value> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.pdf)
        .and_then(|mut m| m.remove(id.as_str()))
        .unwrap_or_default()
}

/// Resolve which provider backs PDF extraction. Absent / empty / `local` ⇒
/// [`PdfProviderId::Local`]; an unknown name warns and falls back to Local.
pub fn resolve_active_pdf(config_dir: &Path) -> PdfProviderId {
    let name = match active_pdf_name(config_dir) {
        Some(n) if !n.trim().is_empty() && n != LOCAL => n,
        _ => return PdfProviderId::Local,
    };
    match PdfProviderId::from_id(&name) {
        Some(id) => id,
        None => {
            log::warn!("activePdf '{name}' is not a known PDF provider; using local");
            PdfProviderId::Local
        }
    }
}

/// Validate a provider's submitted options against its descriptor.
pub fn validate_pdf_options(
    id: PdfProviderId,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<(), String> {
    validate_options(&id.options(), options, &format!("{} PDF", id.label()))
}

/// Set (or clear) the active PDF provider. `None`, an empty string, or `local`
/// clears the scalar so the local extractor is the default.
pub fn set_active_pdf(config_dir: &Path, name: Option<&str>) -> Result<(), String> {
    super::settings::mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let key = serde_yaml::Value::String("activePdf".to_string());
        match name {
            Some(name) if !name.trim().is_empty() && name != LOCAL => {
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
pub fn upsert_pdf_options(
    config_dir: &Path,
    id: PdfProviderId,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<(), String> {
    validate_pdf_options(id, options)?;
    super::settings::mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let pdf = root
            .entry(serde_yaml::Value::String("pdf".to_string()))
            .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
        let pdf = pdf
            .as_mapping_mut()
            .ok_or_else(|| "`pdf` in config is not a mapping".to_string())?;
        pdf.insert(
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

    #[test]
    fn id_string_roundtrip() {
        for id in PdfProviderId::ALL {
            assert_eq!(PdfProviderId::from_id(id.as_str()), Some(id));
        }
        assert_eq!(PdfProviderId::from_id("ghost"), None);
    }

    #[test]
    fn resolve_defaults_to_local() {
        let ws = TempDir::new().unwrap();
        assert_eq!(resolve_active_pdf(ws.path()), PdfProviderId::Local);
        set_active_pdf(ws.path(), Some("ghost")).unwrap();
        assert_eq!(resolve_active_pdf(ws.path()), PdfProviderId::Local);
    }

    #[test]
    fn bmd_selection_and_registry_survive_general_settings_save() {
        let ws = TempDir::new().unwrap();
        upsert_pdf_options(ws.path(), PdfProviderId::Bmd, &HashMap::new()).unwrap();
        set_active_pdf(ws.path(), Some("bmd")).unwrap();
        super::super::web_search::upsert_web_search_options(
            ws.path(),
            super::super::web_search::SearchProviderId::Brave,
            &HashMap::from([("count".to_string(), serde_yaml::Value::from(7))]),
        )
        .unwrap();
        super::super::web_search::set_active_web_search(ws.path(), Some("brave")).unwrap();

        let settings = super::super::settings::load_settings(ws.path());
        super::super::settings::save_settings(ws.path(), &settings).unwrap();

        assert_eq!(active_pdf_name(ws.path()).as_deref(), Some("bmd"));
        assert!(load_pdf_options(ws.path(), PdfProviderId::Bmd).is_empty());
        assert_eq!(
            super::super::web_search::active_web_search_name(ws.path()).as_deref(),
            Some("brave")
        );
        assert_eq!(
            super::super::web_search::load_web_search_options(
                ws.path(),
                super::super::web_search::SearchProviderId::Brave
            )
            .get("count")
            .and_then(serde_yaml::Value::as_i64),
            Some(7)
        );
    }

    #[test]
    fn resolve_picks_bmd_and_preserves_unrelated_keys() {
        let ws = TempDir::new().unwrap();
        std::fs::write(
            super::super::settings::get_settings_path(ws.path()),
            "branchPrefix: custom\n",
        )
        .unwrap();
        set_active_pdf(ws.path(), Some("bmd")).unwrap();
        assert_eq!(resolve_active_pdf(ws.path()), PdfProviderId::Bmd);
        let raw =
            std::fs::read_to_string(super::super::settings::get_settings_path(ws.path())).unwrap();
        assert!(raw.contains("branchPrefix: custom"), "{raw}");
        assert!(raw.contains("activePdf: bmd"), "{raw}");
    }
}
