//! `write cairn://settings` patch over the workspace-global settings document.
//!
//! One patch payload carries any mix of sections; each present key routes to the
//! existing cairn-core store the Settings UI uses. Out-of-worktree (`~/.cairn`),
//! so a workspace settings write is gated by the worktree fence (raised in the
//! change handler before this runs, exactly like a workspace `cairn://mcp`
//! write). GitHub is read-only and OAuth account-add stays UI-only.

use serde_json::Value;

use crate::config::build_services::BuildServiceConfig;
use crate::config::keybinds::Modifier;
use crate::config::settings;
use crate::identity::{ApiProvider, ProviderAuth};
use crate::mcp::types::{ChangeItem, ChangeMode};
use crate::models::UpdateSettings;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{parse_uri, CairnResource};

/// Scalar/app-pref + backend keys that route to `orch.update_settings`.
const PREF_KEYS: &[&str] = &[
    "branchPrefix",
    "maxThinkingTokens",
    "mergeType",
    "pullOnMerge",
    "orphanCleanupDays",
    "repoTargetSweepDays",
    "bugReports",
    "thinkingDisplayMode",
    "logLevel",
    "pendingMemoryThreshold",
    "externalReplies",
    "subscriptionFees",
    "activeBackend",
    "tiers",
    "backends",
    "openrouterRouting",
];

/// Section objects that route to dedicated stores.
const SECTION_KEYS: &[&str] = &["gitIdentities", "accounts", "keybinds", "buildServices"];

/// True when `item` is a `cairn://settings` patch: an out-of-worktree write to
/// `~/.cairn` that the worktree fence must gate, mirroring workspace MCP writes.
pub(crate) fn is_workspace_settings_mutation(item: &ChangeItem) -> bool {
    item.mode == ChangeMode::Patch
        && matches!(parse_uri(&item.target), Some(CairnResource::Settings))
}

/// Reject out-of-scope and unknown top-level settings keys before any section
/// applies. GitHub is read-only; removed/deprecated fields are not writable;
/// everything else must be a known pref or section key.
fn validate_settings_keys<'a>(keys: impl Iterator<Item = &'a str>) -> Result<(), String> {
    for key in keys {
        match key {
            "github" => {
                return Err(
                    "github is read-only via cairn://settings; connect/disconnect is UI-only"
                        .to_string(),
                )
            }
            "systemPrompt" | "autoStartJobs" => {
                return Err(format!("'{key}' is deprecated and not writable"))
            }
            other if PREF_KEYS.contains(&other) || SECTION_KEYS.contains(&other) => {}
            other => {
                return Err(format!(
                    "unknown settings key '{other}'. Accepted: {}, {}",
                    PREF_KEYS.join(", "),
                    SECTION_KEYS.join(", ")
                ))
            }
        }
    }
    Ok(())
}

fn require_str(obj: &Value, key: &str) -> Result<String, String> {
    obj.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("'{key}' is required and must be a non-empty string"))
}

fn opt_str(obj: &Value, key: &str) -> Option<String> {
    obj.get(key).and_then(Value::as_str).map(str::to_string)
}

fn array<'a>(value: &'a Value, key: &str) -> Result<Vec<&'a Value>, String> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.iter().collect()),
        Some(_) => Err(format!("'{key}' must be an array")),
    }
}

fn string_array(value: &Value, key: &str) -> Result<Vec<String>, String> {
    array(value, key)?
        .into_iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("'{key}' entries must be strings"))
        })
        .collect()
}

fn parse_provider(value: &str) -> Result<ApiProvider, String> {
    serde_json::from_value(Value::String(value.to_string()))
        .map_err(|_| format!("unknown provider '{value}'; expected anthropic|openai|google|github"))
}

fn parse_auth(auth_type: &str, auth_value: Option<String>) -> Result<ProviderAuth, String> {
    match auth_type {
        "api_key" => Ok(ProviderAuth::ApiKey {
            value: auth_value.ok_or("authValue is required for authType=api_key")?,
        }),
        "oauth_token" => Ok(ProviderAuth::OAuthToken {
            value: auth_value.ok_or("authValue is required for authType=oauth_token")?,
        }),
        "local_cli" => Ok(ProviderAuth::LocalCli),
        other => Err(format!(
            "unknown authType '{other}'; expected api_key|oauth_token|local_cli (OAuth browser add stays UI-only)"
        )),
    }
}

pub(super) async fn apply_settings_patch(
    orch: &Orchestrator,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let obj = payload
        .as_object()
        .ok_or("payload must be an object of settings sections")?;

    // Validate keys up front so a typo or an out-of-scope write fails before any
    // section applies.
    validate_settings_keys(obj.keys().map(String::as_str))?;

    let mut summary: Vec<String> = Vec::new();

    // --- App prefs + backends (UpdateSettings) ---
    if PREF_KEYS.iter().any(|key| obj.contains_key(*key)) {
        // UpdateSettings ignores unknown keys, so the section objects pass through
        // harmlessly; only the DTO fields are read.
        let update: UpdateSettings = serde_json::from_value(payload.clone())
            .map_err(|error| format!("invalid settings preferences: {error}"))?;
        if !dry_run {
            orch.update_settings(update)?;
        }
        summary.push("app preferences".to_string());
    }

    // --- Git identities ---
    if let Some(section) = obj.get("gitIdentities") {
        let mut count = 0;
        for item in array(section, "add")? {
            let label = require_str(item, "label")?;
            let name = require_str(item, "name")?;
            let email = require_str(item, "email")?;
            if !dry_run {
                orch.add_git_identity(label, name, email)?;
            }
            count += 1;
        }
        for item in array(section, "update")? {
            let id = require_str(item, "id")?;
            if !dry_run {
                orch.update_git_identity(
                    &id,
                    opt_str(item, "label"),
                    opt_str(item, "name"),
                    opt_str(item, "email"),
                )?;
            }
            count += 1;
        }
        for id in string_array(section, "remove")? {
            if !dry_run {
                orch.remove_git_identity(&id)?;
            }
            count += 1;
        }
        let order = string_array(section, "order")?;
        if !order.is_empty() {
            if !dry_run {
                orch.reorder_git_identities(&order)?;
            }
            count += 1;
        }
        summary.push(format!("git identities ({count} op(s))"));
    }

    // --- Provider accounts (non-interactive auth only) ---
    if let Some(section) = obj.get("accounts") {
        let mut count = 0;
        for item in array(section, "add")? {
            let provider = parse_provider(&require_str(item, "provider")?)?;
            let label = require_str(item, "label")?;
            let auth_type = require_str(item, "authType")?;
            let auth = parse_auth(&auth_type, opt_str(item, "authValue"))?;
            if !dry_run {
                orch.add_account(provider, label, auth, None)?;
            }
            count += 1;
        }
        for item in array(section, "update")? {
            let id = require_str(item, "id")?;
            if !dry_run {
                orch.update_account(&id, opt_str(item, "label"))?;
            }
            count += 1;
        }
        for id in string_array(section, "remove")? {
            if !dry_run {
                orch.remove_account(&id)?;
            }
            count += 1;
        }
        if let Some(order) = section.get("order").filter(|v| !v.is_null()) {
            let provider = parse_provider(&require_str(order, "provider")?)?;
            let ids = string_array(order, "ids")?;
            if !dry_run {
                orch.reorder_accounts(provider, &ids)?;
            }
            count += 1;
        }
        summary.push(format!("accounts ({count} op(s))"));
    }

    // --- Keybinds ---
    if let Some(section) = obj.get("keybinds") {
        let mut count = 0;
        for item in array(section, "set")? {
            let action = require_str(item, "action")?;
            let key = opt_str(item, "key").unwrap_or_default();
            let modifiers: Vec<Modifier> = match item.get("modifiers") {
                None | Some(Value::Null) => Vec::new(),
                Some(value) => serde_json::from_value(value.clone())
                    .map_err(|error| format!("invalid modifiers: {error}"))?,
            };
            if !dry_run {
                orch.set_keybind(&action, key, modifiers)?;
            }
            count += 1;
        }
        for action in string_array(section, "reset")? {
            if !dry_run {
                orch.reset_keybind(&action)?;
            }
            count += 1;
        }
        if section
            .get("resetAll")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if !dry_run {
                orch.reset_all_keybinds()?;
            }
            count += 1;
        }
        summary.push(format!("keybinds ({count} op(s))"));
    }

    // --- Build services ---
    if let Some(section) = obj.get("buildServices") {
        let mut count = 0;
        let mut needs_ready = false;
        for item in array(section, "upsert")? {
            let name = require_str(item, "name")?;
            let config_value = item
                .get("config")
                .ok_or("buildServices.upsert entries require a 'config' object")?;
            let config: BuildServiceConfig = serde_json::from_value(config_value.clone())
                .map_err(|error| format!("invalid build service config: {error}"))?;
            if !dry_run {
                settings::upsert_build_service(&orch.config_dir, &name, &config)?;
            }
            needs_ready = needs_ready || config.enabled;
            count += 1;
        }
        for item in array(section, "setEnabled")? {
            let name = require_str(item, "name")?;
            let enabled = item
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or("buildServices.setEnabled entries require a boolean 'enabled'")?;
            if !dry_run {
                settings::set_build_service_enabled(&orch.config_dir, &name, enabled)?;
            }
            needs_ready = needs_ready || enabled;
            count += 1;
        }
        for name in string_array(section, "remove")? {
            if !dry_run {
                settings::delete_build_service(&orch.config_dir, &name)?;
            }
            count += 1;
        }
        if needs_ready && !dry_run {
            orch.ensure_build_services_ready();
        }
        summary.push(format!("build services ({count} op(s))"));
    }

    if summary.is_empty() {
        return Err("payload contained no recognized settings sections".to_string());
    }

    let verb = if dry_run { "Would update" } else { "Updated" };
    Ok(format!("{verb} workspace settings: {}", summary.join("; ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::types::ChangeItem;

    fn validate(keys: &[&str]) -> Result<(), String> {
        validate_settings_keys(keys.iter().copied())
    }

    #[test]
    fn accepts_known_pref_and_section_keys() {
        assert!(validate(&["branchPrefix", "gitIdentities", "keybinds", "buildServices"]).is_ok());
        assert!(validate(&["activeBackend", "tiers", "backends", "accounts"]).is_ok());
    }

    #[test]
    fn accepts_openrouter_routing_and_doc_parity_keys() {
        // openrouterRouting is the new routing object; logLevel/subscriptionFees
        // are documented URI-writable prefs that were missing from the allowlist.
        assert!(validate(&["openrouterRouting", "logLevel", "subscriptionFees"]).is_ok());
    }

    #[test]
    fn pref_keys_is_superset_of_uri_writable_update_settings_fields() {
        // PREF_KEYS gates which keys a `cairn://settings` patch routes to
        // `update_settings`. Every non-deprecated `UpdateSettings` field is meant
        // to be URI-writable (docs/settings.md), so a field added to the DTO
        // without a matching PREF_KEYS entry would be silently rejected as an
        // "unknown settings key". Reflect the DTO's field names via serde and
        // assert PREF_KEYS covers them, so that drift fails CI instead of
        // shipping a dead key.
        const DEPRECATED: &[&str] = &["systemPrompt", "autoStartJobs"];
        let serialized = serde_json::to_value(UpdateSettings::default())
            .expect("UpdateSettings serializes to JSON");
        let fields = serialized
            .as_object()
            .expect("UpdateSettings serializes to an object");
        for key in fields.keys() {
            if DEPRECATED.contains(&key.as_str()) {
                continue;
            }
            assert!(
                PREF_KEYS.contains(&key.as_str()),
                "UpdateSettings field '{key}' is URI-writable but missing from \
                 PREF_KEYS; add it to the allowlist in \
                 resources/mutations/settings.rs so cairn://settings accepts it"
            );
        }
    }

    #[test]
    fn rejects_read_only_github_key() {
        let error = validate(&["github"]).unwrap_err();
        assert!(error.contains("read-only"), "{error}");
    }

    #[test]
    fn rejects_deprecated_keys() {
        assert!(validate(&["systemPrompt"]).is_err());
        assert!(validate(&["autoStartJobs"]).is_err());
    }

    #[test]
    fn rejects_unknown_keys_with_accepted_list() {
        let error = validate(&["bogusKey"]).unwrap_err();
        assert!(error.contains("unknown settings key 'bogusKey'"), "{error}");
        assert!(error.contains("branchPrefix"), "{error}");
    }

    #[test]
    fn detects_workspace_settings_patch_only() {
        let item = |target: &str, mode: ChangeMode| ChangeItem {
            target: target.to_string(),
            mode,
            payload: None,
        };
        assert!(is_workspace_settings_mutation(&item(
            "cairn://settings",
            ChangeMode::Patch
        )));
        // A read-shaped mode or a different target is not a settings write.
        assert!(!is_workspace_settings_mutation(&item(
            "cairn://settings",
            ChangeMode::Create
        )));
        assert!(!is_workspace_settings_mutation(&item(
            "cairn://labels",
            ChangeMode::Patch
        )));
    }
}
