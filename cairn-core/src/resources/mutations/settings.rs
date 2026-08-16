//! `write cairn://settings` patch over the workspace-global settings document.
//!
//! One patch payload carries any mix of sections; each present key routes to the
//! existing cairn-core store the Settings UI uses. Out-of-worktree (`~/.cairn`),
//! so a workspace settings write is gated by the worktree fence (raised in the
//! change handler before this runs, exactly like a workspace `cairn://mcp`
//! write). GitHub is read-only and OAuth account-add stays UI-only.

use serde_json::Value;

use crate::config::build_services::BuildServiceConfig;
use crate::config::keybinds::KeySequence;
use crate::config::settings;
use crate::identity::{ApiProvider, ProviderAuth};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::models::UpdateSettings;
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityRequest;
use cairn_common::contract::{mutation_spec, ResourceKind};
use cairn_common::uri::{parse_uri, CairnResource};

/// Scalar/app-pref + backend keys that route to `orch.update_settings`.
pub(crate) const PREF_KEYS: &[&str] = &[
    "maxThinkingTokens",
    "mergeType",
    "orphanCleanupDays",
    "repoTargetSweepDays",
    "bugReports",
    "thinkingDisplayMode",
    "transcriptTextSize",
    "transcriptDensity",
    "logLevel",
    "logRetentionDays",
    "memoryReviewEnabled",
    "memoryTriageEnabled",
    "maxOpenTriageIssuesPerScope",
    "pendingMemoryThreshold",
    "threadCompactThreshold",
    "externalReplies",
    "subscriptionFees",
    "tierDefaults",
    "tiers",
    "backends",
    "openrouterRouting",
    "routeCallsViaOpenRouter",
    "channels",
];

/// Section objects that route to dedicated stores.
pub(crate) const SECTION_KEYS: &[&str] =
    &["gitIdentities", "accounts", "keybinds", "buildServices"];

/// True when `item` is a `cairn://settings` patch.
pub(crate) fn is_workspace_settings_mutation(item: &ChangeItem) -> bool {
    item.mode == ChangeMode::Patch
        && matches!(parse_uri(&item.target), Some(CairnResource::Settings))
}

/// The authority scopes a `cairn://settings` patch normalizes to — one per
/// section the payload actually touches.
///
/// A patch is not one scope: writing `keybinds` and `backends` in the same call
/// changes two different things, one of which is a capability and one of which
/// is a preference. Naming them separately is what lets the operator approve
/// exactly the section that matters instead of a whole undifferentiated
/// "settings write".
pub(crate) fn workspace_settings_authority(
    item: &ChangeItem,
) -> Vec<Result<AuthorityRequest, String>> {
    if !is_workspace_settings_mutation(item) {
        return Vec::new();
    }
    let Some(sections) = item.payload.as_ref().and_then(Value::as_object) else {
        return Vec::new();
    };
    // A key this surface will reject is not a boundary to approve. Validating
    // first means a typo'd section is refused outright instead of raising an
    // authority card the operator must answer before dispatch rejects the write
    // anyway.
    if let Err(error) = validate_settings_keys(sections.keys().map(String::as_str)) {
        return vec![Err(error)];
    }
    sections
        .keys()
        .map(|section| {
            crate::authorization::normalize::workspace_settings_write(
                crate::authorization::WORKSPACE_ID,
                section,
            )
            .map_err(|error| error.0)
        })
        .collect()
}

/// Reject out-of-scope settings keys before the authority gate raises a card for
/// a write that cannot land anyway.
///
/// The accepted set is the contract's, not a second list: the write gate rejects
/// undeclared keys for every resource, and this consults the same `MutationSpec`
/// so the two cannot disagree. What it adds is a reason for the keys that are
/// *recognizable but deliberately unwritable*, where "not in the accepted list"
/// would be a weaker answer than saying why.
fn validate_settings_keys<'a>(keys: impl Iterator<Item = &'a str>) -> Result<(), String> {
    let spec = mutation_spec(ResourceKind::Settings, ChangeMode::Patch);
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
            other if spec.is_some_and(|spec| spec.accepts_key(other)) => {}
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

/// Authorize every section a patch touches, immediately before it applies.
///
/// Sections that policy calls ordinary preferences resolve to `Direct` and cost
/// nothing here — no grant query, no journal row — so a keybind change stays as
/// cheap as it was.
async fn authorize_settings_sections<'a>(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    sections: impl Iterator<Item = &'a str>,
) -> Result<(), String> {
    let requests = sections
        .map(|section| {
            crate::authorization::normalize::workspace_settings_write(
                crate::authorization::WORKSPACE_ID,
                section,
            )
            .map_err(|error| format!("Refused: {}", error.0))
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Skip actor resolution entirely when nothing here is a boundary, so an
    // ordinary preference write never touches the authorization tables.
    if requests.iter().all(|authority| {
        matches!(
            crate::authorization::policy::classify(&authority.scope, false),
            cairn_common::authorization::AuthorityPolicy::Direct
        )
    }) {
        return Ok(());
    }

    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(
            "Denied: changing capability-bearing workspace settings requires an authenticated \
             run to authorize"
                .to_string(),
        );
    };
    for authority in requests {
        let decision = crate::authorization::authorize(&actor, &authority).await?;
        if !decision.is_allowed() {
            return Err(crate::authorization::refusal_message(
                &authority,
                decision.reason().unwrap_or(
                    cairn_common::authorization::AuthorityReason::WorkspaceSettingsCapability,
                ),
            ));
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
    serde_json::from_value(Value::String(value.to_string())).map_err(|_| {
        format!(
            "unknown provider '{value}'; expected anthropic|openai|google|openrouter|opencode|ollama|github"
        )
    })
}

fn parse_auth(auth_type: &str, auth_value: Option<String>) -> Result<ProviderAuth, String> {
    match auth_type {
        "api_key" => Ok(ProviderAuth::ApiKey {
            value: auth_value.ok_or("authValue is required for authType=api_key")?,
        }),
        "oauth_token" => Ok(ProviderAuth::OAuthToken {
            value: auth_value.ok_or("authValue is required for authType=oauth_token")?,
        }),
        "base_url" => ProviderAuth::base_url(
            &auth_value.ok_or("authValue is required for authType=base_url")?,
        ),
        "claude_profile" => Ok(ProviderAuth::ClaudeProfile),
        other => Err(format!(
            "unknown authType '{other}'; expected api_key|oauth_token|base_url|claude_profile (OAuth browser add stays UI-only)"
        )),
    }
}

pub(super) async fn apply_settings_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let obj = payload
        .as_object()
        .ok_or("payload must be an object of settings sections")?;

    // The final authorization check, immediately before anything persists. The
    // write handler's gate decided whether to prompt; this decides whether THIS
    // write may land, re-matching against live grants and atomically consuming a
    // once-grant. Every section is checked, so a patch mixing a preference with
    // a capability cannot smuggle the capability through on the preference's
    // back.
    if !dry_run {
        authorize_settings_sections(orch, request, obj.keys().map(String::as_str)).await?;
    }

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
        let mut candidate = orch.get_keybinds();
        for item in array(section, "set")? {
            let action = require_str(item, "action")?;
            let sequence: KeySequence = match item.get("sequence") {
                None | Some(Value::Null) => Vec::new(),
                Some(value) => serde_json::from_value(value.clone())
                    .map_err(|error| format!("invalid key sequence: {error}"))?,
            };
            candidate.set_keybind(&action, sequence)?;
            count += 1;
        }
        for action in string_array(section, "reset")? {
            candidate.remove_keybind(&action)?;
            count += 1;
        }
        if section
            .get("resetAll")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            candidate.reset();
            count += 1;
        }
        if !dry_run {
            orch.save_keybinds(&candidate)?;
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

    /// The routing lists and the contract must name the same keys. The contract
    /// decides what the write gate lets through; these lists decide where an
    /// accepted key is then stored. A key in one but not the other is either a
    /// key that gates in and routes nowhere, or a key that routes but can never
    /// arrive — both are the silent-drop class this surface exists to prevent.
    #[test]
    fn routing_lists_and_contract_declare_the_same_keys() {
        let spec = mutation_spec(ResourceKind::Settings, ChangeMode::Patch)
            .expect("cairn://settings supports patch");
        let declared: std::collections::BTreeSet<&str> = spec
            .required
            .iter()
            .chain(spec.optional.iter())
            .map(|key| key.key)
            .collect();
        let routed: std::collections::BTreeSet<&str> = PREF_KEYS
            .iter()
            .chain(SECTION_KEYS.iter())
            .copied()
            .collect();
        assert_eq!(
            declared, routed,
            "cairn://settings patch contract and its routing lists disagree"
        );
    }

    #[test]
    fn accepts_known_pref_and_section_keys() {
        assert!(validate(&["mergeType", "gitIdentities", "keybinds", "buildServices"]).is_ok());
        assert!(validate(&["tierDefaults", "tiers", "backends", "accounts"]).is_ok());
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
    fn parses_ollama_provider_and_normalizes_base_url() {
        assert_eq!(parse_provider("ollama").unwrap(), ApiProvider::Ollama);
        let auth = parse_auth(
            "base_url",
            Some("  http://localhost:11434///  ".to_string()),
        )
        .unwrap();
        assert!(matches!(
            auth,
            ProviderAuth::BaseUrl { url } if url == "http://localhost:11434"
        ));
    }

    #[test]
    fn base_url_requires_valid_http_or_https_url() {
        for value in ["localhost:11434", "ftp://localhost:11434", "://bad"] {
            let error = parse_auth("base_url", Some(value.to_string())).unwrap_err();
            assert!(error.contains("invalid base URL"), "{value}: {error}");
        }
        assert!(parse_auth("base_url", Some("https://ollama.example.test/".to_string())).is_ok());
        assert!(parse_auth("base_url", None).is_err());
    }

    #[test]
    fn rejects_read_only_github_key() {
        let error = validate(&["github"]).unwrap_err();
        assert!(error.contains("read-only"), "{error}");
    }

    #[test]
    fn rejects_deprecated_and_removed_keys() {
        assert!(validate(&["systemPrompt"]).is_err());
        assert!(validate(&["autoStartJobs"]).is_err());
        assert!(validate(&["branchPrefix"]).is_err());
        assert!(validate(&["pullOnMerge"]).is_err());
    }

    #[test]
    fn rejects_unknown_keys_with_accepted_list() {
        let error = validate(&["bogusKey"]).unwrap_err();
        assert!(error.contains("unknown settings key 'bogusKey'"), "{error}");
        assert!(error.contains("mergeType"), "{error}");
    }

    #[test]
    fn every_writable_settings_key_is_classified_by_authorization_policy() {
        // The policy allowlist fails closed on an unknown section, so an
        // unclassified key would start prompting on every write rather than
        // failing loudly. Pin the two lists together here instead, so adding a
        // settings key forces a deliberate answer to "is this a capability?".
        for key in PREF_KEYS.iter().chain(SECTION_KEYS.iter()) {
            assert!(
                crate::authorization::policy::settings_section_is_classified(key),
                "settings key '{key}' is writable via cairn://settings but is not classified in \
                 authorization::policy. Add it to CAPABILITY_BEARING_SETTINGS if it grants \
                 capability, credentials, executable reach, or an outward-facing identity to \
                 every future agent; otherwise add it to LOCAL_PREFERENCE_SETTINGS."
            );
        }
    }

    #[test]
    fn a_settings_patch_names_one_scope_per_section_it_touches() {
        let item = ChangeItem {
            target: "cairn://settings".to_string(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({"keybinds": {}, "backends": {}})),
        };
        let scopes: Vec<String> = workspace_settings_authority(&item)
            .into_iter()
            .map(|request| request.unwrap().scope.shorthand())
            .collect();
        assert_eq!(scopes.len(), 2);
        assert!(scopes.contains(&"workspace/default/settings/backends:write".to_string()));
        assert!(scopes.contains(&"workspace/default/settings/keybinds:write".to_string()));
    }

    #[test]
    fn a_non_settings_item_names_no_authority_scope() {
        let item = ChangeItem {
            target: "cairn://labels".to_string(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({"backends": {}})),
        };
        assert!(workspace_settings_authority(&item).is_empty());
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
