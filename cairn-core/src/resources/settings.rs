//! Workspace settings resource reads (`cairn://settings`).
//!
//! Renders the whole settings document — app preferences, backends (plus a
//! read-only model catalog and provider usage), git identities, provider
//! accounts, keybinds, build services, and read-only GitHub status. Every
//! section is sourced from the same cairn-core stores the Settings UI uses.

use crate::orchestrator::Orchestrator;

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

pub(super) async fn read_settings(orch: &Orchestrator) -> String {
    let mut out = String::from("# Workspace settings\n\n");

    // --- App preferences ---
    let settings = orch.get_settings();
    out.push_str("## App preferences\n\n");
    out.push_str(&format!("- branchPrefix: `{}`\n", settings.branch_prefix));
    out.push_str(&format!(
        "- maxThinkingTokens: {}\n",
        settings
            .max_thinking_tokens
            .map(|t| t.to_string())
            .unwrap_or_else(|| "disabled".to_string())
    ));
    out.push_str(&format!("- mergeType: {:?}\n", settings.merge_type));
    out.push_str(&format!(
        "- pullOnMerge: {}\n",
        yes_no(settings.pull_on_merge)
    ));
    out.push_str(&format!(
        "- orphanCleanupDays: {}\n",
        settings.orphan_cleanup_days
    ));
    out.push_str(&format!(
        "- repoTargetSweepDays: {}\n",
        settings.repo_target_sweep_days
    ));
    out.push_str(&format!("- bugReports: {}\n", yes_no(settings.bug_reports)));
    out.push_str(&format!(
        "- thinkingDisplayMode: {:?}\n",
        settings.thinking_display_mode
    ));
    out.push_str(&format!(
        "- pendingMemoryThreshold: {}\n",
        settings.pending_memory_threshold
    ));
    out.push_str(&format!(
        "- externalReplies: {:?}\n\n",
        settings.external_replies
    ));

    // --- Backends ---
    out.push_str("## Backends\n\n");
    out.push_str(&format!("- activeBackend: `{}`\n", settings.active_backend));
    out.push_str(&format!("- tiers: {}\n\n", settings.tiers.join(", ")));
    let mut backend_names: Vec<&String> = settings.backends.keys().collect();
    backend_names.sort();
    for backend in backend_names {
        let presets = &settings.backends[backend];
        out.push_str(&format!("### {backend}\n\n"));
        for tier in &settings.tiers {
            if let Some(preset) = presets.get(tier) {
                out.push_str(&format!("- {}: `{}`\n", tier, preset.model.as_str()));
            }
        }
        out.push('\n');
    }

    // --- Model catalog (read-only) ---
    let catalog = orch.get_model_catalog();
    if !catalog.is_empty() {
        out.push_str("## Model catalog (read-only)\n\n");
        for entry in &catalog {
            let visible = entry.models.iter().filter(|m| !m.hidden).count();
            out.push_str(&format!("- {}: {} model(s)", entry.backend, visible));
            if let Some(error) = &entry.error {
                out.push_str(&format!(" — error: {error}"));
            }
            out.push('\n');
        }
        out.push('\n');
    }

    // --- Provider usage (read-only) ---
    if let Ok(snapshots) = orch.provider_usage_snapshots.read() {
        if !snapshots.is_empty() {
            out.push_str("## Provider usage (read-only)\n\n");
            let mut keys: Vec<&String> = snapshots.keys().collect();
            keys.sort();
            for key in keys {
                let snapshot = &snapshots[key];
                out.push_str(&format!(
                    "- {}: {} window(s)",
                    snapshot.backend,
                    snapshot.windows.len()
                ));
                if let Some(reason) = &snapshot.unsupported_reason {
                    out.push_str(&format!(" — unsupported: {reason}"));
                } else if let Some(error) = &snapshot.error {
                    out.push_str(&format!(" — error: {error}"));
                }
                out.push('\n');
            }
            out.push('\n');
        }
    }

    // --- Git identities ---
    out.push_str("## Git identities\n\n");
    let identities = orch.list_git_identities();
    if identities.is_empty() {
        out.push_str("None configured.\n\n");
    } else {
        for (index, identity) in identities.iter().enumerate() {
            let default = if index == 0 { " (default)" } else { "" };
            out.push_str(&format!(
                "- `{}` — {} <{}>{}\n",
                identity.id, identity.name, identity.email, default
            ));
        }
        out.push('\n');
    }

    // --- Provider accounts (state; OAuth browser add stays UI-only) ---
    out.push_str("## Provider accounts\n\n");
    let accounts = orch.list_accounts(None);
    if accounts.is_empty() {
        out.push_str("None configured.\n\n");
    } else {
        for account in &accounts {
            out.push_str(&format!(
                "- `{}` — {} [{:?}] auth={} source={:?}\n",
                account.id, account.label, account.api_provider, account.auth_type, account.source
            ));
        }
        out.push('\n');
    }

    // --- Keybinds ---
    out.push_str("## Keybinds\n\n");
    let keybinds = orch.get_keybinds();
    if keybinds.keybinds.is_empty() {
        out.push_str("No customizations (defaults in effect).\n\n");
    } else {
        for keybind in &keybinds.keybinds {
            let mods = keybind
                .modifiers
                .iter()
                .map(|m| format!("{m:?}"))
                .collect::<Vec<_>>()
                .join("+");
            let combo = if keybind.key.is_empty() {
                "(disabled)".to_string()
            } else if mods.is_empty() {
                keybind.key.clone()
            } else {
                format!("{mods}+{}", keybind.key)
            };
            out.push_str(&format!("- {}: {}\n", keybind.action, combo));
        }
        out.push('\n');
    }

    // --- Build services ---
    out.push_str("## Build services\n\n");
    let services = orch.build_service_statuses();
    if services.is_empty() {
        out.push_str("None configured.\n\n");
    } else {
        for service in &services {
            out.push_str(&format!(
                "- `{}` — enabled={} installed={} reachable={}\n",
                service.name,
                yes_no(service.enabled),
                yes_no(service.installed),
                yes_no(service.reachable)
            ));
        }
        out.push('\n');
    }

    // --- GitHub (read-only; connect/disconnect handshake stays UI-only) ---
    out.push_str("## GitHub (read-only)\n\n");
    match crate::github::credentials::get_github_credentials(&orch.db.local).await {
        Ok(creds) => {
            out.push_str(&format!(
                "- configured: {}\n",
                yes_no(creds.app_id.is_some())
            ));
            if let Some(slug) = &creds.app_slug {
                out.push_str(&format!("- app: {slug}\n"));
            }
            if let Some(installation) = creds.installation_id {
                out.push_str(&format!("- installation: {installation}\n"));
            }
            out.push_str(&format!(
                "- relay: {}\n",
                if creds.relay_channel_id.is_some() {
                    "configured"
                } else {
                    "not configured"
                }
            ));
        }
        Err(error) => out.push_str(&format!("- status unavailable: {error}\n")),
    }
    out.push('\n');

    out
}
