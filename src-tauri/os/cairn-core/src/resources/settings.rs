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

/// The workspace's model defaults, each unset one shown as what it means.
fn model_defaults(settings: &crate::models::Settings) -> String {
    let line = |key: &str, value: Option<&str>, unset: String| match value {
        Some(model) => format!("- {key}: `{model}`\n"),
        None => format!("- {key}: unset ({unset})\n"),
    };
    [
        line(
            "decisionModel",
            settings.decision_model.as_deref(),
            "the catalog's best decision offer".into(),
        ),
        line(
            "imageModel",
            settings.image_model.as_deref(),
            "a request naming no model is refused".into(),
        ),
        line(
            "speechModel",
            settings.speech_model.as_deref(),
            "a request naming no model is refused".into(),
        ),
        line(
            "embeddingModel",
            settings.embedding_model.as_deref(),
            format!(
                "`{}`",
                crate::orchestrator::embed::default_embedding_selection()
            ),
        ),
        line(
            "defaultRecipe",
            settings.default_recipe.as_deref(),
            "an execution naming no recipe is refused".into(),
        ),
    ]
    .concat()
}

pub(super) async fn read_settings(orch: &Orchestrator) -> String {
    let mut out = String::from("# Workspace settings\n\n");

    // --- App preferences ---
    let settings = orch.get_settings();
    out.push_str("## App preferences\n\n");
    out.push_str(&format!(
        "- maxThinkingTokens: {}\n",
        settings
            .max_thinking_tokens
            .map(|t| t.to_string())
            .unwrap_or_else(|| "disabled".to_string())
    ));
    out.push_str(&format!("- mergeType: {:?}\n", settings.merge_type));
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
        "- memoryReviewEnabled: {}\n",
        yes_no(settings.memory_review_enabled)
    ));
    out.push_str(&format!(
        "- memoryTriageEnabled: {}\n",
        yes_no(settings.memory_triage_enabled)
    ));
    out.push_str(&format!(
        "- maxOpenTriageIssuesPerScope: {}\n",
        settings.max_open_triage_issues_per_scope
    ));
    out.push_str(&format!(
        "- maxConcurrentExecutions: {}\n",
        settings.max_concurrent_executions
    ));
    out.push_str(&format!(
        "- maxOpenChildrenPerThread: {}\n",
        settings.max_open_children_per_thread
    ));
    out.push_str(&format!(
        "- pendingMemoryThreshold: {}\n",
        settings.pending_memory_threshold
    ));
    out.push_str(&format!(
        "- threadCompactThreshold: {}\n",
        settings.thread_compact_threshold
    ));
    out.push_str(&format!(
        "- seedBudgetTokens: {}\n",
        settings.seed_budget_tokens
    ));
    out.push_str(&format!("- seedEras: {}\n", settings.seed_eras));
    out.push_str(&model_defaults(&settings));
    out.push_str(&format!(
        "- externalReplies: {:?}\n\n",
        settings.external_replies
    ));
    let channel = crate::channels::runtime_status(orch, "imessage").await;
    out.push_str("## Operator presence (read-only)\n\n");
    out.push_str(&format!("- presenceMode: {:?}\n", channel.presence_mode));
    out.push_str(&format!(
        "- operatorPresence: {:?}\n\n",
        channel.operator_presence
    ));

    // --- Backends ---
    out.push_str("## Backends\n\n");
    // Installed, not shipped: a provider absent here is one this workspace has
    // not added, and it takes no part in discovery, pickers, or routing.
    out.push_str(&format!(
        "- enabledProviders: {}\n",
        settings.enabled_providers.join(", ")
    ));
    match &settings.models {
        Some(models) => {
            let pools: Vec<&str> = models.pools.iter().map(|pool| pool.id.as_str()).collect();
            let defaults: Vec<String> = models
                .default_models
                .iter()
                .map(ToString::to_string)
                .collect();
            out.push_str(&format!("- pools: {}\n", pools.join(", ")));
            out.push_str(&format!("- ladder: {} model(s)\n", models.ladder.len()));
            out.push_str(&format!("- defaultModels: [{}]\n", defaults.join(", ")));
        }
        None => out.push_str("- models: not converted yet\n"),
    }
    out.push_str("- The pools, bands and ladder: cairn://models/ladder\n\n");

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
            let mut keys: Vec<&(String, Option<String>)> = snapshots.keys().collect();
            keys.sort();
            for key in keys {
                let snapshot = &snapshots[key];
                out.push_str(&format!(
                    "- {}{}: {} window(s)",
                    snapshot.backend,
                    key.1
                        .as_deref()
                        .map(|id| format!(" account {id}"))
                        .unwrap_or_default(),
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

    // --- Provider accounts (state; sign-in is the runner's OAuth commands) ---
    out.push_str("## Provider accounts\n\n");
    out.push_str(
        "`cairn auth login claude` adds a Claude subscription profile; `--account <id>` re-authenticates one listed here.\n\n",
    );
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
            let combo = if keybind.sequence.is_empty() {
                "(disabled)".to_string()
            } else {
                keybind
                    .sequence
                    .iter()
                    .map(|stroke| {
                        let mut parts = stroke
                            .modifiers
                            .iter()
                            .map(|modifier| modifier.normalized_name())
                            .collect::<Vec<_>>();
                        parts.push(&stroke.key);
                        parts.join("+")
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
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
            // A daemon can answer and still be unfit for one of this runner's
            // managed roots; builds there compile directly, which is the one
            // fact the reachable flag cannot say.
            let fitness: String = service
                .fitness
                .iter()
                .map(|sentence| format!("; {sentence}"))
                .collect();
            out.push_str(&format!(
                "- `{}` — enabled={} installed={} reachable={}{}\n",
                service.name,
                yes_no(service.enabled),
                yes_no(service.installed),
                yes_no(service.reachable),
                fitness
            ));
        }
        out.push('\n');
    }

    // --- Inference profile declarations ---
    out.push_str("## Inference\n\n");
    let inference = crate::config::settings::load_inference(&orch.config_dir);
    if inference.profiles.is_empty() {
        out.push_str("- profile declarations: none\n");
    }
    for (profile, declared) in &inference.profiles {
        let verbs = declared
            .verbs
            .as_ref()
            .map_or("unstated".to_string(), |verbs| {
                verbs
                    .iter()
                    .map(|verb| verb.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            });
        out.push_str(&format!("- profile {profile}: verbs {verbs}\n"));
    }
    if inference.retired_bginf_link {
        out.push_str("- the retired bginf link (`inference.bginf`) is still in settings.yaml and is ignored; writing this section drops it\n");
    }
    out.push_str("- the records themselves: cairn://inference\n\n");

    // --- Served API ---
    out.push_str("## Served API\n\n");
    let served = crate::config::settings::load_served_api(&orch.config_dir);
    match &served.bind {
        None => out.push_str("- listener: off (no `servedApi.bind`)\n"),
        Some(bind) => out.push_str(&format!(
            "- listener: {bind} ({})\n",
            if served.tls.is_some() {
                "TLS"
            } else {
                "plain HTTP"
            }
        )),
    }
    out.push_str(&format!("- load wait: {}s\n", served.load_wait().as_secs()));
    out.push_str("- clients and status: cairn://models/clients\n\n");

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
        }
        Err(error) => out.push_str(&format!("- status unavailable: {error}\n")),
    }
    match crate::ingress::channel::get_relay_channel(&orch.db.local).await {
        Ok(channel) => out.push_str(&format!(
            "- relay channel: {}\n",
            if channel.channel_id.is_some() {
                "configured"
            } else {
                "not configured"
            }
        )),
        Err(error) => out.push_str(&format!("- relay channel unavailable: {error}\n")),
    }
    out.push('\n');

    out
}

#[cfg(test)]
mod tests {
    use crate::models::UpdateSettings;

    #[tokio::test(flavor = "multi_thread")]
    async fn every_model_default_renders_set_or_as_what_unset_means() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let unset = super::read_settings(&orch).await;
        assert!(
            unset.contains("- decisionModel: unset (the catalog's best decision offer)"),
            "{unset}"
        );
        assert!(unset.contains("- imageModel: unset (a request naming no model is refused)"));
        assert!(unset.contains("- speechModel: unset (a request naming no model is refused)"));
        assert!(unset.contains("- embeddingModel: unset (`cairn/cohere-embed-v4`)"));

        orch.store_settings_unchecked_for_test(UpdateSettings {
            decision_model: Some(Some("sm".into())),
            image_model: Some(Some("codex/gpt-image-2".into())),
            speech_model: Some(Some("local-tts".into())),
            embedding_model: Some(Some("ollama/nomic-embed-text".into())),
            ..Default::default()
        });
        let set = super::read_settings(&orch).await;
        for line in [
            "- decisionModel: `sm`",
            "- imageModel: `codex/gpt-image-2`",
            "- speechModel: `local-tts`",
            "- embeddingModel: `ollama/nomic-embed-text`",
        ] {
            assert!(set.contains(line), "{line} in {set}");
        }
    }
}
