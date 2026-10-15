//! Renderers for the cairn-llm facet's resources, `cairn://llm/*`.
//!
//! Each renders an answer from [`Llm`], never from the orchestrator directly,
//! so what these pages show is exactly what the facet interface says, and a
//! test can seed any facet it likes. Accounts are rendered from their
//! serialized form, the same projection the settings UI receives, which is why
//! no credential value can reach a page: it was never in the projection.

use cairn_common::query::QueryParam;
use cairn_llm::interface::{
    Catalog, CatalogProvider, Llm, LlmScope, SessionState, SessionView, TierBinding,
};
use cairn_llm::types::catalog::{
    custom_account_id, DiscoveredModel, DiscoveredModelPricing, ProviderCategory,
};
use cairn_llm::types::pool::{
    ProviderPoolAccountState, ProviderPoolConstraintReach, ProviderPoolSegmentState,
    ProviderPoolSnapshot,
};
use cairn_llm::types::usage::ProviderUsageWindow;
use serde_json::Value;
use std::fmt::Write as _;

use super::common::find_query_value;
use crate::orchestrator::account_events::{CredentialEvent, CredentialEventKind};
use cairn_common::identity::PrincipalRef;
use cairn_llm::subscription::PendingSignIn;

const INDEX: &[(&str, &str)] = &[
    (
        "cairn://llm/catalog",
        "installed providers, what model discovery found on each, and how tiers bind to them",
    ),
    (
        "cairn://llm/accounts",
        "provider accounts by provider, with their live usage windows (never a credential)",
    ),
    (
        "cairn://llm/usage",
        "each provider's pooled capacity, as its Settings → Providers card shows it",
    ),
    (
        "cairn://llm/sessions",
        "live model sessions on this host; stop one at cairn://llm/sessions/<id>",
    ),
];

/// Refuse any query key a resource does not read, naming the ones it does.
fn unsupported(uri: &str, params: &[QueryParam], allowed: &[&str]) -> Option<String> {
    let unknown = params
        .iter()
        .find(|param| !allowed.contains(&param.key.as_str()))?;
    Some(if allowed.is_empty() {
        format!("{uri} takes no query parameters (got '{}').", unknown.key)
    } else {
        format!(
            "Unsupported query parameter '{}' for {uri}; it reads {}.",
            unknown.key,
            allowed.join(", ")
        )
    })
}

fn when(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn tokens(n: i64) -> String {
    if n >= 1_000_000 && n % 1_000_000 == 0 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 && n % 1_000 == 0 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

fn percent(value: f64) -> String {
    format!("{}%", (value * 10.0).round() / 10.0)
}

/// Per-token USD prices, as providers report them, rendered per million.
fn per_million(price: &str) -> String {
    match price.parse::<f64>() {
        Ok(value) => format!("${}/M", (value * 1_000_000.0 * 1000.0).round() / 1000.0),
        Err(_) => price.to_string(),
    }
}

fn pricing(pricing: &DiscoveredModelPricing) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(prompt) = &pricing.prompt {
        parts.push(format!("in {}", per_million(prompt)));
    }
    if let Some(completion) = &pricing.completion {
        parts.push(format!("out {}", per_million(completion)));
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

fn category(category: ProviderCategory) -> &'static str {
    match category {
        ProviderCategory::Subscription => "subscription",
        ProviderCategory::Api => "metered API",
        ProviderCategory::Local => "local",
    }
}

fn heading(provider: &CatalogProvider) -> String {
    format!(
        "{} (`{}`)",
        provider.descriptor.display_name, provider.descriptor.key
    )
}

fn binding_line(binding: &TierBinding) -> String {
    let mut line = format!("{} → `{}`", binding.tier, binding.model);
    if !binding.options.is_empty() {
        let options: Vec<String> = binding
            .options
            .iter()
            .map(|(key, value)| match value {
                Value::String(text) => format!("{key}={text}"),
                other => format!("{key}={other}"),
            })
            .collect();
        let _ = write!(line, " ({})", options.join(", "));
    }
    if binding.is_tier_default {
        line.push_str(" [default]");
    }
    line
}

fn model_line(model: &DiscoveredModel) -> String {
    let mut line = format!("- `{}`", model.model);
    if model.display_name != model.model {
        let _ = write!(line, " {}", model.display_name);
    }
    let mut facts = Vec::new();
    if let Some(window) = model.context_window {
        facts.push(format!("{} context", tokens(window)));
    }
    if let Some(price) = model.pricing.as_ref().and_then(pricing) {
        facts.push(price);
    }
    if model.is_default {
        facts.push("provider default".to_string());
    }
    if !facts.is_empty() {
        let _ = write!(line, " — {}", facts.join(", "));
    }
    if model.hidden {
        let reason = model.description.as_deref().unwrap_or("not servable here");
        let _ = write!(line, " — unavailable: {reason}");
    }
    line
}

/// Whether `account` (serialized [`AccountInfo`]) belongs on `provider`'s
/// tab: a custom provider owns exactly its own account, every other provider
/// owns the accounts of its credential provider.
///
/// [`AccountInfo`]: cairn_llm::accounts::AccountInfo
fn account_serves(account: &Value, provider: &CatalogProvider) -> bool {
    match custom_account_id(&provider.descriptor.key) {
        Some(id) => account["id"].as_str() == Some(id),
        None => account["apiProvider"].as_str() == Some(provider.descriptor.api_provider.as_str()),
    }
}

fn sorted_accounts(llm: &dyn Llm, scope: &LlmScope) -> Result<Vec<Value>, String> {
    let mut accounts: Vec<Value> = llm
        .accounts(scope)?
        .iter()
        .map(|account| serde_json::to_value(account).unwrap_or(Value::Null))
        .collect();
    accounts.sort_by_key(|account| account["sortOrder"].as_i64().unwrap_or_default());
    Ok(accounts)
}

pub(crate) fn render_index() -> String {
    let mut out = String::from(
        "# cairn-llm\n\nHow tokens are produced, routed, and accounted. Every read shows what Settings → Providers shows for this workspace, and none reveals a credential value.\n\n",
    );
    for (uri, what) in INDEX {
        let _ = writeln!(out, "- `{uri}` — {what}");
    }
    out
}

pub(crate) fn render_catalog(llm: &dyn Llm, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://llm/catalog";
    if let Some(error) = unsupported(URI, params, &["view", "provider", "installed"]) {
        return error;
    }
    let scope = LlmScope::workspace();
    let catalog = match llm.catalog(&scope) {
        Ok(catalog) => catalog,
        Err(error) => return format!("Could not read the catalog: {error}"),
    };
    let include_uninstalled = match find_query_value(params, "installed") {
        None => false,
        Some("all") => true,
        Some(other) => return format!("Unknown installed='{other}' for {URI}; expected all."),
    };
    let only = find_query_value(params, "provider");
    if let Some(key) = only {
        if !catalog
            .providers
            .iter()
            .any(|provider| provider.descriptor.key == key)
        {
            let known: Vec<&str> = catalog
                .providers
                .iter()
                .map(|provider| provider.descriptor.key.as_ref())
                .collect();
            return format!(
                "No provider '{key}' in this workspace's catalog. Known: {}.",
                known.join(", ")
            );
        }
    }
    match find_query_value(params, "view") {
        None | Some("providers") => {
            let accounts = sorted_accounts(llm, &scope).unwrap_or_default();
            render_providers(&catalog, &accounts, only, include_uninstalled)
        }
        Some("models") => render_models(&catalog, only),
        Some(other) => format!("Unknown view '{other}' for {URI}; expected providers or models."),
    }
}

fn render_providers(
    catalog: &Catalog,
    accounts: &[Value],
    only: Option<&str>,
    include_uninstalled: bool,
) -> String {
    let installed: Vec<&CatalogProvider> = catalog
        .installed()
        .filter(|provider| only.is_none_or(|key| provider.descriptor.key == key))
        .collect();
    let mut out = String::from("# LLM catalog\n\n");
    let _ = writeln!(
        out,
        "{} installed provider{}, in Settings → Providers order. A tier marked [default] is where an unqualified reference such as `lg` resolves; `provider/tier` (for example `claude/lg`) names the provider outright.",
        installed.len(),
        if installed.len() == 1 { "" } else { "s" }
    );
    for provider in installed {
        let descriptor = &provider.descriptor;
        let _ = writeln!(
            out,
            "\n## {} — {}",
            heading(provider),
            category(descriptor.category)
        );
        let configured = accounts
            .iter()
            .filter(|account| account_serves(account, provider))
            .count();
        if configured == 0 {
            let _ = writeln!(out, "Needs setup: {}", descriptor.setup_requirement);
        } else {
            let _ = writeln!(
                out,
                "Configured: {configured} account{}",
                if configured == 1 { "" } else { "s" }
            );
        }
        if provider.tiers.is_empty() {
            out.push_str("Tiers: none bound here\n");
        } else {
            let bindings: Vec<String> = provider.tiers.iter().map(binding_line).collect();
            let _ = writeln!(out, "Tiers: {}", bindings.join(" · "));
        }
        match &provider.models {
            None => out.push_str("Models: not discovered yet\n"),
            Some(models) => {
                let refreshed = models
                    .refreshed_at
                    .map(|at| format!(", refreshed {}", when(at)))
                    .unwrap_or_default();
                let _ = writeln!(out, "Models ({}{refreshed}):", models.models.len());
                if let Some(error) = &models.error {
                    let _ = writeln!(out, "Discovery error: {error}");
                }
                for model in &models.models {
                    out.push_str(&model_line(model));
                    out.push('\n');
                }
            }
        }
    }
    if include_uninstalled {
        let absent: Vec<&CatalogProvider> = catalog
            .providers
            .iter()
            .filter(|provider| !provider.installed)
            .filter(|provider| only.is_none_or(|key| provider.descriptor.key == key))
            .collect();
        let _ = writeln!(out, "\n## Not installed ({})", absent.len());
        for provider in absent {
            let _ = writeln!(
                out,
                "- {} — {}. {}",
                heading(provider),
                category(provider.descriptor.category),
                provider.descriptor.summary
            );
        }
    }
    out
}

fn render_models(catalog: &Catalog, only: Option<&str>) -> String {
    let models: Vec<_> = catalog
        .models()
        .into_iter()
        .filter(|model| only.is_none_or(|key| model.offers.iter().any(|o| o.provider == key)))
        .collect();
    let mut out = String::from("# LLM catalog — models\n\n");
    let _ = writeln!(
        out,
        "{} model{} across the installed providers, each under its catalog id: the provider's canonical slug when it reports one, otherwise `provider/model`. Offers merge only on a shared slug.\n",
        models.len(),
        if models.len() == 1 { "" } else { "s" }
    );
    for model in models {
        let _ = write!(out, "- `{}`", model.catalog_id);
        if model.display_name != model.catalog_id {
            let _ = write!(out, " {}", model.display_name);
        }
        out.push('\n');
        for offer in &model.offers {
            let mut facts = Vec::new();
            if let Some(window) = offer.context_window {
                facts.push(format!("{} context", tokens(window)));
            }
            if let Some(price) = offer.pricing.as_ref().and_then(pricing) {
                facts.push(price);
            }
            let _ = write!(out, "  - {} `{}`", offer.provider, offer.model);
            if !facts.is_empty() {
                let _ = write!(out, " ({})", facts.join(", "));
            }
            out.push('\n');
        }
        if !model.aliases.is_empty() {
            let aliases: Vec<String> = model
                .aliases
                .iter()
                .map(|alias| format!("`{alias}`"))
                .collect();
            let _ = writeln!(out, "  - aliases: {}", aliases.join(", "));
        }
    }
    out
}

fn credential_kind(account: &Value) -> String {
    let kind = match account["authType"].as_str().unwrap_or_default() {
        "claude_profile" => "Cairn-managed Claude sign-in".to_string(),
        "oauth_token" => "ChatGPT sign-in".to_string(),
        "api_key" => "API key".to_string(),
        "base_url" => "host".to_string(),
        "actual_target" => format!(
            "Actual target ({})",
            account["targetKind"].as_str().unwrap_or("local")
        ),
        "custom_endpoint" => "custom endpoint".to_string(),
        other => other.to_string(),
    };
    let mut parts = vec![kind];
    if let Some(url) = account["baseUrl"].as_str() {
        parts.push(format!("at {url}"));
    }
    if let Some(protocol) = account["protocol"].as_str() {
        parts.push(format!("speaks {protocol}"));
    }
    if account["hasApiKey"].as_bool() == Some(true) {
        parts.push("bearer key stored".to_string());
    }
    if let Some(headers) = account["headerNames"].as_array() {
        let names: Vec<&str> = headers.iter().filter_map(Value::as_str).collect();
        if !names.is_empty() {
            parts.push(format!("headers {} (values hidden)", names.join(", ")));
        }
    }
    parts.join(", ")
}

/// One usage window, labelled by whether it still speaks for the account.
///
/// Liveness is the canonical rule (`ProviderUsageWindow::is_live_at`): a
/// window whose reset has passed has rolled over since it was measured, so its
/// number is history, not remaining capacity. Showing it as "N% left" would
/// report an account that emptied before its reset as still empty after it.
fn window_line(window: &Value, captured_at: Option<i64>, now: i64) -> String {
    let label = window["label"].as_str().unwrap_or("window");
    let remaining = percent(window["remainingPercent"].as_f64().unwrap_or_default());
    let live = serde_json::from_value::<ProviderUsageWindow>(window.clone())
        .is_ok_and(|parsed| parsed.is_live_at(captured_at.unwrap_or_default(), now));
    if !live {
        let measured = captured_at
            .map(|at| format!(" at {}", when(at)))
            .unwrap_or_default();
        return format!(
            "{label}: expired — read {remaining} left{measured}, but the window has rolled over since, so this is history, not current capacity"
        );
    }
    let mut line = format!("{label}: {remaining} left");
    if let Some(target) = window["scopeTarget"].as_str() {
        let _ = write!(line, " ({target} only)");
    }
    if let Some(reset) = window["resetsAt"].as_i64() {
        let _ = write!(line, ", resets {}", when(reset));
    } else if let Some(text) = window["resetAtText"].as_str() {
        let _ = write!(line, ", resets {text}");
    }
    line
}

fn account_block(account: &Value, now: i64, pending: &[PendingSignIn], out: &mut String) {
    let label = account["label"].as_str().unwrap_or("(unnamed)");
    let id = account["id"].as_str().unwrap_or_default();
    let _ = write!(out, "- **{label}** `{id}` — {}", credential_kind(account));
    for key in ["email", "plan"] {
        if let Some(value) = account[key].as_str() {
            let _ = write!(out, " · {key} {value}");
        }
    }
    match account["projectId"].as_str() {
        Some(project) => {
            let _ = write!(out, " · private to project {project}");
        }
        None => out.push_str(" · shared"),
    }
    let _ = write!(
        out,
        " · priority {}",
        account["sortOrder"].as_i64().unwrap_or_default()
    );
    if let Some(used) = account["lastUsedAt"].as_i64() {
        let _ = write!(out, " · last used {}", when(used));
    }
    out.push('\n');
    let health = &account["health"];
    if health.is_object() {
        let captured = health["capturedAt"].as_i64();
        if let Some(captured) = captured {
            let _ = writeln!(out, "  - measured {}", when(captured));
        }
        // Only a block still in force is one; a past block is history, as the
        // settings credential status also reads it.
        match health["blockedUntil"].as_i64() {
            Some(until) if until > now => {
                let _ = writeln!(out, "  - blocked until {}", when(until));
            }
            Some(until) => {
                let _ = writeln!(out, "  - an earlier block ended {}", when(until));
            }
            None => {}
        }
        for window in health["windows"].as_array().into_iter().flatten() {
            let _ = writeln!(out, "  - {}", window_line(window, captured, now));
        }
    }
    let id = account["id"].as_str();
    if let Some(sign_in) = pending
        .iter()
        .find(|sign_in| sign_in.account_id.as_deref() == id)
    {
        let _ = writeln!(
            out,
            "  - sign-in in progress since {}",
            when(sign_in.started_at)
        );
    }
}

pub(crate) fn render_accounts(
    llm: &dyn Llm,
    params: &[QueryParam],
    pending: &[PendingSignIn],
) -> String {
    const URI: &str = "cairn://llm/accounts";
    if let Some(error) = unsupported(URI, params, &["provider"]) {
        return error;
    }
    let scope = LlmScope::workspace();
    let (catalog, accounts) = match (llm.catalog(&scope), sorted_accounts(llm, &scope)) {
        (Ok(catalog), Ok(accounts)) => (catalog, accounts),
        (Err(error), _) | (_, Err(error)) => return format!("Could not read accounts: {error}"),
    };
    let only = find_query_value(params, "provider");
    let now = chrono::Utc::now().timestamp();
    let mut out = String::from(
        "# LLM accounts\n\nGrouped by installed provider, in priority order. A credential is described, never shown. Append {provider: claude|codex} here to start a subscription sign-in, or use Settings → Providers, or `cairn auth login claude` for a managed Claude account.\n",
    );
    let mut placed = vec![false; accounts.len()];
    let mut shown = 0;
    for provider in catalog.installed() {
        let mine: Vec<usize> = (0..accounts.len())
            .filter(|index| account_serves(&accounts[*index], provider))
            .collect();
        for index in &mine {
            placed[*index] = true;
        }
        if only.is_some_and(|key| provider.descriptor.key != key) {
            continue;
        }
        shown += 1;
        let _ = writeln!(out, "\n## {}", heading(provider));
        if mine.is_empty() {
            let _ = writeln!(out, "No account. {}", provider.descriptor.setup_requirement);
        }
        for index in mine {
            account_block(&accounts[index], now, pending, &mut out);
        }
        // A new Codex account has no row until its sign-in completes.
        for sign_in in pending.iter().filter(|sign_in| {
            sign_in.account_id.is_none() && sign_in.provider.backend() == provider.descriptor.key
        }) {
            let _ = writeln!(
                out,
                "- Sign-in in progress since {}; the account appears here when it completes.",
                when(sign_in.started_at)
            );
        }
    }
    if let Some(key) = only {
        if shown == 0 {
            return format!("No installed provider '{key}'. Installed providers are listed at cairn://llm/catalog.");
        }
        return out;
    }
    let others: Vec<&Value> = accounts
        .iter()
        .zip(placed)
        .filter(|(_, placed)| !placed)
        .map(|(account, _)| account)
        .collect();
    if !others.is_empty() {
        out.push_str("\n## Serving no installed provider\n");
        for account in others {
            account_block(account, now, pending, &mut out);
        }
    }
    out
}

/// Who started a sign-in, as a reader may see it. Never the appearance
/// evidence behind it.
fn initiator_line(initiator: Option<&PrincipalRef>) -> String {
    match initiator {
        Some(PrincipalRef::Agent { node, .. }) => node.clone(),
        Some(PrincipalRef::Machine { .. }) => "operator via desktop".to_string(),
        Some(PrincipalRef::Human { .. }) => "signed-in operator".to_string(),
        Some(PrincipalRef::External { .. }) => "external principal".to_string(),
        None => "unattributed".to_string(),
    }
}

fn history_line(event: &CredentialEvent) -> String {
    let at = when(event.occurred_at);
    match event.kind {
        CredentialEventKind::Started => format!(
            "{at} — {} sign-in started by {}",
            event.provider,
            initiator_line(event.initiator.as_ref())
        ),
        CredentialEventKind::Completed => {
            let mut line = format!("{at} — completed");
            if let Some(email) = &event.email {
                let _ = write!(line, " as {email}");
            }
            if let Some(plan) = &event.plan {
                let _ = write!(line, " ({plan})");
            }
            line
        }
        CredentialEventKind::Failed => format!(
            "{at} — failed: {}",
            event
                .failure_kind
                .map(|kind| kind.as_str().replace('_', " "))
                .unwrap_or_else(|| "other".to_string())
        ),
        CredentialEventKind::Cancelled => format!("{at} — cancelled"),
    }
}

/// What an account's page shows about sign-in beyond the account itself.
#[derive(Debug)]
pub(crate) struct AccountSignIns {
    pub pending: Vec<PendingSignIn>,
    pub history: Result<Vec<CredentialEvent>, String>,
}

pub(crate) fn render_account(llm: &dyn Llm, id: &str, sign_ins: &AccountSignIns) -> String {
    let accounts = match sorted_accounts(llm, &LlmScope::workspace()) {
        Ok(accounts) => accounts,
        Err(error) => return format!("Could not read accounts: {error}"),
    };
    let Some(account) = accounts
        .iter()
        .find(|account| account["id"].as_str() == Some(id))
    else {
        return format!(
            "No account `{id}` in this workspace. Accounts are listed at cairn://llm/accounts."
        );
    };
    let now = chrono::Utc::now().timestamp();
    let mut out = format!(
        "# LLM account {}\n\n",
        account["label"].as_str().unwrap_or(id)
    );
    account_block(account, now, &sign_ins.pending, &mut out);
    let serves: Vec<&str> = account["compatibleBackends"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    if !serves.is_empty() {
        let _ = writeln!(out, "\nServes: {}", serves.join(", "));
    }

    out.push_str("\n## Sign-in\n");
    if sign_ins
        .pending
        .iter()
        .any(|sign_in| sign_in.account_id.as_deref() == Some(id))
    {
        out.push_str("A sign-in is in progress: finish it in the browser, patch {action:\"submit_code\", code} with the code#state Claude's manual path shows, or patch {action:\"cancel_sign_in\"} to end it.\n");
    }
    match &sign_ins.history {
        Ok(history) if history.is_empty() => {
            out.push_str("No sign-in recorded for this account.\n");
        }
        Ok(history) => {
            for event in history {
                let _ = writeln!(out, "- {}", history_line(event));
            }
        }
        Err(error) => {
            let _ = writeln!(out, "Sign-in history unavailable: {error}");
        }
    }
    if matches!(
        account["authType"].as_str(),
        Some("claude_profile" | "oauth_token")
    ) {
        out.push_str("\nRe-authenticate with patch {action:\"sign_in\"} (needs the `workspace/default/settings/accounts:write` grant), in Settings → Providers, or with `cairn auth login claude --account <id>` for a managed Claude account.\n");
    } else {
        out.push_str("\nThis credential is not a subscription sign-in; change it through `cairn://settings` `accounts` or Settings → Providers.\n");
    }
    out
}

fn pool_block(pool: &ProviderPoolSnapshot, label: &str, out: &mut String) {
    let _ = write!(out, "\n## {label}");
    if let Some(captured) = pool.captured_at {
        let _ = write!(out, " — measured {}", when(captured));
    }
    out.push('\n');
    if let Some(reason) = &pool.unsupported_reason {
        let _ = writeln!(out, "No provider-side allowance: {reason}");
    }
    let name = |account_id: &str| {
        pool.accounts
            .iter()
            .find(|account| account.account_id == account_id)
            .map(|account| account.label.clone())
            .unwrap_or_else(|| account_id.to_string())
    };
    for family in &pool.families {
        let _ = write!(
            out,
            "- {}: {} of {} left",
            family.label,
            percent(family.remaining_percent),
            percent(family.capacity_percent)
        );
        if family.unknown_count > 0 {
            let _ = write!(out, " ({} account(s) unmeasured)", family.unknown_count);
        }
        if let Some(reset) = family.next_reset_at {
            let _ = write!(out, ", next reset {}", when(reset));
            if family.resets_differ {
                out.push_str(" (accounts reset at different times)");
            }
        }
        out.push('\n');
        for segment in &family.segments {
            let share = match segment.state {
                ProviderPoolSegmentState::Measured => segment
                    .remaining_percent
                    .map(percent)
                    .unwrap_or_else(|| "unknown".to_string()),
                ProviderPoolSegmentState::Blocked => "blocked".to_string(),
                ProviderPoolSegmentState::Unknown => "unknown".to_string(),
                ProviderPoolSegmentState::Error => "error".to_string(),
            };
            let _ = writeln!(out, "  - {}: {share}", name(&segment.account_id));
        }
    }
    for constraint in &pool.constraints {
        let what = constraint
            .family_label
            .as_deref()
            .unwrap_or("every account is blocked");
        let reach = match (constraint.reach, &constraint.scope_target) {
            (ProviderPoolConstraintReach::Model, Some(target)) => format!("{target} models only"),
            _ => "every model".to_string(),
        };
        let _ = write!(out, "- Exhausted: {what} ({reach})");
        if let Some(reset) = constraint.next_reset_at {
            let _ = write!(out, " until {}", when(reset));
        }
        out.push('\n');
    }
    for account in &pool.accounts {
        let state = match account.state {
            ProviderPoolAccountState::Measured => "measured".to_string(),
            ProviderPoolAccountState::Unknown => "not measured yet".to_string(),
            ProviderPoolAccountState::Blocked => match account.blocked_until {
                Some(until) => format!("blocked until {}", when(until)),
                None => "blocked".to_string(),
            },
            ProviderPoolAccountState::Error => format!(
                "last probe failed: {}",
                account.error.as_deref().unwrap_or("unknown error")
            ),
        };
        let _ = write!(out, "- Account {}: {state}", account.label);
        if let Some(captured) = account.captured_at {
            let _ = write!(out, ", measured {}", when(captured));
        }
        out.push('\n');
    }
    if let Some(credits) = &pool.credits {
        let currency = credits.currency.as_deref().unwrap_or("USD");
        let mut parts = Vec::new();
        if let Some(balance) = credits.balance {
            parts.push(format!("balance {balance:.2} {currency}"));
        }
        if let Some(used) = credits.total_used {
            parts.push(format!("used {used:.2} {currency}"));
        }
        if let Some(granted) = credits.total_granted {
            parts.push(format!("granted {granted:.2} {currency}"));
        }
        if !parts.is_empty() {
            let _ = writeln!(out, "- Credits: {}", parts.join(", "));
        }
    }
    if let Some(resets) = &pool.reset_credits {
        let _ = writeln!(out, "- Usage resets available: {}", resets.available_count);
    }
    if let Some(rows) = &pool.model_breakdown {
        if rows.is_empty() {
            out.push_str("- Recorded cost: none yet\n");
        }
        for row in rows {
            let _ = write!(out, "- Recorded cost {}: ${:.4}", row.model, row.cost_usd);
            if let Some(runs) = row.runs {
                let _ = write!(out, " over {runs} run(s)");
            }
            out.push('\n');
        }
    }
}

pub(crate) fn render_usage(llm: &dyn Llm, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://llm/usage";
    if let Some(error) = unsupported(URI, params, &["provider", "view"]) {
        return error;
    }
    let scope = LlmScope::workspace();
    match find_query_value(params, "view") {
        None | Some("card") => {}
        Some("requests") => {
            return match llm.requests(&scope) {
                Ok(rows) => format!("# LLM requests\n\n{} row(s).", rows.len()),
                Err(unavailable) => unavailable.to_string(),
            }
        }
        Some(other) => {
            return format!("Unknown view '{other}' for {URI}; expected card or requests.")
        }
    }
    let pools = match llm.usage(&scope, find_query_value(params, "provider")) {
        Ok(pools) => pools,
        Err(error) => return error,
    };
    let names = llm.catalog(&scope).ok();
    let label = |key: &str| {
        names
            .as_ref()
            .and_then(|catalog| {
                catalog
                    .providers
                    .iter()
                    .find(|provider| provider.descriptor.key == key)
            })
            .map(heading)
            .unwrap_or_else(|| format!("`{key}`"))
    };
    let mut out = String::from(
        "# LLM usage\n\nEach installed provider's capacity as already measured, pooled across its accounts. Reading never probes a provider.\n",
    );
    for pool in &pools {
        pool_block(pool, &label(&pool.backend), &mut out);
    }
    out
}

fn session_line(session: &SessionView) -> String {
    let state = match session.state {
        SessionState::Busy => "busy",
        SessionState::Warm => "warm",
    };
    let mut line = format!(
        "- `{}` — {state} · {} · on {}",
        session.key(),
        session.provider,
        session.host
    );
    if let Some(model) = &session.model {
        let _ = write!(line, " `{model}`");
    }
    if let Some(account) = &session.account_id {
        let _ = write!(line, " · account `{account}`");
    }
    if let Some(node) = &session.node_uri {
        let _ = write!(line, " · {node}");
    }
    if let Some(started) = session.started_at {
        let _ = write!(line, " · started {}", when(started));
    }
    line
}

pub(crate) fn render_sessions(llm: &dyn Llm) -> String {
    let sessions = llm.sessions();
    let mut out = String::from("# LLM sessions\n\n");
    if sessions.is_empty() {
        out.push_str("No live model sessions on this host.\n");
        return out;
    }
    let _ = writeln!(
        out,
        "{} live session{}. Busy is starting, serving a turn, or holding one open; warm is parked between turns and resumable without a new process.\n",
        sessions.len(),
        if sessions.len() == 1 { "" } else { "s" }
    );
    for session in &sessions {
        out.push_str(&session_line(session));
        out.push('\n');
    }
    out
}

pub(crate) fn render_session(llm: &dyn Llm, id: &str) -> String {
    let Some(session) = llm
        .sessions()
        .into_iter()
        .find(|session| session.is_addressed_by(id))
    else {
        return format!(
            "No live session `{id}` on this host. Live sessions are listed at cairn://llm/sessions."
        );
    };
    let mut out = format!("# LLM session {}\n\n", session.key());
    out.push_str(&session_line(&session));
    let _ = write!(out, "\n\nRun: `{}`", session.run_id);
    if let Some(pid) = session.pid {
        let _ = write!(out, " · pid {pid}");
    }
    if let Some(backend_session) = &session.backend_session_id {
        let _ = write!(
            out,
            "\n\nProvider session: `{backend_session}` (the {}'s own id for this conversation)",
            session.provider
        );
    }
    out.push_str("\n\nStopping it interrupts the active turn and parks the session warm and resumable, exactly as stopping its node does.\n");
    out
}

#[cfg(test)]
mod tests {
    use crate::mcp::types::McpCallbackRequest;
    use crate::orchestrator::llm::tests::{
        seeded_orchestrator, API_KEY_SECRET, CUSTOM_KEY_SECRET, HEADER_SECRET, OAUTH_SECRET,
    };
    use crate::orchestrator::Orchestrator;
    use cairn_llm::interface::RequestsUnavailable;

    const READS: &[&str] = &[
        "cairn://llm",
        "cairn://llm/catalog",
        "cairn://llm/catalog?view=models",
        "cairn://llm/catalog?installed=all",
        "cairn://llm/catalog?provider=claude",
        "cairn://llm/accounts",
        "cairn://llm/accounts?provider=claude",
        "cairn://llm/usage",
        "cairn://llm/usage?provider=claude",
        "cairn://llm/usage?view=requests",
        "cairn://llm/sessions",
    ];

    fn request() -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        }
    }

    async fn read(orch: &Orchestrator, uri: &str) -> String {
        crate::resources::read::read_cairn_resource(orch, &request(), uri).await
    }

    /// Every read, plus each account's own page.
    fn all_reads(orch: &Orchestrator) -> Vec<String> {
        let mut uris: Vec<String> = READS.iter().map(|uri| uri.to_string()).collect();
        uris.extend(
            orch.list_accounts(None)
                .into_iter()
                .map(|account| format!("cairn://llm/accounts/{}", account.id)),
        );
        uris
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_llm_read_renders_with_its_affordance_block() {
        let orch = seeded_orchestrator().await;
        for uri in all_reads(&orch) {
            let out = read(&orch, &uri).await;
            let kind = cairn_common::uri::parse_uri(&uri).expect("parses").kind();
            let name = cairn_common::contract::contract_for(kind)
                .expect("registered")
                .name;
            assert!(
                out.contains(&format!("## {name}")),
                "{uri} lacks its affordance block:\n{out}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_pages_say_what_settings_shows() {
        let orch = seeded_orchestrator().await;
        let catalog = read(&orch, "cairn://llm/catalog").await;
        assert!(catalog.contains("## Anthropic (`claude`)"), "{catalog}");
        assert!(catalog.contains("lg → `opus`"), "{catalog}");
        assert!(catalog.contains("[default]"), "{catalog}");
        assert!(catalog.contains("`opus` — 1M context"), "{catalog}");
        assert!(catalog.contains("DGX Spark"), "{catalog}");
        assert!(
            catalog.contains("Discovery error: codex is not installed"),
            "{catalog}"
        );
        assert!(!catalog.contains("## Not installed"), "{catalog}");

        let all = read(&orch, "cairn://llm/catalog?installed=all").await;
        assert!(all.contains("## Not installed"), "{all}");
        assert!(all.contains("OpenRouter (`openrouter`)"), "{all}");

        let models = read(&orch, "cairn://llm/catalog?view=models").await;
        assert!(models.contains("- `claude/opus`"), "{models}");
        assert!(models.contains("`claude/lg`"), "{models}");
        assert!(models.contains("- `ollama/qwen3:32b`"), "{models}");

        let accounts = read(&orch, "cairn://llm/accounts").await;
        assert!(accounts.contains("**Work Max**"), "{accounts}");
        assert!(
            accounts.contains("Cairn-managed Claude sign-in"),
            "{accounts}"
        );
        assert!(accounts.contains("session: 64% left"), "{accounts}");
        assert!(
            accounts.contains("headers x-tenant (values hidden)"),
            "{accounts}"
        );
        assert!(accounts.contains("cairn auth login claude"), "{accounts}");

        let usage = read(&orch, "cairn://llm/usage?provider=claude").await;
        assert!(usage.contains("session: 64% of 100% left"), "{usage}");
        let refused = read(&orch, "cairn://llm/usage?provider=openrouter").await;
        assert!(refused.contains("not installed"), "{refused}");

        let sessions = read(&orch, "cairn://llm/sessions").await;
        assert!(sessions.contains("No live model sessions"), "{sessions}");
        let missing = read(&orch, "cairn://llm/sessions/nope").await;
        assert!(missing.contains("cairn://llm/sessions"), "{missing}");

        let bad = read(&orch, "cairn://llm/catalog?view=bogus").await;
        assert!(bad.contains("Unknown view 'bogus'"), "{bad}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rolled_over_windows_and_past_blocks_read_as_history() {
        use crate::credentials::{ApiProvider, ProviderAuth};
        use cairn_llm::types::usage::{ProviderUsageScope, ProviderUsageWindow};
        let window = |label: &str, remaining: f64, resets_at: i64| ProviderUsageWindow {
            id: label.to_string(),
            label: label.to_string(),
            scope: ProviderUsageScope::Session,
            scope_target: None,
            used_percent: 100.0 - remaining,
            remaining_percent: remaining,
            resets_at: Some(resets_at),
            reset_at_text: None,
            window_duration_mins: None,
        };
        let orch = seeded_orchestrator().await;
        let far_future = chrono::Utc::now().timestamp() + 86_400;
        // Emptied before its reset, which has since passed; its block expired too.
        let stale = orch
            .add_account(
                ApiProvider::Anthropic,
                "Stale".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(&stale.id, vec![window("rolled", 0.0, 1_000)], Some(1_000))
            .unwrap();
        // Still blocked, still measured.
        let current = orch
            .add_account(
                ApiProvider::Anthropic,
                "Current".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(
            &current.id,
            vec![window("fresh", 0.0, far_future)],
            Some(far_future),
        )
        .unwrap();

        let stale_page = read(&orch, &format!("cairn://llm/accounts/{}", stale.id)).await;
        assert!(stale_page.contains("rolled: expired"), "{stale_page}");
        assert!(
            stale_page.contains("history, not current capacity"),
            "{stale_page}"
        );
        assert!(!stale_page.contains("rolled: 0% left"), "{stale_page}");
        assert!(!stale_page.contains("blocked until"), "{stale_page}");
        assert!(
            stale_page.contains("an earlier block ended"),
            "{stale_page}"
        );

        let current_page = read(&orch, &format!("cairn://llm/accounts/{}", current.id)).await;
        assert!(current_page.contains("fresh: 0% left"), "{current_page}");
        assert!(current_page.contains("blocked until"), "{current_page}");
        assert!(!current_page.contains("expired"), "{current_page}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn per_request_usage_is_the_instructive_refusal() {
        let orch = seeded_orchestrator().await;
        let out = read(&orch, "cairn://llm/usage?view=requests").await;
        assert!(out.contains(&RequestsUnavailable.to_string()), "{out}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_llm_read_reveals_a_credential() {
        let orch = seeded_orchestrator().await;
        for uri in all_reads(&orch) {
            let out = read(&orch, &uri).await;
            for secret in [
                API_KEY_SECRET,
                OAUTH_SECRET,
                HEADER_SECRET,
                CUSTOM_KEY_SECRET,
            ] {
                assert!(!out.contains(secret), "{uri} reveals {secret}:\n{out}");
            }
        }
    }
}
