//! `cairn://models/usage?capacity`: what each subscription window has left
//! per account and model family, the inverse (what a given spend would cost
//! each window, and the chance it fits), and `?capacity=fit`, the drain fit's
//! own report. Rates come from the orchestrator's served [`DrainFit`], which
//! is refitted on a cadence. What each window holds now is read live from the
//! latest readings on every read, so a reading is served as soon as it lands.
//! A model the read names is resolved to the generation it is priced as, and
//! a named spend is priced at that generation's own list rates. Accounts come
//! from their serialized form, as every models page renders them, so a name
//! never carries an email.

use std::fmt::Write as _;

use cairn_analytics::drain_inputs::{current_readings, list_price};
use cairn_common::query::QueryParam;
use cairn_models::interface::ModelScope;
use cairn_models::types::drain::report::{dollars, interval, short_account, sig, token_count};
use cairn_models::types::drain::{
    current_windows, family_of, report, AccountRef, Capacity, Components, CurrentWindow, DrainFit,
    LatestReading,
};
use cairn_models::types::usage::agent_account_name;
use serde_json::{json, Value};

use super::common::find_query_value;
use super::models::{sorted_accounts, when};
use crate::orchestrator::Orchestrator;

const URI: &str = "cairn://models/usage?capacity";
const KEYS: &[&str] = &[
    "capacity",
    "account",
    "model",
    "input",
    "cacheRead",
    "cacheWrite",
    "output",
    "format",
];
/// The inverse read's token keys, in [`Components`] order.
const SPEND_KEYS: [&str; 4] = ["input", "cacheRead", "cacheWrite", "output"];

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CapacityView {
    /// What each live account's windows have left, per model family.
    Accounts,
    /// What this spend would cost each window it counts against.
    Inverse(Components),
    /// The drain fit report.
    Fit,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CapacityQuery {
    pub view: CapacityView,
    pub account: Option<String>,
    /// The canonical id of the model asked about.
    pub model: Option<String>,
    pub json: bool,
}

/// Parse a capacity read, or `None` when the read is not one (no `capacity`
/// key). A bare `capacity` parses as the empty value.
pub(crate) fn capacity_query(params: &[QueryParam]) -> Option<Result<CapacityQuery, String>> {
    let value = find_query_value(params, "capacity")?;
    Some(parse(params, value))
}

fn parse(params: &[QueryParam], value: &str) -> Result<CapacityQuery, String> {
    if find_query_value(params, "history").is_some() {
        return Err("capacity and history are separate reads of cairn://models/usage: ?capacity for what each window has left, ?history=ACCOUNT for its recorded readings.".to_string());
    }
    if let Some(unknown) = params
        .iter()
        .find(|param| !KEYS.contains(&param.key.as_str()))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for {URI}; it reads {}.",
            unknown.key,
            KEYS.join(", ")
        ));
    }
    let json = match find_query_value(params, "format") {
        None | Some("markdown") => false,
        Some("json") => true,
        Some(other) => {
            return Err(format!(
                "Unknown format '{other}' for {URI}; expected markdown or json."
            ))
        }
    };
    let account = find_query_value(params, "account").map(str::to_string);
    let model = match find_query_value(params, "model") {
        None => None,
        Some(asked) => Some(
            list_price(asked)
                .ok_or_else(|| {
                    format!(
                        "model '{asked}' has no list price; name a priced model or alias such as opus, claude-opus-4-1, fable or gpt-6.1-sol."
                    )
                })?
                .canonical
                .to_string(),
        ),
    };
    let mut spend = [0.0; 4];
    let mut spent = false;
    for (index, key) in SPEND_KEYS.iter().enumerate() {
        if let Some(raw) = find_query_value(params, key) {
            spend[index] = raw
                .parse::<u64>()
                .map_err(|_| format!("{key} is a whole number of tokens (got '{raw}')."))?
                as f64;
            spent = true;
        }
    }
    let view = match value {
        "" if spent => {
            if model.is_none() {
                return Err(format!(
                    "An inverse read names the model the spend runs on, e.g. {URI}&model=opus&cacheRead=400000000&output=2000000."
                ));
            }
            CapacityView::Inverse(Components::from_array(spend))
        }
        "" => CapacityView::Accounts,
        "fit" => {
            if account.is_some() || model.is_some() || spent {
                return Err(
                    "capacity=fit is the whole drain fit report; it reads only format.".to_string(),
                );
            }
            CapacityView::Fit
        }
        other => {
            return Err(format!(
                "capacity takes no value (what each window has left) or fit (the drain fit report); got '{other}'."
            ))
        }
    };
    Ok(CapacityQuery {
        view,
        account,
        model,
        json,
    })
}

/// One account as the capacity read names it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveAccount {
    pub id: String,
    pub name: String,
    /// Why the account cannot run now: a hold, or a block still in force.
    pub blocked: Option<String>,
}

/// The accounts in their serialized form (`AccountInfo`), each with whether
/// a hold or a block in force stops it now.
pub(crate) fn live_accounts(accounts: &[Value], now: i64) -> Vec<LiveAccount> {
    accounts
        .iter()
        .filter_map(|account| {
            let id = account["id"].as_str()?.to_string();
            let label = account["label"].as_str().unwrap_or_default();
            let health = &account["health"];
            let held =
                serde_json::from_value::<crate::credentials::AccountHold>(health["held"].clone())
                    .ok()
                    .map(|hold| hold.kind.summary().to_string());
            let blocked = held.or_else(|| {
                health["blockedUntil"]
                    .as_i64()
                    .filter(|until| *until > now)
                    .map(|until| format!("blocked until {}", when(until)))
            });
            Some(LiveAccount {
                name: agent_account_name(&id, label).to_string(),
                id,
                blocked,
            })
        })
        .collect()
}

/// A window's latest reading as of now: one whose reset has passed reads as
/// a fresh 100%.
struct WindowNow<'a> {
    window: &'a CurrentWindow,
    used: f64,
    assumed_fresh: bool,
    hours_to_reset: Option<f64>,
}

impl<'a> WindowNow<'a> {
    fn of(window: &'a CurrentWindow, now: i64) -> Self {
        let assumed_fresh = window.resets_at.is_some_and(|at| at <= now);
        Self {
            window,
            used: if assumed_fresh { 0.0 } else { window.used_pct },
            assumed_fresh,
            hours_to_reset: window
                .resets_at
                .filter(|at| *at > now)
                .map(|at| (at - now) as f64 / 3600.0),
        }
    }

    fn latest(&self) -> LatestReading<'a> {
        LatestReading {
            window_name: &self.window.window_name,
            scope_target: self.window.scope_target.as_deref(),
            used_pct: self.used,
        }
    }

    fn label(&self) -> String {
        match &self.window.scope_target {
            Some(target) => format!("{} ({target} only)", self.window.window_name),
            None => self.window.window_name.clone(),
        }
    }

    fn json(&self) -> Value {
        json!({
            "windowId": self.window.window_id,
            "windowName": self.window.window_name,
            "scopeTarget": self.window.scope_target,
            "usedPercent": self.used,
            "assumedFresh": self.assumed_fresh,
            "resetsAt": self.window.resets_at,
            "hoursToReset": self.hours_to_reset,
            "burnPercentPerHour": self.window.burn_pct_per_hour,
            "capturedAt": self.window.captured_at,
        })
    }
}

/// One live account with readings, and its windows as of now.
/// One account's answer to an inverse query: each applicable window with the
/// expected percent and the rate behind it, and the chance the spend fits.
type AccountEstimate<'r, 'a> = (
    &'r AccountNow<'a>,
    Vec<(
        &'r WindowNow<'a>,
        cairn_models::types::drain::Estimate,
        cairn_models::types::drain::Rate,
    )>,
    Option<f64>,
);

struct AccountNow<'a> {
    account: &'a LiveAccount,
    provider: String,
    plan: Option<String>,
    windows: Vec<WindowNow<'a>>,
}

impl AccountNow<'_> {
    fn reference(&self) -> AccountRef<'_> {
        AccountRef {
            id: &self.account.id,
            provider: &self.provider,
            plan: self.plan.as_deref(),
        }
    }

    fn heading(&self) -> String {
        let name = if self.account.name == self.account.id {
            String::new()
        } else {
            format!(" {}", self.account.name)
        };
        format!(
            "`{}`{name} — {} · {}",
            short_account(&self.account.id),
            self.provider,
            self.plan.as_deref().unwrap_or("no priced plan")
        )
    }
}

fn accounts_now<'a>(
    fit: &'a DrainFit,
    current: &'a [CurrentWindow],
    accounts: &'a [LiveAccount],
    only: Option<&str>,
    now: i64,
) -> Vec<AccountNow<'a>> {
    accounts
        .iter()
        .filter(|account| {
            only.is_none_or(|only| only == account.id || only == short_account(&account.id))
        })
        .filter_map(|account| {
            let mut windows: Vec<WindowNow> = current
                .iter()
                .filter(|window| window.account == account.id)
                .map(|window| WindowNow::of(window, now))
                .collect();
            windows.sort_by(|a, b| a.window.window_name.cmp(&b.window.window_name));
            let first = windows.first()?;
            let plan = fit.plan_of(&account.id);
            Some(AccountNow {
                account,
                provider: plan
                    .map_or_else(|| first.window.provider.clone(), |p| p.provider.clone()),
                plan: plan.and_then(|p| p.plan.clone()),
                windows,
            })
        })
        .collect()
}

/// The families to show for an account: the one asked for, else every family
/// spent on its provider, the account's own largest first.
fn families(fit: &DrainFit, account: &AccountNow, model: Option<&str>) -> Vec<String> {
    if let Some(model) = model {
        return family_of(model)
            .filter(|family| fit.families.get(family) == Some(&account.provider))
            .into_iter()
            .collect();
    }
    let mut families: Vec<(String, f64)> = fit
        .families_on(&account.provider)
        .map(|family| {
            let spent = fit
                .mixes
                .iter()
                .find(|mix| mix.account == account.account.id && mix.family == family)
                .map_or(0.0, |mix| mix.all_time.usd.total());
            (family.to_string(), spent)
        })
        .collect();
    families.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    families.into_iter().map(|(family, _)| family).collect()
}

/// Each window's capacity for `family` on the account, and the index of the
/// binding one (the least left).
fn capacities<'a>(
    fit: &DrainFit,
    account: &'a AccountNow,
    family: &str,
) -> (Vec<(&'a WindowNow<'a>, Capacity)>, Option<usize>) {
    let mix = fit.mix(&account.account.id, family);
    let rows: Vec<(&WindowNow, Capacity)> = account
        .windows
        .iter()
        .filter_map(|window| {
            let capacity = fit.remaining(&account.reference(), &window.latest(), family, mix)?;
            Some((window, capacity))
        })
        .collect();
    let binding = rows
        .iter()
        .enumerate()
        .min_by(|a, b| a.1 .1.usd.value.total_cmp(&b.1 .1.usd.value))
        .map(|(index, _)| index);
    (rows, binding)
}

fn estimate_json(estimate: &cairn_models::types::drain::Estimate) -> Value {
    serde_json::to_value(estimate).unwrap_or(Value::Null)
}

fn capacity_json(window: &WindowNow, capacity: &Capacity) -> Value {
    json!({
        "windowName": window.window.window_name,
        "remainingPercent": capacity.remaining_percent,
        "usd": estimate_json(&capacity.usd),
        "tokens": capacity.tokens.as_ref().map(estimate_json),
        "percentPerUsd": estimate_json(&capacity.rate.percent_per_usd),
        "support": capacity.rate.support.label(),
        "instances": capacity.rate.instances,
        "accountScale": capacity.rate.account_scale,
    })
}

fn intro(fit: &DrainFit) -> String {
    format!(
        "From the window drain fit made {} (its report, with every coefficient, interval and held-out error, is cairn://models/usage?capacity=fit). On an all-models window, what is left for a model is what remains if only that model ran; a model's binding window is the one with the least left for it. A window whose reset has passed reads as a fresh 100% (assumed fresh). Intervals are 90% and carry the rate's, the reading's ±0.5-point rounding and the window's residual; n is the instances behind the rate.\n",
        when(fit.fitted_at)
    )
}

fn render_accounts(fit: &DrainFit, accounts: &[AccountNow], query: &CapacityQuery) -> String {
    if query.json {
        let accounts: Vec<Value> = accounts
            .iter()
            .map(|account| {
                let models: Vec<Value> = if account.account.blocked.is_some() {
                    Vec::new()
                } else {
                    families(fit, account, query.model.as_deref())
                        .iter()
                        .map(|family| {
                            let (rows, binding) = capacities(fit, account, family);
                            json!({
                                "model": family,
                                "binding": binding.map(|index| rows[index].0.window.window_name.clone()),
                                "windows": rows.iter().map(|(window, capacity)| capacity_json(window, capacity)).collect::<Vec<_>>(),
                            })
                        })
                        .collect()
                };
                json!({
                    "account": account.account.id,
                    "name": account.account.name,
                    "provider": account.provider,
                    "plan": account.plan,
                    "blocked": account.account.blocked,
                    "windows": account.windows.iter().map(WindowNow::json).collect::<Vec<_>>(),
                    "models": models,
                })
            })
            .collect();
        return serde_json::to_string_pretty(&json!({
            "fittedAt": fit.fitted_at,
            "accounts": accounts,
        }))
        .unwrap_or_default();
    }
    let mut out = String::from("# Usage capacity\n\n");
    out.push_str(&intro(fit));
    if accounts.is_empty() {
        out.push_str(match &query.account {
            Some(_) => "\nNo live account by that id has recorded usage readings; account ids are at cairn://models/accounts.\n",
            None => "\nNo live account has recorded usage readings yet.\n",
        });
        return out;
    }
    for account in accounts {
        let _ = writeln!(out, "\n## {}\n", account.heading());
        out.push_str("| Window | Used | Resets | Hours to reset | Burn, last 24 h |\n|---|---|---|---|---|\n");
        for window in &account.windows {
            let _ = writeln!(
                out,
                "| {} | {}%{} | {} | {} | {} |",
                window.label(),
                sig(window.used),
                if window.assumed_fresh {
                    " (assumed fresh)"
                } else {
                    ""
                },
                window
                    .window
                    .resets_at
                    .map_or_else(|| "—".to_string(), when),
                window
                    .hours_to_reset
                    .map_or_else(|| "—".to_string(), |hours| format!("{hours:.1}")),
                window
                    .window
                    .burn_pct_per_hour
                    .map_or_else(|| "—".to_string(), |burn| format!("{}%/h", sig(burn))),
            );
        }
        if let Some(reason) = &account.account.blocked {
            let _ = writeln!(
                out,
                "\nBlocked ({reason}): no capacity is served until the operator reconnects it (cairn://models/accounts)."
            );
            continue;
        }
        let mut table = String::from("\n| Model | Window | Left, list $ (90%) | Left, tokens at its recent mix (90%) | % of window per list $ | Support | n |\n|---|---|---|---|---|---|---|\n");
        let mut any = false;
        for family in families(fit, account, query.model.as_deref()) {
            let (rows, binding) = capacities(fit, account, &family);
            for (index, (window, capacity)) in rows.iter().enumerate() {
                any = true;
                let _ = writeln!(
                    table,
                    "| {family} | {}{} | {} | {} | {} | {} | {} |",
                    window.label(),
                    if Some(index) == binding {
                        " (binds)"
                    } else {
                        ""
                    },
                    interval(&capacity.usd, dollars),
                    capacity
                        .tokens
                        .as_ref()
                        .map_or_else(|| "—".to_string(), |tokens| interval(tokens, token_count)),
                    sig(capacity.rate.percent_per_usd.value),
                    capacity.rate.support.label(),
                    capacity.rate.instances,
                );
            }
        }
        if any {
            out.push_str(&table);
        } else {
            out.push_str("\nNo fitted rate covers this account's windows yet.\n");
        }
    }
    out
}

fn render_inverse(
    fit: &DrainFit,
    accounts: &[AccountNow],
    model: &str,
    spend: &Components,
    json: bool,
) -> String {
    let family = family_of(model).unwrap_or_default();
    let Some(price) = list_price(model) else {
        return format!("No list price is known for {model}.");
    };
    let usd = price.rates.cost(spend);
    let applicable: Vec<&AccountNow> = accounts
        .iter()
        .filter(|account| fit.families.get(&family) == Some(&account.provider))
        .collect();
    let per_account: Vec<AccountEstimate<'_, '_>> = applicable
        .iter()
        .map(|account| {
            let reference = account.reference();
            let rows = account
                .windows
                .iter()
                .filter_map(|window| {
                    let rate = fit.rate(&reference, &window.window.window_name, model)?;
                    let percent =
                        fit.percent_for(&reference, &window.window.window_name, model, usd)?;
                    Some((window, percent, rate))
                })
                .collect();
            let latest: Vec<LatestReading> =
                account.windows.iter().map(WindowNow::latest).collect();
            let fits = if account.account.blocked.is_some() {
                Some(0.0)
            } else {
                fit.fits(&reference, model, usd, &latest)
            };
            (*account, rows, fits)
        })
        .collect();
    if json {
        let accounts: Vec<Value> = per_account
            .iter()
            .map(|(account, rows, fits)| {
                json!({
                    "account": account.account.id,
                    "name": account.account.name,
                    "plan": account.plan,
                    "blocked": account.account.blocked,
                    "fits": fits,
                    "windows": rows.iter().map(|(window, percent, rate)| json!({
                        "windowName": window.window.window_name,
                        "expectedPercent": estimate_json(percent),
                        "remainingPercent": 100.0 - window.used,
                        "assumedFresh": window.assumed_fresh,
                        "support": rate.support.label(),
                        "instances": rate.instances,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        return serde_json::to_string_pretty(&json!({
            "fittedAt": fit.fitted_at,
            "model": model,
            "family": family,
            "spend": spend,
            "listUsd": usd,
            "accounts": accounts,
        }))
        .unwrap_or_default();
    }
    let mut out = String::from("# Usage capacity: what a spend costs\n\n");
    let _ = writeln!(
        out,
        "{model} (drains as {family}): {} input, {} cache read, {} cache write and {} output tokens, {} at list price. Each row is the share of that window the spend is expected to use; P(fits) is the chance it fits what every window it counts against has left, treating the windows as independent (which understates it when they move together).\n",
        token_count(spend.input),
        token_count(spend.cache_read),
        token_count(spend.cache_write),
        token_count(spend.output),
        dollars(usd),
    );
    out.push_str(&intro(fit));
    if per_account.is_empty() {
        let _ = writeln!(out, "\nNo live account with readings runs {family}.");
        return out;
    }
    for (account, rows, fits) in &per_account {
        let _ = writeln!(out, "\n## {}\n", account.heading());
        if let Some(reason) = &account.account.blocked {
            let _ = writeln!(
                out,
                "Blocked ({reason}): nothing fits until it is reconnected."
            );
            continue;
        }
        if rows.is_empty() {
            out.push_str("No fitted rate covers this account's windows yet.\n");
            continue;
        }
        out.push_str("| Window | Expected % of the window (90%) | Left now | Support | n |\n|---|---|---|---|---|\n");
        for (window, percent, rate) in rows {
            let _ = writeln!(
                out,
                "| {} | {} | {}%{} | {} | {} |",
                window.label(),
                interval(percent, sig),
                sig(100.0 - window.used),
                if window.assumed_fresh {
                    " (assumed fresh)"
                } else {
                    ""
                },
                rate.support.label(),
                rate.instances,
            );
        }
        if let Some(fits) = fits {
            let _ = writeln!(out, "\nP(fits): {}", sig(*fits));
        }
    }
    out
}

/// Render a capacity read from the served fit, each window's current reading
/// and the accounts.
pub(crate) fn render(
    query: &CapacityQuery,
    fit: &DrainFit,
    current: &[CurrentWindow],
    accounts: &[LiveAccount],
    now: i64,
) -> String {
    let accounts = accounts_now(fit, current, accounts, query.account.as_deref(), now);
    match &query.view {
        CapacityView::Fit if query.json => serde_json::to_string_pretty(fit).unwrap_or_default(),
        CapacityView::Fit => report::render_fit_report(fit),
        CapacityView::Accounts => render_accounts(fit, &accounts, query),
        CapacityView::Inverse(spend) => render_inverse(
            fit,
            &accounts,
            query.model.as_deref().unwrap_or_default(),
            spend,
            query.json,
        ),
    }
}

/// The read itself: the served fit (fitted once here when none is held yet),
/// each window's current reading read now, and the workspace's accounts.
pub(crate) async fn read_capacity(orch: &Orchestrator, query: &CapacityQuery) -> String {
    let fit = match orch.drain_fit().await {
        Ok(fit) => fit,
        Err(error) => return format!("Usage capacity is unavailable: {error}"),
    };
    let current = match current_readings(&orch.db.local).await {
        Ok(readings) => current_windows(&readings),
        Err(error) => return format!("Could not read the latest usage readings: {error}"),
    };
    let now = chrono::Utc::now().timestamp();
    let accounts = match sorted_accounts(orch, &ModelScope::workspace()) {
        Ok(accounts) => live_accounts(&accounts, now),
        Err(error) => return format!("Could not read accounts: {error}"),
    };
    render(query, &fit, &current, &accounts, now)
}

#[cfg(test)]
mod tests;
