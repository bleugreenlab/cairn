//! Renderers for the cairn-models facet's resources, `cairn://models/*`.
//!
//! Each renders an answer from [`Models`], never from the orchestrator directly,
//! so what these pages show is exactly what the facet interface says, and a
//! test can seed any facet it likes. The one exception is
//! `cairn://models/embeddings`, which renders the orchestrator's
//! `EmbeddingStatus` handed to it as a value: that status is what the stored
//! vectors hold, which the facet does not know, and taking it as a value keeps
//! the renderer just as testable. Accounts are rendered from their
//! serialized form, the same projection the settings UI receives, which is why
//! no credential value can reach a page: it was never in the projection.

use cairn_common::query::QueryParam;
use cairn_models::interface::{
    Catalog, CatalogListing, CatalogProvider, ListedProvider, ModelScope, Models, SessionState,
    SessionView,
};
use cairn_models::types::catalog::{
    custom_account_id, DiscoveredModel, Modalities, Modality, ModelAvailability, ModelPricing,
    ProviderCategory, ProviderDescriptor, ServedVerb, Verb, VerbKind, Wire,
};
use cairn_models::types::embedding::CAIRN_GATEWAY_KEY;
use cairn_models::types::pool::{
    ProviderPoolAccountResets, ProviderPoolAccountState, ProviderPoolConstraintReach,
    ProviderPoolSegmentState, ProviderPoolSnapshot,
};
use cairn_models::types::usage::ProviderUsageWindow;
use cairn_models::types::usage::{BillingUnit, Charge};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write as _;

use super::common::find_query_value;
use crate::orchestrator::account_events::{CredentialEvent, CredentialEventKind};
use cairn_common::identity::PrincipalRef;
use cairn_models::subscription::PendingSignIn;

const INDEX: &[(&str, &str)] = &[
    (
        "cairn://models/catalog",
        "installed providers and what model discovery found on each, with makers and effort scales",
    ),
    (
        "cairn://models/decide",
        "answer typed questions with a named model: a run target, not a write",
    ),
    (
        "cairn://models/embed",
        "embed texts with a named model: a run target, not a write",
    ),
    (
        "cairn://models/complete",
        "send one completion to a named model: a run target, not a write",
    ),
    (
        "cairn://models/accounts",
        "provider accounts by provider, with their live usage windows (never a credential)",
    ),
    (
        "cairn://models/usage",
        "each provider's capacity per account and pooled, as Settings → Analytics shows it; ?history=<account> lists that account's recorded window readings over time",
    ),
    (
        "cairn://models/spend",
        "what each thread and its child issues cost over a window (?window=24h|7d|30d|90d|all, ?thread=<name>)",
    ),
    (
        "cairn://models/ladder",
        "the pools allotments name and the capability ladder they slice",
    ),
    (
        "cairn://models/selection",
        "what an allotment would run on now, with the one-line why (a preview; ?models=lg,claude/fable)",
    ),
    (
        "cairn://models/embeddings",
        "the embedding model and how far history is embedded in it",
    ),
    (
        "cairn://models/sessions",
        "live model sessions on this host; stop one at cairn://models/sessions/<id>",
    ),
    (
        "cairn://models/clients",
        "outside clients of the served OpenAI-compatible API: their keys and remote grants",
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

fn charge(charge: Charge) -> &'static str {
    match charge {
        Charge::Input => "in",
        Charge::Output => "out",
        Charge::CacheRead => "cache read",
        Charge::CacheWrite => "cache write",
        Charge::Reasoning => "reasoning",
        Charge::Request => "request",
        Charge::WebSearch => "web search",
    }
}

/// Input and output tokens first, as `in $X/M, out $Y/M`, then every other
/// price the provider set, each in its own unit. A price of zero on any other
/// meter is left out: providers report it for every charge they don't make.
fn pricing(pricing: &ModelPricing) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(prompt) = pricing.usd_per(Charge::Input, None, BillingUnit::Tokens) {
        parts.push(format!("in {}", per_million(prompt)));
    }
    if let Some(completion) = pricing.usd_per(Charge::Output, None, BillingUnit::Tokens) {
        parts.push(format!("out {}", per_million(completion)));
    }
    for price in &pricing.prices {
        let meter = &price.meter;
        let headline = matches!(meter.charge, Charge::Input | Charge::Output)
            && meter.modality.is_none()
            && meter.unit == BillingUnit::Tokens;
        if headline || price.usd.parse::<f64>().is_ok_and(|usd| usd == 0.0) {
            continue;
        }
        let label = match &meter.modality {
            Some(modality) => format!("{modality} {}", charge(meter.charge)),
            None => charge(meter.charge).to_string(),
        };
        let amount = match meter.unit {
            BillingUnit::Tokens => per_million(&price.usd),
            unit => format!("${}/{}", price.usd, unit.noun()),
        };
        parts.push(format!("{label} {amount}"));
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// `text+image → text`, or `None` when the provider said nothing.
fn modalities(modalities: &Modalities) -> Option<String> {
    if modalities.input.is_empty() && modalities.output.is_empty() {
        return None;
    }
    let side = |side: &[Modality]| {
        if side.is_empty() {
            "?".to_string()
        } else {
            side.iter()
                .map(Modality::as_str)
                .collect::<Vec<_>>()
                .join("+")
        }
    };
    Some(format!(
        "{} → {}",
        side(&modalities.input),
        side(&modalities.output)
    ))
}

/// The verbs grouped by the wire each rides, `complete, session
/// (openai-chat)`, or `None` for serving nothing. A verb with capabilities
/// says them inside its own group: `speak (openai-speech: segmented ·
/// pcm_24000|mp3|opus · speed 25–400%)`, `image (openrouter-images: ≤4 ·
/// refs ≤16 · no mask)`, or `embed (openai-embeddings: 2560 · native width
/// only · query prefix · ≤32 texts)`.
fn served(serves: &[ServedVerb]) -> Option<String> {
    enum Group<'a> {
        /// Capability-free verbs sharing a wire.
        Verbs(Wire, Vec<&'static str>),
        /// One verb that says what it can do.
        Capable(&'a ServedVerb),
    }
    let mut groups: Vec<Group> = Vec::new();
    for served in serves {
        if matches!(
            served.verb,
            Verb::Speak(_) | Verb::Image(_) | Verb::Embed(_)
        ) {
            groups.push(Group::Capable(served));
            continue;
        }
        let verb = served.verb.kind().as_str();
        let shared = groups.iter_mut().find_map(|group| match group {
            Group::Verbs(wire, verbs) if *wire == served.wire => Some(verbs),
            _ => None,
        });
        match shared {
            Some(verbs) => verbs.push(verb),
            None => groups.push(Group::Verbs(served.wire, vec![verb])),
        }
    }
    (!groups.is_empty()).then(|| {
        groups
            .iter()
            .map(|group| match group {
                Group::Verbs(wire, verbs) => format!("{} ({wire})", verbs.join(", ")),
                Group::Capable(served) => match &served.verb {
                    Verb::Speak(capabilities) => {
                        format!("speak ({}: {capabilities})", served.wire)
                    }
                    Verb::Image(capabilities) => {
                        format!("image ({}: {})", served.wire, capabilities.summary())
                    }
                    Verb::Embed(capabilities) => {
                        format!("embed ({}: {})", served.wire, capabilities.summary())
                    }
                    other => format!("{} ({})", other.kind(), served.wire),
                },
            })
            .collect::<Vec<_>>()
            .join("; ")
    })
}

/// What a model or offer line appends after its context and price: its
/// modalities when known, and what it serves.
fn capability(modalities_known: Option<&Modalities>, serves: &[ServedVerb]) -> String {
    let mut out = String::new();
    if let Some(text) = modalities_known.and_then(modalities) {
        let _ = write!(out, " · {text}");
    }
    match served(serves) {
        Some(text) => {
            let _ = write!(out, " · serves {text}");
        }
        None => out.push_str(" · serves nothing Cairn runs"),
    }
    out
}

fn availability(value: &ModelAvailability) -> String {
    let duration = |ms: u64, round_up: bool| {
        if ms < 1_000 {
            format!("{ms} ms")
        } else {
            crate::fleet::models::settings::duration_label(if round_up {
                ms.div_ceil(1_000)
            } else {
                ms / 1_000
            })
        }
    };
    let (state, pool, expected, elapsed) = match value {
        ModelAvailability::Loaded { pool } => return format!("loaded in pool `{pool}`"),
        ModelAvailability::Loadable {
            pool,
            expected_load_ms,
        } => ("loadable", pool, expected_load_ms, None),
        ModelAvailability::Loading {
            pool,
            expected_load_ms,
            elapsed_ms,
        } => ("loading", pool, expected_load_ms, Some(elapsed_ms)),
    };
    let estimate = match expected {
        Some(ms) => format!("expected load {}", duration(*ms, true)),
        None => "expected load time unknown".to_string(),
    };
    let mut text = format!("{state} in pool `{pool}`, {estimate}");
    if let Some(ms) = elapsed {
        let _ = write!(
            text,
            ", {} elapsed (not remaining time)",
            duration(*ms, false)
        );
    }
    text
}

fn availability_facts(
    aggregate: Option<&ModelAvailability>,
    by_verb: &std::collections::BTreeMap<VerbKind, ModelAvailability>,
    serving: Option<VerbKind>,
) -> Vec<String> {
    let differs = by_verb
        .values()
        .next()
        .is_some_and(|first| by_verb.values().any(|value| value != first));
    if serving.is_none() && differs {
        by_verb
            .iter()
            .map(|(verb, value)| format!("{verb}: {}", availability(value)))
            .collect()
    } else {
        aggregate.map(availability).into_iter().collect()
    }
}

fn category(category: ProviderCategory) -> &'static str {
    match category {
        ProviderCategory::Subscription => "subscription",
        ProviderCategory::Api => "metered API",
        ProviderCategory::Local => "local",
    }
}

fn heading(descriptor: &ProviderDescriptor) -> String {
    format!("{} (`{}`)", descriptor.display_name, descriptor.key)
}

/// One listed model. Its `serves` is the listing's resolved answer; its maker
/// and effort scale are what `provider` (the catalog's record of the same
/// provider) answers, which is what selection reads.
fn model_line(
    model: &DiscoveredModel,
    provider: Option<&CatalogProvider>,
    serving: Option<VerbKind>,
) -> String {
    let mut line = format!("- `{}`", model.model);
    if model.display_name != model.model {
        let _ = write!(line, " {}", model.display_name);
    }
    let mut facts = Vec::new();
    if let Some(window) = model.context_window {
        facts.push(format!("{} context", tokens(window)));
    }
    if let Some(maker) = provider.and_then(|provider| provider.maker(&model.model)) {
        facts.push(format!("by {maker}"));
    }
    if let Some(scale) = provider.and_then(|provider| provider.effort_scale(&model.model)) {
        facts.push(format!("effort {}", scale.levels.join("|")));
    }
    if let Some(price) = model.pricing.as_ref().and_then(pricing) {
        facts.push(price);
    }
    facts.extend(availability_facts(
        model.availability.as_ref(),
        &model.availability_by_verb,
        serving,
    ));
    if model.is_default {
        facts.push("provider default".to_string());
    }
    if !facts.is_empty() {
        let _ = write!(line, " — {}", facts.join(", "));
    }
    if model.hidden {
        let reason = model.description.as_deref().unwrap_or("not servable here");
        let _ = write!(line, " — unavailable: {reason}");
    } else {
        line.push_str(&capability(
            model.modalities.as_ref(),
            model.serves.as_deref().unwrap_or_default(),
        ));
    }
    line
}

/// What a provider that holds no account stands on, when it is one: Cairn's
/// gateway runs on this device's Cairn token and is listed only while one
/// exists.
fn accountless(provider: &ProviderDescriptor) -> Option<&'static str> {
    (provider.key == CAIRN_GATEWAY_KEY).then_some("this device's Cairn token")
}

/// Whether `account` (serialized [`AccountInfo`]) belongs on `provider`'s
/// tab: a custom provider owns exactly its own account, Cairn's gateway owns
/// none (its descriptor shares the custom credential provider, but no custom
/// endpoint's key is ever its credential), and every other provider owns the
/// accounts of its credential provider.
///
/// [`AccountInfo`]: cairn_models::accounts::AccountInfo
fn account_serves(account: &Value, provider: &ProviderDescriptor) -> bool {
    if accountless(provider).is_some() {
        return false;
    }
    match custom_account_id(&provider.key) {
        Some(id) => account["id"].as_str() == Some(id),
        None => account["apiProvider"].as_str() == Some(provider.api_provider.as_str()),
    }
}

/// Refuse a `provider` filter naming none of `known`, listing them.
fn unknown_provider<'a>(
    only: Option<&str>,
    known: impl Iterator<Item = &'a str>,
) -> Option<String> {
    let key = only?;
    let known: Vec<&str> = known.collect();
    (!known.contains(&key)).then(|| {
        format!(
            "No provider '{key}' in this workspace's catalog. Known: {}.",
            known.join(", ")
        )
    })
}

fn sorted_accounts(facet: &dyn Models, scope: &ModelScope) -> Result<Vec<Value>, String> {
    let mut accounts: Vec<Value> = facet
        .accounts(scope)?
        .iter()
        .map(|account| serde_json::to_value(account).unwrap_or(Value::Null))
        .collect();
    accounts.sort_by_key(|account| account["sortOrder"].as_i64().unwrap_or_default());
    Ok(accounts)
}

fn level_text(level: f64) -> String {
    format!("{}", (level * 100.0).round() / 100.0)
}

/// `cairn://models/ladder`: the view the Models page reads, as text.
pub(crate) fn render_ladder(
    params: &[QueryParam],
    view: impl FnOnce() -> Result<crate::orchestrator::model_selection::LadderView, String>,
) -> String {
    use crate::orchestrator::model_selection::LadderRung;
    const URI: &str = "cairn://models/ladder";
    if let Some(error) = unsupported(URI, params, &[]) {
        return error;
    }
    let view = match view() {
        Ok(view) => view,
        Err(error) => return format!("Could not read the ladder: {error}"),
    };
    let mut out = String::from("# Model ladder\n\n");
    out.push_str("An allotment (`models: [sm, md, lg]`, `models: [lg, claude/fable]`) names pools and models; a launch selects inside it. Band pools are slices of the 0..1 capability ladder below; set pools are hand-picked. Change any of it with a `cairn://settings` patch of `models`.\n");
    out.push_str("\nA benchmark level (the expected solve rate on the anchor's reference panel, fitted from licensed public benchmarks) is an input, never a setting. It is a solve rate, not a ladder position, so it is mapped onto this ladder through the entries here that have both a level and a benchmark level. Mapped, it places a model the ladder holds no level for when an allotment names it or a set pool holds it, and it is shown beside a stored level, which always wins. Band pools hold only what the ladder places.\n");
    out.push_str("\n## Pools\n\n");
    for pool in &view.pools {
        let kind = match (&pool.band, &pool.members) {
            (Some(band), _) => format!("band {}–{}", level_text(band.min), level_text(band.max)),
            (None, Some(members)) if members.is_empty() => "set, empty".to_string(),
            (None, Some(members)) => format!("set [{}]", members.join(", ")),
            (None, None) => "band (no bounds)".to_string(),
        };
        let description = if pool.description.is_empty() {
            "(no description)"
        } else {
            pool.description.as_str()
        };
        let _ = writeln!(out, "- `{}` {kind}: {description}", pool.id);
    }
    let rung_line = |rung: &LadderRung| {
        let mut line = format!("- `{}`", rung.model);
        if let Some(level) = rung.level {
            let _ = write!(line, " {}", level_text(level));
            let source = match rung.level_source {
                Some(cairn_config::allotment::LevelSource::Seeded) => "seeded",
                Some(cairn_config::allotment::LevelSource::Measured) => "measured",
                Some(cairn_config::allotment::LevelSource::Benchmarks) => "benchmarks",
                Some(cairn_config::allotment::LevelSource::Set) | None => "set",
            };
            match &rung.level_from {
                Some(from) => {
                    let _ = write!(line, " ({source}: {from})");
                }
                None => {
                    let _ = write!(line, " ({source})");
                }
            }
        }
        let mut facts: Vec<String> = Vec::new();
        facts.push(match &rung.maker {
            Some(maker) => format!("by {maker}"),
            None => "maker unknown".to_string(),
        });
        let scale = rung
            .effort_scale
            .iter()
            .map(|step| step.level.as_str())
            .collect::<Vec<_>>()
            .join("|");
        match (
            scale.is_empty(),
            rung.effort,
            &rung.effort_level,
            &rung.effort_word,
        ) {
            (false, Some(x), Some(level), _) => {
                facts.push(format!("effort {} → {level} of {scale}", level_text(x)))
            }
            (_, None, Some(level), Some(_)) => {
                facts.push(format!("effort {level} (kept from the tier grid) of {scale}"))
            }
            (_, None, None, Some(word)) => facts.push(format!(
                "effort '{word}' kept from the tier grid, not one of this model's known levels; it launches at the provider's default"
            )),
            (false, None, _, _) => facts.push(format!("effort default of {scale}")),
            (true, Some(x), _, _) => facts.push(format!(
                "effort {} (this model takes no effort)",
                level_text(x)
            )),
            _ => {}
        }
        for (key, value) in &rung.options {
            let value = match value {
                crate::models::PresetOptionValue::Str(value) => value.clone(),
                crate::models::PresetOptionValue::Bool(value) => value.to_string(),
                crate::models::PresetOptionValue::Int(value) => value.to_string(),
            };
            facts.push(format!("{key} {value}"));
        }
        if !rung.pools.is_empty() {
            facts.push(format!("in {}", rung.pools.join(", ")));
        }
        if let Some(newer) = &rung.superseded_by {
            facts.push(format!("superseded by {newer}"));
        }
        if let Some(hint) = &rung.benchmark {
            facts.push(match (rung.level, hint.position, &rung.band) {
                (None, Some(position), Some(band)) => format!(
                    "placed by benchmarks at {} ({band}): {}",
                    level_text(position),
                    hint.summary
                ),
                _ => hint.summary.clone(),
            });
        }
        if rung.benchmark.is_none() {
            if let Some(note) = &rung.identity_note {
                facts.push(note.clone());
            }
        }
        let _ = write!(line, " — {}", facts.join(", "));
        line
    };
    out.push_str("\n## Ladder\n");
    for band in view.bands.iter().rev() {
        let _ = writeln!(
            out,
            "\n### {} ({}–{})\n",
            band.id,
            level_text(band.min),
            level_text(band.max)
        );
        let members: Vec<&LadderRung> = view
            .rungs
            .iter()
            .filter(|rung| rung.level.is_some() && rung.band.as_deref() == Some(band.id.as_str()))
            .collect();
        if members.is_empty() {
            out.push_str("Nothing on the ladder here.\n");
        }
        for rung in members {
            out.push_str(&rung_line(rung));
            out.push('\n');
        }
    }
    let off: Vec<&LadderRung> = view
        .rungs
        .iter()
        .filter(|rung| rung.level.is_none())
        .collect();
    if !off.is_empty() {
        out.push_str("\n### Off the ladder (launch options only; selected only when named or in a set pool)\n\n");
        for rung in off {
            out.push_str(&rung_line(rung));
            out.push('\n');
        }
    }
    out.push_str("\n## Unranked\n\n");
    if view.unranked.is_empty() {
        out.push_str("Every served session model on an installed provider is on the ladder.\n");
    } else {
        out.push_str("Served session models on installed providers with no level. Named directly, one the benchmarks place selects in that band; one they do not cover counts as the top of its allotment.\n\n");
        for model in &view.unranked {
            match (&model.benchmark, &model.band) {
                (Some(hint), Some(band)) if hint.position.is_some() => {
                    let _ = writeln!(
                        out,
                        "- `{}` placed by benchmarks at {} ({band}): {}",
                        model.model,
                        level_text(hint.position.unwrap_or(hint.level)),
                        hint.summary
                    );
                }
                (Some(hint), _) => {
                    let _ = writeln!(out, "- `{}` {} (not placed)", model.model, hint.summary);
                }
                (None, _) => {
                    let _ = writeln!(
                        out,
                        "- `{}`{}",
                        model.model,
                        model
                            .identity_note
                            .as_ref()
                            .map(|note| format!(" — {note}"))
                            .unwrap_or_default()
                    );
                }
            }
        }
    }
    let defaults: Vec<String> = view
        .default_models
        .iter()
        .map(ToString::to_string)
        .collect();
    let _ = writeln!(
        out,
        "\nDefault allotment (a definition naming none): [{}]",
        defaults.join(", ")
    );
    if !view.conversion_notes.is_empty() {
        out.push_str("\n## From the conversion of tiers\n\n");
        for note in &view.conversion_notes {
            let _ = writeln!(out, "- {note}");
        }
    }
    if !view.benchmark_attribution.is_empty() {
        out.push_str("\n## Benchmark data\n\n");
        match &view.benchmark_note {
            Some(note) => {
                let _ = writeln!(out, "{note}\n");
            }
            None => {
                let points: Vec<String> = view
                    .benchmark_calibration
                    .iter()
                    .map(|[x, y]| format!("{x:.3} → {}", level_text(*y)))
                    .collect();
                let _ = writeln!(
                    out,
                    "Solve rates map onto this ladder through its own entries that have both a level and a benchmark level (solve rate → level): {}. Between them the map interpolates; beyond them it continues at its own overall slope. Within a product line, releases are placed in release order: one the benchmarks rank above a newer release is pooled with it, and a newer release with too few results of its own is placed at least where its predecessor is, which places it but calibrates nothing.\n",
                    points.join(", ")
                );
            }
        }
        for line in &view.benchmark_attribution {
            let _ = writeln!(out, "- {line}");
        }
    }
    out
}

/// `cairn://models/selection`: what an allotment would run on now.
/// `preview` is handed the allotment as written (`None` for the workspace
/// default) and the verb.
pub(crate) fn render_selection(
    params: &[QueryParam],
    preview: impl FnOnce(
        Option<Vec<String>>,
        VerbKind,
    ) -> Result<crate::execution::model_selection::SelectionRecord, String>,
) -> String {
    const URI: &str = "cairn://models/selection";
    if let Some(error) = unsupported(URI, params, &["models", "verb"]) {
        return error;
    }
    let verb = match crate::orchestrator::model_selection::preview_verb(find_query_value(
        params, "verb",
    )) {
        Ok(verb) => verb,
        Err(error) => return error,
    };
    let allotment: Option<Vec<String>> = find_query_value(params, "models").map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect()
    });
    let asked = match &allotment {
        Some(entries) => format!("[{}]", entries.join(", ")),
        None => "The workspace's default allotment".to_string(),
    };
    let mut out = String::from("# Model selection preview\n\n");
    let _ = writeln!(
        out,
        "{asked} for {}, if it launched now. A preview: nothing is stored, no constraint applies, and usage is what was last measured.\n",
        verb.as_str()
    );
    let record = match preview(allotment, verb) {
        Ok(record) => record,
        Err(refusal) => {
            let _ = writeln!(out, "Refused: {refusal}");
            return out;
        }
    };
    let _ = writeln!(out, "**{}**\n", record.provenance);
    let mut chosen = format!(
        "Chosen: `{}/{}`",
        record.chosen.backend,
        record.chosen.model.as_str()
    );
    if let Some(effort) = &record.extras.reasoning_effort {
        let _ = write!(chosen, " @ {effort}");
    }
    if let Some(account) = &record.account {
        let _ = write!(chosen, " on {}", account.label);
    }
    if let Some(maker) = &record.maker {
        let _ = write!(chosen, ", by {maker}");
    }
    if let Some(band) = &record.band {
        let _ = write!(chosen, ", band {band}");
    }
    let _ = writeln!(out, "{chosen}");
    for relaxation in &record.relaxations {
        let _ = writeln!(out, "Relaxed: {relaxation}");
    }
    out.push_str("\n## Candidates\n\nIn supply order, then everything set aside.\n\n");
    for candidate in &record.candidates {
        let mut line = format!("- `{}`", candidate.model);
        if let Some(band) = &candidate.band {
            let _ = write!(line, " {band}");
        }
        if let Some(level) = candidate.level {
            let _ = write!(line, " {}", level_text(level));
        }
        if let Some(maker) = &candidate.maker {
            let _ = write!(line, ", by {maker}");
        }
        let state = if candidate.class.is_some() {
            ""
        } else {
            "set aside: "
        };
        if let Some(note) = &candidate.identity_note {
            let _ = write!(line, " · {note}");
        }
        let _ = write!(line, " — {state}{}", candidate.note);
        let _ = writeln!(out, "{line}");
    }
    out
}

pub(crate) fn render_index() -> String {
    let mut out = String::from(
        "# cairn-models\n\nHow tokens are produced, routed, and accounted. Every read shows what Settings → Providers shows for this workspace, and none reveals a credential value.\n\n",
    );
    for (uri, what) in INDEX {
        let _ = writeln!(out, "- `{uri}` — {what}");
    }
    out
}

/// The decide door: what it takes, and every offer that could answer through
/// it now.
pub(crate) fn render_decide(facet: &dyn Models) -> String {
    let mut out = String::from(
        "# Decide\n\nInvoke with `run`, not `write`: `run({commands:[{target:\"cairn://models/decide\", payload:{args_json:{model, input, questions, provider?, requirements?}}}]})`. Each question is answered in order with the model's probability over every answer and how it was measured; a declined question comes back as a refusal. The model is never chosen for you.\n\nThe reply carries the call's `cost` and `costSource`: `provider` when the provider reported what it charged, `catalog` when it did not and the cost is the catalog's list price for the tokens it reported. `requirements: {zeroDataRetention: true}` admits only a deployment that keeps none of the input (an OpenRouter model with a zero-retention endpoint, the operator's own hardware, a private custom endpoint, or a provider declared in `zeroDataRetentionProviders`) and refuses anything else before it is sent; the workspace's OpenRouter zero-retention setting requires it of every OpenRouter call. The reply's `zeroDataRetention` says what required it and how it was kept.\n\n",
    );
    let catalog = match facet.catalog(&ModelScope::workspace()) {
        Ok(catalog) => catalog,
        Err(error) => {
            let _ = writeln!(out, "Could not read the catalog: {error}");
            return out;
        }
    };
    let offers = catalog.offers_serving(VerbKind::Decide);
    if offers.is_empty() {
        let _ = writeln!(
            out,
            "Nothing in this workspace serves decide yet. {}",
            cairn_models::interface::fixes(VerbKind::Decide)
        );
        return out;
    }
    out.push_str("## Offers that serve decide\n\n");
    let names = catalog.request_names(&offers);
    for (offer, name) in offers.iter().zip(names) {
        let measured = offer
            .measured_by()
            .map_or("nothing", |method| method.as_str());
        let _ = writeln!(
            out,
            "- `{name}` on `{}` — measured by {measured}, over `{}`",
            offer.provider, offer.served.wire
        );
    }
    out.push_str("\nA logprobs offer not yet seen to answer is asked one probe question before its first decision. Each offer in full: `cairn://models/catalog?serves=decide`.\n");
    out
}

/// The embed door: what it takes, and every offer that could embed through it
/// now, with what each declares.
pub(crate) fn render_embed(facet: &dyn Models) -> String {
    let mut out = String::from(
        "# Embed\n\nInvoke with `run`, not `write`: `run({commands:[{target:\"cairn://models/embed\", payload:{args_json:{model, input, purpose?, width?, save?, requirements?}}}]})`. It embeds each text, in input order, with the exact model and width that produce the vectors. `purpose` is `document` (the default) or `query`; `width` asks for exactly that many dimensions, and is refused when the offer cannot return it. `requirements: {zeroDataRetention: true}` admits only a deployment that keeps none of the texts and refuses anything else before it is sent; the reply's `zeroDataRetention` says how it was kept. The reply's `cost` is the provider's figure, or the catalog's list price for the tokens reported (`costSource` says which). The model is never chosen for you.\n\nThe reply is saved into your node's `$TMPDIR`, and the result is one line (`N × D · model · provider · cost · latency`) and the two files: `embeddings/<stamp>-<id>.json` holds the whole reply (model, dims, provider, usage, cost, and the input texts beside their vectors), and the `.npy` beside it holds the vectors as float32, shape `[N, dims]`. Your next `run` finds them on whatever machine it lands. Load them in Python with `np.load(os.environ[\"TMPDIR\"] + \"/embeddings/<name>.npy\")`, or in Bun with `(await Bun.file(process.env.TMPDIR + \"/embeddings/<name>.json\").json()).vectors`. A script also gets every vector in the item's `data`, with the paths as `saved`. Pass `save: false` when embedding a corpus in many calls, so the copies do not fill the scratch; an unsaved result is the whole reply.\n\n",
    );
    let catalog = match facet.catalog(&ModelScope::workspace()) {
        Ok(catalog) => catalog,
        Err(error) => {
            let _ = writeln!(out, "Could not read the catalog: {error}");
            return out;
        }
    };
    let offers = catalog.offers_serving(VerbKind::Embed);
    if offers.is_empty() {
        let _ = writeln!(
            out,
            "Nothing in this workspace serves embed yet. {}",
            cairn_models::interface::fixes(VerbKind::Embed)
        );
        return out;
    }
    out.push_str("## Offers that serve embed\n\nEach is named as `model` takes it, qualified as `provider/model` where another provider offers the same model; it gives its native width, the other widths it can return, how it tells documents from queries, and its batch limits. A request over the limits is split into consecutive requests and joined in order.\n\n");
    let names = catalog.request_names(&offers);
    for (offer, name) in offers.iter().zip(names) {
        let declared = match &offer.served.verb {
            Verb::Embed(capabilities) => capabilities.summary(),
            _ => "declares nothing".to_string(),
        };
        let _ = writeln!(out, "- `{name}` on `{}` — {declared}", offer.provider);
    }
    out.push_str("\nEach offer in full: `cairn://models/catalog?serves=embed`.\n");
    out
}

/// The image or speak door: what it takes, the workspace default that stands
/// in for an omitted model, and the offers that serve the verb now.
pub(crate) fn render_media(
    facet: &dyn Models,
    verb: VerbKind,
    workspace_default: Option<&str>,
) -> String {
    let (title, uri, takes, setting, section) = match verb {
        VerbKind::Image => (
            "Image",
            "cairn://models/image",
            "prompt, model?, provider?, count?, size? | aspect? | orientation?, references?, edit?, extras?",
            "imageModel",
            "Images",
        ),
        _ => (
            "Speak",
            "cairn://models/speak",
            "text, voice, model?, provider?, format?, speed?, instructions?",
            "speechModel",
            "Speech",
        ),
    };
    let mut out = format!(
        "# {title}\n\nInvoke with `run`, not `write`: `run({{commands:[{{target:\"{uri}\", payload:{{args_json:{{{takes}}}}}}}]}})`. What it makes is stored in the calling run's issue (or thread) and the reply names its address; read cairn://help or the affordances below for the full shape.\n\n"
    );
    match workspace_default
        .map(str::trim)
        .filter(|model| !model.is_empty())
    {
        Some(model) => {
            let _ = writeln!(
                out,
                "Default: `{model}` (`{setting}`), used when a request names no model.\n"
            );
        }
        None => {
            let _ = writeln!(
                out,
                "Default: none. A request that names no model is refused until `{setting}` is set (Settings → Models → {section}).\n"
            );
        }
    }
    let catalog = match facet.catalog(&ModelScope::workspace()) {
        Ok(catalog) => catalog,
        Err(error) => {
            let _ = writeln!(out, "Could not read the catalog: {error}");
            return out;
        }
    };
    let offers = catalog.offers_serving(verb);
    if offers.is_empty() {
        let _ = writeln!(
            out,
            "Nothing in this workspace serves {} yet. {}",
            verb.as_str(),
            cairn_models::interface::fixes(verb)
        );
        return out;
    }
    let _ = writeln!(out, "## Offers that serve {}\n", verb.as_str());
    let names = catalog.request_names(&offers);
    for (offer, name) in offers.iter().zip(names) {
        let declared = match &offer.served.verb {
            Verb::Image(capabilities) => capabilities.summary(),
            Verb::Speak(capabilities) => capabilities.to_string(),
            _ => "declares nothing".to_string(),
        };
        let _ = writeln!(
            out,
            "- `{name}` on `{}` ({}) — {declared}",
            offer.provider, offer.served.wire
        );
    }
    let _ = writeln!(
        out,
        "\nEach offer in full: `cairn://models/catalog?serves={}`.",
        verb.as_str()
    );
    out
}

/// How many of a provider's completion models the complete page names before
/// pointing at the catalog.
const COMPLETE_MODELS_NAMED: usize = 5;

/// The complete door: what it takes, and which providers serve completions
/// now. A routing provider offers hundreds of models, so each provider is a
/// line with its count and a few names, and the catalog holds the rest.
pub(crate) fn render_complete(facet: &dyn Models) -> String {
    let mut out = String::from(
        "# Complete\n\nInvoke with `run`, not `write`: `run({commands:[{target:\"cairn://models/complete\", payload:{args_json:{model, prompt | messages, system?, options?, output?, requirements?}}}]})`. `requirements: {zeroDataRetention: true}` admits only a deployment that keeps none of the conversation and refuses anything else before it is sent; the reply's `zeroDataRetention` says how it was kept. One tool-free completion, sent and journaled exactly as a named response's is, without writing a definition first. `output` is a JSON Schema the answer must meet; the parsed answer comes back as `structured`. The model is never chosen for you.\n\nThe reply contains `text`, nullable `reasoning`, the selected `model` and `provider`, `usage`, optional `cost` (USD) with `costSource` (`provider`, or `catalog` when it is the list price for the tokens reported), `latencyMs`, and `history` (the response-history URI). `structured` is present only when an output schema was supplied and satisfied. `usage` contains `inputTokens`, `outputTokens`, `cachedInputTokens`, and `reasoningTokens`: each is null when unreported, preserves an explicit zero, and otherwise carries the provider-reported count. Reasoning tokens are a detail of output tokens, not an additional total. `reasoning` contains only visible plain reasoning or summaries, concatenated in provider order up to 16 KiB of UTF-8 in total; absent, empty, or over-limit reasoning is null, never truncated. Opaque signatures, redacted thinking, and encrypted continuation state are excluded. Chat accepts `reasoning` and `reasoning_content`; visible structured details take precedence, otherwise plain text is used. Messages exposes visible thinking, and Responses exposes summary text. Claude and Codex one-shot CLI replies currently return null reasoning. After a schema retry, reply text, reasoning, and usage describe the accepted attempt.\n\n",
    );
    let catalog = match facet.catalog(&ModelScope::workspace()) {
        Ok(catalog) => catalog,
        Err(error) => {
            let _ = writeln!(out, "Could not read the catalog: {error}");
            return out;
        }
    };
    let mut providers: Vec<(String, Vec<String>)> = Vec::new();
    for offer in catalog.offers_serving(VerbKind::Complete) {
        match providers.iter_mut().find(|(key, _)| *key == offer.provider) {
            Some((_, models)) => models.push(offer.model),
            None => providers.push((offer.provider, vec![offer.model])),
        }
    }
    if providers.is_empty() {
        let _ = writeln!(
            out,
            "Nothing in this workspace serves complete yet. {}",
            cairn_models::interface::fixes(VerbKind::Complete)
        );
        return out;
    }
    out.push_str("## Providers that serve complete\n\nName a model as `provider/model`, or by its catalog id.\n\n");
    for (provider, models) in providers {
        let named = models
            .iter()
            .take(COMPLETE_MODELS_NAMED)
            .map(|model| format!("`{model}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let more = models.len().saturating_sub(COMPLETE_MODELS_NAMED);
        let _ = write!(
            out,
            "- `{provider}` — {} model{}: {named}",
            models.len(),
            if models.len() == 1 { "" } else { "s" }
        );
        if more > 0 {
            let _ = write!(
                out,
                " and {more} more (`cairn://models/catalog?serves=complete&provider={provider}`)"
            );
        }
        out.push('\n');
    }
    out
}

pub(crate) fn render_catalog(facet: &dyn Models, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://models/catalog";
    if let Some(error) = unsupported(
        URI,
        params,
        &["view", "provider", "installed", "serves", "format"],
    ) {
        return error;
    }
    let json = match find_query_value(params, "format") {
        None | Some("markdown") => false,
        Some("json") => true,
        Some(other) => {
            return format!("Unknown format '{other}' for {URI}; expected markdown or json.")
        }
    };
    let serving = match find_query_value(params, "serves") {
        None => None,
        Some(value) => match VerbKind::parse(value) {
            Some(verb) => Some(verb),
            None => {
                return format!(
                    "Unknown serves='{value}' for {URI}; expected one of {}.",
                    VerbKind::ALL.map(VerbKind::as_str).join(", ")
                )
            }
        },
    };
    let include_uninstalled = match find_query_value(params, "installed") {
        None => false,
        Some("all") => true,
        Some(other) => return format!("Unknown installed='{other}' for {URI}; expected all."),
    };
    let only = find_query_value(params, "provider");
    let scope = ModelScope::workspace();
    match find_query_value(params, "view") {
        // The providers view is the catalog listing, the same answer the app's
        // settings pages read.
        None | Some("providers") => {
            let mut listing = match facet.catalog_listing(&scope, serving) {
                Ok(listing) => listing,
                Err(error) => return format!("Could not read the catalog: {error}"),
            };
            let keys = listing
                .providers
                .iter()
                .map(|provider| provider.descriptor.key.as_ref());
            if let Some(refusal) = unknown_provider(only, keys) {
                return refusal;
            }
            if json {
                listing.providers.retain(|provider| {
                    (include_uninstalled || provider.installed)
                        && only.is_none_or(|key| provider.descriptor.key == key)
                });
                if listing.selected.as_ref().is_some_and(|offer| {
                    !listing
                        .providers
                        .iter()
                        .any(|provider| provider.descriptor.key == offer.provider)
                }) {
                    listing.selected = None;
                }
                return serde_json::to_string_pretty(&listing).expect("catalog listing serializes");
            }
            let accounts = sorted_accounts(facet, &scope).unwrap_or_default();
            let catalog = facet.catalog(&scope).ok();
            render_providers(
                &listing,
                catalog.as_ref(),
                &accounts,
                only,
                include_uninstalled,
            )
        }
        // The catalog-id view is the projection offer selection walks.
        Some("models") => {
            let catalog = match facet.catalog(&scope) {
                Ok(catalog) => catalog,
                Err(error) => return format!("Could not read the catalog: {error}"),
            };
            let keys = catalog
                .providers
                .iter()
                .map(|provider| provider.descriptor.key.as_ref());
            if let Some(refusal) = unknown_provider(only, keys) {
                return refusal;
            }
            render_models(&catalog, only, serving, json)
        }
        Some(other) => format!("Unknown view '{other}' for {URI}; expected providers or models."),
    }
}

fn render_providers(
    listing: &CatalogListing,
    catalog: Option<&Catalog>,
    accounts: &[Value],
    only: Option<&str>,
    include_uninstalled: bool,
) -> String {
    let serving = listing.serves;
    let installed: Vec<&ListedProvider> = listing
        .providers
        .iter()
        .filter(|provider| provider.installed)
        .filter(|provider| only.is_none_or(|key| provider.descriptor.key == key))
        .collect();
    let mut out = String::from("# Model catalog\n\n");
    let _ = writeln!(
        out,
        "{} installed provider{}, in Settings → Providers order. Name a model to any verb as `provider/model`; the pools an allotment selects within are at cairn://models/ladder.",
        installed.len(),
        if installed.len() == 1 { "" } else { "s" }
    );
    for provider in installed {
        let descriptor = &provider.descriptor;
        let _ = writeln!(
            out,
            "\n## {} — {}",
            heading(descriptor),
            category(descriptor.category)
        );
        let configured = accounts
            .iter()
            .filter(|account| account_serves(account, descriptor))
            .count();
        if let Some(credential) = accountless(descriptor) {
            let _ = writeln!(out, "Configured: {credential}");
        } else if configured == 0 {
            let _ = writeln!(out, "Needs setup: {}", descriptor.setup_requirement);
        } else {
            let _ = writeln!(
                out,
                "Configured: {configured} account{}",
                if configured == 1 { "" } else { "s" }
            );
        }
        match served(&provider.serves) {
            Some(text) => {
                let _ = writeln!(out, "Serves: {text}");
            }
            None => out.push_str(
                "Serves: nothing by default; each model serves what its discovery declares\n",
            ),
        }
        for unable in &provider.unable {
            let _ = writeln!(
                out,
                "Unable: `{}` cannot {}: it {}",
                unable.model, unable.verb, unable.why
            );
        }
        match &provider.catalog {
            None => out.push_str("Models: not discovered yet\n"),
            Some(models) => {
                if models.refreshed_at.is_none() {
                    out.push_str("Models: not discovered yet; still discovering. Retry discovery in Settings → Providers if this persists.\n");
                }
                let refreshed = models
                    .refreshed_at
                    .map(|at| format!(", refreshed {}", when(at)))
                    .unwrap_or_default();
                match serving {
                    None => {
                        let _ = writeln!(out, "Models ({}{refreshed}):", provider.discovered);
                    }
                    Some(verb) => {
                        let _ = writeln!(
                            out,
                            "Models serving {verb} ({} of {}{refreshed}):",
                            models.models.len(),
                            provider.discovered
                        );
                    }
                }
                if let Some(error) = &models.error {
                    let _ = writeln!(out, "Discovery error: {error}");
                }
                let record = catalog.and_then(|catalog| {
                    catalog
                        .providers
                        .iter()
                        .find(|known| known.descriptor.key == descriptor.key)
                });
                for model in &models.models {
                    out.push_str(&model_line(model, record, serving));
                    out.push('\n');
                }
            }
        }
    }
    if include_uninstalled {
        let absent: Vec<&ListedProvider> = listing
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
                heading(&provider.descriptor),
                category(provider.descriptor.category),
                provider.descriptor.summary
            );
        }
    }
    out
}

fn render_models(
    catalog: &Catalog,
    only: Option<&str>,
    serving: Option<VerbKind>,
    json: bool,
) -> String {
    let models: Vec<_> = catalog
        .models()
        .into_iter()
        .filter(|model| only.is_none_or(|key| model.offers.iter().any(|o| o.provider == key)))
        .filter_map(|mut model| {
            if let Some(verb) = serving {
                model.offers.retain(|offer| offer.selected(verb).is_some());
                for offer in &mut model.offers {
                    offer.availability = offer.availability_for(verb).cloned();
                }
            }
            (!model.offers.is_empty()).then_some(model)
        })
        .collect();
    if json {
        return serde_json::to_string_pretty(&models).expect("catalog models serialize");
    }
    let mut out = String::from("# Model catalog — models\n\n");
    let _ = writeln!(
        out,
        "{} model{} across the installed providers, each under its canonical model id when known, otherwise `provider/model`. Offers merge only when identity resolves them to the same model, the one benchmark results and the ladder share. Aliases list provider spellings; names resolve by provider-qualified native id, then provider spelling, then canonical id.{}\n",
        models.len(),
        if models.len() == 1 { "" } else { "s" },
        serving
            .map(|verb| format!(" Only offers that serve {verb} are listed."))
            .unwrap_or_default()
    );
    for model in models {
        let _ = write!(out, "- `{}`", model.catalog_id);
        if model.display_name != model.catalog_id {
            let _ = write!(out, " {}", model.display_name);
        }
        out.push('\n');
        for offer in &model.offers {
            let mut facts = Vec::new();
            if let Some(note) = &offer.identity_note {
                facts.push(note.clone());
            }
            if let Some(canonical) = &offer.canonical {
                facts.push(format!("canonical `{canonical}`"));
            }
            if let Some(window) = offer.context_window {
                facts.push(format!("{} context", tokens(window)));
            }
            if let Some(price) = offer.pricing.as_ref().and_then(pricing) {
                facts.push(price);
            }
            facts.extend(availability_facts(
                offer.availability.as_ref(),
                &offer.availability_by_verb,
                serving,
            ));
            let _ = write!(out, "  - {} `{}`", offer.provider, offer.model);
            if !facts.is_empty() {
                let _ = write!(out, " ({})", facts.join(", "));
            }
            out.push_str(&capability(offer.modalities.as_ref(), &offer.serves));
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

/// ` · plan Max 20x $200/mo`, ` · plan Pro, no price (set_plan: pro_200 |
/// pro_500)`, or the provider's raw plan for an account that is not priced
/// by plan.
fn plan_text(account: &Value) -> String {
    let subscription = &account["subscription"];
    let Some(label) = subscription["label"].as_str() else {
        return account["plan"]
            .as_str()
            .map(|plan| format!(" · plan {plan}"))
            .unwrap_or_default();
    };
    let mut text = match subscription["monthlyUsd"].as_u64() {
        Some(usd) => format!(" · plan {label} ${usd}/mo"),
        None => format!(" · plan {label}, no price"),
    };
    let options: Vec<&str> = subscription["options"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|option| option["key"].as_str())
        .collect();
    if !options.is_empty() {
        let _ = write!(text, " (set_plan: {})", options.join(" | "));
    }
    text
}

/// `1 usage reset, soonest expires …`: what an account holds to spend with
/// `use_reset`.
fn resets_text(resets: &ProviderPoolAccountResets) -> String {
    let mut text = format!(
        "{} usage reset{}",
        resets.available_count,
        if resets.available_count == 1 { "" } else { "s" }
    );
    if let Some(expires) = resets.soonest_expires_at {
        let _ = write!(text, ", soonest expires {}", when(expires));
    }
    text
}

/// Each account's usage-reset credits. Only the live usage snapshot holds
/// them, so they are read from the pools rather than the stored account.
fn account_resets(
    facet: &dyn Models,
    scope: &ModelScope,
) -> HashMap<String, ProviderPoolAccountResets> {
    facet
        .usage(scope, None)
        .unwrap_or_default()
        .into_iter()
        .flat_map(|pool| pool.accounts)
        .filter_map(|account| Some((account.account_id, account.reset_credits?)))
        .collect()
}

fn account_block(
    account: &Value,
    now: i64,
    pending: &[PendingSignIn],
    resets: &HashMap<String, ProviderPoolAccountResets>,
    out: &mut String,
) {
    let label = account["label"].as_str().unwrap_or("(unnamed)");
    let id = account["id"].as_str().unwrap_or_default();
    let _ = write!(out, "- **{label}** `{id}` — {}", credential_kind(account));
    if let Some(email) = account["email"].as_str() {
        let _ = write!(out, " · email {email}");
    }
    out.push_str(&plan_text(account));
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
    if let Some(line) = sign_in_line(account, now) {
        let _ = writeln!(out, "  - {line}");
    }
    let health = &account["health"];
    if health.is_object() {
        let captured = health["capturedAt"].as_i64();
        if let Some(captured) = captured {
            let _ = writeln!(out, "  - measured {}", when(captured));
        }
        // A hold is in force until an operator acts, so it carries the act.
        if let Ok(hold) =
            serde_json::from_value::<crate::credentials::AccountHold>(health["held"].clone())
        {
            let signed_in_as = account["email"].as_str().unwrap_or(label);
            let _ = writeln!(
                out,
                "  - blocked: {}",
                hold.describe(&when(hold.since), signed_in_as)
            );
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
    if let Some(held) = resets.get(id) {
        let _ = writeln!(out, "  - {}", resets_text(held));
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

/// The sign-in deadline a serialized [`AccountInfo`] carries.
///
/// [`AccountInfo`]: cairn_models::accounts::AccountInfo
fn sign_in_expiry(account: &Value) -> Option<crate::credentials::SignInExpiry> {
    serde_json::from_value(account["signInExpiry"].clone()).ok()
}

/// Where an account's sign-in stands against its deadline, naming the act
/// that keeps it in rotation once one is due.
fn sign_in_line(account: &Value, now: i64) -> Option<String> {
    use crate::credentials::{SignInExpirySource, SignInStage};
    let expiry = sign_in_expiry(account)?;
    let deadline = crate::credentials::sign_in::utc(expiry.expires_at);
    let source = match expiry.source {
        SignInExpirySource::Provider => "declared by the provider",
        SignInExpirySource::SignIn => "recorded at sign-in",
        SignInExpirySource::Inferred => "inferred: none was declared, so 30 days is assumed",
    };
    let left = crate::orchestrator::sign_in_time_left(now, expiry.expires_at);
    Some(match expiry.stage(now) {
        SignInStage::Current => format!("sign-in renewable until {deadline} ({left}; {source})"),
        SignInStage::Expiring | SignInStage::ExpiringSoon => format!(
            "reconnect by {deadline}: its sign-in expires {left} ({source}). Reconnect from Settings → Providers to keep it in rotation"
        ),
        SignInStage::Lapsed => format!(
            "sign-in expired at {deadline} ({source}): nothing is routed to it. Reconnect from Settings → Providers"
        ),
    })
}

/// The headline over every account: who needs reconnecting soonest. A lapsed
/// or refused account outranks any deadline still ahead.
fn reconnect_headline(accounts: &[Value], now: i64) -> Option<String> {
    let name = |account: &Value| {
        account["email"]
            .as_str()
            .or(account["label"].as_str())
            .unwrap_or("(unnamed)")
            .to_string()
    };
    let refused: Vec<String> = accounts
        .iter()
        .filter(|account| {
            account["health"]["held"]["kind"].as_str() == Some("sign_in_expired")
                || sign_in_expiry(account).is_some_and(|expiry| expiry.is_lapsed(now))
        })
        .map(name)
        .collect();
    if !refused.is_empty() {
        return Some(format!(
            "**Reconnect now:** {}. Their sign-ins can no longer be renewed, so nothing is routed to them.",
            refused.join(", ")
        ));
    }
    let (account, expiry) = accounts
        .iter()
        .filter_map(|account| sign_in_expiry(account).map(|expiry| (account, expiry)))
        .min_by_key(|(_, expiry)| expiry.expires_at)?;
    Some(format!(
        "**Next reconnect:** {} by {} ({}).",
        name(account),
        crate::credentials::sign_in::utc(expiry.expires_at),
        crate::orchestrator::sign_in_time_left(now, expiry.expires_at)
    ))
}

pub(crate) fn render_accounts(
    facet: &dyn Models,
    params: &[QueryParam],
    pending: &[PendingSignIn],
) -> String {
    const URI: &str = "cairn://models/accounts";
    if let Some(error) = unsupported(URI, params, &["provider"]) {
        return error;
    }
    let scope = ModelScope::workspace();
    let (catalog, accounts) = match (facet.catalog(&scope), sorted_accounts(facet, &scope)) {
        (Ok(catalog), Ok(accounts)) => (catalog, accounts),
        (Err(error), _) | (_, Err(error)) => return format!("Could not read accounts: {error}"),
    };
    let only = find_query_value(params, "provider");
    let now = chrono::Utc::now().timestamp();
    let resets = account_resets(facet, &scope);
    let mut out = String::from("# Model accounts\n\n");
    if let Some(headline) = reconnect_headline(&accounts, now) {
        let _ = writeln!(out, "{headline}\n");
    }
    out.push_str(
        "Grouped by installed provider, in priority order. A credential is described, never shown. Append {provider: claude|codex} here to start a subscription sign-in, or use Settings → Providers, or `cairn auth login claude` for a managed Claude account.\n",
    );
    let mut placed = vec![false; accounts.len()];
    let mut shown = 0;
    for provider in catalog.installed() {
        let mine: Vec<usize> = (0..accounts.len())
            .filter(|index| account_serves(&accounts[*index], &provider.descriptor))
            .collect();
        for index in &mine {
            placed[*index] = true;
        }
        if only.is_some_and(|key| provider.descriptor.key != key) {
            continue;
        }
        shown += 1;
        let _ = writeln!(out, "\n## {}", heading(&provider.descriptor));
        if let Some(credential) = accountless(&provider.descriptor) {
            let _ = writeln!(out, "No account of its own: it runs on {credential}.");
        } else if mine.is_empty() {
            let _ = writeln!(out, "No account. {}", provider.descriptor.setup_requirement);
        }
        for index in mine {
            account_block(&accounts[index], now, pending, &resets, &mut out);
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
            return format!("No installed provider '{key}'. Installed providers are listed at cairn://models/catalog.");
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
            account_block(account, now, pending, &resets, &mut out);
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

pub(crate) fn render_account(facet: &dyn Models, id: &str, sign_ins: &AccountSignIns) -> String {
    let accounts = match sorted_accounts(facet, &ModelScope::workspace()) {
        Ok(accounts) => accounts,
        Err(error) => return format!("Could not read accounts: {error}"),
    };
    let Some(account) = accounts
        .iter()
        .find(|account| account["id"].as_str() == Some(id))
    else {
        return format!(
            "No account `{id}` in this workspace. Accounts are listed at cairn://models/accounts."
        );
    };
    let now = chrono::Utc::now().timestamp();
    let mut out = format!(
        "# Model account {}\n\n",
        account["label"].as_str().unwrap_or(id)
    );
    let resets = account_resets(facet, &ModelScope::workspace());
    account_block(account, now, &sign_ins.pending, &resets, &mut out);
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
            // The pool's label is the account's email where it has one, the
            // name its owner signs in with.
            ProviderPoolAccountState::Blocked => match (&account.held, account.blocked_until) {
                (Some(hold), _) => format!(
                    "blocked: {}",
                    hold.describe(&when(hold.since), &account.label)
                ),
                (None, Some(until)) => format!("blocked until {}", when(until)),
                (None, None) => "blocked".to_string(),
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
        if let Some(resets) = &account.reset_credits {
            let _ = write!(out, ", {}", resets_text(resets));
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

/// `1234567` as `1,234,567`.
fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn instant(millis: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| millis.to_string())
}

/// `cairn://models/embeddings`: the embedding model and how far the stored
/// vectors are in it.
pub(crate) fn render_embeddings(
    status: &crate::embeddings::status::EmbeddingStatus,
    params: &[QueryParam],
) -> String {
    use crate::embeddings::status::EmbeddingState;
    const URI: &str = "cairn://models/embeddings";
    if let Some(error) = unsupported(URI, params, &[]) {
        return error;
    }
    let mut out = String::from("# Embeddings\n\n");
    let chosen = if status.explicit {
        "set in `embeddingModel`"
    } else {
        "the default; `embeddingModel` is unset"
    };
    let _ = writeln!(out, "- Model: `{}` ({chosen})", status.selection);
    if let Some(space) = &status.space {
        let width = |dims: Option<u32>| dims.map_or("native".to_string(), |dims| dims.to_string());
        let _ = writeln!(
            out,
            "- Widths: turns {}, resources {}",
            width(space.turn_dims),
            width(space.resource_dims)
        );
    }
    let state = match &status.state {
        EmbeddingState::Running => "running".to_string(),
        EmbeddingState::Complete => "complete".to_string(),
        EmbeddingState::Paused {
            since,
            last_error,
            retry_at,
        } => {
            let mut paused = String::from("paused");
            if let Some(since) = since {
                let _ = write!(paused, " since {}", instant(*since));
            }
            if let Some(error) = last_error {
                let _ = write!(paused, "; last error: {error}");
            }
            let _ = write!(paused, "; next attempt {}", instant(*retry_at));
            paused
        }
        EmbeddingState::Unavailable { reason } => format!("unavailable: {reason}"),
    };
    let _ = writeln!(out, "- State: {state}");
    if let Some(turns) = &status.turns {
        let _ = write!(
            out,
            "- Turns: {} / {}",
            grouped(turns.embedded),
            grouped(turns.eligible)
        );
        if turns.deferred > 0 {
            let _ = write!(out, " ({} deferred)", grouped(turns.deferred));
        }
        out.push('\n');
    }
    if status.space.is_some() {
        match &status.resources {
            Some(resources) => {
                let _ = writeln!(
                    out,
                    "- Resources: {} / {}",
                    grouped(resources.embedded),
                    grouped(resources.eligible)
                );
            }
            None => out.push_str(
                "- Resources: counting (the first walk in this model has not finished)\n",
            ),
        }
    }
    if let Some(vibes) = &status.vibes {
        let _ = writeln!(out, "- Vibe colours: {}", vibe_progress(vibes));
    }
    let history = if status.consented {
        "all of it"
    } else {
        "since this install (choosing a model or connecting an account includes earlier history)"
    };
    let _ = writeln!(out, "- History: {history}");
    out.push_str(
        "\nChange the model with write cairn://settings {embeddingModel: \"provider/model\"}; null returns to the Cairn gateway. A change deletes the old model's vectors and re-embeds history.\n",
    );
    out
}

/// Where learning the active model's vibe axes stands, as one line.
fn vibe_progress(progress: &crate::embeddings::VibeAxesProgress) -> String {
    use crate::embeddings::VibeAxesProgress;
    match progress {
        VibeAxesProgress::Ready => "ready (this model has axes)".to_string(),
        VibeAxesProgress::Sampling => "learning axes: sampling history".to_string(),
        VibeAxesProgress::Embedding { embedded, total } => format!(
            "learning axes: {} / {} sampled events embedded",
            grouped(*embedded),
            grouped(*total)
        ),
        VibeAxesProgress::NotEnoughHistory {
            counts,
            needed,
            retry_at,
        } => format!(
            "neutral, not enough labelled history: phase {}/{}, friction {}/{}, distribution {} (each needs {needed}); sampled again {}",
            counts.phase_positive,
            counts.phase_negative,
            counts.friction_positive,
            counts.friction_negative,
            counts.distribution,
            instant(*retry_at)
        ),
    }
}

/// The served API's clients, or one client with its grants and recent
/// requests. A key hash is never rendered.
pub(crate) async fn render_clients(
    orch: &crate::orchestrator::Orchestrator,
    one: Option<&str>,
) -> String {
    use crate::orchestrator::served_api::{clients, listener_status};
    let db = &orch.db.local;
    let all = match clients::list(db).await {
        Ok(all) => all,
        Err(error) => return format!("Served-API clients are unavailable: {error}"),
    };
    let last_used: HashMap<String, i64> =
        cairn_db::storage::served_api::last_model_request_by_client(db)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();
    let shown: Vec<_> = match one {
        None => all.iter().collect(),
        Some(id) => all.iter().filter(|client| client.id == id).collect(),
    };
    if let (Some(id), true) = (one, shown.is_empty()) {
        return format!(
            "No served-API client `{id}`. Clients are listed at cairn://models/clients."
        );
    }
    let mut out = match one {
        None => format!(
            "# Served API clients\n\nListener: {}. Clients call it with `Authorization: Bearer <key>`; requests are at cairn://models/usage?view=requests.\n\n",
            listener_status()
        ),
        Some(_) => String::new(),
    };
    if shown.is_empty() {
        out.push_str("No clients. Append {name} to mint a key, or {import:[{name, keyHash}]} to keep existing keys.\n");
        return out;
    }
    for client in shown {
        let grants = clients::remote_grants(db, client).await.unwrap_or_default();
        let remote = if grants.is_empty() {
            "local-only".to_string()
        } else {
            grants
                .iter()
                .map(|grant| format!("{} (grant {})", grant.scope.shorthand(), grant.id))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let _ = writeln!(
            out,
            "{}## {} — `{}`\n\n- state: {}\n- key: {}{}\n- remote: {}\n- last request: {}\n- created: {}\n",
            if one.is_some() { "#" } else { "" },
            client.name,
            client.id,
            match client.revoked_at {
                Some(at) => format!("revoked {} by {}", when(at), client.revoked_by.as_deref().unwrap_or("?")),
                None => "live".to_string(),
            },
            client.source,
            client
                .key_hint
                .as_deref()
                .map(|hint| format!(", ends …{hint}"))
                .unwrap_or_default(),
            remote,
            last_used
                .get(&client.id)
                .map(|ms| when(ms / 1000))
                .unwrap_or_else(|| "never".to_string()),
            when(client.created_at),
        );
    }
    out
}

/// The served API's requests: per-client totals, then the rows newest first.
fn render_requests(
    query: &cairn_models::interface::RequestsQuery,
    rows: &[cairn_models::interface::RequestRecord],
) -> String {
    let mut out = format!(
        "# Model requests\n\nRequests outside clients made through Cairn's served API in the last {}h{}: {} row(s), newest first, capped at {}. Prompts and replies are never recorded. Clients: cairn://models/clients.\n\n",
        query.hours,
        query
            .client
            .as_deref()
            .map(|client| format!(" by {client}"))
            .unwrap_or_default(),
        rows.len(),
        query.limit
    );
    if rows.is_empty() {
        return out;
    }
    let mut totals: std::collections::BTreeMap<String, (usize, i64, i64, f64)> =
        std::collections::BTreeMap::new();
    for row in rows {
        let name = row
            .client
            .clone()
            .or_else(|| row.client_id.clone())
            .unwrap_or_else(|| "(unauthenticated)".into());
        let total = totals.entry(name).or_default();
        total.0 += 1;
        total.1 += row.input_tokens.unwrap_or(0);
        total.2 += row.output_tokens.unwrap_or(0);
        total.3 += row.cost.unwrap_or(0.0);
    }
    out.push_str(
        "## Per client\n\n| client | requests | in | out | cost |\n|---|---|---|---|---|\n",
    );
    for (client, (count, input, output, cost)) in &totals {
        let _ = writeln!(
            out,
            "| {client} | {count} | {input} | {output} | ${cost:.4} |"
        );
    }
    out.push_str("\n## Requests\n\n| at | client | door | asked | model | provider | status | outcome | in | out | cost | latency | wait |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    let dash = || "—".to_string();
    for row in rows {
        let _ = writeln!(
            out,
            "| {} | {} | {}{} | {} | {} | {} | {} | {} | {} | {} | {} | {}ms | {} |",
            chrono::DateTime::from_timestamp_millis(row.at)
                .map(|at| at.format("%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| row.at.to_string()),
            row.client
                .as_deref()
                .or(row.client_id.as_deref())
                .unwrap_or("—"),
            row.door,
            if row.stream { " (stream)" } else { "" },
            row.model_asked.as_deref().unwrap_or("—"),
            row.model.as_deref().unwrap_or("—"),
            row.provider.as_deref().unwrap_or("—"),
            row.status,
            row.outcome,
            row.input_tokens.map_or_else(dash, |n| n.to_string()),
            row.output_tokens.map_or_else(dash, |n| n.to_string()),
            row.cost.map_or_else(dash, |cost| format!("${cost:.4}")),
            row.latency_ms,
            row.wait_ms.map_or_else(dash, |ms| format!("{ms}ms")),
        );
    }
    out
}

/// What `cairn://models/usage?history=<account>` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UsageHistoryQuery {
    pub account: String,
    pub window: Option<String>,
    pub json: bool,
    pub hours: Option<u32>,
}

impl UsageHistoryQuery {
    /// The earliest `captured_at` the query reads, given the current time.
    pub fn since(&self, now: i64) -> Option<i64> {
        self.hours.map(|hours| now - i64::from(hours) * 3600)
    }
}

/// Rows shown per window in the markdown history; JSON returns them all.
const USAGE_HISTORY_ROWS_SHOWN: usize = 500;

/// Parse a usage-history read, or `None` when the read is not one (no
/// `history` key), so the caller renders the card instead.
pub(crate) fn usage_history_query(
    params: &[QueryParam],
) -> Option<Result<UsageHistoryQuery, String>> {
    const URI: &str = "cairn://models/usage?history=ACCOUNT";
    let account = find_query_value(params, "history")?;
    if let Some(error) = unsupported(URI, params, &["history", "window", "format", "hours"]) {
        return Some(Err(error));
    }
    if account.is_empty() {
        return Some(Err(
            "history names an account id (see cairn://models/accounts).".to_string(),
        ));
    }
    let json = match find_query_value(params, "format") {
        None | Some("markdown") => false,
        Some("json") => true,
        Some(other) => {
            return Some(Err(format!(
                "Unknown format '{other}' for usage history; expected markdown or json."
            )))
        }
    };
    let hours = match find_query_value(params, "hours") {
        None => None,
        Some(hours) => match hours.parse::<u32>() {
            Ok(hours) if hours > 0 => Some(hours),
            _ => {
                return Some(Err(format!(
                    "hours must be a positive whole number (got '{hours}')."
                )))
            }
        },
    };
    Some(Ok(UsageHistoryQuery {
        account: account.to_string(),
        window: find_query_value(params, "window").map(str::to_string),
        json,
        hours,
    }))
}

/// An account's recorded usage-window readings, grouped by window.
pub(crate) fn render_usage_history(
    query: &UsageHistoryQuery,
    rows: &[cairn_db::storage::usage_readings::UsageWindowReadingRow],
) -> String {
    if query.json {
        let readings: Vec<Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "provider": row.provider,
                    "windowId": row.window_id,
                    "windowName": row.window_name,
                    "scope": row.scope,
                    "scopeTarget": row.scope_target,
                    "remainingPercent": row.remaining_percent,
                    "windowDurationMins": row.window_duration_mins,
                    "resetsAt": row.resets_at,
                    "capturedAt": row.captured_at,
                    "source": row.source,
                })
            })
            .collect();
        return serde_json::to_string_pretty(&serde_json::json!({
            "account": query.account,
            "window": query.window,
            "readings": readings,
        }))
        .unwrap_or_default();
    }
    let mut out = format!("# Usage history: {}\n\n", query.account);
    let span = query.hours.map_or_else(
        || "all retained readings".to_string(),
        |hours| format!("the last {hours}h"),
    );
    let _ = writeln!(
        out,
        "Each usage refresh that changed a window, {span}{}. Readings are kept for the last 8 resets or 60 days, whichever is longer.\n",
        query
            .window
            .as_deref()
            .map(|window| format!(", window `{window}`"))
            .unwrap_or_default()
    );
    if rows.is_empty() {
        out.push_str("No usage readings recorded for this account yet. A reading is written each time a usage refresh measures it; see cairn://models/accounts.\n");
        return out;
    }
    let mut windows: std::collections::BTreeMap<_, Vec<_>> = std::collections::BTreeMap::new();
    for row in rows {
        let key = (
            row.window_name.as_str(),
            row.window_id.as_str(),
            row.provider.as_str(),
        );
        windows.entry(key).or_default().push(row);
    }
    for ((name, id, provider), group) in windows {
        let _ = writeln!(out, "## {name} (`{id}`, {provider})\n");
        let shown = &group[group.len().saturating_sub(USAGE_HISTORY_ROWS_SHOWN)..];
        if shown.len() < group.len() {
            let _ = writeln!(
                out,
                "Showing the newest {} of {} readings; format=json returns all of them.\n",
                shown.len(),
                group.len()
            );
        }
        out.push_str(
            "| Captured | Remaining | Resets at | Duration | Source |\n|---|---|---|---|---|\n",
        );
        for row in shown {
            let _ = writeln!(
                out,
                "| {} | {}% | {} | {} | {} |",
                when(row.captured_at),
                row.remaining_percent,
                row.resets_at.map_or_else(|| "—".to_string(), when),
                row.window_duration_mins
                    .map_or_else(|| "—".to_string(), |mins| format!("{mins} min")),
                row.source
            );
        }
        out.push('\n');
    }
    out
}

pub(crate) fn render_usage(facet: &dyn Models, params: &[QueryParam]) -> String {
    const URI: &str = "cairn://models/usage";
    if let Some(key) = ["window", "format"]
        .into_iter()
        .find(|key| find_query_value(params, key).is_some())
    {
        return format!(
            "{key} reads only with history=<account>, e.g. cairn://models/usage?history=ACCOUNT_ID&window=Opus&format=json (account ids are at cairn://models/accounts)."
        );
    }
    if let Some(error) = unsupported(
        URI,
        params,
        &["provider", "view", "client", "hours", "history"],
    ) {
        return error;
    }
    let scope = ModelScope::workspace();
    match find_query_value(params, "view") {
        None | Some("card") => {}
        Some("requests") => {
            let mut query = cairn_models::interface::RequestsQuery {
                client: find_query_value(params, "client").map(str::to_string),
                ..Default::default()
            };
            if let Some(hours) = find_query_value(params, "hours") {
                match hours.parse::<u32>() {
                    Ok(hours) if hours > 0 => query.hours = hours,
                    _ => return format!("hours must be a positive whole number (got '{hours}')."),
                }
            }
            return match facet.requests(&query) {
                Ok(rows) => render_requests(&query, &rows),
                Err(error) => format!("Per-request rows are unavailable: {error}"),
            };
        }
        Some(other) => {
            return format!("Unknown view '{other}' for {URI}; expected card or requests.")
        }
    }
    let pools = match facet.usage(&scope, find_query_value(params, "provider")) {
        Ok(pools) => pools,
        Err(error) => return error,
    };
    let names = facet.catalog(&scope).ok();
    let label = |key: &str| {
        names
            .as_ref()
            .and_then(|catalog| {
                catalog
                    .providers
                    .iter()
                    .find(|provider| provider.descriptor.key == key)
            })
            .map(|provider| heading(&provider.descriptor))
            .unwrap_or_else(|| format!("`{key}`"))
    };
    let mut out = String::from(
        "# Model usage\n\nEach installed provider's capacity as already measured, pooled across its accounts. Reading never probes a provider.\n",
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

pub(crate) fn render_sessions(facet: &dyn Models) -> String {
    let sessions = facet.sessions();
    let mut out = String::from("# Model sessions\n\n");
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

pub(crate) fn render_session(facet: &dyn Models, id: &str) -> String {
    let Some(session) = facet
        .sessions()
        .into_iter()
        .find(|session| session.is_addressed_by(id))
    else {
        return format!(
            "No live session `{id}` on this host. Live sessions are listed at cairn://models/sessions."
        );
    };
    let mut out = format!("# Model session {}\n\n", session.key());
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
    use crate::orchestrator::models::tests::{
        seeded_orchestrator, API_KEY_SECRET, CUSTOM_KEY_SECRET, HEADER_SECRET, OAUTH_SECRET,
    };
    use crate::orchestrator::Orchestrator;

    const READS: &[&str] = &[
        "cairn://models",
        "cairn://models/catalog",
        "cairn://models/catalog?view=models",
        "cairn://models/catalog?installed=all",
        "cairn://models/catalog?provider=claude",
        "cairn://models/decide",
        "cairn://models/embed",
        "cairn://models/complete",
        "cairn://models/accounts",
        "cairn://models/accounts?provider=claude",
        "cairn://models/usage",
        "cairn://models/usage?provider=claude",
        "cairn://models/usage?view=requests",
        "cairn://models/usage?history=nobody",
        "cairn://models/spend",
        "cairn://models/spend?window=all&format=json",
        "cairn://models/embeddings",
        "cairn://models/sessions",
    ];

    fn request() -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            origin: None,
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
                .map(|account| format!("cairn://models/accounts/{}", account.id)),
        );
        uris
    }

    #[test]
    fn the_gateway_owns_no_account_beside_an_installed_custom_endpoint() {
        use cairn_models::types::catalog::{cairn_gateway_descriptor, unknown_descriptor};
        let custom = serde_json::json!({ "id": "acc_local", "apiProvider": "custom" });
        assert!(!super::account_serves(&custom, &cairn_gateway_descriptor()));
        assert!(super::account_serves(
            &custom,
            &unknown_descriptor("custom:acc_local")
        ));
        assert!(!super::account_serves(
            &custom,
            &unknown_descriptor("custom:acc_other")
        ));
        assert_eq!(
            super::accountless(&cairn_gateway_descriptor()),
            Some("this device's Cairn token")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_complete_page_documents_the_full_reply() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let page = read(&orch, "cairn://models/complete").await;
        for term in [
            "`text`",
            "`reasoning`",
            "`structured`",
            "`model`",
            "`provider`",
            "`usage`",
            "`cost`",
            "`latencyMs`",
            "`history`",
            "`inputTokens`",
            "`outputTokens`",
            "`cachedInputTokens`",
            "`reasoningTokens`",
            "null when unreported",
            "explicit zero",
            "16 KiB of UTF-8",
            "never truncated",
            "encrypted continuation state",
            "accepted attempt",
        ] {
            assert!(page.contains(term), "missing {term}: {page}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_decide_page_lists_the_offers_that_serve_decide_now() {
        let (orch, server, key) = crate::orchestrator::decide_fixtures::workspace_with_server(
            crate::credentials::CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            Vec::new(),
        )
        .await;
        let page = read(&orch, "cairn://models/decide").await;
        assert!(
            page.contains(&format!("`qwen3` on `{key}` — measured by logprobs")),
            "{page}"
        );
        assert!(page.contains("Invoke with `run`"), "{page}");
        assert!(
            page.contains("run({commands:[{target:\"cairn://models/decide\""),
            "the affordance carries the invocation: {page}"
        );
        assert_eq!(server.calls(), 0, "a read asks nothing");

        let bare = crate::orchestrator::models::tests::bare_orchestrator().await;
        let nothing = read(&bare, "cairn://models/decide").await;
        assert!(
            nothing.contains("Nothing in this workspace serves decide yet"),
            "{nothing}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_embed_page_lists_each_offer_with_what_it_declares() {
        let (orch, _, key) =
            crate::orchestrator::embed::tests::workspace_with_hub(Vec::new()).await;
        let page = read(&orch, "cairn://models/embed").await;
        assert!(
            page.contains(&format!(
                "`qwen/qwen3-embedding-4b` on `{key}` — 4 · native width only"
            )),
            "{page}"
        );
        assert!(page.contains("≤32 texts"), "the batch limit: {page}");
        assert!(
            page.contains("run({commands:[{target:\"cairn://models/embed\""),
            "the affordance carries the invocation: {page}"
        );
        assert!(
            !page.contains("`big`"),
            "a chat model is not an embed offer: {page}"
        );

        let bare = crate::orchestrator::models::tests::bare_orchestrator().await;
        let nothing = read(&bare, "cairn://models/embed").await;
        assert!(
            nothing.contains("Nothing in this workspace serves embed yet"),
            "{nothing}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_models_read_renders_with_its_affordance_block() {
        let orch = seeded_orchestrator().await;
        for uri in all_reads(&orch) {
            let out = read(&orch, &uri).await;
            let kind = cairn_uri::parse_uri(&uri).expect("parses").kind();
            let name = cairn_common::contract::contract_for(kind)
                .expect("registered")
                .name;
            assert!(
                out.contains(&format!("## {name}")),
                "{uri} lacks its affordance block:\n{out}"
            );
        }
    }

    #[test]
    fn model_view_groups_canonical_identity_and_lists_provider_spellings() {
        use cairn_models::interface::{Catalog, CatalogProvider};
        use cairn_models::types::catalog::{claude_models, descriptor, ProviderModelCatalog};

        let providers = [
            ("openrouter", "z-ai/glm-5.2", Some("z-ai/glm-5.2-20261001")),
            ("opencode-go", "glm-5.2", None),
        ]
        .into_iter()
        .map(|(provider, native, slug)| {
            let mut model = claude_models().remove(0);
            model.id = native.into();
            model.model = native.into();
            model.display_name = native.into();
            model.canonical_slug = slug.map(str::to_string);
            CatalogProvider {
                descriptor: descriptor(provider).unwrap().clone(),
                installed: true,
                models: Some(ProviderModelCatalog {
                    backend: provider.into(),
                    models: vec![model],
                    options: vec![],
                    refreshed_at: None,
                    error: None,
                }),
                serves: vec![],
                unable: vec![],
                alias_resolutions: vec![],
            }
        })
        .collect();
        let rendered = super::render_models(&Catalog { providers }, None, None);
        assert!(rendered.contains("1 model across"), "{rendered}");
        assert!(rendered.contains("- `glm-5.2`"), "{rendered}");
        assert!(
            rendered.contains("  - openrouter `z-ai/glm-5.2`"),
            "{rendered}"
        );
        assert!(rendered.contains("  - opencode-go `glm-5.2`"), "{rendered}");
        assert!(
            rendered.contains("aliases: `z-ai/glm-5.2`, `z-ai/glm-5.2-20261001`"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("merge only on a shared slug"),
            "{rendered}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_pages_say_what_settings_shows() {
        let orch = seeded_orchestrator().await;
        let catalog = read(&orch, "cairn://models/catalog").await;
        assert!(catalog.contains("## Anthropic (`claude`)"), "{catalog}");
        assert!(catalog.contains("`opus` — 1M context"), "{catalog}");
        assert!(catalog.contains("DGX Spark"), "{catalog}");
        assert!(
            catalog.contains("Discovery error: codex is not installed"),
            "{catalog}"
        );
        assert!(!catalog.contains("## Not installed"), "{catalog}");

        let all = read(&orch, "cairn://models/catalog?installed=all").await;
        assert!(all.contains("## Not installed"), "{all}");
        assert!(all.contains("OpenRouter (`openrouter`)"), "{all}");

        let models = read(&orch, "cairn://models/catalog?view=models").await;
        assert!(models.contains("- `claude/opus`"), "{models}");
        assert!(models.contains("- `ollama/qwen3:32b`"), "{models}");

        let accounts = read(&orch, "cairn://models/accounts").await;
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

        let usage = read(&orch, "cairn://models/usage?provider=claude").await;
        assert!(usage.contains("session: 64% of 100% left"), "{usage}");
        let refused = read(&orch, "cairn://models/usage?provider=openrouter").await;
        assert!(refused.contains("not installed"), "{refused}");

        let sessions = read(&orch, "cairn://models/sessions").await;
        assert!(sessions.contains("No live model sessions"), "{sessions}");
        let missing = read(&orch, "cairn://models/sessions/nope").await;
        assert!(missing.contains("cairn://models/sessions"), "{missing}");

        let bad = read(&orch, "cairn://models/catalog?view=bogus").await;
        assert!(bad.contains("Unknown view 'bogus'"), "{bad}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_availability_renders_each_state_in_both_views() {
        use cairn_models::interface::{ModelScope, Models};
        use cairn_models::types::catalog::ModelAvailability;
        let orch = seeded_orchestrator().await;
        let mut catalog = orch.catalog(&ModelScope::workspace()).unwrap();
        let cases = [
            (Some(ModelAvailability::Loaded { pool: "gpu".into() }), "loaded in pool `gpu`"),
            (Some(ModelAvailability::Loadable { pool: "gpu".into(), expected_load_ms: Some(1_211_000) }), "loadable in pool `gpu`, expected load 20m 11s"),
            (Some(ModelAvailability::Loadable { pool: "gpu".into(), expected_load_ms: None }), "loadable in pool `gpu`, expected load time unknown"),
            (Some(ModelAvailability::Loading { pool: "gpu".into(), expected_load_ms: Some(1_211_000), elapsed_ms: 5100 }), "loading in pool `gpu`, expected load 20m 11s, 5s elapsed (not remaining time)"),
            (Some(ModelAvailability::Loading { pool: "gpu".into(), expected_load_ms: None, elapsed_ms: 800 }), "loading in pool `gpu`, expected load time unknown, 800 ms elapsed (not remaining time)"),
            (None, ""),
        ];
        for (value, expected) in cases {
            let provider = catalog
                .providers
                .iter_mut()
                .find(|p| p.descriptor.key == "claude")
                .unwrap();
            let model = provider
                .models
                .as_mut()
                .unwrap()
                .models
                .iter_mut()
                .find(|m| m.model == "opus")
                .unwrap();
            model.availability = value.clone();
            let listing = catalog.listing(None);
            let providers =
                super::render_providers(&listing, Some(&catalog), &[], Some("claude"), false);
            let models = super::render_models(&catalog, Some("claude"), None, false);
            for page in [&providers, &models] {
                if value.is_some() {
                    assert!(page.contains(expected), "{page}");
                } else {
                    assert!(!page.contains("pool `gpu`"), "{page}");
                }
            }
            let models: serde_json::Value =
                serde_json::from_str(&super::render_models(&catalog, Some("claude"), None, true))
                    .unwrap();
            let offer = models
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|m| m["offers"].as_array().unwrap())
                .find(|o| o["model"] == "opus")
                .unwrap();
            let providers = serde_json::to_value(&listing).unwrap();
            let discovered = providers["providers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["descriptor"]["key"] == "claude")
                .unwrap()["catalog"]["models"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["model"] == "opus")
                .unwrap()
                .clone();
            let expected_json = serde_json::to_value(&value).unwrap();
            assert_eq!(offer["availability"], expected_json);
            assert_eq!(discovered["availability"], expected_json);
            if value.is_none() {
                assert!(offer.get("availability").is_none());
                assert!(discovered.get("availability").is_none());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_availability_is_specific_to_the_requested_verb_in_both_views() {
        use cairn_models::interface::{ModelScope, Models};
        use cairn_models::types::catalog::{ModelAvailability, VerbKind};
        let orch = seeded_orchestrator().await;
        let mut catalog = orch.catalog(&ModelScope::workspace()).unwrap();
        let loaded = ModelAvailability::Loaded {
            pool: "warm".into(),
        };
        let loadable = ModelAvailability::Loadable {
            pool: "cold".into(),
            expected_load_ms: Some(1_211_000),
        };
        let by_verb = std::collections::BTreeMap::from([
            (VerbKind::Complete, loaded.clone()),
            (VerbKind::Session, loadable.clone()),
        ]);
        let model = catalog
            .providers
            .iter_mut()
            .find(|p| p.descriptor.key == "claude")
            .unwrap()
            .models
            .as_mut()
            .unwrap()
            .models
            .iter_mut()
            .find(|m| m.model == "opus")
            .unwrap();
        model.availability = Some(loadable.clone());
        model.availability_by_verb = by_verb.clone();

        for serving in [None, Some(VerbKind::Complete), Some(VerbKind::Session)] {
            let listing = catalog.listing(serving);
            let providers =
                super::render_providers(&listing, Some(&catalog), &[], Some("claude"), false);
            let models = super::render_models(&catalog, Some("claude"), serving, false);
            for page in [&providers, &models] {
                match serving {
                    None => {
                        assert!(page.contains("complete: loaded in pool `warm`"), "{page}");
                        assert!(
                            page.contains(
                                "session: loadable in pool `cold`, expected load 20m 11s"
                            ),
                            "{page}"
                        );
                    }
                    Some(VerbKind::Complete) => {
                        assert!(page.contains("loaded in pool `warm`"), "{page}");
                        assert!(!page.contains("pool `cold`"), "{page}");
                    }
                    Some(VerbKind::Session) => {
                        assert!(
                            page.contains("loadable in pool `cold`, expected load 20m 11s"),
                            "{page}"
                        );
                        assert!(!page.contains("pool `warm`"), "{page}");
                    }
                    _ => unreachable!(),
                }
            }
            let models: serde_json::Value = serde_json::from_str(&super::render_models(
                &catalog,
                Some("claude"),
                serving,
                true,
            ))
            .unwrap();
            let offer = models
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|m| m["offers"].as_array().unwrap())
                .find(|o| o["model"] == "opus")
                .unwrap();
            let providers = serde_json::to_value(&listing).unwrap();
            let discovered = providers["providers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["descriptor"]["key"] == "claude")
                .unwrap()["catalog"]["models"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["model"] == "opus")
                .unwrap();
            let expected = if serving == Some(VerbKind::Complete) {
                &loaded
            } else {
                &loadable
            };
            for value in [offer, discovered] {
                assert_eq!(
                    value["availability"],
                    serde_json::to_value(expected).unwrap()
                );
                assert_eq!(
                    value["availabilityByVerb"],
                    serde_json::to_value(&by_verb).unwrap()
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_json_preserves_view_filters_and_refusals() {
        use cairn_common::query::parse_query_params;
        use cairn_models::interface::{ModelScope, Models};
        use cairn_models::types::catalog::VerbKind;
        let orch = seeded_orchestrator().await;
        let render =
            |query: &str| super::render_catalog(&orch, &parse_query_params(query).unwrap());
        let json =
            |query: &str| -> serde_json::Value { serde_json::from_str(&render(query)).unwrap() };
        let providers = json("format=json");
        assert!(providers["providers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["installed"] == true));
        let all = json("format=json&installed=all");
        assert!(all["providers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["installed"] == false));
        let only = json("format=json&provider=claude&serves=session");
        assert_eq!(only["providers"].as_array().unwrap().len(), 1);
        assert_eq!(only["providers"][0]["descriptor"]["key"], "claude");
        assert_eq!(only["serves"], "session");
        for verb in VerbKind::ALL {
            let expected = orch
                .catalog_listing(&ModelScope::workspace(), Some(verb))
                .unwrap();
            let rendered = json(&format!("format=json&serves={verb}&installed=all"));
            assert_eq!(rendered, serde_json::to_value(expected).unwrap());
            let models = json(&format!(
                "format=json&view=models&serves={verb}&provider=claude"
            ));
            let catalog = orch.catalog(&ModelScope::workspace()).unwrap();
            let expected: Vec<_> = catalog
                .models()
                .into_iter()
                .filter(|m| m.offers.iter().any(|o| o.provider == "claude"))
                .filter_map(|mut m| {
                    m.offers.retain(|o| o.selected(verb).is_some());
                    for offer in &mut m.offers {
                        offer.availability = offer.availability_for(verb).cloned();
                    }
                    (!m.offers.is_empty()).then_some(m)
                })
                .collect();
            assert_eq!(models, serde_json::to_value(expected).unwrap());
        }
        assert_eq!(
            json("format=json&view=models"),
            json("format=json&view=models&installed=all")
        );
        for view in ["providers", "models"] {
            assert!(
                render(&format!("view={view}&format=yaml")).starts_with("Unknown format 'yaml'")
            );
            assert!(
                render(&format!("view={view}&format=json&provider=not-a-provider"))
                    .starts_with("No provider")
            );
            assert!(
                render(&format!("view={view}&format=json&serves=not-a-verb"))
                    .starts_with("Unknown serves=")
            );
        }
        let page = read(&orch, "cairn://models/catalog").await;
        assert!(page.contains("format=markdown (default) | json"), "{page}");
        assert!(page.contains("expectedLoadMs"), "{page}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_catalog_says_what_each_provider_and_offer_serves() {
        let orch = seeded_orchestrator().await;
        let catalog = read(&orch, "cairn://models/catalog").await;
        assert!(
            catalog.contains("Serves: complete, session (claude-stream-json)"),
            "{catalog}"
        );
        assert!(
            catalog.contains("Serves: complete, session, decide (openai-chat)"),
            "the custom chat provider decides from logprobs: {catalog}"
        );
        assert!(
            catalog.contains("`opus` — 1M context")
                && catalog.contains("· serves complete, session (claude-stream-json)"),
            "{catalog}"
        );

        let deciding = read(&orch, "cairn://models/catalog?serves=decide").await;
        assert!(
            deciding.contains("Models serving decide (0 of "),
            "no Claude model decides: {deciding}"
        );
        assert!(!deciding.contains("`opus` — 1M context"), "{deciding}");

        let models = read(&orch, "cairn://models/catalog?view=models&serves=session").await;
        assert!(
            models.contains("Only offers that serve session"),
            "{models}"
        );
        assert!(
            models.contains(
                "claude `opus` (alias opus has no observed identity; awaiting a Claude session, 1M context) · serves complete, session (claude-stream-json)"
            ),
            "{models}"
        );
        let none = read(&orch, "cairn://models/catalog?view=models&serves=decide").await;
        assert!(!none.contains("`claude/opus`"), "{none}");

        let drawing = read(&orch, "cairn://models/catalog?serves=image").await;
        assert!(
            drawing.contains("Models serving image (0 of "),
            "no seeded model draws: {drawing}"
        );

        let bad = read(&orch, "cairn://models/catalog?serves=speech").await;
        assert!(
            bad.starts_with(
                "Unknown serves='speech' for cairn://models/catalog; expected one of complete, session, decide, embed, image, speak."
            ),
            "{bad}"
        );
    }

    /// The app's settings pages read `get_catalog_listing`, which answers
    /// `Models::catalog_listing`. This resource must list exactly the same
    /// models, provider by provider, for every verb.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_resource_lists_exactly_what_the_app_reads_for_every_verb() {
        use cairn_models::interface::{ModelScope, Models};
        use cairn_models::types::catalog::VerbKind;
        let orch = seeded_orchestrator().await;
        for key in orch.enabled_providers() {
            if key.starts_with("custom:") {
                orch.run_provider_model_catalog_refresh(&key).await.unwrap();
            }
        }
        let section = |rendered: &str, heading: &str| -> String {
            let start = rendered
                .find(heading)
                .unwrap_or_else(|| panic!("no {heading} in:\n{rendered}"));
            let rest = &rendered[start + heading.len()..];
            rest[..rest.find("\n## ").unwrap_or(rest.len())].to_string()
        };
        let mut listed_any = 0;
        for verb in VerbKind::ALL {
            let listing = orch
                .catalog_listing(&ModelScope::workspace(), Some(verb))
                .unwrap();
            let rendered = read(&orch, &format!("cairn://models/catalog?serves={verb}")).await;
            for provider in listing
                .providers
                .iter()
                .filter(|provider| provider.installed)
            {
                let heading = format!(
                    "\n## {} (`{}`)",
                    provider.descriptor.display_name, provider.descriptor.key
                );
                let rendered_models: Vec<String> = section(&rendered, &heading)
                    .lines()
                    .filter_map(|line| line.strip_prefix("- `"))
                    .map(|rest| rest.split('`').next().unwrap_or_default().to_string())
                    .collect();
                let listed: Vec<String> = provider
                    .catalog
                    .iter()
                    .flat_map(|catalog| catalog.models.iter().map(|model| model.model.clone()))
                    .collect();
                assert_eq!(
                    rendered_models, listed,
                    "{verb} on {}",
                    provider.descriptor.key
                );
                listed_any += listed.len();
            }
        }
        assert!(listed_any > 0, "the seeded workspace serves something");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_catalog_says_what_a_speech_offer_can_do() {
        use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth};
        let orch = seeded_orchestrator().await;
        let account = orch
            .add_custom_provider(
                "Kokoro".into(),
                ProviderAuth::custom_endpoint(
                    "http://127.0.0.1:1",
                    CustomProtocol::OpenAiSpeech,
                    None,
                    &[],
                    CustomModelPolicy::Explicit {
                        ids: vec!["kokoro".into(), "gpt-4o-mini-tts".into()],
                    },
                    CustomSampling::default(),
                    32_768,
                )
                .unwrap(),
                None,
            )
            .unwrap();
        let key = cairn_models::types::catalog::custom_key(&account.id);
        orch.run_provider_model_catalog_refresh(&key).await.unwrap();

        let catalog = read(&orch, "cairn://models/catalog").await;
        assert!(
            catalog.contains(
                "Serves: nothing by default; each model serves what its discovery declares"
            ),
            "a speech provider declares nothing provider-wide: {catalog}"
        );
        let speaking = read(&orch, "cairn://models/catalog?serves=speak").await;
        assert!(
            speaking.contains(
                "serves speak (openai-speech: segmented · pcm_24000|mp3|opus · speed 25–400%)"
            ),
            "{speaking}"
        );
        assert!(
            speaking.contains("speed 25–400% · instructions)"),
            "gpt-4o-mini-tts takes instructions: {speaking}"
        );
        assert!(!speaking.contains("`opus` — 1M context"), "{speaking}");
        let models = read(&orch, "cairn://models/catalog?view=models&serves=speak").await;
        assert!(
            models.contains("`kokoro`") || models.contains("kokoro"),
            "{models}"
        );
        assert!(!models.contains("qwen3.8-27b"), "{models}");
    }

    #[test]
    fn an_embedding_offer_renders_its_width_purpose_and_limits() {
        use cairn_models::embed::{families, local_server_limits};
        use cairn_models::types::catalog::{ServedVerb, Verb, Wire};
        use cairn_models::types::embedding::EmbeddingCapabilities;
        let hub = ServedVerb {
            verb: Verb::Embed(families::capabilities(
                "qwen/qwen3-embedding-4b",
                Some(2560),
                false,
                local_server_limits(None),
            )),
            wire: Wire::OpenAiEmbeddings,
        };
        let gateway = ServedVerb {
            verb: Verb::Embed(EmbeddingCapabilities::cairn_gateway()),
            wire: Wire::CairnEmbed,
        };
        assert_eq!(
            super::served(&[hub]).as_deref(),
            Some("embed (openai-embeddings: 2560 · native width only · query prefix · ≤32 texts · ≤32000 chars)")
        );
        let mut both = ServedVerb::conversation(Wire::OpenAiChatCompletions);
        both.push(gateway);
        assert_eq!(
            super::served(&both).as_deref(),
            Some("complete, session (openai-chat); embed (cairn-embed: 1536 · native width only · native purpose · ≤96 texts · ≤48000 chars)")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_catalog_filters_on_embed() {
        let orch = seeded_orchestrator().await;
        let embedding = read(&orch, "cairn://models/catalog?view=models&serves=embed").await;
        assert!(
            embedding.contains("Only offers that serve embed"),
            "{embedding}"
        );
        assert!(!embedding.contains("`claude/opus`"), "{embedding}");
    }

    #[test]
    fn an_image_offer_says_what_it_can_draw_beside_its_wire() {
        use super::{ServedVerb, Wire};
        use cairn_models::types::catalog::Verb;
        use cairn_models::types::image::ImageCapabilities;
        let image = ServedVerb {
            verb: Verb::Image(ImageCapabilities::openai_gpt_image()),
            wire: Wire::OpenAiImages,
        };
        assert_eq!(
            super::served(std::slice::from_ref(&image)).as_deref(),
            Some("image (openai-images: ≤10 · refs ≤16 · size any ×16 ≤3840 ≤3:1 · mask)")
        );
        let mut both = ServedVerb::conversation(Wire::OpenAiChatCompletions);
        both.push(ServedVerb {
            verb: Verb::Image(ImageCapabilities::openai_images_minimal()),
            wire: Wire::OpenRouterImages,
        });
        assert_eq!(
            super::served(&both).as_deref(),
            Some("complete, session (openai-chat); image (openrouter-images: ≤1 · size 1024x1024|1024x1536|1536x1024 · no edit)")
        );
    }

    #[test]
    fn prices_beyond_tokens_render_in_their_own_unit() {
        use super::{BillingUnit, Charge, Modality, ModelPricing};
        use cairn_models::types::catalog::Price;
        use cairn_models::types::usage::Meter;
        let price = |meter: Meter, usd: &str| Price {
            meter,
            usd: usd.into(),
        };
        let pricing = ModelPricing {
            prices: vec![
                price(Meter::tokens(Charge::Input), "0.000002"),
                price(Meter::tokens(Charge::Output), "0.000012"),
                price(Meter::tokens(Charge::CacheRead), "0.0000002"),
                price(
                    Meter {
                        charge: Charge::Output,
                        modality: Some(Modality::Image),
                        unit: BillingUnit::Tokens,
                    },
                    "0.00003",
                ),
                price(
                    Meter {
                        charge: Charge::Input,
                        modality: Some(Modality::Image),
                        unit: BillingUnit::Images,
                    },
                    "0.04",
                ),
                price(
                    Meter {
                        charge: Charge::Request,
                        modality: None,
                        unit: BillingUnit::Requests,
                    },
                    "0",
                ),
            ],
            unplaced: Default::default(),
        };
        assert_eq!(
            super::pricing(&pricing).unwrap(),
            "in $2/M, out $12/M, cache read $0.2/M, image out $30/M, image in $0.04/image"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rolled_over_windows_and_past_blocks_read_as_history() {
        use crate::credentials::{ApiProvider, ProviderAuth};
        use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageWindow};
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
        orch.record_account_health(
            &stale.id,
            vec![window("rolled", 0.0, 1_000)],
            Some(crate::credentials::AccountBlock::Declared(1_000)),
        )
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
            Some(crate::credentials::AccountBlock::Declared(far_future)),
        )
        .unwrap();

        let stale_page = read(&orch, &format!("cairn://models/accounts/{}", stale.id)).await;
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

        let current_page = read(&orch, &format!("cairn://models/accounts/{}", current.id)).await;
        assert!(current_page.contains("fresh: 0% left"), "{current_page}");
        assert!(current_page.contains("blocked until"), "{current_page}");
        assert!(!current_page.contains("expired"), "{current_page}");
    }

    /// Golden: the usage page's block for a metered provider with recorded
    /// per-model cost and credits.
    #[test]
    fn a_metered_pool_renders_as_its_golden() {
        let pool: cairn_models::types::pool::ProviderPoolSnapshot =
            serde_json::from_value(serde_json::json!({
                "backend": "openrouter",
                "accounts": [{
                    "accountId": "acc_1",
                    "label": "Main key",
                    "state": "measured",
                    "source": "openrouter_generation",
                    "capturedAt": 1790000000,
                    "blockedUntil": null,
                    "error": null,
                    "errorKind": null
                }],
                "families": [],
                "constraints": [],
                "credits": {"balance": 12.5, "totalGranted": 20.0, "totalUsed": 7.5, "currency": "USD"},
                "modelBreakdown": [
                    {"model": "z-ai/glm-4.6", "costUsd": 0.4213, "tokens": 120000, "runs": 3},
                    {"model": "openai/gpt-5", "costUsd": 1.0, "tokens": null, "runs": null}
                ],
                "unsupportedReason": "OpenRouter meters by credit, not by window",
                "capturedAt": 1790000000
            }))
            .unwrap();
        let mut out = String::new();
        super::pool_block(&pool, "OpenRouter (`openrouter`)", &mut out);
        crate::backends::assert_golden_file(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/resources/testdata/models_usage_pool.md"
            ),
            &out,
        );
    }

    #[test]
    fn a_held_pool_account_names_its_hold_and_the_act_that_lifts_it() {
        use cairn_models::types::pool::{build_provider_pool, PoolAccountInput};
        let hold = crate::credentials::AccountHold {
            since: 1_790_000_000,
            kind: crate::credentials::AccountHoldKind::TermsAcceptance,
            detail: "Please accept the updated Consumer Terms.".to_string(),
        };
        let pool = build_provider_pool(
            "claude",
            vec![PoolAccountInput {
                account_id: "acc_1".to_string(),
                label: "work@example.com".to_string(),
                blocked_until: Some(1_790_000_600),
                held: Some(hold.clone()),
                ..Default::default()
            }],
            None,
            None,
            None,
            1_790_000_100,
        );
        let mut out = String::new();
        super::pool_block(&pool, "Claude (`claude`)", &mut out);
        let line = format!(
            "- Account work@example.com: blocked: {}",
            hold.describe(&super::when(hold.since), "work@example.com")
        );
        assert!(out.contains(&line), "{out}");
        // A hold is not lifted by the timer beneath it.
        assert!(!out.contains("blocked until"), "{out}");
        assert!(
            !out.contains("Exhausted: every account is blocked (every model) until"),
            "{out}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn usage_history_lists_an_accounts_readings_per_window() {
        use cairn_db::storage::usage_readings::{record_usage_readings, UsageReadingInput};
        let orch = seeded_orchestrator().await;
        let empty = read(&orch, "cairn://models/usage?history=acct-a").await;
        assert!(
            empty.contains("No usage readings recorded for this account yet"),
            "{empty}"
        );

        let window =
            |id: &str, name: &str, target: Option<&str>, remaining: f64| UsageReadingInput {
                window_id: id.into(),
                window_name: name.into(),
                scope: "weekly".into(),
                scope_target: target.map(str::to_string),
                remaining_percent: remaining,
                window_duration_mins: None,
                resets_at: Some(2_000_000_000),
            };
        let now = chrono::Utc::now().timestamp();
        for (at, remaining) in [(now - 600, 50.0), (now - 300, 45.0)] {
            record_usage_readings(
                &orch.db.local,
                "claude",
                "acct-a",
                "claude_usage_oauth",
                at,
                vec![
                    window("weekly_all", "Week", None, remaining + 20.0),
                    window("weekly_opus-opus", "Opus", Some("Opus"), remaining),
                ],
            )
            .await
            .unwrap();
        }

        let page = read(&orch, "cairn://models/usage?history=acct-a").await;
        assert!(page.contains("# Usage history: acct-a"), "{page}");
        assert!(
            page.contains("## Opus (`weekly_opus-opus`, claude)"),
            "{page}"
        );
        assert!(page.contains("## Week (`weekly_all`, claude)"), "{page}");
        assert!(page.contains("| 45% |"), "{page}");
        assert!(
            page.contains("by family name (Session, Week, Opus, 5-hour)"),
            "the affordance block lists the history keys: {page}"
        );

        let json = read(
            &orch,
            "cairn://models/usage?history=acct-a&window=Opus&format=json",
        )
        .await;
        // The JSON leads the page; the affordance block follows it.
        let parsed: serde_json::Value = serde_json::Deserializer::from_str(&json)
            .into_iter::<serde_json::Value>()
            .next()
            .expect(&json)
            .expect(&json);
        assert_eq!(parsed["account"], "acct-a");
        assert_eq!(parsed["window"], "Opus");
        let readings = parsed["readings"].as_array().unwrap();
        assert_eq!(readings.len(), 2);
        assert_eq!(readings[0]["windowId"], "weekly_opus-opus");
        assert_eq!(readings[0]["windowName"], "Opus");
        assert_eq!(readings[0]["scopeTarget"], "Opus");
        assert_eq!(readings[1]["remainingPercent"], 45.0);
        assert_eq!(readings[1]["source"], "claude_usage_oauth");

        let bounded = read(
            &orch,
            "cairn://models/usage?history=acct-a&hours=1&format=json",
        )
        .await;
        assert!(bounded.contains("weekly_all"), "{bounded}");

        let refused = read(&orch, "cairn://models/usage?window=Opus").await;
        assert!(
            refused.contains("reads only with history=<account>"),
            "{refused}"
        );
        let refused = read(&orch, "cairn://models/usage?format=json").await;
        assert!(
            refused.contains("reads only with history=<account>"),
            "{refused}"
        );
        let refused = read(&orch, "cairn://models/usage?history=acct-a&view=requests").await;
        assert!(
            refused.contains("Unsupported query parameter 'view'"),
            "{refused}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn per_request_usage_lists_each_clients_rows_and_totals() {
        let orch = seeded_orchestrator().await;
        let out = read(&orch, "cairn://models/usage?view=requests").await;
        assert!(out.contains("0 row(s)"), "{out}");

        let (dig, _) = crate::orchestrator::served_api::clients::mint(&orch.db.local, "dig")
            .await
            .unwrap();
        let db = orch.db.local.clone();
        tokio::task::spawn_blocking(move || {
            let mut ledger = crate::orchestrator::served_api::requests::RequestLedger::begin(
                db,
                cairn_models::interface::relay::RelayDoor::ChatCompletions,
                Some(&dig),
                false,
                Some("big".into()),
            );
            ledger.resolved("deepseek/deepseek-v4-flash-vision-exp");
            ledger.finish(
                200,
                crate::orchestrator::served_api::requests::Outcome::Answered,
                Some(&cairn_models::interface::relay::RelayUsage {
                    input_tokens: Some(40),
                    output_tokens: Some(9),
                    ..Default::default()
                }),
                None,
            );
        })
        .await
        .unwrap();
        let out = read(&orch, "cairn://models/usage?view=requests&client=dig").await;
        assert!(out.contains("1 row(s)"), "{out}");
        assert!(out.contains("| dig | 1 | 40 | 9 |"), "{out}");
        assert!(
            out.contains("| big | deepseek/deepseek-v4-flash-vision-exp |"),
            "{out}"
        );
        let out = read(&orch, "cairn://models/usage?view=requests&client=nobody").await;
        assert!(
            out.contains("no served-API client is named nobody"),
            "{out}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_models_read_reveals_a_credential() {
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

    fn signed_in(email: &str, expires_at: i64, source: &str) -> serde_json::Value {
        serde_json::json!({
            "id": format!("acc_{email}"),
            "label": email,
            "email": email,
            "signInExpiry": {"expiresAt": expires_at, "source": source},
        })
    }

    #[test]
    fn the_soonest_reconnect_heads_the_accounts_and_a_lapsed_one_outranks_it() {
        const DAY: i64 = 24 * 60 * 60;
        let now = 1_000 * DAY;
        let mut accounts = vec![
            signed_in("later@example.com", now + 20 * DAY, "sign_in"),
            signed_in("soon@example.com", now + 5 * DAY, "provider"),
            serde_json::json!({"id": "acc_key", "label": "API key"}),
        ];
        let headline = super::reconnect_headline(&accounts, now).unwrap();
        assert!(
            headline.starts_with("**Next reconnect:** soon@example.com by "),
            "{headline}"
        );
        assert!(headline.contains("in 5 days"), "{headline}");

        let line = super::sign_in_line(&accounts[1], now).unwrap();
        assert!(line.contains("renewable until"), "{line}");
        assert!(line.contains("declared by the provider"), "{line}");
        let due = super::sign_in_line(&accounts[1], now + 3 * DAY).unwrap();
        assert!(due.starts_with("reconnect by "), "{due}");
        assert!(super::sign_in_line(&accounts[2], now).is_none());

        accounts.push(signed_in("gone@example.com", now - 1, "inferred"));
        let headline = super::reconnect_headline(&accounts, now).unwrap();
        assert!(
            headline.starts_with("**Reconnect now:** gone@example.com."),
            "{headline}"
        );
        let gone = super::sign_in_line(&accounts[3], now).unwrap();
        assert!(gone.contains("nothing is routed to it"), "{gone}");
        assert!(gone.contains("inferred"), "{gone}");

        assert!(super::reconnect_headline(&accounts[2..3], now).is_none());
    }
}
