//! The fit report: every window's rates with their intervals and support,
//! the held-out error, the component check, account heterogeneity,
//! week-by-week stability, what was left out, and the exhausted-window seed
//! cross-check. `cairn://models/usage?capacity=fit` renders it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::{Components, DrainFit, Estimate, Rates, WindowFit};

/// One account read at 0% of the all-models weekly window, with the tokens
/// spent since the window began, in millions: cache read, cache write,
/// output (uncached input was under 0.1M).
pub struct ExhaustedSeed {
    pub account: &'static str,
    pub opus: [f64; 3],
    pub all_models: [f64; 3],
}

/// Four Claude Max 20x accounts read at 0% of the all-models weekly window
/// (recorded on cairn/6270, 2026-11-05). They were attributed through
/// `runs.account_id`, the last account of each run, which is weaker than
/// per-event attribution, so they are a cross-check and never fitted.
pub const EXHAUSTED_SEEDS: [ExhaustedSeed; 4] = [
    ExhaustedSeed {
        account: "acc_d2db948b",
        opus: [3274.0, 78.0, 15.3],
        all_models: [3774.0, 97.0, 19.3],
    },
    ExhaustedSeed {
        account: "acc_97630bdd",
        opus: [2641.0, 75.0, 16.2],
        all_models: [3190.0, 99.0, 21.2],
    },
    ExhaustedSeed {
        account: "acc_3593bdaa",
        opus: [2843.0, 68.0, 12.9],
        all_models: [3423.0, 90.0, 16.3],
    },
    ExhaustedSeed {
        account: "acc_ec6e925a",
        opus: [2851.0, 97.0, 16.3],
        all_models: [3289.0, 124.0, 20.8],
    },
];

fn millions(tokens: [f64; 3]) -> Components {
    Components {
        input: 0.0,
        cache_read: tokens[0] * 1e6,
        cache_write: tokens[1] * 1e6,
        output: tokens[2] * 1e6,
    }
}

/// A number to three significant figures.
pub fn sig(value: f64) -> String {
    if !value.is_finite() {
        return "∞".to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }
    let decimals = (2 - value.abs().log10().floor() as i32).clamp(0, 6) as usize;
    format!("{value:.decimals$}")
}

/// `value [low, high]` with each part through `show`.
pub fn interval(estimate: &Estimate, show: impl Fn(f64) -> String) -> String {
    format!(
        "{} [{}, {}]",
        show(estimate.value),
        show(estimate.low),
        show(estimate.high)
    )
}

/// Whole dollars with thousands separators: `$2,140`.
pub fn dollars(value: f64) -> String {
    if !value.is_finite() {
        return "∞".to_string();
    }
    if value.abs() < 10.0 {
        return format!("${value:.2}");
    }
    let whole = value.round() as i64;
    let digits = whole.abs().to_string();
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    format!("{}${grouped}", if whole < 0 { "-" } else { "" })
}

/// A token count: `1.23B`, `456M`, `12.3k`.
pub fn token_count(value: f64) -> String {
    if !value.is_finite() {
        return "∞".to_string();
    }
    let (scaled, unit) = if value >= 1e9 {
        (value / 1e9, "B")
    } else if value >= 1e6 {
        (value / 1e6, "M")
    } else if value >= 1e3 {
        (value / 1e3, "k")
    } else {
        (value, "")
    };
    format!("{}{unit}", sig(scaled))
}

fn when(at: i64) -> String {
    // Days and hours since the epoch, as a UTC date, without a date crate.
    let days = at.div_euclid(86_400);
    let seconds = at.rem_euclid(86_400);
    let (year, month, day) = civil(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02} UTC",
        seconds / 3600,
        seconds % 3600 / 60
    )
}

/// The proleptic Gregorian date of a day count since 1970-01-01 (Howard
/// Hinnant's `civil_from_days`).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn date(at: i64) -> String {
    when(at)[..10].to_string()
}

/// The first block of an account id, enough to tell accounts apart.
pub fn short_account(id: &str) -> &str {
    match id.find('-') {
        Some(end) if id.starts_with("acc_") => &id[..end],
        _ => id,
    }
}

fn window_heading(window: &WindowFit) -> String {
    let scope = match &window.scope_target {
        Some(target) => format!("{target} only"),
        None => "all models".to_string(),
    };
    format!(
        "{} · {} · {} ({scope}, {})",
        window.provider,
        window.plan,
        window.window_name,
        duration(window.duration_mins)
    )
}

fn duration(mins: i64) -> String {
    if mins % (24 * 60) == 0 {
        format!("{} d", mins / (24 * 60))
    } else if mins % 60 == 0 {
        format!("{} h", mins / 60)
    } else {
        format!("{mins} min")
    }
}

fn rates_table(window: &WindowFit, out: &mut String) {
    out.push_str(
        "| Family | % of window per list $ (90%) | Support | n | List $ per window | Tokens per window (plan mix) | Output tokens per window | Spend seen |\n|---|---|---|---|---|---|---|---|\n",
    );
    for rate in &window.families {
        let usd = (rate.percent_per_usd.value > 0.0).then(|| 100.0 / rate.percent_per_usd.value);
        let tokens = rate.tokens_per_window();
        let output_share = if rate.mix.tokens.total() > 0.0 {
            rate.mix.tokens.output / rate.mix.tokens.total()
        } else {
            0.0
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            rate.family,
            interval(&rate.percent_per_usd, sig),
            rate.support.label(),
            rate.instances,
            usd.map_or_else(|| "—".to_string(), dollars),
            tokens
                .as_ref()
                .map_or_else(|| "—".to_string(), |t| interval(t, token_count)),
            tokens
                .as_ref()
                .map_or_else(|| "—".to_string(), |t| token_count(t.value * output_share)),
            dollars(rate.spend_usd),
        );
    }
    let _ = writeln!(
        out,
        "\nAll families as one rate: {} %/$ (n {}).",
        interval(&window.all, sig),
        window.instances
    );
}

fn validation_block(window: &WindowFit, out: &mut String) {
    let Some(validation) = &window.validation else {
        out.push_str(
            "\n**Held out:** no complete instance to hold out, or nothing left to train on.\n",
        );
        return;
    };
    let _ = writeln!(
        out,
        "\n**Held out** (each account's latest complete instance, predicted by a fit of the other {}): end-of-instance mean absolute error {} pp with pooled rates, {} pp with each account's own scale; per-sample RMSE {} pp.\n",
        validation.training_instances,
        sig(validation.end_mae),
        sig(validation.end_mae_per_account),
        sig(validation.sample_rmse),
    );
    out.push_str("| Account | Instance | Observed used % | Predicted | With account scale |\n|---|---|---|---|---|\n");
    for row in &validation.rows {
        let _ = writeln!(
            out,
            "| `{}` | {} → {} | {} | {} | {} |",
            short_account(&row.account),
            date(row.started_at),
            date(row.ended_at),
            sig(row.observed),
            sig(row.predicted),
            sig(row.predicted_per_account),
        );
    }
}

fn component_block(window: &WindowFit, out: &mut String) {
    let Some(check) = &window.component_check else {
        return;
    };
    let _ = writeln!(
        out,
        "\n**Components** of {} (percent of the window per million tokens: a free fit of each component beside what the family's dollar rate implies at list-price ratios; the components move together, so the free fit is wide):\n",
        check.family
    );
    out.push_str("| Component | Share of list $ | Fitted (90%) | List-implied | Verdict |\n|---|---|---|---|---|\n");
    for (index, name) in Components::NAMES.iter().enumerate() {
        let fitted = check.fitted[index];
        let implied = check.list_implied[index];
        let verdict = if check.usd_share[index] == 0.0 {
            "not spent"
        } else if check.usd_share[index] < 0.01 {
            "too little spent to tell"
        } else if fitted.contains(implied) {
            "keeps its list ratio"
        } else if fitted.high < implied {
            "counts less than list"
        } else {
            "counts more than list"
        };
        let _ = writeln!(
            out,
            "| {name} | {}% | {} | {} | {verdict} |",
            sig(check.usd_share[index] * 100.0),
            interval(&fitted, sig),
            sig(implied)
        );
    }
}

fn scale_block(window: &WindowFit, out: &mut String) {
    if window.account_scales.is_empty() {
        return;
    }
    out.push_str("\n**Accounts on one plan** (each account's drain over the pooled rates, family ratios held fixed; served only where the interval excludes 1 over at least 3 instances):\n\n| Account | Scale (90%) | n | Served |\n|---|---|---|---|\n");
    for scale in &window.account_scales {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} |",
            short_account(&scale.account),
            interval(&scale.scale, sig),
            scale.instances,
            sig(scale.served),
        );
    }
}

fn stability_block(window: &WindowFit, out: &mut String) {
    if window.stability.is_empty() {
        return;
    }
    let families: Vec<&str> = window
        .families
        .iter()
        .filter(|rate| rate.support == super::Support::Measured)
        .map(|rate| rate.family.as_str())
        .collect();
    out.push_str("\n**Stability** (measured families refitted on each calendar week's instances, by start):\n\n| Week of | n |");
    for family in &families {
        let _ = write!(out, " {family} |");
    }
    out.push_str("\n|---|---|");
    for _ in &families {
        out.push_str("---|");
    }
    out.push('\n');
    let mut spread: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for week in &window.stability {
        let _ = write!(out, "| {} | {} |", date(week.week_start), week.instances);
        for family in &families {
            match week.percent_per_usd.get(*family) {
                Some(k) => {
                    spread.entry(family).or_default().push(*k);
                    let _ = write!(out, " {} |", sig(*k));
                }
                None => out.push_str(" — |"),
            }
        }
        out.push('\n');
    }
    let lines: Vec<String> = spread
        .iter()
        .filter(|(_, values)| values.len() > 1)
        .map(|(family, values)| {
            let low = values.iter().copied().fold(f64::INFINITY, f64::min);
            let high = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            format!(
                "{family} {}–{} ({}% of its mean)",
                sig(low),
                sig(high),
                sig((high - low) / mean * 100.0)
            )
        })
        .collect();
    if !lines.is_empty() {
        let _ = writeln!(out, "\nWeek-to-week range: {}.", lines.join("; "));
    }
}

fn scope_block(window: &WindowFit, out: &mut String) {
    if let Some(scope) = &window.scope_check {
        let _ = writeln!(
            out,
            "\n**Scope check:** {} of out-of-scope spend, fitted beside the in-scope families, comes back at {} %/$; near zero means the window counts only what its scope names.",
            dollars(scope.out_of_scope_usd),
            interval(&scope.percent_per_usd, sig)
        );
    }
}

fn seed_block(fit: &DrainFit, out: &mut String) {
    let Some(week) = fit.window_fit("claude", "max_20x", "Week") else {
        return;
    };
    let (Some(opus), Some(rates)) = (week.family("opus"), fit.rates.get("opus")) else {
        return;
    };
    // The seeds do not say which models the non-Opus tokens ran on, so they
    // are bounded by the cheapest and dearest family the window prices.
    let others: Vec<(&str, f64, &Rates)> = week
        .families
        .iter()
        .filter(|rate| rate.family != "opus")
        .filter_map(|rate| {
            Some((
                rate.family.as_str(),
                rate.percent_per_usd.value,
                fit.rates.get(&rate.family)?,
            ))
        })
        .collect();
    out.push_str("\n## Cross-check: four exhausted windows\n\nFour Max 20x accounts read 0% left of the all-models weekly window on 2026-11-05. Their spend since the window began was attributed through `runs.account_id` (weaker than per-event attribution), so it is never fitted; here the Week rates above predict how much of the window it used, which should be about 100%. Opus is priced at its rate; the seeds do not say which models the other tokens ran on, so they are bounded between the cheapest and the dearest other family's rate.\n\n| Account | Opus tokens (cache read / write / output) | Opus alone | Other tokens | All models |\n|---|---|---|---|---|\n");
    for seed in &EXHAUSTED_SEEDS {
        let opus_tokens = millions(seed.opus);
        let other_tokens = Components::from_array(std::array::from_fn(|c| {
            millions(seed.all_models).as_array()[c] - opus_tokens.as_array()[c]
        }));
        let opus_pct = opus.percent_per_usd.scaled(rates.cost(&opus_tokens));
        let other: Vec<f64> = others
            .iter()
            .map(|(_, k, rates)| k * rates.cost(&other_tokens))
            .collect();
        let low = other.iter().copied().fold(f64::INFINITY, f64::min);
        let high = other.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let (other_text, all_text) = if other.is_empty() {
            ("not priced".to_string(), "—".to_string())
        } else {
            (
                format!("{}–{}%", sig(low), sig(high)),
                format!(
                    "{}–{}%",
                    sig(opus_pct.value + low),
                    sig(opus_pct.value + high)
                ),
            )
        };
        let _ = writeln!(
            out,
            "| `{}` | {}M / {}M / {}M | {}% | {other_text} | {all_text} |",
            seed.account,
            sig(seed.opus[0]),
            sig(seed.opus[1]),
            sig(seed.opus[2]),
            interval(&opus_pct, sig),
        );
    }
    if !others.is_empty() {
        let names: Vec<&str> = others.iter().map(|(family, ..)| *family).collect();
        let _ = writeln!(out, "\nThe other tokens' range spans {}.", names.join(", "));
    }
}

fn left_out(fit: &DrainFit, out: &mut String) {
    out.push_str("\n## Left out\n\n");
    if fit.skipped_instances.is_empty() {
        out.push_str("Every window instance was fitted.\n");
    } else {
        let parts: Vec<String> = fit
            .skipped_instances
            .iter()
            .map(|(reason, count)| format!("{count} {reason}"))
            .collect();
        let _ = writeln!(out, "Window instances not fitted: {}.", parts.join(", "));
    }
    let mut by_reason: BTreeMap<&str, (f64, Vec<&str>)> = BTreeMap::new();
    for dropped in &fit.dropped {
        let entry = by_reason.entry(dropped.reason.as_str()).or_default();
        entry.0 += dropped.tokens;
        if !entry.1.contains(&dropped.model.as_str()) {
            entry.1.push(dropped.model.as_str());
        }
    }
    if by_reason.is_empty() {
        out.push_str("No attributed tokens were dropped.\n");
    }
    for (reason, (tokens, models)) in by_reason {
        let _ = writeln!(
            out,
            "Dropped tokens, {reason}: {} ({}).",
            token_count(tokens),
            models.join(", ")
        );
    }
}

/// The whole report, as markdown.
pub fn render_fit_report(fit: &DrainFit) -> String {
    let mut out = String::from("# Window drain fit\n\n");
    let _ = writeln!(
        out,
        "Fitted {} from {} usage-window readings. A rate is percent of the window per list-price dollar of the family (cairn-analytics' price table), fitted jointly over every family a window counts, with a free intercept per window instance. Intervals are 90%, percentiles of a bootstrap that resamples whole window instances (300 draws, seeded) unless the window holds fewer than 3, where they are per-sample and too narrow. n is the instance count behind each number. A family is measured when its interval is above zero and at most half as wide as its value over at least 3 instances; otherwise it is pooled with the window's other unsupported families, or takes the window's all-family rate.",
        when(fit.fitted_at),
        fit.readings
    );
    if fit.windows.is_empty() {
        out.push_str("\nNo window instance has enough readings and attributed spend to fit yet.\n");
    }
    for window in &fit.windows {
        let _ = writeln!(out, "\n## {}\n", window_heading(window));
        let _ = writeln!(
            out,
            "{} instance{} on {} account{}, {} hourly samples, intervals {}, residual sd {} pp.\n",
            window.instances,
            if window.instances == 1 { "" } else { "s" },
            window.accounts,
            if window.accounts == 1 { "" } else { "s" },
            window.samples,
            match window.interval {
                super::IntervalKind::Bootstrap => "bootstrapped",
                super::IntervalKind::SampleLevel => "per-sample (fewer than 3 instances)",
            },
            sig(window.residual_sd)
        );
        rates_table(window, &mut out);
        validation_block(window, &mut out);
        component_block(window, &mut out);
        scale_block(window, &mut out);
        stability_block(window, &mut out);
        scope_block(window, &mut out);
    }
    seed_block(fit, &mut out);
    left_out(fit, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_read_at_a_glance() {
        assert_eq!(sig(0.04712), "0.0471");
        assert_eq!(sig(0.231), "0.231");
        assert_eq!(sig(2140.4), "2140");
        assert_eq!(sig(f64::INFINITY), "∞");
        assert_eq!(dollars(2140.4), "$2,140");
        assert_eq!(dollars(1_234_567.0), "$1,234,567");
        assert_eq!(dollars(0.4612), "$0.46");
        assert_eq!(token_count(1_234_000_000.0), "1.23B");
        assert_eq!(token_count(456_000_000.0), "456M");
        assert_eq!(when(1_794_690_000), "2026-11-14 21:00 UTC");
        assert_eq!(short_account("acc_3593bdaa-059d-4f72"), "acc_3593bdaa");
    }
}
