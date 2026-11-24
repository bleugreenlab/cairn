use super::*;
use crate::drain::{
    dollars_per_percent, family_of, fit as fit_all, AccountRef, LatestReading, Rates, Reading,
};

const H: i64 = 3600;

fn reading(account: &str, window: &str, used: f64, resets_at: Option<i64>, at: i64) -> Reading {
    let (id, name, scope) = match window {
        "Fable" => ("weekly_scoped-fable", "Fable", Some("Fable".to_string())),
        "Session" => ("session", "Session", None),
        _ => ("weekly_all", "Week", None),
    };
    Reading {
        provider: "claude".into(),
        account: account.into(),
        window_id: id.into(),
        window_name: name.into(),
        scope_target: scope,
        used_pct: used,
        resets_at,
        duration_mins: None,
        captured_at: at,
    }
}

fn span(account: &str, plan: &str, started_at: i64, ended_at: Option<i64>) -> PlanSpan {
    PlanSpan {
        account: account.into(),
        provider: "claude".into(),
        plan: Some(plan.into()),
        started_at,
        ended_at,
    }
}

#[test]
fn nnls_clips_a_negative_coefficient_and_resolves_the_rest() {
    // Unconstrained, G⁻¹b = (1, -1); with k ≥ 0 the second is 0 and the first
    // takes b₁/G₁₁.
    let k = nnls(&[2.0, 1.0, 1.0, 2.0], &[1.0, -1.0], 2);
    assert!((k[0] - 0.5).abs() < 1e-12 && k[1] == 0.0, "{k:?}");
    // An interior solution is the plain one.
    let k = nnls(&[2.0, 1.0, 1.0, 2.0], &[3.0, 3.0], 2);
    assert!(
        (k[0] - 1.0).abs() < 1e-12 && (k[1] - 1.0).abs() < 1e-12,
        "{k:?}"
    );
    // A duplicated column is left at zero rather than failing.
    let k = nnls(&[1.0, 1.0, 1.0, 1.0], &[2.0, 2.0], 2);
    assert!((k[0] + k[1] - 2.0).abs() < 1e-9, "{k:?}");
}

#[test]
fn a_family_is_the_model_without_maker_or_version() {
    for (model, family) in [
        ("claude-opus-5-5", "opus"),
        ("claude-opus-5.5", "opus"),
        ("opus[1m]", "opus"),
        ("Opus", "opus"),
        ("claude-fable-5-2", "fable"),
        ("claude-sonnet-4-5-20250929", "sonnet"),
        ("gpt-6.1-sol", "gpt-sol"),
        ("gpt-5.6-sol", "gpt-sol"),
        ("gpt-6-luna", "gpt-luna"),
    ] {
        assert_eq!(family_of(model).as_deref(), Some(family), "{model}");
    }
    assert_eq!(family_of("5.5"), None);
}

#[test]
fn jittered_resets_are_one_instance_and_a_drop_starts_another() {
    let plans = vec![span("a", "max_20x", 0, None)];
    let base = 1_794_000_000;
    let readings = vec![
        reading("a", "Week", 10.0, Some(1_794_689_999), base),
        reading("a", "Week", 11.0, Some(1_794_690_000), base + H),
        reading("a", "Week", 12.0, Some(1_794_689_999), base + 2 * H),
        // Used falls by more than a point at an unchanged reset: a new window.
        reading("a", "Week", 3.0, Some(1_794_690_000), base + 3 * H),
        reading("a", "Week", 4.0, Some(1_794_690_000), base + 4 * H),
    ];
    let inputs = DrainInputs {
        readings,
        plans,
        now: base + 5 * H,
        ..Default::default()
    };
    let (instances, skipped) = segment(&inputs);
    assert!(skipped.is_empty());
    assert_eq!(instances.len(), 2);
    assert_eq!(instances[0].readings.len(), 3);
    assert!(instances[0].complete);
    assert!(!instances[1].complete);
}

#[test]
fn a_session_series_without_reset_times_splits_on_drops_alone() {
    let plans = vec![span("a", "max_20x", 0, None)];
    let base = 1_794_000_000;
    let readings: Vec<Reading> = [10.0, 40.0, 41.0, 2.0, 30.0]
        .iter()
        .enumerate()
        .map(|(i, used)| reading("a", "Session", *used, None, base + i as i64 * H))
        .collect();
    let inputs = DrainInputs {
        readings,
        plans,
        now: base + 10 * H,
        ..Default::default()
    };
    let (instances, _) = segment(&inputs);
    assert_eq!(
        instances
            .iter()
            .map(|i| i.readings.len())
            .collect::<Vec<_>>(),
        [3, 2]
    );
}

#[test]
fn an_instance_spanning_a_plan_change_is_dropped() {
    let base = 1_794_000_000;
    let plans = vec![
        span("a", "max_5x", 0, Some(base + 2 * H)),
        span("a", "max_20x", base + 2 * H, None),
    ];
    let readings = (0..5)
        .map(|i| {
            reading(
                "a",
                "Week",
                10.0 + i as f64,
                Some(base + 100 * H),
                base + i * H,
            )
        })
        .collect();
    let inputs = DrainInputs {
        readings,
        plans,
        now: base + 6 * H,
        ..Default::default()
    };
    let (instances, skipped) = segment(&inputs);
    assert!(instances.is_empty());
    assert_eq!(skipped.get("spans a plan change"), Some(&1));
}

// ---------------------------------------------------------------------------
// A synthetic world with known drain rates
// ---------------------------------------------------------------------------

const K_WEEK: [(&str, f64); 4] = [
    ("opus", 0.047),
    ("fable", 0.116),
    ("sonnet", 0.058),
    ("haiku", 0.05),
];
const K_FABLE: f64 = 0.231;
const WEEKS: i64 = 8;
/// Account b starts on Max 5x and upgrades this long after the data begins,
/// which resets its windows early.
const UPGRADE_AFTER: i64 = 20 * H;

fn rates(family: &str) -> Rates {
    let (input, cache_read, cache_write, output) = match family {
        "opus" => (4.0, 0.2, 5.0, 20.0),
        "fable" => (8.0, 0.2, 10.0, 40.0),
        "sonnet" => (2.0, 0.1, 2.5, 10.0),
        _ => (0.1, 0.01, 0.125, 0.5),
    };
    Rates {
        input,
        cache_read,
        cache_write,
        output,
    }
}

struct Options {
    /// Account b's drain relative to the pool.
    scale_b: f64,
    /// Sonnet's hourly spend relative to its usual.
    sonnet: f64,
}

/// Hourly token draws per family: (activity probability, max cache read,
/// max cache write, max output), each uniform.
fn draw_tokens(rng: &mut SplitMix, family: &str, sonnet: f64) -> Components {
    let (p, cache_read, cache_write, output) = match family {
        "opus" => (0.5, 40e6, 1.0e6, 0.3e6),
        "fable" => (0.4, 6e6, 0.2e6, 0.05e6),
        "sonnet" => (0.4, 20e6 * sonnet, 0.5e6 * sonnet, 0.1e6 * sonnet),
        // Haiku is a trickle: a few cents a week.
        _ => (0.05, 0.2e6, 0.0, 0.01e6),
    };
    if rng.unit() >= p {
        return Components::default();
    }
    Components {
        input: rng.unit() * 5_000.0,
        cache_read: rng.unit() * cache_read,
        cache_write: rng.unit() * cache_write,
        output: rng.unit() * output,
    }
}

fn synthetic(options: &Options) -> DrainInputs {
    let start = 1_790_006_400; // an hour boundary
    let end = start + WEEKS * 7 * 24 * H - 2 * H;
    let hours = ((end - start) / H) as usize + 1;
    let mut rng = SplitMix(42);
    let mut inputs = DrainInputs {
        now: end,
        ..Default::default()
    };
    for (family, _) in K_WEEK {
        inputs.rates.insert(family.to_string(), rates(family));
    }
    inputs.plans = vec![
        span("a", "max_20x", start - H, None),
        span("b", "max_5x", start - H, Some(start + UPGRADE_AFTER)),
        span("b", "max_20x", start + UPGRADE_AFTER, None),
    ];
    for (account, offset) in [("a", 0), ("b", 37 * H)] {
        // Per hour, each family's list dollars; then the cumulative sum.
        let mut usd: Vec<[f64; 4]> = vec![[0.0; 4]; hours];
        for (hour, slot) in usd.iter_mut().enumerate() {
            for (f, (family, _)) in K_WEEK.iter().enumerate() {
                let tokens = draw_tokens(&mut rng, family, options.sonnet);
                if tokens.total() <= 0.0 {
                    continue;
                }
                let mix = priced(tokens, &rates(family));
                slot[f] = mix.usd.total();
                inputs.spend.push(Spend {
                    account: account.into(),
                    hour: start + hour as i64 * H,
                    family: family.to_string(),
                    mix,
                });
            }
        }
        let cumulative = |t: i64, f: usize| -> f64 {
            let offset = (t - start).max(0);
            let whole = (offset / H) as usize;
            let mut sum: f64 = usd[..whole.min(hours)].iter().map(|row| row[f]).sum();
            if whole < hours {
                sum += usd[whole][f] * (offset % H) as f64 / H as f64;
            }
            sum
        };
        // Window boundaries and the multiple each instance drains at.
        let mut boundaries: Vec<(i64, f64)> = Vec::new();
        if account == "a" {
            for week in 0..=WEEKS + 1 {
                boundaries.push((start + week * 7 * 24 * H, 1.0));
            }
        } else {
            boundaries.push((start, 4.0 * options.scale_b));
            for week in 0..=WEEKS {
                boundaries.push((start + UPGRADE_AFTER + week * 7 * 24 * H, options.scale_b));
            }
        }
        let _ = offset;
        let mut written: HashMap<&str, (f64, i64)> = HashMap::new();
        let mut t = start + 60;
        while t <= end {
            let index = boundaries.iter().rposition(|(at, _)| *at <= t).unwrap_or(0);
            let (began, multiple) = boundaries[index];
            let reset = boundaries
                .get(index + 1)
                .map_or(began + 7 * 24 * H, |(at, _)| *at);
            let spent = |f: usize| cumulative(t, f) - cumulative(began, f);
            let week: f64 = K_WEEK
                .iter()
                .enumerate()
                .map(|(f, (_, k))| k * spent(f))
                .sum::<f64>()
                * multiple;
            let fable = K_FABLE * spent(1) * multiple;
            let jitter = (rng.next_u64() % 3) as i64 - 1;
            for (window, value) in [("Week", week.floor()), ("Fable", fable.round())] {
                let value = value.min(100.0);
                let reset = reset + jitter;
                if written.get(window) != Some(&(value, reset)) {
                    written.insert(window, (value, reset));
                    inputs
                        .readings
                        .push(reading(account, window, value, Some(reset), t));
                }
            }
            t += 300 + (rng.unit() * 600.0) as i64;
        }
    }
    inputs
}

fn true_k(family: &str) -> f64 {
    K_WEEK.iter().find(|(name, _)| *name == family).unwrap().1
}

#[test]
fn known_drain_rates_are_recovered_with_rounding_jitter_and_resets() {
    let fit = fit_all(&synthetic(&Options {
        scale_b: 1.0,
        sonnet: 1.0,
    }));
    let week = fit
        .window_fit("claude", "max_20x", "Week")
        .expect("a 20x week fit");
    assert_eq!(week.interval, IntervalKind::Bootstrap);
    // A 90% interval misses about one time in ten, so of the four measured
    // here (three on Week, Fable on its own window) at least three cover.
    let mut covered = Vec::new();
    for family in ["opus", "fable", "sonnet"] {
        let rate = week.family(family).unwrap();
        let k = rate.percent_per_usd;
        let truth = true_k(family);
        assert_eq!(rate.support, Support::Measured, "{family}: {k:?}");
        assert!(
            (k.value - truth).abs() / truth < 0.10,
            "{family}: {k:?} vs {truth}"
        );
        covered.push((family, k, k.contains(truth)));
    }
    // A trickle of Haiku cannot be measured and falls back, saying so.
    let haiku = week.family("haiku").unwrap();
    assert!(
        haiku.support.label().starts_with("pooled("),
        "{:?}",
        haiku.support
    );

    // The Fable-scoped window counts only Fable, at its own rate, and an
    // out-of-scope term fitted beside it comes back about zero.
    let scoped = fit.window_fit("claude", "max_20x", "Fable").unwrap();
    assert_eq!(
        scoped
            .families
            .iter()
            .map(|f| f.family.as_str())
            .collect::<Vec<_>>(),
        ["fable"]
    );
    let k = scoped.families[0].percent_per_usd;
    assert!((k.value - K_FABLE).abs() / K_FABLE < 0.10, "{k:?}");
    covered.push(("fable-scoped", k, k.contains(K_FABLE)));
    assert!(
        covered.iter().filter(|(_, _, covers)| *covers).count() >= 3,
        "{covered:?}"
    );
    let scope = scoped.scope_check.as_ref().unwrap();
    assert!(scope.percent_per_usd.value < 0.003, "{scope:?}");

    // The 5x prefix window is its own plan's, never pooled with 20x.
    let prefix = fit.window_fit("claude", "max_5x", "Week").unwrap();
    assert_eq!(prefix.instances, 1);
    assert_eq!(prefix.interval, IntervalKind::SampleLevel);
    assert!(week.instances >= 14, "{}", week.instances);

    // Held out, the latest instance per account is predicted closely.
    let validation = week.validation.as_ref().unwrap();
    assert_eq!(validation.rows.len(), 2);
    assert!(validation.end_mae < 2.0, "{validation:?}");
    // Every account drains alike, so none gets a scale.
    assert!(week.account_scales.iter().all(|scale| scale.served == 1.0));
    // The component check sees list ratios: cache reads count.
    let check = week.component_check.as_ref().unwrap();
    assert_eq!(check.family, "opus");
    let cache_read = check.fitted[1];
    assert!(
        cache_read.contains(check.list_implied[1])
            || (cache_read.value - check.list_implied[1]).abs() / check.list_implied[1] < 0.2,
        "{check:?}"
    );
    assert!(!week.stability.is_empty());
}

#[test]
fn an_account_that_drains_faster_is_found_and_served_its_scale() {
    let fit = fit_all(&synthetic(&Options {
        scale_b: 1.3,
        sonnet: 1.0,
    }));
    let week = fit.window_fit("claude", "max_20x", "Week").unwrap();
    let scale = |account: &str| {
        week.account_scales
            .iter()
            .find(|scale| scale.account == account)
            .unwrap()
            .clone()
    };
    let (a, b) = (scale("a"), scale("b"));
    assert!(a.scale.value < 1.0 && b.scale.value > 1.0, "{a:?} {b:?}");
    assert!(a.served != 1.0 && b.served != 1.0, "{a:?} {b:?}");
    let ratio = b.served / a.served;
    assert!((ratio - 1.3).abs() < 0.08, "{ratio}");
    // Per-account rates predict the holdout better than pooled ones.
    let validation = week.validation.as_ref().unwrap();
    assert!(
        validation.end_mae_per_account <= validation.end_mae,
        "{validation:?}"
    );
}

#[test]
fn unsupported_families_pool_and_a_plan_without_instances_takes_a_prior() {
    let fit = fit_all(&synthetic(&Options {
        scale_b: 1.0,
        sonnet: 0.01,
    }));
    let week = fit.window_fit("claude", "max_20x", "Week").unwrap();
    for family in ["sonnet", "haiku"] {
        let label = week.family(family).unwrap().support.label();
        assert!(label.starts_with("pooled("), "{family}: {label}");
    }
    assert_eq!(week.family("opus").unwrap().support, Support::Measured);

    // Claude Pro has no instance: it borrows Max 20x at 1/20 the capacity.
    let pro = AccountRef {
        id: "c",
        provider: "claude",
        plan: Some("pro"),
    };
    let rate = fit.rate(&pro, "Week", "opus").unwrap();
    assert_eq!(rate.support.label(), "prior(max_20x × 1/20)");
    let k = week.family("opus").unwrap().percent_per_usd.value;
    assert!((rate.percent_per_usd.value - 20.0 * k).abs() < 1e-9);
    assert_eq!(rate.instances, week.family("opus").unwrap().instances);
}

#[test]
fn the_queries_invert_one_another_and_price_a_percent() {
    let fit = fit_all(&synthetic(&Options {
        scale_b: 1.0,
        sonnet: 1.0,
    }));
    let account = AccountRef {
        id: "a",
        provider: "claude",
        plan: Some("max_20x"),
    };
    let latest = LatestReading {
        window_name: "Week",
        scope_target: None,
        used_pct: 40.0,
    };
    let mix = priced(
        Components {
            input: 1_000.0,
            cache_read: 10e6,
            cache_write: 0.2e6,
            output: 0.05e6,
        },
        &rates("opus"),
    );
    let capacity = fit
        .remaining(&account, &latest, "opus", Some(&mix))
        .unwrap();
    let k = capacity.rate.percent_per_usd.value;
    assert!((capacity.usd.value - 60.0 / k).abs() < 1e-9);
    assert!(capacity.usd.low < capacity.usd.value && capacity.usd.value < capacity.usd.high);
    let tokens = capacity.tokens.unwrap();
    assert!((tokens.value - capacity.usd.value / mix.usd_per_token().unwrap()).abs() < 1e-6);

    // Spending exactly what remains, at the mix, costs what remains.
    let spend = mix.tokens.scaled(tokens.value / mix.tokens.total());
    let usd = |tokens: &Components| rates("opus").cost(tokens);
    let percent = fit
        .percent_for(&account, "Week", "opus", usd(&spend))
        .unwrap();
    assert!((percent.value - 60.0).abs() < 1e-6, "{percent:?}");
    assert!(percent.low <= 60.0 && 60.0 <= percent.high);

    // Whether it fits: a sliver does, ten windows' worth does not.
    let windows = [latest];
    let small = fit
        .fits(&account, "opus", usd(&mix.tokens.scaled(0.01)), &windows)
        .unwrap();
    let huge = fit
        .fits(&account, "opus", usd(&spend.scaled(10.0)), &windows)
        .unwrap();
    assert!(small > 0.99 && huge < 0.01, "{small} {huge}");

    // $200 a month on a weekly window is about $0.46 per 1%.
    let per_percent = dollars_per_percent(200.0, 7 * 24 * 60);
    assert!((per_percent - 0.46).abs() < 0.005, "{per_percent}");
    let per_token = fit
        .dollars_per_token(&account, "opus", 200.0, "Week", &mix)
        .unwrap();
    let expected = per_percent * k * mix.usd_per_token().unwrap();
    assert!((per_token.value - expected).abs() < 1e-15, "{per_token:?}");
    let per_dollar = fit
        .percent_per_list_dollar(&account, "Week", "opus")
        .unwrap();
    assert_eq!(per_dollar.value, k);

    // The Fable-scoped window never constrains Opus, and a family spent only
    // on Claude has no rate on another provider's account.
    assert!(fit.rate(&account, "Fable", "opus").is_none());
    let codex = AccountRef {
        id: "x",
        provider: "codex",
        plan: Some("pro_200"),
    };
    assert!(fit.rate(&codex, "Week", "opus").is_none());
    assert!(fit.rate(&account, "Fable", "claude-fable-5-2").is_some());
}
