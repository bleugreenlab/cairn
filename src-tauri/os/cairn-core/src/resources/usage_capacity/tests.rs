use super::*;
use cairn_common::query::parse_query_params;
use cairn_models::types::drain::{
    AccountMix, AccountPlan, CurrentWindow, DrainFit, Estimate, FamilyRate, IntervalKind, Mix,
    Rates, Support, WindowFit,
};

const NOW: i64 = 1_795_000_000;
const H: i64 = 3600;

fn query(raw: &str) -> Result<CapacityQuery, String> {
    capacity_query(&parse_query_params(raw).unwrap()).expect("a capacity read")
}

#[test]
fn a_bare_capacity_key_and_the_inverse_keys_parse_from_a_raw_query() {
    assert_eq!(query("capacity").unwrap().view, CapacityView::Accounts);
    assert!(query("capacity&format=json").unwrap().json);
    assert_eq!(query("capacity=fit").unwrap().view, CapacityView::Fit);
    let inverse =
        query("capacity&model=opus&cacheRead=400000000&output=2000000&cacheWrite=10").unwrap();
    let opus = list_price("opus").unwrap().canonical;
    assert_eq!(inverse.model.as_deref(), Some(opus));
    let CapacityView::Inverse(spend) = inverse.view else {
        panic!("{inverse:?}");
    };
    assert_eq!(spend.cache_read, 400_000_000.0);
    assert_eq!(spend.output, 2_000_000.0);
    assert_eq!(spend.cache_write, 10.0);
    assert_eq!(spend.input, 0.0);
    let one = query("capacity&account=acc_1234").unwrap();
    assert_eq!(one.account.as_deref(), Some("acc_1234"));
    // Not a capacity read at all.
    assert!(capacity_query(&parse_query_params("history=acct").unwrap()).is_none());
}

#[test]
fn capacity_reads_refuse_what_they_cannot_answer() {
    for (raw, says) in [
        ("capacity&history=acct", "separate reads"),
        (
            "capacity&window=Week",
            "Unsupported query parameter 'window'",
        ),
        ("capacity=1", "capacity takes no value"),
        ("capacity&output=5", "names the model"),
        ("capacity&model=opus&output=lots", "whole number of tokens"),
        ("capacity=fit&account=acc_1", "reads only format"),
        ("capacity&model=4.5", "has no list price"),
        ("capacity&model=claude-opus-99-7", "has no list price"),
        ("capacity&format=csv", "Unknown format"),
    ] {
        let error = query(raw).unwrap_err();
        assert!(error.contains(says), "{raw}: {error}");
    }
}

fn rate(family: &str, k: f64, support: Support, instances: usize) -> FamilyRate {
    FamilyRate {
        family: family.into(),
        percent_per_usd: Estimate {
            value: k,
            low: k * 0.9,
            high: k * 1.1,
        },
        support,
        instances,
        spend_usd: 1_000.0,
        mix: Mix::default(),
    }
}

fn window(account: &str, used_pct: f64, resets_at: i64) -> CurrentWindow {
    CurrentWindow {
        provider: "claude".into(),
        account: account.into(),
        window_id: "weekly_all".into(),
        window_name: "Week".into(),
        scope_target: None,
        used_pct,
        resets_at: Some(resets_at),
        captured_at: NOW - 60,
        burn_pct_per_hour: Some(0.75),
    }
}

fn plan(account: &str, plan: &str) -> AccountPlan {
    AccountPlan {
        account: account.into(),
        provider: "claude".into(),
        plan: Some(plan.into()),
    }
}

/// Account `a` on Max 20x has 60% of its week left; `b` is on Max 5x, which
/// has no instances, and its week reset an hour ago; `c` is blocked.
fn fixture() -> (DrainFit, Vec<CurrentWindow>, Vec<LiveAccount>) {
    let opus_mix = Mix {
        tokens: Components {
            cache_read: 1_000_000.0,
            ..Default::default()
        },
        usd: Components {
            cache_read: 0.2,
            ..Default::default()
        },
    };
    let fit = DrainFit {
        fitted_at: NOW - 600,
        windows: vec![WindowFit {
            provider: "claude".into(),
            plan: "max_20x".into(),
            window_name: "Week".into(),
            scope_target: None,
            duration_mins: 7 * 24 * 60,
            instances: 12,
            accounts: 2,
            samples: 900,
            interval: IntervalKind::Bootstrap,
            families: vec![
                rate("opus", 0.05, Support::Measured, 12),
                rate(
                    "haiku",
                    0.06,
                    Support::Pooled {
                        families: vec!["sonnet".into(), "haiku".into()],
                    },
                    5,
                ),
            ],
            all: Estimate {
                value: 0.055,
                low: 0.05,
                high: 0.06,
            },
            residual_sd: 0.5,
            account_scales: Vec::new(),
            validation: None,
            component_check: None,
            stability: Vec::new(),
            scope_check: None,
        }],
        plans: vec![
            plan("a", "max_20x"),
            plan("b", "max_5x"),
            plan("c", "max_20x"),
        ],
        mixes: vec![AccountMix {
            account: "a".into(),
            family: "opus".into(),
            recent: Some(opus_mix),
            all_time: opus_mix,
        }],
        rates: [(
            "opus".to_string(),
            Rates {
                input: 4.0,
                cache_read: 0.2,
                cache_write: 5.0,
                output: 20.0,
            },
        )]
        .into(),
        families: [
            ("opus".to_string(), "claude".to_string()),
            ("haiku".to_string(), "claude".to_string()),
        ]
        .into(),
        ..Default::default()
    };
    let current = vec![
        window("a", 40.0, NOW + 10 * H),
        window("b", 90.0, NOW - H),
        window("c", 99.0, NOW + 10 * H),
    ];
    let accounts = vec![
        LiveAccount {
            id: "a".into(),
            name: "a".into(),
            blocked: None,
        },
        LiveAccount {
            id: "b".into(),
            name: "Work".into(),
            blocked: None,
        },
        LiveAccount {
            id: "c".into(),
            name: "c".into(),
            blocked: Some("credits exhausted".into()),
        },
    ];
    (fit, current, accounts)
}

#[test]
fn each_account_reads_what_its_windows_hold_per_model_with_provenance() {
    let (fit, current, accounts) = fixture();
    let page = render(&query("capacity").unwrap(), &fit, &current, &accounts, NOW);
    assert!(page.starts_with("# Usage capacity"), "{page}");
    // 60% left at 0.05 %/$ is $1,200 of Opus, at its recent mix 6B tokens.
    assert!(page.contains("| opus | Week (binds) | $1,200 ["), "{page}");
    assert!(page.contains("| 6.00B ["), "{page}");
    assert!(page.contains("measured | 12 |"), "{page}");
    assert!(page.contains("pooled(sonnet+haiku) | 5 |"), "{page}");
    // Max 5x has no instance: the 20x rate, scaled, labelled as a prior; its
    // week reset an hour ago, so it reads as fresh.
    assert!(page.contains("## `b` Work — claude · max_5x"), "{page}");
    assert!(page.contains("prior(max_20x × 5/20)"), "{page}");
    assert!(page.contains("| Week | 0% (assumed fresh) |"), "{page}");
    // A blocked account is shown, marked, and given nothing.
    assert!(
        page.contains("Blocked (credits exhausted): no capacity"),
        "{page}"
    );
    assert!(page.contains("0.750%/h"), "{page}");

    let only = render(
        &query("capacity&account=a&model=opus").unwrap(),
        &fit,
        &current,
        &accounts,
        NOW,
    );
    assert!(
        only.contains("## `a`") && !only.contains("## `b`"),
        "{only}"
    );
    assert!(!only.contains("haiku"), "{only}");
}

#[test]
fn the_json_form_carries_the_same_numbers_for_programs() {
    let (fit, current, accounts) = fixture();
    let body = render(
        &query("capacity&format=json").unwrap(),
        &fit,
        &current,
        &accounts,
        NOW,
    );
    let parsed: Value = serde_json::from_str(&body).unwrap();
    let accounts = parsed["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 3);
    let a = &accounts[0];
    assert_eq!(a["plan"], "max_20x");
    let opus = &a["models"][0];
    assert_eq!(opus["model"], "opus");
    assert_eq!(opus["binding"], "Week");
    let week = &opus["windows"][0];
    assert!((week["usd"]["value"].as_f64().unwrap() - 1_200.0).abs() < 1e-6);
    assert_eq!(week["support"], "measured");
    assert_eq!(week["instances"], 12);
    assert!(a["windows"][0]["hoursToReset"].as_f64().unwrap() > 9.9);
    let b = &accounts[1];
    assert_eq!(b["windows"][0]["assumedFresh"], true);
    assert_eq!(
        b["models"][0]["windows"][0]["support"],
        "prior(max_20x × 5/20)"
    );
    let c = &accounts[2];
    assert_eq!(c["blocked"], "credits exhausted");
    assert!(c["models"].as_array().unwrap().is_empty());
}

#[test]
fn the_inverse_prices_a_spend_against_every_window_it_counts_against() {
    let (fit, current, accounts) = fixture();
    // 100M cache reads of Opus: $20 at list, 1% of a 20x week.
    let raw = "capacity&model=claude-opus-5-5&cacheRead=100000000";
    let page = render(&query(raw).unwrap(), &fit, &current, &accounts, NOW);
    assert!(page.contains("$20 at list price"), "{page}");
    assert!(
        page.contains("| Week | 1.00 [0.900, 1.10] | 60.0% | measured | 12 |"),
        "{page}"
    );
    assert!(
        page.contains("Blocked (credits exhausted): nothing fits"),
        "{page}"
    );
    let body = render(
        &query(&format!("{raw}&format=json")).unwrap(),
        &fit,
        &current,
        &accounts,
        NOW,
    );
    let parsed: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["family"], "opus");
    let a = &parsed["accounts"][0];
    assert!(a["fits"].as_f64().unwrap() > 0.999, "{a}");
    assert_eq!(parsed["accounts"][2]["fits"], 0.0);

    // A priced model no live account runs: nobody to answer for.
    let none = render(
        &query("capacity&model=gpt-6.1-sol&output=5").unwrap(),
        &fit,
        &current,
        &accounts,
        NOW,
    );
    assert!(
        none.contains("No live account with readings runs gpt-sol"),
        "{none}"
    );
}

#[test]
fn the_inverse_prices_each_generation_of_a_family_at_its_own_list_price() {
    let (fit, current, accounts) = fixture();
    let list_usd = |model: &str| {
        let raw = format!("capacity&model={model}&cacheRead=100000000&output=1000000&format=json");
        let body = render(&query(&raw).unwrap(), &fit, &current, &accounts, NOW);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["family"], "opus", "{parsed}");
        parsed["listUsd"].as_f64().unwrap()
    };
    let spend = Components {
        cache_read: 100_000_000.0,
        output: 1_000_000.0,
        ..Default::default()
    };
    let current_usd = list_usd("claude-opus-5-5");
    let legacy_usd = list_usd("claude-opus-4-1");
    assert_eq!(
        current_usd,
        list_price("claude-opus-5-5").unwrap().rates.cost(&spend)
    );
    assert_eq!(
        legacy_usd,
        list_price("claude-opus-4-1").unwrap().rates.cost(&spend)
    );
    // One family's drain rate, but each generation's own dollars.
    assert!(legacy_usd > current_usd, "{legacy_usd} vs {current_usd}");
    // The bare alias is the generation it runs.
    assert_eq!(list_usd("opus"), current_usd);
}

#[test]
fn the_fit_view_is_the_fit_report() {
    let (fit, current, accounts) = fixture();
    let page = render(
        &query("capacity=fit").unwrap(),
        &fit,
        &current,
        &accounts,
        NOW,
    );
    assert!(page.starts_with("# Window drain fit"), "{page}");
    assert!(
        page.contains("## claude · max_20x · Week (all models, 7 d)"),
        "{page}"
    );
    assert!(page.contains("pooled(sonnet+haiku)"), "{page}");
    let json = render(
        &query("capacity=fit&format=json").unwrap(),
        &fit,
        &current,
        &accounts,
        NOW,
    );
    let parsed: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        parsed["windows"][0]["families"][1]["support"]["kind"],
        "pooled"
    );
}
