//! What the window-drain fit reads (`cairn_models_types::drain`): every
//! account's usage-window readings, the account-attributed token rollup
//! priced at list rates per component, and the plan periods.
//!
//! Rollup rows are grouped by (account, hour, backend, model) and priced
//! through [`crate::pricing`], each component at its prompt-length tier, so
//! Haiku's long-prompt rate is respected. Thinking tokens are a subset of
//! output tokens (both providers report reasoning inside output), so they are
//! never priced again. A row whose model has no price, or whose model's maker
//! is not the account's provider (an `opus`-keyed row on a Codex account), is
//! left out and counted in [`DrainInputs::dropped`]. Rows attributed to no
//! account (folded before the rollup carried one) are not read at all.

use std::collections::{BTreeMap, HashMap};

use cairn_db::storage::usage_readings::list_usage_readings;
use cairn_db::storage::{DbResult, LocalDb, RowExt};
use cairn_models_types::drain::{
    family_of, Components, DrainInputs, DroppedSpend, Mix, PlanSpan, Rates, Reading, Spend,
    Watermark,
};
use cairn_models_types::identity;

use crate::pricing::{self, TieredUsage};
use crate::subscriptions::plan_periods;

/// The backend whose subscriptions serve a maker's models.
fn maker_backend(maker: &str) -> Option<&'static str> {
    match maker {
        "anthropic" => Some("claude"),
        "openai" => Some("codex"),
        _ => None,
    }
}

/// How far the fit's inputs currently reach.
pub async fn drain_watermark(db: &LocalDb) -> DbResult<Watermark> {
    let reading_id = db
        .query_opt_i64("SELECT MAX(id) FROM usage_window_readings", ())
        .await?
        .unwrap_or(0);
    let rollup = db
        .query_opt(
            "SELECT COALESCE(MAX(bucket_start), 0), COALESCE(SUM(token_event_count), 0)
             FROM token_rollup WHERE account_id IS NOT NULL",
            (),
            |row| Ok((row.i64(0)?, row.i64(1)?)),
        )
        .await?
        .unwrap_or((0, 0));
    Ok(Watermark {
        reading_id,
        rollup_bucket: rollup.0,
        rollup_events: rollup.1,
    })
}

struct RollupRow {
    account: String,
    hour: i64,
    backend: String,
    model: Option<String>,
    usage: TieredUsage,
}

fn components(row: &impl RowExt, at: usize) -> DbResult<pricing::Components> {
    Ok(pricing::Components {
        input: row.i64(at)?,
        cache_read: row.i64(at + 1)?,
        cache_create: row.i64(at + 2)?,
        output: row.i64(at + 3)?,
    })
}

async fn rollup_rows(db: &LocalDb) -> DbResult<Vec<RollupRow>> {
    db.query_all(
        "SELECT account_id, bucket_start, backend, model,
                SUM(input_tokens), SUM(cache_read_tokens), SUM(cache_create_tokens),
                SUM(output_tokens),
                SUM(long_input_tokens), SUM(long_cache_read_tokens),
                SUM(long_cache_create_tokens), SUM(long_output_tokens)
         FROM token_rollup
         WHERE account_id IS NOT NULL AND token_event_count > 0
         GROUP BY account_id, bucket_start, backend, model
         ORDER BY account_id, bucket_start",
        (),
        |row| {
            Ok(RollupRow {
                account: row.text(0)?,
                hour: row.i64(1)?,
                backend: row.text(2)?,
                model: row.opt_text(3)?,
                usage: TieredUsage {
                    all: components(row, 4)?,
                    long_prompt: components(row, 8)?,
                },
            })
        },
    )
    .await
}

/// A model a caller named, resolved to the generation it is priced as.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ListPrice {
    pub canonical: &'static str,
    pub rates: Rates,
}

/// The list price of a model a caller names: a concrete id or a bare alias,
/// resolved through the identity table exactly as stored usage is (a bare
/// Claude alias prices as the generation it runs), at its base tier. `None`
/// for a model with no canonical id or no price row, so an unknown version
/// is refused rather than priced as some other generation of its family.
pub fn list_price(model: &str) -> Option<ListPrice> {
    // Codex first: `canonical_model` falls back to Claude's offers itself.
    let canonical = pricing::canonical_model("codex", model)?;
    let [input, cache_read, cache_write, output] = pricing::base_rates(canonical)?;
    Some(ListPrice {
        canonical,
        rates: Rates {
            input,
            cache_read,
            cache_write,
            output,
        },
    })
}

/// How far back [`current_readings`] reads before each account's oldest
/// latest reading: the longest window (a week) and the day its burn is read
/// over.
const CURRENT_HORIZON_SECS: i64 = 8 * 24 * 3600;

fn reading_of(row: cairn_db::storage::usage_readings::UsageWindowReadingRow) -> Reading {
    Reading {
        provider: row.provider,
        account: row.account_id,
        window_id: row.window_id,
        window_name: row.window_name,
        scope_target: row.scope_target,
        used_pct: 100.0 - row.remaining_percent,
        resets_at: row.resets_at,
        duration_mins: row.window_duration_mins,
        captured_at: row.captured_at,
    }
}

/// The readings that say what each window holds now: per account, everything
/// from [`CURRENT_HORIZON_SECS`] before the oldest of its series' latest
/// readings, which spans every series' current instance. This is read live on
/// each capacity read, apart from the fit, so a reading that lands between
/// refits is served at once.
pub async fn current_readings(db: &LocalDb) -> DbResult<Vec<Reading>> {
    let horizons = db
        .query_all(
            "SELECT account_id, MIN(latest) FROM (
                 SELECT account_id, MAX(captured_at) AS latest
                 FROM usage_window_readings GROUP BY account_id, window_id
             ) GROUP BY account_id ORDER BY account_id",
            (),
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await?;
    let mut readings = Vec::new();
    for (account, latest) in horizons {
        let since = latest - CURRENT_HORIZON_SECS;
        readings.extend(
            list_usage_readings(db, &account, None, Some(since))
                .await?
                .into_iter()
                .map(reading_of),
        );
    }
    Ok(readings)
}

fn token_count(usage: &TieredUsage) -> f64 {
    (usage.all.prompt() + usage.all.output) as f64
}

/// Read the fit's inputs as they stand at `now`.
pub async fn drain_inputs(db: &LocalDb, now: i64) -> DbResult<DrainInputs> {
    let plans: Vec<PlanSpan> = plan_periods(db)
        .await?
        .into_iter()
        .map(|period| PlanSpan {
            account: period.account_id,
            provider: period.provider,
            plan: period.plan_key,
            started_at: period.started_at,
            ended_at: period.ended_at,
        })
        .collect();
    let accounts = db
        .query_all(
            "SELECT DISTINCT account_id FROM usage_window_readings ORDER BY account_id",
            (),
            |row| row.text(0),
        )
        .await?;
    let mut readings = Vec::new();
    for account in &accounts {
        readings.extend(
            list_usage_readings(db, account, None, None)
                .await?
                .into_iter()
                .map(reading_of),
        );
    }
    let mut provider_of: HashMap<String, String> = HashMap::new();
    for reading in &readings {
        provider_of
            .entry(reading.account.clone())
            .or_insert_with(|| reading.provider.clone());
    }
    for span in &plans {
        provider_of
            .entry(span.account.clone())
            .or_insert_with(|| span.provider.clone());
    }

    let mut spend = Vec::new();
    let mut dropped: BTreeMap<(String, String, &'static str), f64> = BTreeMap::new();
    // Each family's newest canonical model, whose rates price a named spend.
    let mut newest: BTreeMap<String, (i64, &'static str)> = BTreeMap::new();
    for row in rollup_rows(db).await? {
        let model = row.model.clone().unwrap_or_default();
        let mut drop = |reason: &'static str| {
            *dropped
                .entry((row.account.clone(), model.clone(), reason))
                .or_default() += token_count(&row.usage);
        };
        let Some(provider) = provider_of.get(&row.account) else {
            drop("account has no readings or plan");
            continue;
        };
        let Some(canonical) = pricing::canonical_model(&row.backend, &model) else {
            drop("unpriced model");
            continue;
        };
        let maker = identity::canonical(canonical).and_then(|model| model.maker.as_deref());
        if maker.and_then(maker_backend) != Some(provider.as_str()) {
            drop("model's maker is not the account's provider");
            continue;
        }
        let (Some(family), Some(usd)) = (
            family_of(canonical),
            pricing::component_costs_usd(&row.backend, row.model.as_deref(), &row.usage),
        ) else {
            drop("unpriced model");
            continue;
        };
        let entry = newest
            .entry(family.clone())
            .or_insert((row.hour, canonical));
        if row.hour >= entry.0 {
            *entry = (row.hour, canonical);
        }
        let all = row.usage.all;
        spend.push(Spend {
            account: row.account,
            hour: row.hour,
            family,
            mix: Mix {
                tokens: Components {
                    input: all.input as f64,
                    cache_read: all.cache_read as f64,
                    cache_write: all.cache_create as f64,
                    output: all.output as f64,
                },
                usd: Components::from_array(usd),
            },
        });
    }
    let rates = newest
        .into_iter()
        .filter_map(|(family, (_, canonical))| {
            let [input, cache_read, cache_write, output] = pricing::base_rates(canonical)?;
            Some((
                family,
                Rates {
                    input,
                    cache_read,
                    cache_write,
                    output,
                },
            ))
        })
        .collect();
    Ok(DrainInputs {
        readings,
        spend,
        plans,
        rates,
        dropped: dropped
            .into_iter()
            .map(|((account, model, reason), tokens)| DroppedSpend {
                account,
                model,
                reason: reason.to_string(),
                tokens,
            })
            .collect(),
        now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::storage::usage_readings::{record_usage_readings, UsageReadingInput};

    async fn seeded() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.keep().join("drain-inputs.db"))
            .await
            .unwrap();
        db.execute_script(
            "
            INSERT INTO subscription_periods
                (id, account_id, provider, plan_key, plan_label, account_label, started_at, ended_at)
             VALUES ('p1', 'acct-c', 'claude', 'max_5x', 'Max 5x', 'c', 0, 5000),
                    ('p2', 'acct-c', 'claude', 'max_20x', 'Max 20x', 'c', 5000, NULL),
                    ('p3', 'acct-x', 'codex', 'pro_200', 'Pro 200', 'x', 0, NULL);
            INSERT INTO token_rollup (id, bucket_start, run_id, backend, model, input_tokens,
                cache_read_tokens, cache_create_tokens, output_tokens, thinking_tokens,
                token_event_count, long_input_tokens, long_cache_read_tokens,
                long_cache_create_tokens, long_output_tokens, account_id)
             VALUES
                ('r1', 7200, 'run', 'claude', 'claude-opus-5-5', 1000, 1000000, 10000, 2000, 500, 3, 0, 0, 0, 0, 'acct-c'),
                ('r2', 7200, 'run2', 'claude', 'opus', 0, 1000000, 0, 0, 0, 1, 0, 0, 0, 0, 'acct-c'),
                ('r3', 7200, 'run', 'claude', 'claude-haiku-5-5', 1, 200000, 0, 100, 0, 1, 1, 200000, 0, 100, 'acct-c'),
                ('r4', 10800, 'run', 'codex', 'opus', 5, 50, 0, 7, 0, 1, 0, 0, 0, 0, 'acct-x'),
                ('r5', 10800, 'run', 'codex', 'gpt-6.1-sol', 10, 100, 0, 20, 0, 1, 0, 0, 0, 0, 'acct-x'),
                ('r6', 10800, 'run', 'claude', 'mystery-model', 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 'acct-c'),
                ('r7', 10800, 'run', 'claude', 'claude-opus-5-5', 9, 9, 9, 9, 0, 1, 0, 0, 0, 0, NULL);
            ",
        )
        .await
        .unwrap();
        record_usage_readings(
            &db,
            "claude",
            "acct-c",
            "claude_usage_oauth",
            6000,
            vec![UsageReadingInput {
                window_id: "weekly_all".into(),
                window_name: "Week".into(),
                scope: "weekly".into(),
                scope_target: None,
                remaining_percent: 70.0,
                window_duration_mins: Some(10080),
                resets_at: Some(600_000),
            }],
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn rollup_rows_are_priced_per_component_and_mismatches_are_counted() {
        let db = seeded().await;
        let inputs = drain_inputs(&db, 9_000).await.unwrap();

        // Readings arrive as used percent.
        assert_eq!(inputs.readings.len(), 1);
        assert_eq!(inputs.readings[0].used_pct, 30.0);
        assert_eq!(inputs.readings[0].duration_mins, Some(10080));

        // The opus-keyed alias row and the concrete one are one family, each
        // priced exactly as the price table prices it.
        let opus: Vec<&Spend> = inputs.spend.iter().filter(|s| s.family == "opus").collect();
        assert_eq!(opus.len(), 2);
        let concrete = TieredUsage {
            all: pricing::Components {
                input: 1000,
                cache_read: 1_000_000,
                cache_create: 10_000,
                output: 2000,
            },
            long_prompt: pricing::Components::default(),
        };
        let expected = pricing::cost_usd("claude", Some("claude-opus-5-5"), &concrete);
        let priced = opus
            .iter()
            .find(|s| s.mix.tokens.input == 1000.0)
            .unwrap()
            .mix;
        assert!((priced.usd.total() - expected).abs() < 1e-12);
        // Thinking tokens are inside output, never added.
        assert_eq!(priced.tokens.output, 2000.0);
        // Cache reads cost $0.20/M on Opus 5.5.
        assert!((priced.usd.cache_read - 0.2).abs() < 1e-12);

        // Haiku's long-prompt part is priced at its long tier.
        let haiku = inputs.spend.iter().find(|s| s.family == "haiku").unwrap();
        let long = TieredUsage {
            all: pricing::Components {
                input: 1,
                cache_read: 200_000,
                cache_create: 0,
                output: 100,
            },
            long_prompt: pricing::Components {
                input: 1,
                cache_read: 200_000,
                cache_create: 0,
                output: 100,
            },
        };
        let expected = pricing::cost_usd("claude", Some("claude-haiku-5-5"), &long);
        assert!((haiku.mix.usd.total() - expected).abs() < 1e-12);

        // An Opus row on the Codex account and an unknown model are left out
        // and counted; an unattributed row is not read at all.
        let reasons: Vec<(&str, &str, f64)> = inputs
            .dropped
            .iter()
            .map(|d| (d.account.as_str(), d.reason.as_str(), d.tokens))
            .collect();
        assert!(reasons.contains(&(
            "acct-x",
            "model's maker is not the account's provider",
            62.0
        )));
        assert!(reasons.contains(&("acct-c", "unpriced model", 3.0)));
        assert_eq!(inputs.dropped.len(), 2);
        assert_eq!(
            inputs
                .spend
                .iter()
                .filter(|s| s.account == "acct-x")
                .count(),
            1
        );

        // Plans come from the periods; rates from each family's newest model.
        assert_eq!(inputs.plans.len(), 3);
        let open = inputs
            .plans
            .iter()
            .find(|span| span.account == "acct-c" && span.ended_at.is_none())
            .unwrap();
        assert_eq!(open.plan.as_deref(), Some("max_20x"));
        assert_eq!(open.started_at, 5000);
        assert_eq!(inputs.rates["opus"].cache_read, 0.2);
        assert_eq!(inputs.rates["gpt-sol"].output, 10.0);

        let watermark = drain_watermark(&db).await.unwrap();
        assert_eq!(watermark.rollup_bucket, 10_800);
        assert!(watermark.reading_id > 0);
    }

    #[test]
    fn a_named_model_is_priced_as_its_own_generation_and_unknown_ones_are_refused() {
        let current = list_price("claude-opus-5-5").unwrap();
        let legacy = list_price("claude-opus-4-1").unwrap();
        assert_eq!(current.canonical, "claude-opus-5.5");
        assert_eq!(legacy.canonical, "claude-opus-4.1");
        // One family, two prices: each generation keeps its own row.
        assert_eq!(family_of(current.canonical), family_of(legacy.canonical));
        assert_eq!(
            [
                legacy.rates.input,
                legacy.rates.cache_read,
                legacy.rates.cache_write,
                legacy.rates.output
            ],
            pricing::base_rates("claude-opus-4.1").unwrap()
        );
        assert_ne!(current.rates, legacy.rates);
        // A bare alias is the generation it runs; Codex models resolve too.
        assert_eq!(list_price("opus").unwrap(), current);
        assert_eq!(list_price("gpt-6.1-sol").unwrap().canonical, "gpt-6.1-sol");
        // An unknown version is refused, not priced as another Opus.
        assert!(list_price("claude-opus-99-7").is_none());
        assert!(list_price("4.5").is_none());
    }

    #[tokio::test]
    async fn current_readings_reach_back_over_each_series_current_instance() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.keep().join("current.db"))
            .await
            .unwrap();
        let week = |remaining_percent: f64| UsageReadingInput {
            window_id: "weekly_all".into(),
            window_name: "Week".into(),
            scope: "weekly".into(),
            scope_target: None,
            remaining_percent,
            window_duration_mins: Some(10080),
            resets_at: Some(10_000_000),
        };
        let day = 24 * 3600;
        for (at, remaining) in [
            (0, 99.0),
            (20 * day, 80.0),
            (25 * day, 70.0),
            (26 * day, 60.0),
        ] {
            record_usage_readings(&db, "claude", "acct-c", "oauth", at, vec![week(remaining)])
                .await
                .unwrap();
        }
        let readings = current_readings(&db).await.unwrap();
        // Eight days before the latest: the reading from day 0 is left behind.
        assert_eq!(
            readings.iter().map(|r| r.used_pct).collect::<Vec<_>>(),
            [20.0, 30.0, 40.0]
        );
        let current = cairn_models_types::drain::current_windows(&readings);
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].used_pct, 40.0);
    }
}
