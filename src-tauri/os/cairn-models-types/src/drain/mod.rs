//! Subscription window drain: how fast each model family empties a usage
//! window, measured from the recorded readings and the tokens attributed to
//! each account, and what that says about the capacity left.
//!
//! # The model
//!
//! For one (provider, plan, window kind) — `Week`, `Session`, or a
//! model-scoped window such as `Fable` — the used share of window instance `i`
//! at time `t` is
//!
//! `used%(i, t) = a_i + Σ_f k_f · s_acct · D_f(i, t) + ε`
//!
//! where `D_f(i, t)` is the list-price dollars family `f` spent on the
//! instance's account from the instance's first reading to `t`, counting only
//! families the window's scope admits; `a_i` is a free per-instance intercept
//! (it absorbs the first reading's rounding and any spend before attribution
//! began); `k_f ≥ 0` is the drain rate in percent of the window per list
//! dollar; and `s_acct` is a per-account scale, served only where the data
//! shows one. List prices only fix the ratio between a family's components;
//! the measured quantity is `k_f`, and the fit report checks whether the
//! components keep their list ratios ([`ComponentCheck`]).
//!
//! [`fit::fit`] turns [`DrainInputs`] into a [`DrainFit`]; the queries on
//! [`DrainFit`] answer what remains ([`DrainFit::remaining`]), what a spend
//! would cost ([`DrainFit::percent_for`]), whether it fits
//! ([`DrainFit::fits`]), and what a token is worth in subscription dollars
//! ([`DrainFit::dollars_per_token`]). Every served number carries a
//! [`Support`] label and the instance count behind it.
//!
//! A fit holds coefficients, which change slowly and are refitted on a
//! cadence. What a window holds right now is not part of it:
//! [`current_windows`] reads that from the latest readings whenever it is
//! asked. A spend a caller names is priced by the caller at the named
//! model's own list rates, because generations within a family are priced
//! differently. The queries take those list dollars, and the fit turns them
//! into percent at the family's rate.

pub mod fit;
pub mod report;

use std::collections::BTreeMap;

use serde::Serialize;

use crate::subscription::{plan_price, SubscriptionProvider};
use crate::usage::model_scope_matches;

pub use fit::{current_windows, fit};

/// The z value of a two-sided 90% interval.
pub const Z90: f64 = 1.644_853_6;

/// The variance of a reading rounded to a whole percent: uniform on ±0.5.
pub const ROUNDING_VARIANCE: f64 = 1.0 / 12.0;

/// Average days in a month, for turning a monthly plan price into a price per
/// window.
pub const DAYS_PER_MONTH: f64 = 30.44;

/// Token counts (or their list-price dollars) by billed component.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Components {
    pub input: f64,
    pub cache_read: f64,
    pub cache_write: f64,
    pub output: f64,
}

impl Components {
    /// Display names, in [`Self::as_array`] order.
    pub const NAMES: [&'static str; 4] = ["input", "cache read", "cache write", "output"];

    pub fn total(&self) -> f64 {
        self.input + self.cache_read + self.cache_write + self.output
    }

    pub fn as_array(&self) -> [f64; 4] {
        [self.input, self.cache_read, self.cache_write, self.output]
    }

    pub fn from_array(values: [f64; 4]) -> Self {
        Self {
            input: values[0],
            cache_read: values[1],
            cache_write: values[2],
            output: values[3],
        }
    }

    pub fn add(&mut self, other: &Self) {
        self.input += other.input;
        self.cache_read += other.cache_read;
        self.cache_write += other.cache_write;
        self.output += other.output;
    }

    pub fn scaled(&self, by: f64) -> Self {
        Self::from_array(self.as_array().map(|value| value * by))
    }
}

/// List rates in USD per million tokens.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rates {
    pub input: f64,
    pub cache_read: f64,
    pub cache_write: f64,
    pub output: f64,
}

impl Rates {
    /// The list-price USD of `tokens` at these rates.
    pub fn cost(&self, tokens: &Components) -> f64 {
        (tokens.input * self.input
            + tokens.cache_read * self.cache_read
            + tokens.cache_write * self.cache_write
            + tokens.output * self.output)
            / 1_000_000.0
    }
}

/// Tokens and the list-price dollars they cost, by component.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Mix {
    pub tokens: Components,
    pub usd: Components,
}

impl Mix {
    pub fn add(&mut self, other: &Self) {
        self.tokens.add(&other.tokens);
        self.usd.add(&other.usd);
    }

    /// List dollars per token at this mix, `None` with no tokens.
    pub fn usd_per_token(&self) -> Option<f64> {
        let tokens = self.tokens.total();
        (tokens > 0.0).then(|| self.usd.total() / tokens)
    }
}

/// The family a model drains a window as: its canonical name without the
/// maker prefix or any version, so every generation of one tier shares it
/// (`claude-opus-5-5`, `claude-opus-5.5` and `opus` are `opus`; `gpt-6.1-sol`
/// is `gpt-sol`). Dates and numeric parts are dropped, as is a `[1m]`
/// context suffix.
pub fn family_of(model: &str) -> Option<String> {
    let model = model.trim().to_ascii_lowercase();
    let model = model.strip_suffix("[1m]").unwrap_or(&model);
    let parts: Vec<&str> = model
        .split(['-', '/', ' ', '_'])
        .filter(|part| !part.is_empty())
        .filter(|part| !part.starts_with(|c: char| c.is_ascii_digit()))
        .filter(|part| !matches!(*part, "claude" | "anthropic" | "openai"))
        .collect();
    (!parts.is_empty()).then(|| parts.join("-"))
}

/// Whether a window with this model scope counts `family`'s spend.
pub fn window_admits(scope_target: Option<&str>, family: &str) -> bool {
    scope_target.is_none_or(|target| model_scope_matches(target, family))
}

/// How many of a plan's base allowance it holds, for scaling a fitted plan's
/// capacity onto one with no instances: a Claude `max_Nx` holds N, Claude Pro
/// one, and any other plan its monthly list price.
pub fn nominal_multiple(provider: &str, plan: &str) -> Option<f64> {
    if let Some(n) = plan
        .strip_prefix("max_")
        .and_then(|rest| rest.strip_suffix('x'))
        .and_then(|n| n.parse::<f64>().ok())
    {
        return Some(n);
    }
    let provider = SubscriptionProvider::for_backend(provider)?;
    if provider == SubscriptionProvider::Claude && plan == "pro" {
        return Some(1.0);
    }
    plan_price(provider, plan)
        .and_then(|price| price.monthly_usd)
        .filter(|usd| *usd > 0)
        .map(f64::from)
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// One stored usage-window reading, as used percent.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub provider: String,
    pub account: String,
    pub window_id: String,
    /// The family name the reading was stored under (`Week`, `Session`,
    /// `Fable`): the window kind a fit groups by.
    pub window_name: String,
    pub scope_target: Option<String>,
    pub used_pct: f64,
    pub resets_at: Option<i64>,
    pub duration_mins: Option<i64>,
    pub captured_at: i64,
}

/// One hour of one family's spend on one account.
#[derive(Debug, Clone, PartialEq)]
pub struct Spend {
    pub account: String,
    /// The hour's start, epoch seconds.
    pub hour: i64,
    pub family: String,
    pub mix: Mix,
}

/// One stretch of one account on one plan.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanSpan {
    pub account: String,
    pub provider: String,
    /// The priced plan key (`max_20x`); `None` for an unpriced plan.
    pub plan: Option<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
}

/// Attributed tokens left out of the fit, and why.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DroppedSpend {
    pub account: String,
    pub model: String,
    pub reason: String,
    pub tokens: f64,
}

/// How far the inputs reach: the newest reading, and the attributed rollup's
/// newest bucket and event count. Inputs with an unchanged watermark fit to
/// the same result, so a refit can be skipped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Watermark {
    pub reading_id: i64,
    pub rollup_bucket: i64,
    pub rollup_events: i64,
}

/// Everything a fit reads.
#[derive(Debug, Clone, Default)]
pub struct DrainInputs {
    pub readings: Vec<Reading>,
    pub spend: Vec<Spend>,
    pub plans: Vec<PlanSpan>,
    /// Each family's most recently spent generation's list rates. They price
    /// the report's cross-check of spend whose model is known only by
    /// family, never a spend a caller names.
    pub rates: BTreeMap<String, Rates>,
    pub dropped: Vec<DroppedSpend>,
    pub now: i64,
}

/// One instance sampled at an hour boundary: the used percent then, and the
/// cumulative spend per family since the instance began.
#[derive(Debug, Clone, PartialEq)]
pub struct DrainSample {
    pub at: i64,
    pub used_pct: f64,
    pub spend: BTreeMap<String, Mix>,
}

// ---------------------------------------------------------------------------
// The fit
// ---------------------------------------------------------------------------

/// A point estimate with a 90% interval.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Estimate {
    pub value: f64,
    pub low: f64,
    /// `f64::INFINITY` (JSON `null`) when the interval has no upper bound.
    pub high: f64,
}

impl Estimate {
    pub fn exact(value: f64) -> Self {
        Self {
            value,
            low: value,
            high: value,
        }
    }

    pub fn scaled(&self, by: f64) -> Self {
        Self {
            value: self.value * by,
            low: self.low * by,
            high: self.high * by,
        }
    }

    /// Half the interval's width over the value; infinite at a zero value.
    pub fn relative_half_width(&self) -> f64 {
        if self.value > 0.0 {
            (self.high - self.low) / 2.0 / self.value
        } else {
            f64::INFINITY
        }
    }

    pub fn contains(&self, value: f64) -> bool {
        self.low <= value && value <= self.high
    }
}

/// Where an interval came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum IntervalKind {
    /// Percentiles of a bootstrap that resamples whole instances.
    Bootstrap,
    /// Fewer instances than a bootstrap can use: a per-sample standard error,
    /// which ignores autocorrelation within a window and so is too narrow.
    SampleLevel,
}

/// What a served rate stands on.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Support {
    /// The family's own coefficient: its interval is above zero and at most
    /// half as wide as its value, over at least
    /// [`fit::MIN_MEASURED_INSTANCES`] instances.
    Measured,
    /// The unsupported families of the window, fitted as one term.
    Pooled { families: Vec<String> },
    /// The window's single all-family rate.
    PooledAll,
    /// No instance on this plan: the nearest fitted plan of the provider,
    /// its capacity scaled by `multiple / from_multiple`.
    #[serde(rename_all = "camelCase")]
    Prior {
        from: String,
        multiple: f64,
        from_multiple: f64,
    },
}

impl Support {
    /// `measured`, `pooled(sonnet+haiku)`, `pooled(all)`,
    /// `prior(max_20x × 5/20)`.
    pub fn label(&self) -> String {
        match self {
            Self::Measured => "measured".to_string(),
            Self::Pooled { families } => format!("pooled({})", families.join("+")),
            Self::PooledAll => "pooled(all)".to_string(),
            Self::Prior {
                from,
                multiple,
                from_multiple,
            } => format!("prior({from} × {multiple}/{from_multiple})"),
        }
    }
}

/// One family's drain rate on one window.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FamilyRate {
    pub family: String,
    /// Percent of the window per list dollar.
    pub percent_per_usd: Estimate,
    pub support: Support,
    /// Instances in which the family spent anything.
    pub instances: usize,
    /// The family's list dollars over every instance of the window.
    pub spend_usd: f64,
    /// The family's tokens and dollars over every instance: the plan's mix.
    pub mix: Mix,
}

impl FamilyRate {
    /// Tokens a whole window holds of this family alone, at the plan's mix.
    pub fn tokens_per_window(&self) -> Option<Estimate> {
        let per_token = self.mix.usd_per_token()?;
        let k = self.percent_per_usd;
        (k.value > 0.0).then(|| Estimate {
            value: 100.0 / k.value / per_token,
            low: if k.high.is_finite() && k.high > 0.0 {
                100.0 / k.high / per_token
            } else {
                0.0
            },
            high: if k.low > 0.0 {
                100.0 / k.low / per_token
            } else {
                f64::INFINITY
            },
        })
    }
}

/// Whether one account drains its plan's windows faster or slower than the
/// pool, with the family ratios held fixed.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountScale {
    pub account: String,
    pub scale: Estimate,
    pub instances: usize,
    /// The scale queries apply: the estimate where its interval excludes 1
    /// over enough instances, else 1.
    pub served: f64,
}

/// One held-out instance: its observed end-of-instance used percent beside
/// what the training fit predicts from the tokens actually spent.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HoldoutRow {
    pub account: String,
    pub started_at: i64,
    pub ended_at: i64,
    pub observed: f64,
    pub predicted: f64,
    /// The prediction with the account's own scale applied.
    pub predicted_per_account: f64,
}

/// Held-out error of the window's fit.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Validation {
    pub training_instances: usize,
    /// End-of-instance mean absolute error, percentage points, pooled rates.
    pub end_mae: f64,
    /// The same with each account's own scale.
    pub end_mae_per_account: f64,
    /// Root-mean-square error over every held-out sample.
    pub sample_rmse: f64,
    pub rows: Vec<HoldoutRow>,
}

/// Whether the dominant family's components keep their list-price ratios: a
/// free fit of percent per million tokens of each component, beside what the
/// family's list-dollar rate implies.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentCheck {
    pub family: String,
    /// Percent of the window per million tokens, by component.
    pub fitted: [Estimate; 4],
    pub list_implied: [f64; 4],
    /// Each component's share of the family's list dollars.
    pub usd_share: [f64; 4],
}

/// The window refitted on one calendar week's instances (by start).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WeekFit {
    /// Monday 00:00 UTC.
    pub week_start: i64,
    pub instances: usize,
    pub percent_per_usd: BTreeMap<String, f64>,
}

/// On a model-scoped window, the coefficient its out-of-scope spend gets
/// when fitted beside the in-scope families. Near zero when scoping holds.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeCheck {
    pub out_of_scope_usd: f64,
    pub percent_per_usd: Estimate,
}

/// The fit for one (provider, plan, window kind).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowFit {
    pub provider: String,
    pub plan: String,
    pub window_name: String,
    pub scope_target: Option<String>,
    pub duration_mins: i64,
    pub instances: usize,
    pub accounts: usize,
    pub samples: usize,
    pub interval: IntervalKind,
    /// Every family the window admits that spent anything, largest first.
    pub families: Vec<FamilyRate>,
    /// The single all-family rate.
    pub all: Estimate,
    /// Residual standard deviation, percentage points, floored at the
    /// rounding's.
    pub residual_sd: f64,
    pub account_scales: Vec<AccountScale>,
    pub validation: Option<Validation>,
    pub component_check: Option<ComponentCheck>,
    pub stability: Vec<WeekFit>,
    pub scope_check: Option<ScopeCheck>,
}

impl WindowFit {
    pub fn family(&self, family: &str) -> Option<&FamilyRate> {
        self.families.iter().find(|rate| rate.family == family)
    }
}

/// An account's latest reading of one window, with how fast it moved.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentWindow {
    pub provider: String,
    pub account: String,
    pub window_id: String,
    pub window_name: String,
    pub scope_target: Option<String>,
    pub used_pct: f64,
    pub resets_at: Option<i64>,
    pub captured_at: i64,
    /// Percent per hour over the last 24 hours of the current instance.
    pub burn_pct_per_hour: Option<f64>,
}

/// The plan an account holds now.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountPlan {
    pub account: String,
    pub provider: String,
    pub plan: Option<String>,
}

/// One account's spend mix of one family: the last seven days, and all time.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountMix {
    pub account: String,
    pub family: String,
    pub recent: Option<Mix>,
    pub all_time: Mix,
}

/// Every window's fit, and what queries need beside it.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainFit {
    pub fitted_at: i64,
    pub windows: Vec<WindowFit>,
    pub plans: Vec<AccountPlan>,
    pub mixes: Vec<AccountMix>,
    pub rates: BTreeMap<String, Rates>,
    /// The provider each family was spent on: a query for a family on
    /// another provider's account has no rate.
    pub families: BTreeMap<String, String>,
    pub dropped: Vec<DroppedSpend>,
    /// Window instances left out of every fit, by reason.
    pub skipped_instances: BTreeMap<String, usize>,
    pub readings: usize,
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// The account a query is about.
#[derive(Debug, Clone, Copy)]
pub struct AccountRef<'a> {
    pub id: &'a str,
    pub provider: &'a str,
    pub plan: Option<&'a str>,
}

/// A window's latest reading, as a query takes it.
#[derive(Debug, Clone, Copy)]
pub struct LatestReading<'a> {
    pub window_name: &'a str,
    pub scope_target: Option<&'a str>,
    pub used_pct: f64,
}

/// The rate a query serves for one (account, window, model).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rate {
    pub family: String,
    pub window_name: String,
    /// With the account's scale and any prior multiple applied.
    pub percent_per_usd: Estimate,
    pub support: Support,
    pub instances: usize,
    pub residual_sd: f64,
    pub account_scale: f64,
    pub duration_mins: i64,
}

/// What remains of one window for one model.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capacity {
    pub remaining_percent: f64,
    /// List dollars of the model the window still holds.
    pub usd: Estimate,
    /// Tokens at the given mix, when one was given.
    pub tokens: Option<Estimate>,
    pub rate: Rate,
}

impl DrainFit {
    /// The fit for exactly this provider, plan and window kind.
    pub fn window_fit(&self, provider: &str, plan: &str, window_name: &str) -> Option<&WindowFit> {
        self.windows.iter().find(|window| {
            window.provider == provider && window.plan == plan && window.window_name == window_name
        })
    }

    /// The families spent on `provider`'s accounts.
    pub fn families_on<'a>(&'a self, provider: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.families
            .iter()
            .filter(move |(_, on)| on.as_str() == provider)
            .map(|(family, _)| family.as_str())
    }

    /// The plan an account holds now, by the recorded periods.
    pub fn plan_of(&self, account: &str) -> Option<&AccountPlan> {
        self.plans.iter().find(|plan| plan.account == account)
    }

    /// The account's mix of `family`: the last seven days when it spent any,
    /// else all time.
    pub fn mix(&self, account: &str, family: &str) -> Option<&Mix> {
        self.mixes
            .iter()
            .find(|mix| mix.account == account && mix.family == family)
            .map(|mix| mix.recent.as_ref().unwrap_or(&mix.all_time))
    }

    /// The drain rate of `model` on the account's `window_name` window, or
    /// `None` when the window does not count the model or nothing is fitted
    /// for the provider and window kind. A plan with no instances borrows the
    /// provider's most-observed plan of the same window kind, scaled by the
    /// ratio of their nominal multiples.
    pub fn rate(&self, account: &AccountRef, window_name: &str, model: &str) -> Option<Rate> {
        let family = family_of(model)?;
        if self.families.get(&family).map(String::as_str) != Some(account.provider) {
            return None;
        }
        let (window, prior) = match account
            .plan
            .and_then(|plan| self.window_fit(account.provider, plan, window_name))
        {
            Some(window) => (window, None),
            None => self.prior_window(account, window_name)?,
        };
        if !window_admits(window.scope_target.as_deref(), &family) {
            return None;
        }
        let (k, support, instances) = match window.family(&family) {
            Some(rate) => (rate.percent_per_usd, rate.support.clone(), rate.instances),
            None => (window.all, Support::PooledAll, window.instances),
        };
        let (k, support, account_scale) = match prior {
            // A plan holding `multiple` against the source's `from_multiple`
            // drains `from_multiple / multiple` as fast per dollar.
            Some((multiple, from_multiple)) => (
                k.scaled(from_multiple / multiple),
                Support::Prior {
                    from: window.plan.clone(),
                    multiple,
                    from_multiple,
                },
                1.0,
            ),
            None => {
                let scale = window
                    .account_scales
                    .iter()
                    .find(|scale| scale.account == account.id)
                    .map_or(1.0, |scale| scale.served);
                (k.scaled(scale), support, scale)
            }
        };
        Some(Rate {
            family,
            window_name: window.window_name.clone(),
            percent_per_usd: k,
            support,
            instances,
            residual_sd: window.residual_sd,
            account_scale,
            duration_mins: window.duration_mins,
        })
    }

    fn prior_window(
        &self,
        account: &AccountRef,
        window_name: &str,
    ) -> Option<(&WindowFit, Option<(f64, f64)>)> {
        let multiple = nominal_multiple(account.provider, account.plan?)?;
        self.windows
            .iter()
            .filter(|window| {
                window.provider == account.provider && window.window_name == window_name
            })
            .filter_map(|window| Some((window, nominal_multiple(&window.provider, &window.plan)?)))
            .max_by_key(|(window, _)| window.instances)
            .map(|(window, from)| (window, Some((multiple, from))))
    }

    /// The list dollars of `model` the window still holds given its latest
    /// reading, and the tokens that is at `mix`. The interval carries the
    /// rate's, the latest reading's ±0.5 rounding and the window's residual.
    /// On an all-models window this is what remains if only `model` ran.
    pub fn remaining(
        &self,
        account: &AccountRef,
        latest: &LatestReading,
        model: &str,
        mix: Option<&Mix>,
    ) -> Option<Capacity> {
        let rate = self.rate(account, latest.window_name, model)?;
        let k = rate.percent_per_usd;
        if k.value <= 0.0 {
            return None;
        }
        let remaining = (100.0 - latest.used_pct).clamp(0.0, 100.0);
        let slack = 0.5 + Z90 * rate.residual_sd;
        let usd = Estimate {
            value: remaining / k.value,
            low: if k.high.is_finite() && k.high > 0.0 {
                (remaining - slack).max(0.0) / k.high
            } else {
                0.0
            },
            high: if k.low > 0.0 {
                (remaining + slack).min(100.0) / k.low
            } else {
                f64::INFINITY
            },
        };
        let tokens = mix
            .and_then(Mix::usd_per_token)
            .map(|per_token| usd.scaled(1.0 / per_token));
        Some(Capacity {
            remaining_percent: remaining,
            usd,
            tokens,
            rate,
        })
    }

    /// The percent of the account's `window_name` window that a spend of
    /// `model` costing `usd` at that model's list price is expected to use,
    /// with the rate's interval.
    pub fn percent_for(
        &self,
        account: &AccountRef,
        window_name: &str,
        model: &str,
        usd: f64,
    ) -> Option<Estimate> {
        let rate = self.rate(account, window_name, model)?;
        Some(rate.percent_per_usd.scaled(usd))
    }

    /// The probability that a spend of `model` costing `usd` at that model's
    /// list price fits what every applicable
    /// window has left: the product over windows of P(cost ≤ remaining),
    /// each a normal approximation over the rate's interval, the window's
    /// residual and the reading's rounding. Treating windows as independent
    /// understates the answer when they move together. `None` when no
    /// window is fitted for the model.
    pub fn fits(
        &self,
        account: &AccountRef,
        model: &str,
        usd: f64,
        latest: &[LatestReading],
    ) -> Option<f64> {
        let family = family_of(model)?;
        let mut probability = None;
        for reading in latest {
            if !window_admits(reading.scope_target, &family) {
                continue;
            }
            let Some(rate) = self.rate(account, reading.window_name, model) else {
                continue;
            };
            let k = rate.percent_per_usd;
            let k_sd = if k.high.is_finite() {
                (k.high - k.low) / (2.0 * Z90)
            } else {
                k.value
            };
            let mean = k.value * usd;
            let sd = ((k_sd * usd).powi(2) + rate.residual_sd.powi(2) + ROUNDING_VARIANCE).sqrt();
            let remaining = (100.0 - reading.used_pct).clamp(0.0, 100.0);
            let p = normal_cdf((remaining - mean) / sd);
            probability = Some(probability.unwrap_or(1.0) * p);
        }
        probability
    }

    /// Percent of the window per list dollar of `model`: the rate 6031 can
    /// price in list-dollar units.
    pub fn percent_per_list_dollar(
        &self,
        account: &AccountRef,
        window_name: &str,
        model: &str,
    ) -> Option<Estimate> {
        self.rate(account, window_name, model)
            .map(|rate| rate.percent_per_usd)
    }

    /// What one token of `model` at `mix` costs in subscription dollars on
    /// the account's `window_name` window: dollars per 1% of the window (the
    /// monthly price over the windows a month holds, over 100) times the
    /// percent a token uses. A $200 plan on a weekly window is about $0.46
    /// per 1%.
    pub fn dollars_per_token(
        &self,
        account: &AccountRef,
        model: &str,
        plan_monthly_usd: f64,
        window_name: &str,
        mix: &Mix,
    ) -> Option<Estimate> {
        let rate = self.rate(account, window_name, model)?;
        let per_token = mix.usd_per_token()?;
        Some(
            rate.percent_per_usd
                .scaled(dollars_per_percent(plan_monthly_usd, rate.duration_mins) * per_token),
        )
    }
}

/// Subscription dollars per 1% of a window `duration_mins` long on a plan
/// costing `monthly_usd`.
pub fn dollars_per_percent(monthly_usd: f64, duration_mins: i64) -> f64 {
    let windows_per_month = DAYS_PER_MONTH * 24.0 * 60.0 / duration_mins.max(1) as f64;
    monthly_usd / windows_per_month / 100.0
}

/// The standard normal CDF, through Abramowitz and Stegun 7.1.26 (absolute
/// error under 1.5e-7).
pub fn normal_cdf(x: f64) -> f64 {
    if x.is_nan() {
        return 0.5;
    }
    let z = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.327_591_1 * z);
    let poly = t
        * (0.254_829_592
            + t * (-0.284_496_736
                + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
    let erf = 1.0 - poly * (-z * z).exp();
    if x >= 0.0 {
        0.5 * (1.0 + erf)
    } else {
        0.5 * (1.0 - erf)
    }
}
