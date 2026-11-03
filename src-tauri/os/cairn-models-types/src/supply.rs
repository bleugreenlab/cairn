//! What running on one account costs right now, as the supply ranking reads
//! it.
//!
//! Cost is marginal and present-tense: an allowance that will expire unused
//! before it resets costs nothing to spend, other subscription headroom costs
//! a little of next week, and a metered model costs its price. So a candidate
//! falls in one of four classes, cheapest first:
//!
//! 1. **Expiring**: at the account's current burn rate, at least
//!    [`EXPIRING_THRESHOLD_PERCENT`] of every window that applies will still be
//!    unused when it resets. Scored by that projected unused share.
//! 2. **Headroom**: measured windows, but nothing projected to expire. Scored
//!    by the tightest window's remaining share.
//! 3. **Headroom unknown**: a subscription account whose every window rolled
//!    over since it was measured. (An account never measured at all is a
//!    fresh sign-in on a fresh allowance, so it competes as Headroom at 100%:
//!    ranking it after every measured account would starve a new login of the
//!    sessions that would measure it.)
//! 4. **Metered**: billed per token. Scored by output price, then input price;
//!    an unpriced model sorts last.
//!
//! For one window, with `elapsed = duration - hours_to_reset`:
//! `burn = used% / elapsed` and
//! `projected_unused = max(0, remaining% - burn * hours_to_reset)`. It is
//! undefined when the duration is unknown or nothing has elapsed. An account's
//! expiring share is the minimum over its windows, defined only when every
//! window's is; its headroom is the tightest window's remaining share.
//!
//! Every number here comes from what the provider reported; none is guessed.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::catalog::ModelPricing;
use crate::usage::{BillingUnit, Charge, ProviderUsageWindow};

/// The projected unused share below which an expiring allowance is noise
/// rather than a reason to prefer an account.
pub const EXPIRING_THRESHOLD_PERCENT: f64 = 5.0;

/// The cost classes, cheapest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CostClass {
    Expiring,
    Headroom,
    HeadroomUnknown,
    Metered,
}

/// The window a subscription cost was read from.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowReading {
    /// The window's short name (`Week`, `5-hour`, a model family).
    pub name: String,
    pub remaining_percent: f64,
    pub hours_to_reset: Option<f64>,
    pub projected_unused_percent: Option<f64>,
}

/// What one candidate costs right now.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", tag = "class")]
pub enum Cost {
    /// `percent` of the binding window is projected to expire unused.
    Expiring { percent: f64, window: WindowReading },
    /// `percent` of the tightest window is left.
    Headroom { percent: f64, window: WindowReading },
    /// No live measurement. `measured_before` says whether one existed and
    /// rolled over (class [`CostClass::HeadroomUnknown`]); an account never
    /// measured competes in [`CostClass::Headroom`] as if full.
    HeadroomUnknown { measured_before: bool },
    /// USD per million tokens, when priced.
    Metered {
        output_per_mtok: Option<f64>,
        input_per_mtok: Option<f64>,
    },
}

impl Cost {
    pub fn class(&self) -> CostClass {
        match self {
            Self::Expiring { .. } => CostClass::Expiring,
            Self::Headroom { .. }
            | Self::HeadroomUnknown {
                measured_before: false,
            } => CostClass::Headroom,
            Self::HeadroomUnknown {
                measured_before: true,
            } => CostClass::HeadroomUnknown,
            Self::Metered { .. } => CostClass::Metered,
        }
    }

    /// The share a Headroom-class cost competes with.
    fn headroom(&self) -> Option<f64> {
        match self {
            Self::Headroom { percent, .. } => Some(*percent),
            Self::HeadroomUnknown {
                measured_before: false,
            } => Some(100.0),
            _ => None,
        }
    }

    /// `Less` when `self` is cheaper than `other`: class first, then the
    /// class's score (more expiring or more headroom is cheaper; a lower price
    /// is cheaper, and unpriced is dearest).
    pub fn compare(&self, other: &Self) -> Ordering {
        self.class()
            .cmp(&other.class())
            .then_with(|| match (self, other) {
                (Self::Expiring { percent: a, .. }, Self::Expiring { percent: b, .. }) => {
                    b.total_cmp(a)
                }
                _ if self.class() == CostClass::Headroom => {
                    let a = self.headroom().unwrap_or(0.0);
                    let b = other.headroom().unwrap_or(0.0);
                    b.total_cmp(&a)
                }
                (
                    Self::Metered {
                        output_per_mtok: ao,
                        input_per_mtok: ai,
                    },
                    Self::Metered {
                        output_per_mtok: bo,
                        input_per_mtok: bi,
                    },
                ) => price_order(*ao, *bo).then_with(|| price_order(*ai, *bi)),
                _ => Ordering::Equal,
            })
    }

    /// The cost as one clause of a provenance line, after the account's
    /// name: `Week 64% left, resets in 14h, ~31% would expire unused`.
    pub fn describe(&self) -> String {
        match self {
            Self::Expiring { percent, window } => format!(
                "{}, ~{}% would expire unused",
                describe_window(window),
                percent.round()
            ),
            Self::Headroom { window, .. } => describe_window(window),
            Self::HeadroomUnknown {
                measured_before: false,
            } => "no usage measured yet".to_string(),
            Self::HeadroomUnknown {
                measured_before: true,
            } => "usage windows reset since last measured".to_string(),
            Self::Metered {
                output_per_mtok,
                input_per_mtok,
            } => match (input_per_mtok, output_per_mtok) {
                (Some(input), Some(output)) => {
                    format!("metered ${input:.2}/${output:.2} per Mtok")
                }
                (None, Some(output)) => format!("metered ${output:.2} per Mtok out"),
                _ => "metered, unpriced".to_string(),
            },
        }
    }
}

fn price_order(a: Option<f64>, b: Option<f64>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn describe_window(window: &WindowReading) -> String {
    let mut text = format!("{} {}% left", window.name, window.remaining_percent.round());
    if let Some(hours) = window.hours_to_reset {
        text.push_str(&format!(", resets in {}", describe_hours(hours)));
    }
    text
}

fn describe_hours(hours: f64) -> String {
    if hours < 1.0 {
        format!("{}m", (hours * 60.0).round().max(1.0))
    } else if hours < 48.0 {
        format!("{}h", hours.round())
    } else {
        format!("{}d", (hours / 24.0).round())
    }
}

/// The share of `window` projected to be unused when it resets, at the burn
/// rate so far, or `None` when that cannot be told.
pub fn projected_unused(window: &ProviderUsageWindow, now: i64) -> Option<f64> {
    let duration_h = f64::from(window.window_duration_mins?) / 60.0;
    let hours_to_reset = (window.resets_at? - now) as f64 / 3600.0;
    let elapsed_h = duration_h - hours_to_reset;
    if duration_h <= 0.0 || elapsed_h <= 0.0 || hours_to_reset < 0.0 {
        return None;
    }
    let burn = window.used_percent / elapsed_h;
    Some((window.remaining_percent - burn * hours_to_reset).max(0.0))
}

fn reading(window: &ProviderUsageWindow, now: i64) -> WindowReading {
    WindowReading {
        name: window.family_name(),
        remaining_percent: window.remaining_percent,
        hours_to_reset: window
            .resets_at
            .map(|resets_at| ((resets_at - now) as f64 / 3600.0).max(0.0)),
        projected_unused_percent: projected_unused(window, now),
    }
}

/// What a subscription account costs for `requested_model` at `now`, from
/// its usage windows, or `None` when an applicable window is empty (the
/// account cannot serve). `windows` is `None` for an account never measured.
/// Blocks and holds are the caller's to apply first.
pub fn account_cost(
    windows: Option<(&[ProviderUsageWindow], i64)>,
    requested_model: Option<&str>,
    now: i64,
) -> Option<Cost> {
    let Some((windows, captured_at)) = windows else {
        return Some(Cost::HeadroomUnknown {
            measured_before: false,
        });
    };
    let live: Vec<&ProviderUsageWindow> = windows
        .iter()
        .filter(|window| window.is_live_at(captured_at, now))
        .filter(|window| window.matches_model(requested_model))
        .collect();
    if live.is_empty() {
        return Some(Cost::HeadroomUnknown {
            measured_before: !windows.is_empty(),
        });
    }
    if live.iter().any(|window| window.remaining_percent <= 0.0) {
        return None;
    }
    let tightest = live
        .iter()
        .min_by(|a, b| a.remaining_percent.total_cmp(&b.remaining_percent))
        .expect("non-empty");
    let projections: Option<Vec<(f64, &ProviderUsageWindow)>> = live
        .iter()
        .map(|window| projected_unused(window, now).map(|x| (x, *window)))
        .collect();
    let binding = projections
        .and_then(|projections| projections.into_iter().min_by(|a, b| a.0.total_cmp(&b.0)));
    Some(match binding {
        Some((percent, window)) if percent >= EXPIRING_THRESHOLD_PERCENT => Cost::Expiring {
            percent,
            window: reading(window, now),
        },
        _ => Cost::Headroom {
            percent: tightest.remaining_percent,
            window: reading(tightest, now),
        },
    })
}

/// What a metered model costs, from its catalog price.
pub fn metered_cost(pricing: Option<&ModelPricing>) -> Cost {
    let per_mtok = |charge: Charge| {
        pricing
            .and_then(|pricing| pricing.usd_per(charge, None, BillingUnit::Tokens))
            .and_then(|usd| usd.parse::<f64>().ok())
            .map(|usd| usd * 1_000_000.0)
    };
    Cost::Metered {
        output_per_mtok: per_mtok(Charge::Output),
        input_per_mtok: per_mtok(Charge::Input),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Price;
    use crate::usage::{Meter, ProviderUsageScope};

    const HOUR: i64 = 3600;
    const NOW: i64 = 1_000_000;

    fn week(used: f64, hours_to_reset: i64) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: "week".into(),
            label: "Weekly".into(),
            scope: ProviderUsageScope::Weekly,
            scope_target: None,
            used_percent: used,
            remaining_percent: 100.0 - used,
            resets_at: Some(NOW + hours_to_reset * HOUR),
            reset_at_text: None,
            window_duration_mins: Some(7 * 24 * 60),
        }
    }

    fn five_hour(used: f64, hours_to_reset: i64) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: "5h".into(),
            label: "5h".into(),
            scope: ProviderUsageScope::RollingWindow,
            window_duration_mins: Some(300),
            ..week(used, hours_to_reset)
        }
    }

    fn cost(windows: &[ProviderUsageWindow]) -> Option<Cost> {
        account_cost(Some((windows, NOW)), None, NOW)
    }

    #[test]
    fn a_slow_burn_late_in_the_week_is_expiring() {
        // 36% used over 154h elapsed, 14h to go: ~3.3% more will be used, so
        // ~60.7% of the week would be unused at reset.
        let cost = cost(&[week(36.0, 14)]).unwrap();
        let Cost::Expiring { percent, .. } = cost else {
            panic!("{cost:?}")
        };
        assert!((percent - 60.73).abs() < 0.01, "{percent}");
        assert_eq!(
            cost.describe(),
            "Week 64% left, resets in 14h, ~61% would expire unused"
        );
    }

    #[test]
    fn a_fast_burn_is_headroom_and_the_tightest_window_binds() {
        // 78% used in 2 days of 7 will run out before reset: nothing expires.
        let fast = cost(&[week(78.0, 120)]).unwrap();
        assert_eq!(fast.class(), CostClass::Headroom);
        assert_eq!(fast.describe(), "Week 22% left, resets in 5d");
        // The minimum across windows binds: a week that would expire is held
        // back by a five-hour window burning fast.
        let both = cost(&[week(36.0, 14), five_hour(60.0, 2)]).unwrap();
        let Cost::Headroom { percent, window } = both else {
            panic!("{both:?}")
        };
        assert_eq!((percent, window.name.as_str()), (40.0, "5-hour"));
    }

    #[test]
    fn an_undefined_projection_is_headroom_not_expiring() {
        let mut unknown = week(10.0, 14);
        unknown.window_duration_mins = None;
        assert_eq!(cost(&[unknown]).unwrap().class(), CostClass::Headroom);
        // Nothing elapsed yet.
        assert_eq!(
            cost(&[week(0.0, 168)]).unwrap().class(),
            CostClass::Headroom
        );
    }

    #[test]
    fn an_empty_window_excludes_the_account() {
        assert_eq!(cost(&[week(30.0, 14), five_hour(100.0, 1)]), None);
    }

    #[test]
    fn rolled_over_usage_ranks_after_measured_and_a_fresh_login_competes_as_full() {
        let fresh = account_cost(None, None, NOW).unwrap();
        let mut rolled = week(10.0, 14);
        rolled.resets_at = Some(NOW - HOUR);
        let stale = cost(&[rolled]).unwrap();
        let measured = cost(&[week(90.0, 120)]).unwrap();
        assert_eq!(stale.class(), CostClass::HeadroomUnknown);
        assert_eq!(measured.compare(&stale), Ordering::Less);
        assert_eq!(fresh.compare(&measured), Ordering::Less);
        assert_eq!(
            cost(&[week(36.0, 14)]).unwrap().compare(&fresh),
            Ordering::Less
        );
    }

    #[test]
    fn classes_order_expiring_headroom_unknown_metered() {
        let expiring = cost(&[week(36.0, 14)]).unwrap();
        let headroom = cost(&[week(10.0, 160)]).unwrap();
        let mut rolled = week(10.0, 14);
        rolled.resets_at = Some(NOW - HOUR);
        let unknown = cost(&[rolled]).unwrap();
        let metered = metered_cost(None);
        let mut costs = vec![
            metered.clone(),
            unknown.clone(),
            headroom.clone(),
            expiring.clone(),
        ];
        costs.sort_by(Cost::compare);
        assert_eq!(costs, [expiring, headroom, unknown, metered]);
    }

    #[test]
    fn metered_orders_by_output_then_input_price_with_unpriced_last() {
        let priced = |input: &str, output: &str| {
            metered_cost(Some(&ModelPricing {
                prices: vec![
                    Price {
                        meter: Meter::tokens(Charge::Input),
                        usd: input.into(),
                    },
                    Price {
                        meter: Meter::tokens(Charge::Output),
                        usd: output.into(),
                    },
                ],
                ..ModelPricing::default()
            }))
        };
        let sonnet = priced("0.000003", "0.000015");
        let cheap_out = priced("0.000005", "0.000010");
        let same_out_cheaper_in = priced("0.000001", "0.000015");
        let unpriced = metered_cost(None);
        let mut costs = vec![
            unpriced.clone(),
            sonnet.clone(),
            same_out_cheaper_in.clone(),
            cheap_out.clone(),
        ];
        costs.sort_by(Cost::compare);
        assert_eq!(
            costs,
            [cheap_out, same_out_cheaper_in, sonnet.clone(), unpriced]
        );
        assert_eq!(sonnet.describe(), "metered $3.00/$15.00 per Mtok");
    }
}
