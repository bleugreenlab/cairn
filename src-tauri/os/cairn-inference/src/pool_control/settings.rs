//! A controlled pool's operator settings: the two idle windows, the minimum
//! residency and the load bound.
//!
//! Each setting the operator leaves unset is derived from the slowest load the
//! act side has recorded for a deployment of the pool, so the windows scale
//! with what a load actually costs; with no load recorded yet a built-in value
//! stands in. The multiples are starting values, not a cost model: tenet 1321
//! rejects ski-rental-style unload timing until loads are measured.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The operator's overrides, in seconds. `None` derives the value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolSettings {
    /// How long a loaded deployment goes without a request before it is idle
    /// (rule 2 and the two-model hand-over).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_idle_secs: Option<u64>,
    /// How long the pool goes without batch work before it loads its default
    /// (rule 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_idle_secs: Option<u64>,
    /// How long a deployment stays after it comes healthy, whatever else wants
    /// the pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_residency_secs: Option<u64>,
    /// How long a load may take, from the act side reporting it starting, before
    /// the controller gives it up and fails the requests waiting on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_bound_secs: Option<u64>,
}

/// Where an effective setting came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SettingOrigin {
    /// The operator set it.
    Operator,
    /// Derived from the slowest recorded load of a deployment of the pool.
    Measured { load_secs: u64 },
    /// No load recorded yet.
    BuiltIn,
}

/// One effective setting and its origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Setting {
    pub secs: u64,
    pub origin: SettingOrigin,
}

impl Setting {
    pub fn ms(self) -> i64 {
        i64::try_from(self.secs.saturating_mul(1_000)).unwrap_or(i64::MAX)
    }
}

impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", duration_label(self.secs))?;
        match self.origin {
            SettingOrigin::Operator => f.write_str(" (operator)"),
            SettingOrigin::Measured { load_secs } => {
                write!(
                    f,
                    " (from a measured load of {})",
                    duration_label(load_secs)
                )
            }
            SettingOrigin::BuiltIn => f.write_str(" (built-in; no load measured yet)"),
        }
    }
}

/// The settings a controlled pool runs with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveSettings {
    pub model_idle: Setting,
    pub pool_idle: Setting,
    pub min_residency: Setting,
    pub load_bound: Setting,
}

/// One setting's derivation: `multiple` times the measured load, never below
/// `floor`, or `built_in` with nothing measured.
struct Rule {
    multiple: u64,
    floor: u64,
    built_in: u64,
}

const MINUTE: u64 = 60;

const MODEL_IDLE: Rule = Rule {
    multiple: 6,
    floor: 15 * MINUTE,
    built_in: 30 * MINUTE,
};
const POOL_IDLE: Rule = Rule {
    multiple: 3,
    floor: 10 * MINUTE,
    built_in: 15 * MINUTE,
};
const MIN_RESIDENCY: Rule = Rule {
    multiple: 4,
    floor: 10 * MINUTE,
    built_in: 20 * MINUTE,
};
const LOAD_BOUND: Rule = Rule {
    multiple: 3,
    floor: 5 * MINUTE,
    built_in: 30 * MINUTE,
};

fn effective(operator: Option<u64>, rule: Rule, measured_load_secs: Option<u64>) -> Setting {
    match (operator, measured_load_secs) {
        (Some(secs), _) => Setting {
            secs,
            origin: SettingOrigin::Operator,
        },
        (None, Some(load_secs)) => Setting {
            secs: load_secs.saturating_mul(rule.multiple).max(rule.floor),
            origin: SettingOrigin::Measured { load_secs },
        },
        (None, None) => Setting {
            secs: rule.built_in,
            origin: SettingOrigin::BuiltIn,
        },
    }
}

impl PoolSettings {
    /// The settings in force, given the slowest load recorded for a deployment
    /// of the pool.
    pub fn effective(&self, measured_load_secs: Option<u64>) -> EffectiveSettings {
        EffectiveSettings {
            model_idle: effective(self.model_idle_secs, MODEL_IDLE, measured_load_secs),
            pool_idle: effective(self.pool_idle_secs, POOL_IDLE, measured_load_secs),
            min_residency: effective(self.min_residency_secs, MIN_RESIDENCY, measured_load_secs),
            load_bound: effective(self.load_bound_secs, LOAD_BOUND, measured_load_secs),
        }
    }
}

/// A duration in seconds as `1h 5m`, `12m 30s` or `45s`.
pub fn duration_label(secs: u64) -> String {
    let (h, m, s) = (secs / 3_600, (secs % 3_600) / 60, secs % 60);
    match (h, m, s) {
        (0, 0, s) => format!("{s}s"),
        (0, m, 0) => format!("{m}m"),
        (0, m, s) => format!("{m}m {s}s"),
        (h, 0, _) => format!("{h}h"),
        (h, m, _) => format!("{h}h {m}m"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_operator_wins_then_the_measured_load_then_the_built_in() {
        let settings = PoolSettings {
            min_residency_secs: Some(90),
            ..PoolSettings::default()
        };
        let measured = settings.effective(Some(8 * MINUTE));
        assert_eq!(
            measured.min_residency,
            Setting {
                secs: 90,
                origin: SettingOrigin::Operator
            }
        );
        assert_eq!(measured.model_idle.secs, 48 * MINUTE);
        assert_eq!(measured.pool_idle.secs, 24 * MINUTE);
        assert_eq!(measured.load_bound.secs, 24 * MINUTE);
        assert_eq!(
            measured.load_bound.origin,
            SettingOrigin::Measured {
                load_secs: 8 * MINUTE
            }
        );

        let built_in = PoolSettings::default().effective(None);
        assert_eq!(built_in.model_idle.secs, 30 * MINUTE);
        assert_eq!(built_in.load_bound.origin, SettingOrigin::BuiltIn);
    }

    #[test]
    fn a_fast_load_never_derives_below_the_floor() {
        let fast = PoolSettings::default().effective(Some(20));
        assert_eq!(fast.model_idle.secs, 15 * MINUTE);
        assert_eq!(fast.pool_idle.secs, 10 * MINUTE);
        assert_eq!(fast.min_residency.secs, 10 * MINUTE);
        assert_eq!(fast.load_bound.secs, 5 * MINUTE);
    }

    #[test]
    fn settings_render_with_their_origin() {
        let settings = PoolSettings::default().effective(Some(250));
        assert_eq!(
            settings.load_bound.to_string(),
            "12m 30s (from a measured load of 4m 10s)"
        );
        assert_eq!(
            PoolSettings::default()
                .effective(None)
                .pool_idle
                .to_string(),
            "15m (built-in; no load measured yet)"
        );
        assert_eq!(duration_label(3_900), "1h 5m");
    }
}
