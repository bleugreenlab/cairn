//! A pool's three windows for the models placement runs on it: the idle
//! window, the minimum residency and the load bound.
//!
//! One idle window governs both directions of a pool's idleness: a resident
//! with no request for that long may be evicted when other work is refused
//! for its room, and a pool with no batch work for that long places its warm
//! model. It derives from what a reload costs now, with one operator override
//! per pool. The minimum residency and the load bound always derive: a load
//! measured at four minutes is protected for sixteen and given up at twelve,
//! whatever anyone sets. The multiples are starting values, not a cost model.
//!
//! What a reload costs now is each deployment's latest healthy load, passing
//! over one whose start downloaded what it serves (a container image pull)
//! while a load from cache is on record
//! ([`inference_control::reference_load`](crate::orchestrator::inference_control::reference_load)),
//! and the slowest of those across the pool's deployments, since the pool's
//! windows must cover reloading whichever model it holds. It is never the
//! slowest load on record: a first load that pulled its image would otherwise
//! govern the pool for as long as the record kept it.

use std::fmt;

use serde::Serialize;

/// Epoch milliseconds.
pub type Ms = i64;

/// The load a pool's windows derive from: its time to health in whole
/// seconds, and whether it downloaded what it serves first, which it is only
/// when no load from cache is on record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MeasuredLoad {
    pub secs: u64,
    pub cold: bool,
}

/// Where an effective setting came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SettingOrigin {
    /// The operator set the pool's idle window.
    Operator,
    /// Derived from the latest measured load on the pool (see the module).
    Measured(MeasuredLoad),
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
    pub fn ms(self) -> Ms {
        Ms::try_from(self.secs.saturating_mul(1_000)).unwrap_or(Ms::MAX)
    }
}

impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", duration_label(self.secs))?;
        match self.origin {
            SettingOrigin::Operator => f.write_str(" (operator)"),
            SettingOrigin::Measured(load) => {
                write!(
                    f,
                    " (from the latest measured load of {}",
                    duration_label(load.secs)
                )?;
                if load.cold {
                    f.write_str(", which downloaded first; no load from cache is measured yet")?;
                }
                f.write_str(")")
            }
            SettingOrigin::BuiltIn => f.write_str(" (built-in; no load measured yet)"),
        }
    }
}

/// The windows a pool's models run under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveSettings {
    /// How long a resident goes without a request before it may be evicted
    /// for room, and how long the pool goes without batch work before it
    /// places its warm model.
    pub idle: Setting,
    /// How long a resident stays after it comes healthy, whatever else wants
    /// its machines.
    pub min_residency: Setting,
    /// How long a load may take, from the head starting it, before it is
    /// given up and the requests waiting on it fail.
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

const IDLE: Rule = Rule {
    multiple: 6,
    floor: 15 * MINUTE,
    built_in: 30 * MINUTE,
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

fn derived(rule: Rule, measured: Option<MeasuredLoad>) -> Setting {
    match measured {
        Some(load) => Setting {
            secs: load.secs.saturating_mul(rule.multiple).max(rule.floor),
            origin: SettingOrigin::Measured(load),
        },
        None => Setting {
            secs: rule.built_in,
            origin: SettingOrigin::BuiltIn,
        },
    }
}

impl EffectiveSettings {
    /// The windows in force, given the pool's idle override and the load
    /// they derive from (see the module).
    pub fn derive(idle_override_secs: Option<u64>, measured: Option<MeasuredLoad>) -> Self {
        Self {
            idle: match idle_override_secs {
                Some(secs) => Setting {
                    secs,
                    origin: SettingOrigin::Operator,
                },
                None => derived(IDLE, measured),
            },
            min_residency: derived(MIN_RESIDENCY, measured),
            load_bound: derived(LOAD_BOUND, measured),
        }
    }
}

impl Default for EffectiveSettings {
    fn default() -> Self {
        Self::derive(None, None)
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

/// How long ago `at` was, or how far off, relative to `now`.
pub fn relative(at: Ms, now: Ms) -> String {
    let secs = |ms: Ms| u64::try_from(ms / 1_000).unwrap_or(0);
    if at <= now {
        format!("{} ago", duration_label(secs(now - at)))
    } else {
        format!("in {}", duration_label(secs(at - now)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warm(secs: u64) -> Option<MeasuredLoad> {
        Some(MeasuredLoad { secs, cold: false })
    }

    #[test]
    fn the_override_wins_for_the_idle_window_and_the_rest_always_derive() {
        let measured = EffectiveSettings::derive(Some(90), warm(8 * MINUTE));
        assert_eq!(
            measured.idle,
            Setting {
                secs: 90,
                origin: SettingOrigin::Operator
            }
        );
        assert_eq!(measured.min_residency.secs, 32 * MINUTE);
        assert_eq!(measured.load_bound.secs, 24 * MINUTE);
        assert_eq!(
            measured.load_bound.origin,
            SettingOrigin::Measured(MeasuredLoad {
                secs: 8 * MINUTE,
                cold: false
            })
        );
        assert_eq!(
            EffectiveSettings::derive(None, warm(8 * MINUTE)).idle.secs,
            48 * MINUTE
        );

        let built_in = EffectiveSettings::default();
        assert_eq!(built_in.idle.secs, 30 * MINUTE);
        assert_eq!(built_in.min_residency.secs, 20 * MINUTE);
        assert_eq!(built_in.load_bound.origin, SettingOrigin::BuiltIn);
    }

    #[test]
    fn a_fast_load_never_derives_below_the_floor() {
        let fast = EffectiveSettings::derive(None, warm(20));
        assert_eq!(fast.idle.secs, 15 * MINUTE);
        assert_eq!(fast.min_residency.secs, 10 * MINUTE);
        assert_eq!(fast.load_bound.secs, 5 * MINUTE);
    }

    #[test]
    fn settings_render_with_their_origin() {
        let settings = EffectiveSettings::derive(None, warm(250));
        assert_eq!(
            settings.load_bound.to_string(),
            "12m 30s (from the latest measured load of 4m 10s)"
        );
        let pulled = EffectiveSettings::derive(
            None,
            Some(MeasuredLoad {
                secs: 211,
                cold: true,
            }),
        );
        assert_eq!(
            pulled.idle.to_string(),
            "21m 6s (from the latest measured load of 3m 31s, which downloaded first; no load from cache is measured yet)"
        );
        assert_eq!(
            EffectiveSettings::default().idle.to_string(),
            "30m (built-in; no load measured yet)"
        );
        assert_eq!(
            EffectiveSettings::derive(Some(1_200), None)
                .idle
                .to_string(),
            "20m (operator)"
        );
        assert_eq!(duration_label(3_900), "1h 5m");
    }
}
