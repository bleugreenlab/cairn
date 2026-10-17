use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::Modality;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageScope {
    Session,
    Weekly,
    RollingWindow,
    Custom,
}

/// Collapse a human label into a stable identifier fragment.
///
/// Shared with model-scope comparison so a window id, a family key, and a
/// routing eligibility check all spell the same model the same way.
pub fn slugify(label: &str) -> String {
    label
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Usage reports name scoped model families for humans (for example `Fable`),
/// while a launch may carry either that family or a provider model identifier.
/// Compare normalized dash-delimited names so a family constrains its concrete
/// versions without letting a substring accidentally match another family.
pub fn model_scope_matches(scope_target: &str, requested_model: &str) -> bool {
    let target = slugify(scope_target);
    let model = slugify(requested_model);
    if target.is_empty() || model.is_empty() {
        return false;
    }
    model == target
        || model
            .split('-')
            .collect::<Vec<_>>()
            .windows(target.split('-').count())
            .any(|parts| parts.join("-") == target)
}

/// The logical allowance a window measures, independent of which account
/// reported it. Two accounts' windows pool only when their families match:
/// a model-scoped Fable window is capacity for Fable, never for Opus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUsageFamily {
    pub key: String,
    pub label: String,
    pub scope: ProviderUsageScope,
    pub scope_target: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageErrorKind {
    UsageProbeAuthentication,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageWindow {
    pub id: String,
    pub label: String,
    pub scope: ProviderUsageScope,
    pub scope_target: Option<String>,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub resets_at: Option<i64>,
    pub reset_at_text: Option<String>,
    pub window_duration_mins: Option<i32>,
}

impl ProviderUsageWindow {
    /// Whether this window still speaks for the account at `now`.
    ///
    /// A window whose reset time has passed has rolled over since it was
    /// measured, so its number is history rather than evidence. `captured_at`
    /// is the snapshot's own capture time, which only the reset-less legacy
    /// `claude` window needs: those predate persisted `resets_at` but carry the
    /// same five-minute bounded quarantine contract.
    pub fn is_live_at(&self, captured_at: i64, now: i64) -> bool {
        match self.resets_at {
            Some(resets_at) => resets_at > now,
            None if self.id == "claude" => captured_at + 5 * 60 > now,
            None => true,
        }
    }

    /// Whether this window constrains the requested model. A window with no
    /// model scope constrains every model; asking without a model in hand takes
    /// every window into account.
    pub fn matches_model(&self, requested_model: Option<&str>) -> bool {
        self.scope_target.as_deref().is_none_or(|target| {
            requested_model.is_none_or(|model| model_scope_matches(target, model))
        })
    }

    /// The pooled allowance this window is a measurement of.
    ///
    /// The key has to be stable across accounts and across providers that name
    /// the same allowance differently — Claude reports `session`/`weekly_all`/
    /// `weekly_scoped-fable` where Codex reports `primary`/`secondary` — so it
    /// is derived from what the window measures rather than from its id.
    pub fn family(&self) -> ProviderUsageFamily {
        let (key, label) = match (self.scope, self.scope_target.as_deref()) {
            (ProviderUsageScope::Session, _) => {
                ("session".to_string(), "Current session".to_string())
            }
            (ProviderUsageScope::Weekly, None) => (
                "weekly".to_string(),
                "Current week (all models)".to_string(),
            ),
            (ProviderUsageScope::Weekly, Some(model)) => (
                format!("weekly:{}", slugify(model)),
                format!("Current week ({model})"),
            ),
            (ProviderUsageScope::RollingWindow, _) => (
                match self.window_duration_mins {
                    Some(mins) => format!("rolling:{mins}"),
                    None => format!("rolling:{}", slugify(&self.id)),
                },
                self.label.clone(),
            ),
            (ProviderUsageScope::Custom, _) => {
                (format!("custom:{}", slugify(&self.id)), self.label.clone())
            }
        };
        ProviderUsageFamily {
            key,
            label,
            scope: self.scope,
            scope_target: self.scope_target.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCreditsSnapshot {
    pub balance: Option<f64>,
    pub total_granted: Option<f64>,
    pub total_used: Option<f64>,
    pub currency: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageResetCredit {
    pub expires_at: Option<i64>,
    pub expires_at_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageResetCredits {
    pub available_count: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credits: Vec<ProviderUsageResetCredit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageResetResult {
    pub outcome: String,
    pub snapshot: ProviderUsageSnapshot,
}

/// One model's recorded usage for a metered backend: its real billed cost over
/// the snapshot's scope, plus billable tokens and run count when known.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelUsageRow {
    pub model: String,
    pub cost_usd: f64,
    pub tokens: Option<i64>,
    pub runs: Option<i64>,
}

/// Derives `Default` so adding an optional field never forces edits at every
/// struct-literal construction site; new optional fields fill in via
/// `..Default::default()` only where they matter.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageSnapshot {
    pub backend: String,
    pub source: String,
    pub captured_at: i64,
    pub windows: Vec<ProviderUsageWindow>,
    pub credits: Option<ProviderCreditsSnapshot>,
    pub reset_credits: Option<ProviderUsageResetCredits>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<ProviderUsageErrorKind>,
    pub unsupported_reason: Option<String>,
    pub raw: Option<Value>,
    /// Per-model usage breakdown for metered backends (OpenRouter). `Some(vec![])`
    /// means "this is a breakdown-style snapshot with no usage yet" (drives the
    /// empty state); `None` (the default) means the snapshot carries no breakdown
    /// at all, keeping window-based Claude/Codex JSON unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_breakdown: Option<Vec<ProviderModelUsageRow>>,
}

impl ProviderUsageSnapshot {
    pub fn unsupported(backend: &str, source: &str, reason: impl Into<String>) -> Self {
        Self {
            backend: backend.to_string(),
            source: source.to_string(),
            captured_at: 0,
            windows: Vec::new(),
            credits: None,
            reset_credits: None,
            error: None,
            error_kind: None,
            unsupported_reason: Some(reason.into()),
            raw: None,
            model_breakdown: None,
        }
    }

    pub fn error_with_kind(
        backend: &str,
        source: &str,
        kind: ProviderUsageErrorKind,
        message: impl Into<String>,
        raw: Option<Value>,
    ) -> Self {
        let mut snapshot = Self::error(backend, source, message, raw);
        snapshot.error_kind = Some(kind);
        snapshot
    }

    pub fn error(
        backend: &str,
        source: &str,
        message: impl Into<String>,
        raw: Option<Value>,
    ) -> Self {
        Self {
            backend: backend.to_string(),
            source: source.to_string(),
            captured_at: 0,
            windows: Vec::new(),
            credits: None,
            reset_credits: None,
            error: Some(message.into()),
            error_kind: None,
            unsupported_reason: None,
            raw,
            model_breakdown: None,
        }
    }

    /// Whether this snapshot carries anything the usage panel can display.
    fn has_measurement(&self) -> bool {
        !self.windows.is_empty() || self.model_breakdown.is_some() || self.credits.is_some()
    }

    /// Relative richness of this snapshot for the Backends usage panel.
    ///
    /// Claude's usage endpoint and Codex's rate-limit report carry the
    /// canonical 5-hour + weekly windows; the live Claude `rate_limit_event`
    /// only carries a single coarse status window. The store path uses this so a coarse live snapshot never
    /// downgrades a richer one already on display, keeping the panel's shape
    /// stable regardless of which source last fired. An error or unsupported
    /// result that measured nothing ranks lowest, so a real snapshot always
    /// wins over a failed probe.
    ///
    /// An error recorded *alongside* a measurement is a different thing: it
    /// describes the most recent attempt, not the windows it is carried with,
    /// and must not demote what those windows are worth. Without that
    /// distinction a retained snapshot would be knocked back to rank 0 by the
    /// failure stamped on it and the next coarse event would erase it.
    pub fn panel_rank(&self) -> u8 {
        if !self.has_measurement() && (self.error.is_some() || self.unsupported_reason.is_some()) {
            return 0;
        }
        match self.source.as_str() {
            "claude_usage_oauth" | "codex_rate_limits" => 2,
            _ => 1,
        }
    }
}

// ============================================================================
// Metering: what one call consumed, and what one unit of it costs
// ============================================================================
//
// A price and a usage count are about the same thing, so they share one
// description of it, the [`Meter`]. A model priced per output image token and a
// call that consumed output image tokens name the same meter, which is what
// lets one be multiplied by the other without a per-provider table in between.

/// The unit a quantity is counted, and a price is charged, in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BillingUnit {
    Tokens,
    Images,
    Characters,
    Seconds,
    Requests,
}

impl BillingUnit {
    /// The singular noun for one unit, as a price is quoted ("$0.04/image").
    pub fn noun(self) -> &'static str {
        match self {
            Self::Tokens => "token",
            Self::Images => "image",
            Self::Characters => "character",
            Self::Seconds => "second",
            Self::Requests => "request",
        }
    }
}

/// What a quantity is charged as. Input that was served from a cache is its own
/// charge rather than a kind of input, because providers price it separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Charge {
    Input,
    Output,
    CacheRead,
    CacheWrite,
    Reasoning,
    Request,
    WebSearch,
}

/// What one count or price is about: the thing charged, the modality when the
/// provider distinguishes one (an output image billed per token), and the unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Meter {
    pub charge: Charge,
    /// `None` when the provider does not distinguish a modality for this
    /// charge, which for tokens is the ordinary case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modality: Option<Modality>,
    pub unit: BillingUnit,
}

impl Meter {
    /// A meter counting tokens of any modality.
    pub fn tokens(charge: Charge) -> Self {
        Self {
            charge,
            modality: None,
            unit: BillingUnit::Tokens,
        }
    }
}

/// A quantity counted on one meter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Measure {
    pub meter: Meter,
    pub quantity: u64,
}

/// What one call consumed, in the units it was counted in.
///
/// Only what the provider actually reported is measured. A meter that is absent
/// was not reported, which is not the same as having consumed nothing on it.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeteredUsage {
    #[serde(default)]
    pub measures: Vec<Measure>,
    /// What the provider said the call cost, when it said.
    #[serde(default)]
    pub cost_usd: Option<f64>,
}

impl MeteredUsage {
    /// The total counted as `charge` in `unit`, across every modality, or `None`
    /// when nothing was measured on it.
    pub fn quantity(&self, charge: Charge, unit: BillingUnit) -> Option<u64> {
        self.measures
            .iter()
            .filter(|measure| measure.meter.charge == charge && measure.meter.unit == unit)
            .map(|measure| measure.quantity)
            .reduce(u64::saturating_add)
    }

    /// The whole prompt: every token charged on the way in, cached or not.
    /// This is what a provider means by "prompt tokens", and what the responses
    /// journal records as a call's input.
    pub fn prompt_tokens(&self) -> Option<u64> {
        [Charge::Input, Charge::CacheRead, Charge::CacheWrite]
            .into_iter()
            .filter_map(|charge| self.quantity(charge, BillingUnit::Tokens))
            .reduce(u64::saturating_add)
    }

    /// This usage with its cost replaced: for a caller that learns the cost
    /// from somewhere other than the usage body, or knows nobody is charging.
    pub fn with_cost(self, cost_usd: Option<f64>) -> Self {
        Self { cost_usd, ..self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_meter_serializes_its_modality_only_when_it_names_one() {
        assert_eq!(
            serde_json::to_value(Meter::tokens(Charge::CacheRead)).unwrap(),
            serde_json::json!({"charge": "cacheRead", "unit": "tokens"})
        );
        let image_out = Meter {
            charge: Charge::Output,
            modality: Some(Modality::Image),
            unit: BillingUnit::Tokens,
        };
        let json = serde_json::to_value(&image_out).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"charge": "output", "modality": "image", "unit": "tokens"})
        );
        assert_eq!(serde_json::from_value::<Meter>(json).unwrap(), image_out);
    }

    #[test]
    fn a_quantity_sums_its_charge_across_modalities_and_is_absent_when_unmeasured() {
        let usage = MeteredUsage {
            measures: vec![
                Measure {
                    meter: Meter::tokens(Charge::Output),
                    quantity: 40,
                },
                Measure {
                    meter: Meter {
                        charge: Charge::Output,
                        modality: Some(Modality::Image),
                        unit: BillingUnit::Tokens,
                    },
                    quantity: 1290,
                },
                Measure {
                    meter: Meter {
                        charge: Charge::Output,
                        modality: Some(Modality::Image),
                        unit: BillingUnit::Images,
                    },
                    quantity: 1,
                },
                Measure {
                    meter: Meter::tokens(Charge::CacheRead),
                    quantity: 0,
                },
            ],
            cost_usd: None,
        };
        assert_eq!(
            usage.quantity(Charge::Output, BillingUnit::Tokens),
            Some(1330)
        );
        assert_eq!(usage.quantity(Charge::Output, BillingUnit::Images), Some(1));
        // Reported as zero is a measurement; never reported is not.
        assert_eq!(
            usage.quantity(Charge::CacheRead, BillingUnit::Tokens),
            Some(0)
        );
        assert_eq!(usage.quantity(Charge::Input, BillingUnit::Tokens), None);
        assert_eq!(
            MeteredUsage::default().quantity(Charge::Output, BillingUnit::Tokens),
            None
        );
    }

    #[test]
    fn the_prompt_is_every_input_side_token() {
        let measure = |charge, quantity| Measure {
            meter: Meter::tokens(charge),
            quantity,
        };
        let usage = MeteredUsage {
            measures: vec![
                measure(Charge::Input, 12),
                measure(Charge::CacheRead, 900),
                measure(Charge::CacheWrite, 100),
                measure(Charge::Output, 5),
            ],
            cost_usd: Some(0.5),
        };
        assert_eq!(usage.prompt_tokens(), Some(1012));
        assert_eq!(MeteredUsage::default().prompt_tokens(), None);
        assert_eq!(usage.with_cost(None).cost_usd, None);
    }

    #[test]
    fn unsupported_and_error_snapshots_do_not_claim_a_capture_time() {
        assert_eq!(
            ProviderUsageSnapshot::unsupported("ollama", "ollama_usage", "unsupported").captured_at,
            0
        );
        assert_eq!(
            ProviderUsageSnapshot::error("claude", "claude_usage", "failed", None).captured_at,
            0
        );
    }

    fn snapshot(source: &str) -> ProviderUsageSnapshot {
        ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: source.to_string(),
            captured_at: 0,
            windows: Vec::new(),
            credits: None,
            reset_credits: None,
            error: None,
            error_kind: None,
            unsupported_reason: None,
            raw: None,
            model_breakdown: None,
        }
    }

    #[test]
    fn model_breakdown_serializes_camel_case_and_omits_when_absent() {
        let mut snap = snapshot("openrouter_generation");
        snap.model_breakdown = Some(vec![ProviderModelUsageRow {
            model: "anthropic/claude".to_string(),
            cost_usd: 0.1234,
            tokens: Some(2000),
            runs: Some(3),
        }]);
        let json = serde_json::to_value(&snap).unwrap();
        assert_eq!(json["modelBreakdown"][0]["model"], "anthropic/claude");
        assert_eq!(json["modelBreakdown"][0]["costUsd"], 0.1234);
        assert_eq!(json["modelBreakdown"][0]["tokens"], 2000);
        assert_eq!(json["modelBreakdown"][0]["runs"], 3);

        // A window-based snapshot omits the key entirely (Claude/Codex unchanged).
        let without = snapshot("claude_usage_oauth");
        let json = serde_json::to_value(&without).unwrap();
        assert!(json.get("modelBreakdown").is_none());
    }

    fn test_window(
        id: &str,
        scope: ProviderUsageScope,
        scope_target: Option<&str>,
    ) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: id.to_string(),
            label: format!("{id} label"),
            scope,
            scope_target: scope_target.map(str::to_string),
            used_percent: 0.0,
            remaining_percent: 100.0,
            resets_at: Some(9_999),
            reset_at_text: None,
            window_duration_mins: None,
        }
    }

    #[test]
    fn family_keys_are_stable_across_providers_that_name_windows_differently() {
        assert_eq!(
            test_window("session", ProviderUsageScope::Session, None)
                .family()
                .key,
            "session"
        );
        let weekly = test_window("weekly_all", ProviderUsageScope::Weekly, None).family();
        assert_eq!(weekly.key, "weekly");
        assert_eq!(weekly.label, "Current week (all models)");
        // Codex calls the same allowance `secondary`; the key has to agree so
        // the two providers' windows are classified by what they measure.
        assert_eq!(
            test_window("secondary", ProviderUsageScope::Weekly, None)
                .family()
                .key,
            "weekly"
        );

        let fable = test_window(
            "weekly_scoped-fable",
            ProviderUsageScope::Weekly,
            Some("Fable"),
        )
        .family();
        assert_eq!(fable.key, "weekly:fable");
        assert_eq!(fable.label, "Current week (Fable)");
        assert_eq!(fable.scope_target.as_deref(), Some("Fable"));

        let mut rolling = test_window("primary", ProviderUsageScope::RollingWindow, None);
        rolling.window_duration_mins = Some(300);
        rolling.label = "5-hour window".to_string();
        assert_eq!(rolling.family().key, "rolling:300");
        assert_eq!(rolling.family().label, "5-hour window");

        // Two rolling windows of different lengths are different allowances.
        let mut hourly = rolling.clone();
        hourly.window_duration_mins = Some(60);
        assert_ne!(hourly.family().key, rolling.family().key);
    }

    #[test]
    fn a_model_scoped_window_constrains_its_own_family_only() {
        let fable = test_window(
            "weekly_scoped-fable",
            ProviderUsageScope::Weekly,
            Some("Fable"),
        );
        assert!(fable.matches_model(Some("claude-fable-1")));
        assert!(fable.matches_model(Some("Fable")));
        assert!(!fable.matches_model(Some("claude-opus-4-1")));
        // Asking without a model in hand takes every window into account.
        assert!(fable.matches_model(None));
        // A window with no model scope constrains everything.
        assert!(test_window("weekly_all", ProviderUsageScope::Weekly, None)
            .matches_model(Some("claude-opus-4-1")));
    }

    #[test]
    fn an_empty_model_scope_never_constrains_a_launch() {
        assert!(!model_scope_matches("", "opus"));
        assert!(!model_scope_matches("Fable", ""));
    }

    #[test]
    fn a_rolled_over_window_stops_speaking_for_the_account() {
        let mut window = test_window("session", ProviderUsageScope::Session, None);
        window.resets_at = Some(1_000);
        assert!(window.is_live_at(0, 999));
        assert!(!window.is_live_at(0, 1_000));

        // A reset-less window with no expiry contract stays live.
        window.resets_at = None;
        assert!(window.is_live_at(0, 10_000));

        // The legacy terminal-429 `claude` window predates persisted resets and
        // carries a five-minute bounded quarantine instead.
        let mut legacy = test_window("claude", ProviderUsageScope::RollingWindow, None);
        legacy.resets_at = None;
        assert!(legacy.is_live_at(0, 299));
        assert!(!legacy.is_live_at(0, 300));
    }

    #[test]
    fn panel_rank_prefers_rich_probe_sources_over_coarse_live() {
        assert_eq!(snapshot("claude_usage_oauth").panel_rank(), 2);
        assert_eq!(snapshot("codex_rate_limits").panel_rank(), 2);
        assert_eq!(snapshot("claude_rate_limit_event").panel_rank(), 1);
    }

    #[test]
    fn a_failure_recorded_beside_real_windows_keeps_their_rank() {
        let mut retained = snapshot("claude_usage_oauth");
        retained.windows = vec![test_window("session", ProviderUsageScope::Session, None)];
        retained.error = Some("the last refresh failed".to_string());
        // Otherwise the failure stamped onto a retained snapshot would demote
        // it, and the next coarse live event would erase the windows it holds.
        assert_eq!(retained.panel_rank(), 2);
    }

    #[test]
    fn panel_rank_zero_for_error_and_unsupported() {
        assert_eq!(
            ProviderUsageSnapshot::error("claude", "claude_usage_oauth", "boom", None).panel_rank(),
            0
        );
        assert_eq!(
            ProviderUsageSnapshot::unsupported("claude", "claude_usage_oauth", "nope").panel_rank(),
            0
        );
    }
}
