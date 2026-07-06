use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageScope {
    Session,
    Weekly,
    RollingWindow,
    Custom,
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
            captured_at: chrono::Utc::now().timestamp(),
            windows: Vec::new(),
            credits: None,
            reset_credits: None,
            error: None,
            unsupported_reason: Some(reason.into()),
            raw: None,
            model_breakdown: None,
        }
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
            captured_at: chrono::Utc::now().timestamp(),
            windows: Vec::new(),
            credits: None,
            reset_credits: None,
            error: Some(message.into()),
            unsupported_reason: None,
            raw,
            model_breakdown: None,
        }
    }

    /// Relative richness of this snapshot for the Backends usage panel.
    ///
    /// The manual probe sources carry the canonical 5-hour + weekly windows;
    /// the live Claude `rate_limit_event` only carries a single coarse status
    /// window. The store path uses this so a coarse live snapshot never
    /// downgrades a richer one already on display, keeping the panel's shape
    /// stable regardless of which source last fired. Errors and unsupported
    /// results rank lowest so a real snapshot always wins over a failed probe.
    pub fn panel_rank(&self) -> u8 {
        if self.error.is_some() || self.unsupported_reason.is_some() {
            return 0;
        }
        match self.source.as_str() {
            "claude_usage_tui" | "codex_rate_limits" => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(source: &str) -> ProviderUsageSnapshot {
        ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: source.to_string(),
            captured_at: 0,
            windows: Vec::new(),
            credits: None,
            reset_credits: None,
            error: None,
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
        let without = snapshot("claude_usage_tui");
        let json = serde_json::to_value(&without).unwrap();
        assert!(json.get("modelBreakdown").is_none());
    }

    #[test]
    fn panel_rank_prefers_rich_probe_sources_over_coarse_live() {
        assert_eq!(snapshot("claude_usage_tui").panel_rank(), 2);
        assert_eq!(snapshot("codex_rate_limits").panel_rank(), 2);
        assert_eq!(snapshot("claude_rate_limit_event").panel_rank(), 1);
    }

    #[test]
    fn panel_rank_zero_for_error_and_unsupported() {
        assert_eq!(
            ProviderUsageSnapshot::error("claude", "claude_usage_tui", "boom", None).panel_rank(),
            0
        );
        assert_eq!(
            ProviderUsageSnapshot::unsupported("claude", "claude_usage_tui", "nope").panel_rank(),
            0
        );
    }
}
