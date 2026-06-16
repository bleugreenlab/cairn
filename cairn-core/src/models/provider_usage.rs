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
pub struct ProviderUsageSnapshot {
    pub backend: String,
    pub source: String,
    pub captured_at: i64,
    pub windows: Vec<ProviderUsageWindow>,
    pub credits: Option<ProviderCreditsSnapshot>,
    pub error: Option<String>,
    pub unsupported_reason: Option<String>,
    pub raw: Option<Value>,
}

impl ProviderUsageSnapshot {
    pub fn unsupported(backend: &str, source: &str, reason: impl Into<String>) -> Self {
        Self {
            backend: backend.to_string(),
            source: source.to_string(),
            captured_at: chrono::Utc::now().timestamp(),
            windows: Vec::new(),
            credits: None,
            error: None,
            unsupported_reason: Some(reason.into()),
            raw: None,
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
            error: Some(message.into()),
            unsupported_reason: None,
            raw,
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
            error: None,
            unsupported_reason: None,
            raw: None,
        }
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
