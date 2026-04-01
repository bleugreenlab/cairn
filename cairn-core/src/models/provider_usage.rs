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
}
