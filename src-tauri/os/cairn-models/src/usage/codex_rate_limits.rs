//! Codex's account rate limits, as the app-server reports them.
//!
//! Shared by the live streamed-event path and the usage probe, so a window
//! reads the same whichever way it arrived.

use cairn_models_types::usage::{
    ProviderCreditsSnapshot, ProviderUsageResetCredit, ProviderUsageResetCredits,
    ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow,
};
use serde_json::Value;

pub fn codex_rate_limit_snapshot_from_value(raw: Value) -> Option<ProviderUsageSnapshot> {
    let mut windows = Vec::new();
    for id in ["primary", "secondary"] {
        let Some(window) = raw.get(id) else {
            continue;
        };
        if window.is_null() {
            continue;
        }
        let used_percent = window.get("usedPercent").and_then(Value::as_f64)?;
        let window_duration_mins = window
            .get("windowDurationMins")
            .and_then(Value::as_i64)
            .map(|mins| mins as i32);
        windows.push(ProviderUsageWindow {
            id: id.to_string(),
            label: codex_rate_limit_window_label(id, window_duration_mins),
            scope: codex_rate_limit_scope(window_duration_mins),
            scope_target: None,
            used_percent,
            remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
            resets_at: window.get("resetsAt").and_then(Value::as_i64),
            reset_at_text: None,
            window_duration_mins,
        });
    }

    let credits = raw.get("credits").and_then(|value| {
        if value.is_null() {
            return None;
        }
        Some(ProviderCreditsSnapshot {
            balance: value.get("balance").and_then(Value::as_f64),
            total_granted: value.get("totalGranted").and_then(Value::as_f64),
            total_used: value.get("totalUsed").and_then(Value::as_f64),
            currency: value
                .get("currency")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    });
    let reset_credits = codex_reset_credits_from_value(&raw);

    Some(ProviderUsageSnapshot {
        backend: "codex".to_string(),
        source: "codex_rate_limits".to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        credits,
        reset_credits,
        error: None,
        error_kind: None,
        unsupported_reason: None,
        raw: Some(raw),
        model_breakdown: None,
    })
}

fn codex_reset_credits_from_value(raw: &Value) -> Option<ProviderUsageResetCredits> {
    let value = raw
        .get("rateLimitResetCredits")
        .or_else(|| raw.get("rate_limit_reset_credits"))
        .or_else(|| raw.get("resetCredits"))
        .or_else(|| raw.get("reset_credits"))?;
    if value.is_null() {
        return None;
    }

    let available_count = value
        .get("availableCount")
        .or_else(|| value.get("available_count"))
        .and_then(Value::as_i64)
        .or_else(|| value.as_i64())
        .unwrap_or(0);

    let credits = value
        .get("credits")
        .or_else(|| value.get("items"))
        .or_else(|| value.get("available"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(codex_reset_credit_from_value)
                .collect()
        })
        .unwrap_or_default();

    Some(ProviderUsageResetCredits {
        available_count,
        credits,
    })
}

fn codex_reset_credit_from_value(value: &Value) -> Option<ProviderUsageResetCredit> {
    if value.is_null() {
        return None;
    }
    let expires_at = value
        .get("expiresAt")
        .or_else(|| value.get("expires_at"))
        .or_else(|| value.get("expirationTime"))
        .or_else(|| value.get("expiration_time"))
        .and_then(|value| {
            value.as_i64().or_else(|| {
                value.as_str().and_then(|text| {
                    chrono::DateTime::parse_from_rfc3339(text)
                        .ok()
                        .map(|dt| dt.timestamp())
                })
            })
        })
        .or_else(|| {
            value
                .get("expiresInSeconds")
                .or_else(|| value.get("expires_in_seconds"))
                .and_then(Value::as_i64)
                .map(|seconds| chrono::Utc::now().timestamp() + seconds)
        });
    let expires_at_text = value
        .get("expiresAtText")
        .or_else(|| value.get("expires_at_text"))
        .or_else(|| value.get("expirationText"))
        .or_else(|| value.get("expiration_text"))
        .and_then(Value::as_str)
        .map(str::to_string);

    Some(ProviderUsageResetCredit {
        expires_at,
        expires_at_text,
    })
}

fn codex_rate_limit_scope(window_duration_mins: Option<i32>) -> ProviderUsageScope {
    if window_duration_mins == Some(10_080) {
        ProviderUsageScope::Weekly
    } else {
        ProviderUsageScope::RollingWindow
    }
}

pub fn codex_rate_limit_window_label(id: &str, window_duration_mins: Option<i32>) -> String {
    match window_duration_mins {
        Some(10_080) => "Weekly window".to_string(),
        Some(mins) if mins % 60 == 0 => format!("{}-hour window", mins / 60),
        Some(mins) => format!("{}-minute window", mins),
        None => match id {
            "primary" => "Primary window".to_string(),
            "secondary" => "Secondary window".to_string(),
            _ => "Usage window".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rate_limits_snapshot_uses_remaining_capacity_and_credits() {
        let snapshot = codex_rate_limit_snapshot_from_value(json!({
            "primary": {
                "usedPercent": 18.0,
                "resetsAt": 1_700_000_000,
                "windowDurationMins": 300
            },
            "secondary": {
                "usedPercent": 105.0,
                "resetsAt": 1_700_000_500,
                "windowDurationMins": 10080
            },
            "credits": {
                "balance": 12.5,
                "totalGranted": 20.0,
                "totalUsed": 7.5,
                "currency": "USD"
            },
            "rateLimitResetCredits": {
                "availableCount": 2,
                "credits": [
                    { "expiresAt": 1_700_086_400 },
                    { "expiresAtText": "expires in 12 days" }
                ]
            }
        }))
        .expect("snapshot present when usedPercent is set");

        assert_eq!(snapshot.backend, "codex");
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5-hour window");
        assert_eq!(snapshot.windows[0].remaining_percent, 82.0);
        assert_eq!(snapshot.windows[1].scope, ProviderUsageScope::Weekly);
        assert_eq!(snapshot.windows[1].remaining_percent, 0.0);
        assert_eq!(
            snapshot
                .credits
                .as_ref()
                .and_then(|credits| credits.balance),
            Some(12.5)
        );
        let reset_credits = snapshot.reset_credits.expect("reset credits parsed");
        assert_eq!(reset_credits.available_count, 2);
        assert_eq!(reset_credits.credits[0].expires_at, Some(1_700_086_400));
        assert_eq!(
            reset_credits.credits[1].expires_at_text.as_deref(),
            Some("expires in 12 days")
        );
    }

    #[test]
    fn rate_limits_snapshot_is_none_when_used_percent_missing() {
        let snapshot = codex_rate_limit_snapshot_from_value(json!({
            "primary": { "windowDurationMins": 300 }
        }));
        assert!(snapshot.is_none());
    }

    #[test]
    fn rate_limit_labels_are_humanized() {
        assert_eq!(
            codex_rate_limit_window_label("primary", Some(300)),
            "5-hour window"
        );
        assert_eq!(
            codex_rate_limit_window_label("secondary", Some(10_080)),
            "Weekly window"
        );
        assert_eq!(
            codex_rate_limit_window_label("primary", Some(45)),
            "45-minute window"
        );
    }
}
