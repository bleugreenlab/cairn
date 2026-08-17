//! OpenCode Go provider usage snapshot producer.
//!
//! Two independent measurements compose one snapshot, and keeping them
//! independent is the point:
//!
//! - **Live windows** come from Go's own `usage` endpoint, which is the only
//!   thing that knows the subscription's real headroom.
//! - **The model breakdown** comes from Cairn's own recorded metered cost
//!   (`events.cost_usd`, via the analytics layer), which is a real measurement of
//!   this workspace's Go usage no matter what the provider exposes.
//!
//! Neither is derived from the other. Cairn's recorded spend is not the
//! provider's headroom — other clients share the same subscription — so a failed
//! usage fetch leaves `windows` empty and lets the card fall back to Go's
//! published policy, rather than dressing up analytics as headroom. A wrong
//! number about someone's spending is not a small error.

use super::{opencode_api_key, OPENCODE_BACKEND_KEY};
use crate::models::{
    ProviderModelUsageRow, ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow,
};
use crate::orchestrator::Orchestrator;
use cairn_analytics::{self as analytics, types::Scope, types::TimeRange};
use serde::Deserialize;
use std::time::Duration;

/// Snapshot source tag for the Go usage snapshot.
const OPENCODE_USAGE_SOURCE: &str = "opencode_go_usage";

const USAGE_URL: &str = "https://opencode.ai/zen/go/v1/usage";

/// The usage fetch is a settings-screen refresh, not a background timer, so it
/// is bounded tightly: a provider that is slow to answer should fall back to the
/// published policy quickly rather than hold the card.
const USAGE_TIMEOUT: Duration = Duration::from_secs(10);

// === Wire shape ===
//
// Captured from the live endpoint with a subscribed key on 2026-08-16:
//
// ```json
// {"usage":{
//   "rolling":{"status":"ok","percent":0,"resetsAt":"2026-08-17T03:08:13.701Z"},
//   "weekly":{"status":"ok","percent":40,"resetsAt":"2026-08-17T00:00:00.701Z"},
//   "monthly":{"status":"ok","percent":20,"resetsAt":"2026-09-15T20:44:06.701Z"}}}
// ```
//
// The endpoint reports PERCENTAGES, not the dollar figures Go's published policy
// is written in, so there is no division to do and no limit to divide by. That
// `percent` means *used* rather than *remaining* is not an assumption: against
// the published $30/week and $60/month limits, one spend of ~$12 is exactly 40%
// and 20% respectively, which is what the capture shows. Read as "remaining",
// the same capture would claim a freshly-reset 5-hour window was fully consumed.

#[derive(Debug, Deserialize)]
struct UsageResponse {
    /// Required, deliberately. With a default, an error body — which is what
    /// both the unauthenticated and unsubscribed responses are — would
    /// deserialize into zero windows and read as "nothing to report" instead of
    /// "we could not ask", quietly presenting a full subscription as an empty
    /// one.
    usage: UsageWindows,
}

#[derive(Debug, Default, Deserialize)]
struct UsageWindows {
    #[serde(default)]
    rolling: Option<UsageWindow>,
    #[serde(default)]
    weekly: Option<UsageWindow>,
    #[serde(default)]
    monthly: Option<UsageWindow>,
}

#[derive(Debug, Deserialize)]
struct UsageWindow {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    percent: Option<f64>,
    #[serde(default, rename = "resetsAt")]
    resets_at: Option<String>,
}

/// One window's identity, label, and known duration.
struct WindowShape {
    id: &'static str,
    label: &'static str,
    scope: ProviderUsageScope,
    duration_mins: i32,
}

const ROLLING: WindowShape = WindowShape {
    id: "rolling",
    label: "5 hours",
    scope: ProviderUsageScope::RollingWindow,
    duration_mins: 5 * 60,
};
const WEEKLY: WindowShape = WindowShape {
    id: "weekly",
    label: "Week",
    scope: ProviderUsageScope::Weekly,
    duration_mins: 7 * 24 * 60,
};
const MONTHLY: WindowShape = WindowShape {
    id: "monthly",
    label: "Month",
    // The window contract has no monthly scope; `Custom` carries the duration
    // rather than mislabelling a month as a week.
    scope: ProviderUsageScope::Custom,
    duration_mins: 30 * 24 * 60,
};

/// Map one captured window onto the percentage-based contract.
///
/// A window the provider did not report `ok` for, or reported without a
/// percentage, is dropped rather than rendered as 0% used — which would read as
/// "you have spent nothing", the most misleading answer available.
fn map_window(shape: &WindowShape, window: Option<UsageWindow>) -> Option<ProviderUsageWindow> {
    let window = window?;
    if window
        .status
        .as_deref()
        .is_some_and(|status| status != "ok")
    {
        return None;
    }
    let used_percent = window.percent?.clamp(0.0, 100.0);
    let resets_at = window
        .resets_at
        .as_deref()
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
        .map(|parsed| parsed.timestamp());
    Some(ProviderUsageWindow {
        id: shape.id.to_string(),
        label: shape.label.to_string(),
        scope: shape.scope.clone(),
        scope_target: None,
        used_percent,
        remaining_percent: 100.0 - used_percent,
        resets_at,
        // Keep the raw timestamp only when it could not be parsed, so nothing
        // observed is silently dropped.
        reset_at_text: match resets_at {
            Some(_) => None,
            None => window.resets_at,
        },
        window_duration_mins: Some(shape.duration_mins),
    })
}

/// Parse a usage response body into windows. Pure, so the contract is testable
/// without credentials or a network.
pub(crate) fn parse_usage_windows(body: &str) -> Result<Vec<ProviderUsageWindow>, String> {
    let response: UsageResponse = serde_json::from_str(body)
        .map_err(|error| format!("OpenCode Go usage response could not be read: {error}"))?;
    Ok([
        map_window(&ROLLING, response.usage.rolling),
        map_window(&WEEKLY, response.usage.weekly),
        map_window(&MONTHLY, response.usage.monthly),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// Fetch the subscription's live windows.
///
/// Every failure mode — no key, auth, entitlement, timeout, transport, or a body
/// that does not match the captured contract — returns `Err` and leaves the
/// caller with no windows. None of them may invent a number.
fn fetch_usage_windows(api_key: &str) -> Result<Vec<ProviderUsageWindow>, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(USAGE_TIMEOUT)
        .build()
        .map_err(|error| format!("OpenCode Go usage client failed: {error}"))?;
    let response = client
        .get(USAGE_URL)
        .bearer_auth(api_key)
        .send()
        .map_err(|error| format!("OpenCode Go usage request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("OpenCode Go usage body failed: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "OpenCode Go usage returned HTTP {}: {}",
            status.as_u16(),
            crate::backends::openai_compat::http::upstream_error_detail(&body)
        ));
    }
    parse_usage_windows(&body)
}

/// Build the OpenCode Go usage snapshot: the subscription's live rolling windows
/// when the provider answers, plus the real metered cost Cairn recorded for Go
/// runs (workspace-wide, all-time).
///
/// An empty breakdown (`Some(vec![])`) is a real snapshot meaning "no recorded
/// Go usage yet", not an unsupported result. Empty `windows` means the provider
/// did not tell us its headroom this time; the card then states Go's published
/// policy as the policy it is, which is honest in a way a stale or inferred
/// number would not be.
pub async fn collect_opencode_usage_snapshot(orch: &Orchestrator) -> ProviderUsageSnapshot {
    let Some(api_key) = opencode_api_key(orch) else {
        return ProviderUsageSnapshot::unsupported(
            OPENCODE_BACKEND_KEY,
            OPENCODE_USAGE_SOURCE,
            "Connect an OpenCode account to see usage.",
        );
    };

    // A blocking HTTP client must not run on the async runtime's thread.
    let windows = match tokio::task::spawn_blocking(move || fetch_usage_windows(&api_key)).await {
        Ok(Ok(windows)) => windows,
        Ok(Err(error)) => {
            // Not a snapshot error: the model breakdown below is still a real
            // measurement, and surfacing this as a failure would hide it behind
            // an error banner. The card falls back to published policy.
            log::warn!("OpenCode Go live usage unavailable: {error}");
            Vec::new()
        }
        Err(error) => {
            log::warn!("OpenCode Go usage fetch panicked: {error}");
            Vec::new()
        }
    };

    let rows = match analytics::provider_model_costs(
        &orch.db.local,
        OPENCODE_BACKEND_KEY,
        &Scope::new(None),
        &TimeRange::default(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return ProviderUsageSnapshot {
                backend: OPENCODE_BACKEND_KEY.to_string(),
                source: OPENCODE_USAGE_SOURCE.to_string(),
                captured_at: chrono::Utc::now().timestamp(),
                // Live windows survive an analytics failure, for the same reason
                // the breakdown survives a usage-fetch failure: they measure
                // different things.
                windows,
                error: Some(format!("Failed to load OpenCode Go usage: {err}")),
                ..Default::default()
            };
        }
    };

    let model_breakdown: Vec<ProviderModelUsageRow> = rows
        .into_iter()
        .map(|row| ProviderModelUsageRow {
            model: row.model,
            cost_usd: row.cost_usd,
            tokens: Some(row.billable_tokens),
            runs: Some(row.runs),
        })
        .collect();

    ProviderUsageSnapshot {
        backend: OPENCODE_BACKEND_KEY.to_string(),
        source: OPENCODE_USAGE_SOURCE.to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        credits: None,
        reset_credits: None,
        error: None,
        unsupported_reason: None,
        raw: None,
        model_breakdown: Some(model_breakdown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact body the live endpoint returned for a subscribed account.
    const CAPTURED: &str = r#"{"usage":{
        "rolling":{"status":"ok","percent":0,"resetsAt":"2026-08-17T03:08:13.701Z"},
        "weekly":{"status":"ok","percent":40,"resetsAt":"2026-08-17T00:00:00.701Z"},
        "monthly":{"status":"ok","percent":20,"resetsAt":"2026-09-15T20:44:06.701Z"}}}"#;

    #[test]
    fn the_captured_response_maps_to_exact_percentages_and_resets() {
        let windows = parse_usage_windows(CAPTURED).expect("the captured body parses");
        assert_eq!(windows.len(), 3);

        // `percent` is what has been USED. Against Go's published $30/week and
        // $60/month, one ~$12 spend is exactly 40% and 20% — which is what makes
        // this reading verifiable rather than assumed.
        assert_eq!(windows[0].id, "rolling");
        assert_eq!(windows[0].used_percent, 0.0);
        assert_eq!(windows[0].remaining_percent, 100.0);
        assert_eq!(windows[0].window_duration_mins, Some(300));

        assert_eq!(windows[1].id, "weekly");
        assert_eq!(windows[1].used_percent, 40.0);
        assert_eq!(windows[1].remaining_percent, 60.0);
        assert_eq!(windows[1].window_duration_mins, Some(10_080));

        assert_eq!(windows[2].id, "monthly");
        assert_eq!(windows[2].used_percent, 20.0);
        assert_eq!(windows[2].remaining_percent, 80.0);
        assert_eq!(windows[2].window_duration_mins, Some(43_200));

        // 2026-08-17T03:08:13.701Z
        assert_eq!(windows[0].resets_at, Some(1_786_936_093));
        assert!(windows[0].reset_at_text.is_none());
    }

    #[test]
    fn an_over_limit_percentage_clamps_instead_of_reporting_negative_headroom() {
        // A window can be overspent. "-7% remaining" is not a thing the card can
        // draw, and an unclamped bar would render past its own track.
        let windows = parse_usage_windows(
            r#"{"usage":{"weekly":{"status":"ok","percent":107,"resetsAt":null}}}"#,
        )
        .expect("parses");
        assert_eq!(windows[0].used_percent, 100.0);
        assert_eq!(windows[0].remaining_percent, 0.0);
    }

    #[test]
    fn a_window_without_a_percentage_is_omitted_rather_than_shown_as_unused() {
        // Rendering a missing measurement as 0% used would tell the reader they
        // have spent nothing, which is the most misleading answer available.
        let windows = parse_usage_windows(
            r#"{"usage":{"rolling":{"status":"ok","resetsAt":null},
                          "weekly":{"status":"ok","percent":12,"resetsAt":null}}}"#,
        )
        .expect("parses");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].id, "weekly");
    }

    #[test]
    fn a_window_the_provider_did_not_report_ok_for_is_omitted() {
        let windows = parse_usage_windows(
            r#"{"usage":{"weekly":{"status":"unavailable","percent":40,"resetsAt":null}}}"#,
        )
        .expect("parses");
        assert!(windows.is_empty());
    }

    #[test]
    fn an_unparseable_reset_timestamp_is_kept_as_written() {
        let windows = parse_usage_windows(
            r#"{"usage":{"weekly":{"status":"ok","percent":5,"resetsAt":"next tuesday"}}}"#,
        )
        .expect("parses");
        assert_eq!(windows[0].resets_at, None);
        assert_eq!(windows[0].reset_at_text.as_deref(), Some("next tuesday"));
    }

    #[test]
    fn an_error_body_is_a_parse_failure_not_an_empty_success() {
        // The unauthenticated and unsubscribed responses both look like this.
        // Reading either as "no windows" would silently mean "nothing to report"
        // instead of "we could not ask".
        for body in [
            r#"{"type":"error","error":{"type":"AuthError","message":"Missing API key."}}"#,
            r#"{"type":"error","error":{"type":"EntitlementError","message":"OpenCode Go subscription required."}}"#,
            "<html>502</html>",
        ] {
            assert!(
                parse_usage_windows(body).is_err(),
                "{body} must not read as a successful empty snapshot"
            );
        }
    }
}
