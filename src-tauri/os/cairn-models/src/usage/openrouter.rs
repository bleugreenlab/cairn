//! OpenRouter's account credit balance.

use std::time::Duration;

use serde_json::Value;

use cairn_models_types::usage::ProviderCreditsSnapshot;

/// OpenRouter account credits endpoint.
const OPENROUTER_CREDITS_URL: &str = "https://openrouter.ai/api/v1/credits";

/// Hard ceiling on the best-effort credits fetch; the breakdown never waits on
/// it longer than this.
const CREDITS_TIMEOUT: Duration = Duration::from_secs(8);

/// Best-effort fetch of the OpenRouter account credit balance. Any failure,
/// non-success status, or timeout yields `(None, None)`; a parsed body with no
/// recognizable credit fields yields `(None, Some(body))` so the raw response is
/// still available for inspection.
pub async fn fetch_openrouter_credits(
    api_key: &str,
) -> (Option<ProviderCreditsSnapshot>, Option<Value>) {
    let client = match reqwest::Client::builder().timeout(CREDITS_TIMEOUT).build() {
        Ok(client) => client,
        Err(_) => return (None, None),
    };
    let response = match client
        .get(OPENROUTER_CREDITS_URL)
        .bearer_auth(api_key)
        .header("HTTP-Referer", "https://cairn.computer")
        .header("X-OpenRouter-Title", "Cairn")
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return (None, None),
    };
    if !response.status().is_success() {
        return (None, None);
    }
    let body: Value = match response.json().await {
        Ok(body) => body,
        Err(_) => return (None, None),
    };
    // OpenRouter returns `{ "data": { "total_credits", "total_usage" } }`.
    let data = body.get("data").unwrap_or(&body);
    let total_granted = data.get("total_credits").and_then(Value::as_f64);
    let total_used = data.get("total_usage").and_then(Value::as_f64);
    if total_granted.is_none() && total_used.is_none() {
        return (None, Some(body));
    }
    let balance = match (total_granted, total_used) {
        (Some(granted), Some(used)) => Some(granted - used),
        _ => None,
    };
    (
        Some(ProviderCreditsSnapshot {
            balance,
            total_granted,
            total_used,
            currency: Some("USD".to_string()),
        }),
        Some(body),
    )
}
