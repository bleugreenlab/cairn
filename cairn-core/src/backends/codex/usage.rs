//! Codex provider usage snapshot collector.
//!
//! Drives the `codex app-server` handshake to read the account rate limits and
//! render them as a [`ProviderUsageSnapshot`] (source `codex_rate_limits`). This
//! is the manual probe behind the Providers settings card; the rate-limit
//! parsing itself is shared with the live streamed-event path through
//! [`codex_rate_limit_snapshot_from_value`].

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use super::app_server::AppServerClient;
use super::events::codex_rate_limit_snapshot_from_value;
use super::refresh_codex_oauth_tokens_for_current_account;
use crate::env::find_binary;
use crate::identity::CodexAuth;
use crate::models::ProviderUsageSnapshot;
use crate::orchestrator::Orchestrator;

pub fn collect_codex_usage_snapshot(orch: &Orchestrator) -> ProviderUsageSnapshot {
    let codex_path = match find_binary("codex") {
        Ok(path) => path,
        Err(err) => {
            return ProviderUsageSnapshot::unsupported(
                "codex",
                "codex_rate_limits",
                format!("Codex CLI not found: {err}"),
            );
        }
    };

    let mut env = HashMap::new();
    let mut temp_home = None;
    let mut uses_codex_oauth = false;
    if let Some(identity) = orch.get_identity() {
        match identity.codex_auth {
            Some(CodexAuth::OAuthToken(auth_json)) => match prepare_codex_auth_home(&auth_json) {
                Ok(home) => {
                    env.insert("CODEX_HOME".to_string(), home.to_string_lossy().to_string());
                    temp_home = Some(home);
                    uses_codex_oauth = true;
                }
                Err(err) => {
                    return ProviderUsageSnapshot::error("codex", "codex_rate_limits", err, None);
                }
            },
            Some(CodexAuth::ApiKey(key)) => {
                env.insert("OPENAI_API_KEY".to_string(), key);
            }
            None => {}
        }
    }

    let cwd = temp_home
        .as_ref()
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| orch.config_dir.to_string_lossy().to_string());

    let snapshot = (|| -> Result<ProviderUsageSnapshot, String> {
        let client = Arc::new(
            AppServerClient::spawn(orch.services.process.as_ref(), &codex_path, &env, &cwd)
                .map_err(|e| format!("Failed to start Codex app-server: {e}"))?,
        );
        let stop_notifications = Arc::new(AtomicBool::new(false));
        let notification_thread = uses_codex_oauth.then(|| {
            spawn_codex_usage_refresh_handler(
                (*orch).clone(),
                Arc::clone(&client),
                Arc::clone(&stop_notifications),
            )
        });

        let result = (|| -> Result<ProviderUsageSnapshot, String> {
            client.send_request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "cairn",
                        "title": "Cairn",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {
                        "experimentalApi": true,
                    }
                }),
            )?;
            client.send_notification("initialized", json!({}))?;

            if env.contains_key("OPENAI_API_KEY") {
                client.send_request(
                    "account/login/start",
                    json!({
                        "type": "apiKey",
                        "apiKey": env.get("OPENAI_API_KEY").cloned().unwrap_or_default(),
                    }),
                )?;
            } else {
                let _ = client.send_request("account/read", json!({ "refreshToken": true }));
            }

            let response = client.send_request("account/rateLimits/read", Value::Null)?;
            let raw = response.get("rateLimits").cloned().unwrap_or(Value::Null);
            // A malformed / missing rate-limit payload surfaces as a clean error
            // snapshot rather than empty windows; the shared parser returns None
            // when `usedPercent` is absent.
            Ok(
                codex_rate_limit_snapshot_from_value(raw.clone()).unwrap_or_else(|| {
                    ProviderUsageSnapshot::error(
                        "codex",
                        "codex_rate_limits",
                        "Codex returned no rate-limit data",
                        Some(raw),
                    )
                }),
            )
        })();

        stop_notifications.store(true, Ordering::Relaxed);
        if let Ok(mut child_guard) = client.child_handle().lock() {
            if let Some(mut child) = child_guard.take() {
                let _ = child.kill();
                let _ = child.try_wait();
            }
        }
        if let Some(handle) = notification_thread {
            let _ = handle.join();
        }

        result
    })();

    if let Some(home) = temp_home {
        let _ = fs::remove_dir_all(home);
    }

    snapshot
        .unwrap_or_else(|err| ProviderUsageSnapshot::error("codex", "codex_rate_limits", err, None))
}

fn spawn_codex_usage_refresh_handler(
    orch: Orchestrator,
    client: Arc<AppServerClient>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let notifications = client.notifications();
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match notifications.recv_timeout(Duration::from_millis(100)) {
                Ok(msg) => {
                    if msg.get("method").and_then(Value::as_str)
                        != Some("account/chatgptAuthTokens/refresh")
                    {
                        continue;
                    }

                    let id_value = msg.get("id").cloned().unwrap_or(Value::Null);
                    match refresh_codex_oauth_tokens_for_current_account(&orch) {
                        Ok(tokens) => {
                            let Some(account_id) = tokens.chatgpt_account_id else {
                                let _ = client.respond_error(
                                    &id_value,
                                    -32000,
                                    "Codex token refresh did not provide a ChatGPT account id; reconnect your Codex account in the Providers settings",
                                );
                                continue;
                            };
                            let _ = client.respond(
                                &id_value,
                                json!({
                                    "accessToken": tokens.access_token,
                                    "chatgptAccountId": account_id,
                                }),
                            );
                        }
                        Err(err) => {
                            let _ = client.respond_error(
                                &id_value,
                                -32000,
                                &format!(
                                    "Codex token refresh failed: {}. Please reconnect your Codex account in the Providers settings.",
                                    err
                                ),
                            );
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    })
}

fn prepare_codex_auth_home(auth_json: &str) -> Result<PathBuf, String> {
    let home = std::env::temp_dir().join(format!("cairn-codex-usage-{}", Uuid::new_v4()));
    fs::create_dir_all(&home).map_err(|e| format!("Failed to create temp Codex home: {e}"))?;
    fs::write(home.join("auth.json"), auth_json)
        .map_err(|e| format!("Failed to write Codex auth.json: {e}"))?;
    Ok(home)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::codex::events::codex_rate_limit_window_label;
    use crate::models::ProviderUsageScope;

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
