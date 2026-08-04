//! Noninteractive Claude subscription usage probe.

use regex::Regex;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;

use crate::identity::ClaudeAuth;
use crate::models::{ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow};
use crate::orchestrator::session::get_claude_path;
use crate::orchestrator::Orchestrator;

pub fn collect_claude_usage_snapshot(
    orch: &Orchestrator,
    account_id: Option<&str>,
) -> ProviderUsageSnapshot {
    let claude = match get_claude_path(&orch.process_state) {
        Ok(path) => path,
        Err(err) => {
            return ProviderUsageSnapshot::unsupported(
                "claude",
                "claude_usage",
                format!("Claude CLI not found: {err}"),
            )
        }
    };
    let identity = match account_id {
        Some(id) => match orch.resolve_provider_account("claude", id) {
            Some(identity) => Some(identity),
            None => {
                return ProviderUsageSnapshot::error(
                    "claude",
                    "claude_usage",
                    format!("Claude account '{id}' is unavailable"),
                    None,
                )
            }
        },
        None => orch.get_identity(),
    };
    let profile = match claude_usage_profile(
        identity
            .as_ref()
            .and_then(|identity| identity.claude_auth.as_ref()),
        account_id,
    ) {
        Ok(profile) => profile,
        Err(reason) => return ProviderUsageSnapshot::unsupported("claude", "claude_usage", reason),
    };
    collect_with_profile(Path::new(&claude), profile)
}

fn claude_usage_profile<'a>(
    auth: Option<&'a ClaudeAuth>,
    account_id: Option<&str>,
) -> Result<Option<&'a Path>, String> {
    match (auth, account_id) {
        (Some(ClaudeAuth::ConfigDir(path)), _) => Ok(Some(path.as_path())),
        (_, Some(id)) => Err(format!(
            "Claude account '{id}' cannot report subscription usage because only Claude profile accounts can scope the CLI usage probe."
        )),
        _ => Ok(None),
    }
}

pub fn collect_with_profile(claude: &Path, profile: Option<&Path>) -> ProviderUsageSnapshot {
    let mut command = Command::new(claude);
    command.args([
        "-p",
        "/usage",
        "--strict-mcp-config",
        "--mcp-config",
        r#"{"mcpServers":{}}"#,
        "--output-format",
        "json",
    ]);
    command
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    if let Some(profile) = profile {
        if let Err(err) = crate::identity::claude_profile::provision_profile(profile) {
            return ProviderUsageSnapshot::error("claude", "claude_usage", err, None);
        }
        command.env("CLAUDE_CONFIG_DIR", profile);
    }

    let output = match command.output() {
        Ok(output) => output,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                "claude",
                "claude_usage",
                format!("Failed to run Claude usage probe: {err}"),
                None,
            )
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let text = serde_json::from_str::<Value>(&stdout)
        .ok()
        .and_then(|value| {
            value
                .get("result")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| stdout.into_owned());
    parse_claude_usage_snapshot(&text)
}

fn parse_claude_usage_snapshot(output: &str) -> ProviderUsageSnapshot {
    let line_re = Regex::new(
        r"(?mi)^\s*(?P<label>Current\s+(?:session|week(?:\s*\([^)]+\))?))\s*:\s*(?P<used>\d+(?:\.\d+)?)\s*%\s*used(?:\s*[·•-]\s*resets?\s*(?P<reset>.+))?\s*$",
    ).expect("valid Claude usage regex");
    let paren_re = Regex::new(r"\((?P<target>[^)]+)\)").expect("valid target regex");
    let windows = line_re
        .captures_iter(output)
        .filter_map(|caps| {
            let label = caps.name("label")?.as_str().trim().to_string();
            let used_percent = caps.name("used")?.as_str().parse::<f64>().ok()?;
            let scope = if label.to_ascii_lowercase().contains("session") {
                ProviderUsageScope::Session
            } else {
                ProviderUsageScope::Weekly
            };
            let scope_target = paren_re
                .captures(&label)
                .and_then(|c| c.name("target"))
                .map(|m| m.as_str().trim().to_string());
            Some(ProviderUsageWindow {
                id: slugify(&label),
                label,
                scope,
                scope_target,
                used_percent,
                remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
                resets_at: None,
                reset_at_text: caps.name("reset").map(|m| m.as_str().trim().to_string()),
                window_duration_mins: None,
            })
        })
        .collect::<Vec<_>>();

    if windows.is_empty() {
        return ProviderUsageSnapshot::error(
            "claude",
            "claude_usage",
            "Claude CLI did not return subscription usage windows. Check profile authentication.",
            Some(json!({ "output": output })),
        );
    }
    ProviderUsageSnapshot {
        backend: "claude".into(),
        source: "claude_usage".into(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        raw: Some(json!({ "output": output })),
        ..Default::default()
    }
}

fn slugify(label: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_usage_accounts_require_a_profile() {
        let api_key = ClaudeAuth::ApiKey("key".into());
        let oauth = ClaudeAuth::OAuthToken("token".into());
        assert!(claude_usage_profile(Some(&api_key), Some("api-account")).is_err());
        assert!(claude_usage_profile(Some(&oauth), Some("oauth-account")).is_err());
        assert!(claude_usage_profile(None, Some("local-cli")).is_err());

        let profile = ClaudeAuth::ConfigDir("/tmp/claude-profile".into());
        assert_eq!(
            claude_usage_profile(Some(&profile), Some("profile-account")).unwrap(),
            Some(Path::new("/tmp/claude-profile"))
        );
    }
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn usage_probe_sets_selected_profile_directory() {
        let temp = tempfile::tempdir().unwrap();
        let profile = temp.path().join("selected-profile");
        let script = temp.path().join("claude");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf 'Current session: 1%% used\\nPROFILE=%s\\n' \"$CLAUDE_CONFIG_DIR\"\n",
        ).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script, permissions).unwrap();

        let snapshot = collect_with_profile(&script, Some(&profile));
        assert!(snapshot.error.is_none());
        assert!(snapshot.raw.unwrap()["output"]
            .as_str()
            .unwrap()
            .contains(profile.to_str().unwrap()));
    }

    #[test]
    fn parses_real_subscription_payload_with_model_window() {
        let snapshot = parse_claude_usage_snapshot(concat!(
            "Current session: 80% used · resets Jul 26 at 7:50pm (America/Los_Angeles)\n",
            "Current week (all models): 6% used · resets Jul 30 at 1am (America/Los_Angeles)\n",
            "Current week (Fable): 4% used · resets Jul 30 at 12:59am (America/Los_Angeles)\n",
        ));
        assert!(snapshot.error.is_none());
        assert_eq!(snapshot.windows.len(), 3);
        assert_eq!(snapshot.windows[0].remaining_percent, 20.0);
        assert_eq!(snapshot.windows[2].scope_target.as_deref(), Some("Fable"));
    }

    #[test]
    fn logged_out_cost_summary_is_unknown_not_zero_usage() {
        let snapshot = parse_claude_usage_snapshot("Total cost: $0.0000\nTotal duration (API): 0s");
        assert!(snapshot.windows.is_empty());
        assert!(snapshot.error.is_some());
    }

    #[test]
    fn requires_percentage_on_a_positive_window_line() {
        let snapshot =
            parse_claude_usage_snapshot("Current session: unavailable\nCurrent week: unknown");
        assert!(snapshot.windows.is_empty());
        assert!(snapshot.error.is_some());
    }
}
