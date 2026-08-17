//! Noninteractive Claude subscription usage probe.

use serde::Deserialize;
use std::path::Path;

use crate::identity::ClaudeAuth;
use crate::models::{ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow};
use crate::orchestrator::Orchestrator;

/// Anthropic's subscription-usage endpoint — the same one the Claude CLI reads
/// to render `/usage`.
///
/// Cairn asks for these numbers directly rather than through the CLI because
/// every indirect route is a dead end: `claude -p /usage` only re-prints a
/// cache in the profile's `.claude.json` without refreshing it, and that cache
/// is written by the interactive TUI — a headless run never populates it, so
/// an account Cairn drives would report nothing forever. The stream's
/// `rate_limit_event` carries a blocked/allowed status and no percentages.
/// This endpoint answers with the windows themselves.
const USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
/// Identifies these snapshots wherever their richness is compared — the panel's
/// canonical-source set and `panel_rank`. Both must know this name, or a full
/// set of windows is treated as provisional and re-requested.
pub const CLAUDE_USAGE_SOURCE: &str = "claude_usage_oauth";
/// The beta header the CLI's OAuth-authenticated calls carry.
const OAUTH_BETA: &str = "oauth-2025-04-20";
const USAGE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// The CLI keys its credential store by profile directory, so a managed profile
/// has its own entry rather than sharing one account across all of them.
///
/// macOS only, like the keychain it names: every other platform reads the
/// credential file beside the profile and never asks for a service name.
#[cfg(target_os = "macos")]
const CREDENTIAL_SERVICE_PREFIX: &str = "Claude Code-credentials-";
/// Where the CLI keeps credentials when there is no OS keychain to use.
const CREDENTIAL_FILE: &str = ".credentials.json";

#[derive(Debug, Deserialize)]
struct StoredCredentials {
    #[serde(rename = "claudeAiOauth")]
    claude_ai_oauth: Option<StoredOAuth>,
}

#[derive(Debug, Deserialize)]
struct StoredOAuth {
    #[serde(rename = "accessToken")]
    access_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UsageResponse {
    /// The presentation-shaped list; the sibling per-window keys carry the same
    /// numbers under rotating internal codenames.
    #[serde(default)]
    limits: Vec<UsageLimit>,
}

#[derive(Debug, Deserialize)]
struct UsageLimit {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    group: String,
    percent: Option<f64>,
    resets_at: Option<String>,
    scope: Option<LimitScope>,
}

#[derive(Debug, Deserialize)]
struct LimitScope {
    model: Option<ScopedModel>,
}

#[derive(Debug, Deserialize)]
struct ScopedModel {
    display_name: Option<String>,
}

pub fn collect_claude_usage_snapshot(
    orch: &Orchestrator,
    account_id: Option<&str>,
) -> ProviderUsageSnapshot {
    let identity = match account_id {
        Some(id) => match orch.resolve_provider_account("claude", id) {
            Some(identity) => Some(identity),
            None => {
                return ProviderUsageSnapshot::error(
                    "claude",
                    CLAUDE_USAGE_SOURCE,
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
        Err(reason) => {
            return ProviderUsageSnapshot::unsupported("claude", CLAUDE_USAGE_SOURCE, reason)
        }
    };
    match profile {
        Some(profile) => collect_with_profile(profile),
        None => ProviderUsageSnapshot::unsupported(
            "claude",
            CLAUDE_USAGE_SOURCE,
            "Only a Cairn-managed Claude profile reports subscription usage.",
        ),
    }
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

pub fn collect_with_profile(profile: &Path) -> ProviderUsageSnapshot {
    let token = match profile_access_token(profile) {
        Ok(Some(token)) => token,
        Ok(None) => {
            return ProviderUsageSnapshot::unsupported(
                "claude",
                CLAUDE_USAGE_SOURCE,
                "This Claude account is not signed in, so it has no subscription usage to report.",
            )
        }
        Err(err) => return ProviderUsageSnapshot::error("claude", CLAUDE_USAGE_SOURCE, err, None),
    };

    let client = match reqwest::blocking::Client::builder()
        .timeout(USAGE_TIMEOUT)
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                "claude",
                CLAUDE_USAGE_SOURCE,
                format!("Failed to build the Claude usage client: {err}"),
                None,
            )
        }
    };
    let response = client
        .get(USAGE_URL)
        .bearer_auth(&token)
        .header("anthropic-beta", OAUTH_BETA)
        .send();
    let response = match response {
        Ok(response) => response,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                "claude",
                CLAUDE_USAGE_SOURCE,
                format!("Could not reach Claude for subscription usage: {err}"),
                None,
            )
        }
    };
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return ProviderUsageSnapshot::error(
            "claude",
            CLAUDE_USAGE_SOURCE,
            "Claude is rate-limiting usage checks right now. Try again in a moment.",
            None,
        );
    }
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        // Deliberately not refreshed here. The refresh token belongs to the
        // CLI's own credential store, and spending a single-use one behind its
        // back is how a working login gets broken; the next session refreshes it.
        return ProviderUsageSnapshot::error(
            "claude",
            CLAUDE_USAGE_SOURCE,
            "This Claude sign-in has expired. Run a session on this account, or sign in again, to refresh it.",
            None,
        );
    }
    if !response.status().is_success() {
        let status = response.status();
        return ProviderUsageSnapshot::error(
            "claude",
            CLAUDE_USAGE_SOURCE,
            format!("Claude returned {status} for subscription usage."),
            None,
        );
    }
    let usage: UsageResponse = match response.json() {
        Ok(usage) => usage,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                "claude",
                CLAUDE_USAGE_SOURCE,
                format!("Claude's subscription usage response was not readable: {err}"),
                None,
            )
        }
    };
    snapshot_from_limits(&usage.limits)
}

fn snapshot_from_limits(limits: &[UsageLimit]) -> ProviderUsageSnapshot {
    let windows: Vec<ProviderUsageWindow> = limits.iter().filter_map(window_from_limit).collect();
    if windows.is_empty() {
        return ProviderUsageSnapshot::unsupported(
            "claude",
            CLAUDE_USAGE_SOURCE,
            "Claude reported no subscription usage windows for this account.",
        );
    }
    ProviderUsageSnapshot {
        backend: "claude".into(),
        // Names the source, and marks it canonical: a snapshot whose source the
        // panel does not recognise reads as provisional, and the card keeps
        // asking for a better one — which against a rate-limited endpoint means
        // a real number flickering into 429.
        source: CLAUDE_USAGE_SOURCE.into(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        ..Default::default()
    }
}

/// The profile's OAuth access token, from wherever the CLI put it.
///
/// Read-only, and never refreshed: this is the CLI's credential, and Cairn
/// borrows it to ask a question rather than taking ownership of its lifecycle.
fn profile_access_token(profile: &Path) -> Result<Option<String>, String> {
    let raw = match read_credential_payload(profile)? {
        Some(raw) => raw,
        None => return Ok(None),
    };
    let stored: StoredCredentials = serde_json::from_str(&raw)
        .map_err(|err| format!("Claude credentials are not readable: {err}"))?;
    Ok(stored
        .claude_ai_oauth
        .and_then(|oauth| oauth.access_token)
        .filter(|token| !token.is_empty()))
}

#[cfg(target_os = "macos")]
fn read_credential_payload(profile: &Path) -> Result<Option<String>, String> {
    // A file beside the profile wins when present: the CLI writes one when the
    // keychain is unavailable, and honouring it keeps both shapes readable.
    if let Some(raw) = read_credential_file(profile)? {
        return Ok(Some(raw));
    }
    let output = std::process::Command::new("security")
        .args([
            "find-generic-password",
            "-s",
            &credential_service(profile),
            "-w",
        ])
        .output()
        .map_err(|err| format!("Failed to read the Claude keychain entry: {err}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&output.stdout).into_owned()))
}

#[cfg(not(target_os = "macos"))]
fn read_credential_payload(profile: &Path) -> Result<Option<String>, String> {
    read_credential_file(profile)
}

fn read_credential_file(profile: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(profile.join(CREDENTIAL_FILE)) {
        Ok(raw) => Ok(Some(raw)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("Failed to read Claude credentials: {err}")),
    }
}

/// The keychain service the CLI stores this profile's credential under:
/// its prefix plus the first eight hex characters of the SHA-256 of the
/// profile directory path.
///
/// Reached only from the macOS branch of [`read_credential_payload`], so it is
/// compiled there and nowhere else.
#[cfg(target_os = "macos")]
fn credential_service(profile: &Path) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(profile.to_string_lossy().as_bytes());
    format!("{CREDENTIAL_SERVICE_PREFIX}{:x}", digest)
        .chars()
        .take(CREDENTIAL_SERVICE_PREFIX.len() + 8)
        .collect()
}

fn window_from_limit(limit: &UsageLimit) -> Option<ProviderUsageWindow> {
    let used_percent = limit.percent?;
    let scope_target = limit
        .scope
        .as_ref()
        .and_then(|scope| scope.model.as_ref())
        .and_then(|model| model.display_name.clone());
    let scope = if limit.group == "session" {
        ProviderUsageScope::Session
    } else {
        ProviderUsageScope::Weekly
    };
    let label = match (limit.kind.as_str(), scope_target.as_deref()) {
        ("session", _) => "Current session".to_string(),
        ("weekly_all", _) => "Current week (all models)".to_string(),
        (_, Some(model)) => format!("Current week ({model})"),
        _ => format!("Current {}", limit.group),
    };
    Some(ProviderUsageWindow {
        id: match &scope_target {
            Some(model) => format!("{}-{}", limit.kind, slugify(model)),
            None => limit.kind.clone(),
        },
        label,
        scope,
        scope_target,
        used_percent,
        remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
        resets_at: limit
            .resets_at
            .as_deref()
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
            .map(|at| at.timestamp()),
        reset_at_text: None,
        window_duration_mins: None,
    })
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
        assert!(claude_usage_profile(Some(&api_key), Some("api-account")).is_err());
        assert!(claude_usage_profile(None, Some("no-credential")).is_err());

        let profile = ClaudeAuth::ConfigDir("/tmp/claude-profile".into());
        assert_eq!(
            claude_usage_profile(Some(&profile), Some("profile-account")).unwrap(),
            Some(Path::new("/tmp/claude-profile"))
        );
    }

    /// Verbatim `limits` from a live `GET /api/oauth/usage` response.
    const USAGE_BODY: &str = r#"{
      "five_hour": {"utilization": 64.0},
      "seven_day_opus": null,
      "limits": [
        {"kind": "session", "group": "session", "percent": 64, "severity": "normal",
         "resets_at": "2026-08-16T02:09:59.596745+00:00", "scope": null, "is_active": true},
        {"kind": "weekly_all", "group": "weekly", "percent": 10, "severity": "normal",
         "resets_at": "2026-08-20T07:59:59.596765+00:00", "scope": null, "is_active": false},
        {"kind": "weekly_scoped", "group": "weekly", "percent": 8, "severity": "normal",
         "resets_at": "2026-08-20T07:59:59.596987+00:00",
         "scope": {"model": {"id": null, "display_name": "Fable"}, "surface": null},
         "is_active": false}
      ]
    }"#;

    fn limits_of(body: &str) -> Vec<UsageLimit> {
        serde_json::from_str::<UsageResponse>(body).unwrap().limits
    }

    #[test]
    fn reads_every_window_the_endpoint_reports() {
        let snapshot = snapshot_from_limits(&limits_of(USAGE_BODY));

        assert!(snapshot.error.is_none());
        assert_eq!(snapshot.windows.len(), 3);

        let session = &snapshot.windows[0];
        assert_eq!(session.label, "Current session");
        assert_eq!(session.scope, ProviderUsageScope::Session);
        assert_eq!(session.used_percent, 64.0);
        assert_eq!(session.remaining_percent, 36.0);
        // The epoch of this window's own `resets_at` in USAGE_BODY
        // (2026-08-16T02:09:59+00:00). Spelled as a literal rather than parsed
        // here, so the test still fails if the parse ever loses the timezone;
        // refresh both together when the captured body is recaptured.
        assert_eq!(session.resets_at, Some(1786846199));

        let weekly = &snapshot.windows[1];
        assert_eq!(weekly.label, "Current week (all models)");
        assert_eq!(weekly.scope, ProviderUsageScope::Weekly);
        assert_eq!(weekly.remaining_percent, 90.0);

        // The scoped window carries the model, so two weekly rows stay distinct.
        let scoped = &snapshot.windows[2];
        assert_eq!(scoped.label, "Current week (Fable)");
        assert_eq!(scoped.scope_target.as_deref(), Some("Fable"));
        assert_eq!(scoped.id, "weekly_scoped-fable");
    }

    #[test]
    fn an_empty_window_list_is_reported_rather_than_treated_as_a_fault() {
        let snapshot = snapshot_from_limits(&limits_of(r#"{"limits": []}"#));
        assert!(snapshot.error.is_none());
        assert!(snapshot.windows.is_empty());
        assert!(snapshot.unsupported_reason.is_some());
    }

    #[test]
    fn a_profile_with_no_stored_credential_has_no_usage_to_report() {
        // A profile directory that was provisioned but never signed in. Not a
        // fault, and specifically not an authentication warning about a login
        // that does not exist yet.
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(profile_access_token(temp.path()).unwrap(), None);
    }

    #[test]
    fn reads_a_credential_file_when_the_cli_wrote_one() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join(CREDENTIAL_FILE),
            r#"{"claudeAiOauth": {"accessToken": "sk-ant-oat-test", "scopes": []}}"#,
        )
        .unwrap();
        assert_eq!(
            profile_access_token(temp.path()).unwrap().as_deref(),
            Some("sk-ant-oat-test")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn keychain_service_is_keyed_by_the_profile_directory() {
        // Verified against a live entry: the CLI stores each profile's
        // credential under the first eight hex characters of the SHA-256 of its
        // config directory, which is what keeps two managed accounts separate.
        let service = credential_service(Path::new(
            "/Users/mitch/.cairn-dev-agent-cairn-4155-builder-0/claude-profiles/acc_73b9291e-4477-494e-9a8c-3c426c2187a5",
        ));
        assert_eq!(service, "Claude Code-credentials-7b396017");
    }
}
