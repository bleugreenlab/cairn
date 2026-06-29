//! Desktop → api team sync wiring: fetch a team's sync config and mint its
//! short-lived sync token, reusing the device-JWT bearer-auth pattern from
//! [`super::org_tokens`].
//!
//! These calls hit the member-authed team endpoints:
//! - `GET  /teams/:id/sync-config` → broker `syncUrl` + `dbName` + `status`,
//!   distinguishing 200 (active) / 404 (not configured) / 503 (provisioning).
//! - `POST /teams/:id/sync-token`  → a short-lived `{ token, expiresAt }` the
//!   desktop presents to the broker (the broker rechecks membership per request,
//!   so the token is the rotation unit, not the revocation gate).
//!
//! The device JWT is read straight from the private DB ([`read_device_jwt`]) so
//! the [`super::team_token_minter::TeamTokenMinter`] can mint without the full
//! [`super::AccountManager`] refresh loop — the ordering the host wiring relies
//! on (the minter is constructed at `db/init` time, before `AccountManager`).

use serde::{Deserialize, Serialize};

use crate::api::ApiConfig;
use crate::storage::{LocalDb, RowExt};

/// The active-team body of `GET /teams/:id/sync-config` (HTTP 200).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncConfig {
    pub team_id: String,
    /// The BROKER url to hand the Turso Sync client as its remote url.
    pub sync_url: String,
    pub db_name: String,
    pub status: String,
}

/// The outcome of fetching a team's sync config, distinguishing the three
/// availability states the api reports so the caller can fail soft (treat
/// 404/503 as not-yet-available rather than an error).
#[derive(Debug, Clone)]
pub enum SyncConfigStatus {
    /// HTTP 200: the team is provisioned and active.
    Active(SyncConfig),
    /// HTTP 404: no sync config row — team sync was never enabled.
    NotConfigured,
    /// HTTP 503: a provisioning row exists but is not yet active.
    Provisioning,
}

/// The body of `POST /teams/:id/sync-token`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncTokenResponse {
    token: String,
    /// RFC3339 expiry timestamp.
    expires_at: String,
}

/// The runtime result of [`crate::orchestrator::Orchestrator::connect_team`],
/// surfaced to the Tauri command (and pollable by the join/share UX).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum TeamConnectStatus {
    /// The replica opened and the team's projects were routed to it.
    Connected,
    /// The api reports provisioning in progress (503); poll again later.
    Provisioning,
    /// The api reports the team has no sync config (404); not enabled.
    NotConfigured,
    /// No device JWT is stored — the account is not connected.
    NotAuthenticated,
}

/// A team's sync readiness as reported by
/// [`crate::orchestrator::Orchestrator::list_team_sync_status`] — the read-only
/// probe behind the desktop create-into-team selector. Carries the team id so the
/// frontend can join it with the account's org memberships for display names.
///
/// Distinct from [`TeamConnectStatus`]: this NEVER opens a replica or writes the
/// `teams` registry. It only reads `/sync-config` to decide whether a team can
/// currently receive a project, letting the selector pre-gate honestly before the
/// user picks (`connect_team` performs the actual replica open at submit).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamSyncStatus {
    pub team_id: String,
    pub status: TeamSyncReadiness,
}

/// The readiness states the create-into-team selector distinguishes, mapped
/// fail-soft from the `/sync-config` probe (a transport/other error collapses to
/// [`TeamSyncReadiness::NotConfigured`] so one unreachable team never sinks the
/// whole list).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TeamSyncReadiness {
    /// 200: sync config active — the team can receive a project.
    Active,
    /// 503: provisioning in progress — not yet selectable.
    Provisioning,
    /// 404 (or a fail-soft transport/other error): team sync is not enabled.
    NotConfigured,
    /// No device JWT stored — the account is not connected, so no probe is made.
    NotAuthenticated,
}

/// Fetch a team's sync config, distinguishing 200/404/503. Any other status (or
/// a transport failure) is a hard error the caller surfaces.
pub async fn fetch_team_sync_config(
    client: &reqwest::Client,
    device_jwt: &str,
    team_id: &str,
    api: &ApiConfig,
) -> Result<SyncConfigStatus, String> {
    let url = api.team_sync_config_url(team_id);
    let resp = client
        .get(&url)
        .bearer_auth(device_jwt)
        .send()
        .await
        .map_err(|e| format!("Failed to request team sync config: {e}"))?;

    match resp.status().as_u16() {
        200 => {
            let cfg: SyncConfig = resp
                .json()
                .await
                .map_err(|e| format!("Failed to parse team sync config: {e}"))?;
            Ok(SyncConfigStatus::Active(cfg))
        }
        404 => Ok(SyncConfigStatus::NotConfigured),
        503 => Ok(SyncConfigStatus::Provisioning),
        other => {
            let body = resp.text().await.unwrap_or_default();
            Err(format!("Team sync config request failed ({other}): {body}"))
        }
    }
}

/// Probe each team's sync readiness WITHOUT opening any replica — the read-only
/// backing for the desktop create-into-team selector. It composes the same
/// member-authed `/sync-config` fetch `connect_team` uses, but only to read
/// status; it touches no `DbState`, so it can never open a replica or mutate the
/// `teams` registry. A per-team transport/other error fails soft to
/// `NotConfigured` so one unreachable team never sinks the whole list.
pub async fn probe_team_sync_status(
    client: &reqwest::Client,
    device_jwt: &str,
    team_ids: &[String],
    api: &ApiConfig,
) -> Vec<TeamSyncStatus> {
    let mut out = Vec::with_capacity(team_ids.len());
    for team_id in team_ids {
        let status = match fetch_team_sync_config(client, device_jwt, team_id, api).await {
            Ok(SyncConfigStatus::Active(_)) => TeamSyncReadiness::Active,
            Ok(SyncConfigStatus::Provisioning) => TeamSyncReadiness::Provisioning,
            Ok(SyncConfigStatus::NotConfigured) => TeamSyncReadiness::NotConfigured,
            Err(_) => TeamSyncReadiness::NotConfigured,
        };
        out.push(TeamSyncStatus {
            team_id: team_id.clone(),
            status,
        });
    }
    out
}

/// Mint a fresh team sync token, returning `(token, expires_at_unix)`. Mirrors
/// [`super::org_tokens::fetch_org_token`]'s bearer/error/expiry handling.
pub async fn mint_team_sync_token(
    client: &reqwest::Client,
    device_jwt: &str,
    team_id: &str,
    api: &ApiConfig,
) -> Result<(String, i64), String> {
    let url = api.team_sync_token_url(team_id);
    let resp = client
        .post(&url)
        .bearer_auth(device_jwt)
        .send()
        .await
        .map_err(|e| format!("Failed to request team sync token: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Team sync token request failed ({status}): {body}"));
    }

    let body: SyncTokenResponse = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse team sync token response: {e}"))?;

    let expires_at = chrono::DateTime::parse_from_rfc3339(&body.expires_at)
        .map(|dt| dt.timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp() + 3600);

    Ok((body.token, expires_at))
}

/// Read and decrypt the stored device JWT directly from the private database,
/// without the [`super::AccountManager`] refresh machinery. Returns `None` when
/// no account is connected. This is the read the team token minter uses at each
/// cache-miss mint, so it always reflects the current stored JWT.
pub async fn read_device_jwt(local: &LocalDb) -> Result<Option<String>, String> {
    let encrypted = local
        .query_opt("SELECT jwt_encrypted FROM account LIMIT 1", (), |row| {
            row.opt_text(0)
        })
        .await
        .map_err(|e| format!("Failed to read device JWT: {e}"))?
        .flatten();

    match encrypted {
        Some(enc) => super::jwt::decrypt_jwt_from_storage(&enc).map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    /// One-shot mock HTTP server: replies to the first request with the given
    /// status line and JSON body, then closes. Returns the base URL to point an
    /// `ApiConfig` at. Std sockets on a background thread keep this independent
    /// of tokio's IO feature set and the worktree fence (loopback only).
    fn mock_server(status_line: &str, body: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let response = format!(
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf);
                let _ = sock.write_all(response.as_bytes());
                let _ = sock.flush();
            }
        });
        format!("http://{addr}")
    }

    fn api(base_url: String) -> ApiConfig {
        ApiConfig { base_url }
    }

    #[tokio::test]
    async fn fetch_sync_config_200_is_active() {
        let body = r#"{"teamId":"t1","syncUrl":"http://broker/teams/t1/sync","dbName":"db1","status":"active"}"#;
        let base = mock_server("HTTP/1.1 200 OK", body);
        let client = reqwest::Client::new();
        match fetch_team_sync_config(&client, "jwt", "t1", &api(base))
            .await
            .unwrap()
        {
            SyncConfigStatus::Active(cfg) => {
                assert_eq!(cfg.sync_url, "http://broker/teams/t1/sync");
                assert_eq!(cfg.db_name, "db1");
                assert_eq!(cfg.status, "active");
            }
            other => panic!("expected Active, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_sync_config_404_is_not_configured() {
        let base = mock_server("HTTP/1.1 404 Not Found", r#"{"error":"x"}"#);
        let client = reqwest::Client::new();
        assert!(matches!(
            fetch_team_sync_config(&client, "jwt", "t1", &api(base))
                .await
                .unwrap(),
            SyncConfigStatus::NotConfigured
        ));
    }

    #[tokio::test]
    async fn fetch_sync_config_503_is_provisioning() {
        let base = mock_server("HTTP/1.1 503 Service Unavailable", r#"{"error":"x"}"#);
        let client = reqwest::Client::new();
        assert!(matches!(
            fetch_team_sync_config(&client, "jwt", "t1", &api(base))
                .await
                .unwrap(),
            SyncConfigStatus::Provisioning
        ));
    }

    #[tokio::test]
    async fn fetch_sync_config_500_is_err() {
        let base = mock_server("HTTP/1.1 500 Internal Server Error", r#"{"error":"boom"}"#);
        let client = reqwest::Client::new();
        assert!(fetch_team_sync_config(&client, "jwt", "t1", &api(base))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn mint_token_parses_token_and_expiry() {
        let body = r#"{"token":"sync-tok","expiresAt":"2099-01-01T00:00:00Z"}"#;
        let base = mock_server("HTTP/1.1 200 OK", body);
        let client = reqwest::Client::new();
        let (token, expires_at) = mint_team_sync_token(&client, "jwt", "t1", &api(base))
            .await
            .unwrap();
        assert_eq!(token, "sync-tok");
        assert!(expires_at > chrono::Utc::now().timestamp());
    }

    #[tokio::test]
    async fn mint_token_error_status_is_err() {
        let base = mock_server("HTTP/1.1 403 Forbidden", r#"{"error":"nope"}"#);
        let client = reqwest::Client::new();
        assert!(mint_team_sync_token(&client, "jwt", "t1", &api(base))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn probe_maps_200_to_active() {
        let body = r#"{"teamId":"t1","syncUrl":"http://broker/teams/t1/sync","dbName":"db1","status":"active"}"#;
        let base = mock_server("HTTP/1.1 200 OK", body);
        let client = reqwest::Client::new();
        let out = probe_team_sync_status(&client, "jwt", &["t1".to_string()], &api(base)).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].team_id, "t1");
        assert_eq!(out[0].status, TeamSyncReadiness::Active);
    }

    #[tokio::test]
    async fn probe_maps_404_to_not_configured() {
        let base = mock_server("HTTP/1.1 404 Not Found", r#"{"error":"x"}"#);
        let client = reqwest::Client::new();
        let out = probe_team_sync_status(&client, "jwt", &["t1".to_string()], &api(base)).await;
        assert_eq!(out[0].status, TeamSyncReadiness::NotConfigured);
    }

    #[tokio::test]
    async fn probe_maps_503_to_provisioning() {
        let base = mock_server("HTTP/1.1 503 Service Unavailable", r#"{"error":"x"}"#);
        let client = reqwest::Client::new();
        let out = probe_team_sync_status(&client, "jwt", &["t1".to_string()], &api(base)).await;
        assert_eq!(out[0].status, TeamSyncReadiness::Provisioning);
    }

    #[tokio::test]
    async fn probe_fails_soft_on_transport_error() {
        // Point at an unroutable port: the underlying fetch errors, and the probe
        // must map that to NotConfigured rather than propagating the failure so
        // one unreachable team never sinks the whole list.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let out = probe_team_sync_status(
            &client,
            "jwt",
            &["t1".to_string()],
            &api("http://127.0.0.1:1".to_string()),
        )
        .await;
        assert_eq!(out[0].status, TeamSyncReadiness::NotConfigured);
    }
}
