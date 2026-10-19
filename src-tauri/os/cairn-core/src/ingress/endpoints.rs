//! Minting, revoking, and refreshing endpoints through the api.
//!
//! An endpoint is created on the api under this install's relay channel,
//! authenticated by the channel secret, and mirrored into the local
//! `ingress_endpoints` table. The api row is authoritative (the operator grant
//! is set there); the mirror is what the relay consumer and the endpoints
//! resource read.

use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use super::EndpointRecord;
use crate::orchestrator::Orchestrator;

/// The first-party topics an endpoint may subscribe to, matching the api.
pub const FIRST_PARTY_TOPICS: &[&str] = &["cairn.bugs"];

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub fn valid_endpoint_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes[0] != b'-'
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// The relay channel endpoints are minted under.
struct Channel {
    id: String,
    secret: String,
}

/// Endpoints ride the install's relay channel, which today is registered by
/// connecting the GitHub App.
pub const NO_CHANNEL: &str = "This install has no relay channel yet, and ingress endpoints are delivered over it. Connect the GitHub App in Settings → GitHub to register one, then mint the endpoint again.";

async fn channel(orch: &Orchestrator) -> Result<Channel, String> {
    let creds = crate::github::credentials::get_github_credentials(&orch.db.local).await?;
    match (creds.relay_channel_id, creds.relay_secret) {
        (Some(id), Some(secret)) => Ok(Channel { id, secret }),
        _ => Err(NO_CHANNEL.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct WireEndpoint {
    id: String,
    name: String,
    provider: String,
    topic: Option<String>,
    project_ref: Option<String>,
    granted_at: Option<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
struct OneEndpoint {
    endpoint: WireEndpoint,
}

#[derive(Debug, Deserialize)]
struct EndpointList {
    endpoints: Vec<WireEndpoint>,
}

fn record(channel_id: &str, wire: WireEndpoint, created_at: i64) -> EndpointRecord {
    let revoked = wire.status != "active";
    EndpointRecord {
        id: wire.id,
        channel_id: channel_id.to_string(),
        project_id: wire.project_ref,
        name: wire.name,
        provider: wire.provider,
        topic: wire.topic,
        status: if revoked { "revoked" } else { "active" }.to_string(),
        granted_at: wire.granted_at,
        created_at,
        revoked_at: revoked.then_some(created_at),
    }
}

async fn refusal(response: reqwest::Response) -> String {
    let status = response.status();
    let detail = response
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|body| body["error"].as_str().map(str::to_owned))
        .unwrap_or_else(|| "no detail".to_string());
    format!("the relay refused the request ({status}): {detail}")
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| error.to_string())
}

/// Mint a first-party endpoint for a project and mirror it. The endpoint
/// receives nothing until an operator grants it on the api.
pub async fn mint(
    orch: &Orchestrator,
    relay_url: &str,
    project_id: &str,
    project_key: &str,
    name: &str,
    provider: &str,
    topic: &str,
) -> Result<EndpointRecord, String> {
    if !valid_endpoint_name(name) {
        return Err(format!(
            "Endpoint name '{name}' must be 1-63 lowercase letters, digits, or hyphens, not starting with a hyphen"
        ));
    }
    if provider != "first-party" {
        return Err(format!(
            "Provider '{provider}' cannot be minted yet: only first-party endpoints are available"
        ));
    }
    if !FIRST_PARTY_TOPICS.contains(&topic) {
        return Err(format!(
            "Unknown first-party topic '{topic}'. Available: {}",
            FIRST_PARTY_TOPICS.join(", ")
        ));
    }
    if super::active_endpoint_named(&orch.db.local, project_id, name)
        .await?
        .is_some()
    {
        return Err(format!(
            "This project already has a live endpoint named '{name}'; revoke it first to reuse the name"
        ));
    }
    let channel = channel(orch).await?;
    let response = client()?
        .post(format!(
            "{}/channels/{}/endpoints",
            relay_url.trim_end_matches('/'),
            channel.id
        ))
        .bearer_auth(&channel.secret)
        .json(&json!({
            "name": name,
            "provider": provider,
            "topic": topic,
            "project_ref": project_id,
            "project_key": project_key,
        }))
        .send()
        .await
        .map_err(|error| format!("could not reach the relay to mint the endpoint: {error}"))?;
    if !response.status().is_success() {
        return Err(refusal(response).await);
    }
    let OneEndpoint { endpoint } = response
        .json()
        .await
        .map_err(|error| format!("invalid relay response: {error}"))?;
    let endpoint = record(&channel.id, endpoint, chrono::Utc::now().timestamp_millis());
    super::upsert_endpoint(&orch.db.local, &endpoint).await?;
    Ok(endpoint)
}

/// Revoke an endpoint on the api and in the mirror.
pub async fn revoke(
    orch: &Orchestrator,
    relay_url: &str,
    endpoint: &EndpointRecord,
) -> Result<EndpointRecord, String> {
    let channel = channel(orch).await?;
    let response = client()?
        .delete(format!(
            "{}/channels/{}/endpoints/{}",
            relay_url.trim_end_matches('/'),
            channel.id,
            endpoint.id
        ))
        .bearer_auth(&channel.secret)
        .send()
        .await
        .map_err(|error| format!("could not reach the relay to revoke the endpoint: {error}"))?;
    // A 404 means the api no longer holds it live; the mirror catches up.
    if !response.status().is_success() && response.status() != reqwest::StatusCode::NOT_FOUND {
        return Err(refusal(response).await);
    }
    let revoked = EndpointRecord {
        status: "revoked".into(),
        revoked_at: Some(chrono::Utc::now().timestamp_millis()),
        ..endpoint.clone()
    };
    super::upsert_endpoint(&orch.db.local, &revoked).await?;
    Ok(revoked)
}

/// Bring the mirror's grant and status up to date with the api. Endpoints the
/// mirror lost (a reinstall on the same channel) are restored.
pub async fn refresh(
    orch: &Orchestrator,
    relay_url: &str,
    timeout: Duration,
) -> Result<(), String> {
    let channel = channel(orch).await?;
    let response = client()?
        .get(format!(
            "{}/channels/{}/endpoints",
            relay_url.trim_end_matches('/'),
            channel.id
        ))
        .bearer_auth(&channel.secret)
        .timeout(timeout)
        .send()
        .await
        .map_err(|error| format!("could not reach the relay: {error}"))?;
    if !response.status().is_success() {
        return Err(refusal(response).await);
    }
    let EndpointList { endpoints } = response
        .json()
        .await
        .map_err(|error| format!("invalid relay response: {error}"))?;
    for wire in endpoints {
        let known = super::get_endpoint(&orch.db.local, &wire.id).await?;
        let created_at = known
            .as_ref()
            .map(|endpoint| endpoint.created_at)
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        let mut fresh = record(&channel.id, wire, created_at);
        if let Some(known) = known {
            fresh.revoked_at = known.revoked_at.or(fresh.revoked_at);
        }
        super::upsert_endpoint(&orch.db.local, &fresh).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::fixture::orchestrator;
    use std::sync::{Arc, Mutex};

    #[test]
    fn endpoint_names_are_short_lowercase_slugs() {
        for good in ["bugs", "a", "ci-2", &"x".repeat(63)] {
            assert!(valid_endpoint_name(good), "{good}");
        }
        for bad in ["", "-bugs", "Bugs", "bugs!", "a b", &"x".repeat(64)] {
            assert!(!valid_endpoint_name(bad), "{bad}");
        }
    }

    type Seen = Arc<Mutex<Vec<(String, String, String, serde_json::Value)>>>;

    /// A stand-in relay recording each request's method, path, Authorization,
    /// and body, answering like the api does.
    async fn relay() -> (String, Seen) {
        use axum::{body::Bytes, http::HeaderMap, http::Method, http::Uri, Router};
        let seen: Seen = Arc::default();
        let record = seen.clone();
        let app = Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let record = record.clone();
                async move {
                    let body: serde_json::Value =
                        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                    record.lock().unwrap().push((
                        method.to_string(),
                        uri.path().to_string(),
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        body.clone(),
                    ));
                    let endpoint = json!({
                        "id": "src_0123456789abcdefghijkl",
                        "name": body["name"].as_str().unwrap_or("bugs"),
                        "provider": "first-party",
                        "topic": "cairn.bugs",
                        "project_ref": "p",
                        "granted_at": if method == Method::GET { json!("2026-10-18T01:00:00.000Z") } else { json!(null) },
                        "status": if method == Method::DELETE { "revoked" } else { "active" },
                    });
                    let reply = if method == Method::GET {
                        json!({ "endpoints": [endpoint] })
                    } else {
                        json!({ "endpoint": endpoint })
                    };
                    axum::Json(reply)
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/relay", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, seen)
    }

    async fn connect_channel(orch: &Orchestrator) {
        crate::github::credentials::update_github_credentials(&orch.db.local, |creds| {
            creds.relay_channel_id = Some("ch-1".into());
            creds.relay_secret = Some("channel-secret".into());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn minting_without_a_relay_channel_is_refused_with_the_way_forward() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _) = orchestrator(temp.path(), &[]).await;
        let error = mint(
            &orch,
            "http://127.0.0.1:9/relay",
            "p",
            "cairn",
            "bugs",
            "first-party",
            "cairn.bugs",
        )
        .await
        .unwrap_err();
        assert_eq!(error, NO_CHANNEL);
    }

    #[tokio::test]
    async fn mint_refresh_and_revoke_go_through_the_channel_and_keep_the_mirror() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        connect_channel(&orch).await;
        let (url, seen) = relay().await;

        let minted = mint(
            &orch,
            &url,
            "p",
            "cairn",
            "bugs",
            "first-party",
            "cairn.bugs",
        )
        .await
        .unwrap();
        assert_eq!(minted.granted_at, None);
        assert!(mint(
            &orch,
            &url,
            "p",
            "cairn",
            "bugs",
            "first-party",
            "cairn.bugs"
        )
        .await
        .unwrap_err()
        .contains("already has a live endpoint"));

        refresh(&orch, &url, Duration::from_secs(3)).await.unwrap();
        let refreshed = crate::ingress::get_endpoint(&db, &minted.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            refreshed.granted_at.as_deref(),
            Some("2026-10-18T01:00:00.000Z")
        );

        let revoked = revoke(&orch, &url, &refreshed).await.unwrap();
        assert_eq!(revoked.status, "revoked");
        assert!(crate::ingress::active_endpoint_named(&db, "p", "bugs")
            .await
            .unwrap()
            .is_none());

        let seen = seen.lock().unwrap().clone();
        let calls: Vec<_> = seen
            .iter()
            .map(|(method, path, auth, _)| (method.as_str(), path.as_str(), auth.as_str()))
            .collect();
        assert_eq!(
            calls,
            vec![
                (
                    "POST",
                    "/relay/channels/ch-1/endpoints",
                    "Bearer channel-secret"
                ),
                (
                    "GET",
                    "/relay/channels/ch-1/endpoints",
                    "Bearer channel-secret"
                ),
                (
                    "DELETE",
                    "/relay/channels/ch-1/endpoints/src_0123456789abcdefghijkl",
                    "Bearer channel-secret"
                ),
            ]
        );
        assert_eq!(
            seen[0].3,
            json!({"name": "bugs", "provider": "first-party", "topic": "cairn.bugs", "project_ref": "p", "project_key": "cairn"})
        );
    }

    #[tokio::test]
    async fn only_first_party_endpoints_on_a_known_topic_are_minted() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _) = orchestrator(temp.path(), &[]).await;
        connect_channel(&orch).await;
        let url = "http://127.0.0.1:9/relay";
        assert!(
            mint(&orch, url, "p", "cairn", "bugs", "cairn-hmac", "cairn.bugs")
                .await
                .unwrap_err()
                .contains("only first-party")
        );
        assert!(mint(
            &orch,
            url,
            "p",
            "cairn",
            "bugs",
            "first-party",
            "cairn.secrets"
        )
        .await
        .unwrap_err()
        .contains("Unknown first-party topic"));
        assert!(mint(
            &orch,
            url,
            "p",
            "cairn",
            "Bugs",
            "first-party",
            "cairn.bugs"
        )
        .await
        .unwrap_err()
        .contains("lowercase"));
    }
}
