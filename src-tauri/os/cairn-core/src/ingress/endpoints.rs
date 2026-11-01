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

impl Channel {
    fn of(channel: &super::channel::RelayChannel) -> Option<Self> {
        let (id, secret) = channel.credentials()?;
        Some(Self {
            id: id.to_string(),
            secret: secret.to_string(),
        })
    }
}

/// The install's channel, registered if it has none: minting is what first
/// needs one.
async fn ensured_channel(orch: &Orchestrator, relay_url: &str) -> Result<Channel, String> {
    let channel = super::channel::ensure(&orch.db.local, relay_url).await?;
    Channel::of(&channel)
        .ok_or_else(|| "the relay channel has no secret after registration".to_string())
}

/// The install's channel as it stands, never registering one: an install
/// with no channel has no endpoints to refresh or revoke.
async fn existing_channel(orch: &Orchestrator) -> Result<Option<Channel>, String> {
    Ok(Channel::of(
        &super::channel::get_relay_channel(&orch.db.local).await?,
    ))
}

/// Every provider the api mints, matching its `PROVIDERS`.
pub const PROVIDERS: &[&str] = &["first-party", "cairn-hmac", "github", "linear", "bearer"];

#[derive(Debug, Deserialize)]
struct WireEndpoint {
    id: String,
    name: String,
    provider: String,
    topic: Option<String>,
    project_ref: Option<String>,
    granted_at: Option<String>,
    status: String,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    rejected_count: i64,
}

/// A mint or rotation's reply. `secret` is present only when the api
/// generated it, and it is never seen again after this response.
#[derive(Deserialize)]
struct OneEndpoint {
    endpoint: WireEndpoint,
    #[serde(default)]
    secret: Option<String>,
}

impl std::fmt::Debug for OneEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneEndpoint")
            .field("endpoint", &self.endpoint)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .finish()
    }
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
        url: wire.url,
        rejected_count: wire.rejected_count,
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

/// What to mint.
#[derive(Debug, Clone, Copy, Default)]
pub struct MintSpec<'a> {
    /// The project (id, key) the endpoint belongs to; `None` for an
    /// install-scoped endpoint such as the GitHub App's.
    pub project: Option<(&'a str, &'a str)>,
    pub name: &'a str,
    pub provider: &'a str,
    /// First-party only.
    pub topic: Option<&'a str>,
    /// External only: where the event type comes from (the api's `type_from`).
    pub type_from: Option<&'a serde_json::Value>,
    pub max_body_bytes: Option<i64>,
    pub rate_per_min: Option<i64>,
    /// A provider's own signing secret, for internal callers only: the
    /// GitHub App's webhook secret, or Linear's. Never from an agent.
    pub provider_secret: Option<&'a str>,
}

fn validate(spec: &MintSpec<'_>) -> Result<(), String> {
    let name = spec.name;
    if !valid_endpoint_name(name) {
        return Err(format!(
            "Endpoint name '{name}' must be 1-63 lowercase letters, digits, or hyphens, not starting with a hyphen"
        ));
    }
    let provider = spec.provider;
    if !PROVIDERS.contains(&provider) {
        return Err(format!(
            "Unknown provider '{provider}'. Available: {}",
            PROVIDERS.join(", ")
        ));
    }
    if provider == "first-party" {
        let topic = spec.topic.unwrap_or_default();
        if !FIRST_PARTY_TOPICS.contains(&topic) {
            return Err(format!(
                "A first-party endpoint needs a topic. Available: {}",
                FIRST_PARTY_TOPICS.join(", ")
            ));
        }
        if spec.type_from.is_some()
            || spec.max_body_bytes.is_some()
            || spec.rate_per_min.is_some()
            || spec.provider_secret.is_some()
        {
            return Err(
                "A first-party endpoint takes no type_from, max_body_bytes, rate_per_min, or secret"
                    .to_string(),
            );
        }
    } else if spec.topic.is_some() {
        return Err("topic applies only to first-party endpoints".to_string());
    }
    if matches!(provider, "cairn-hmac" | "bearer") && spec.provider_secret.is_some() {
        return Err(format!(
            "A {provider} secret is generated by the server and kept in this machine's keychain"
        ));
    }
    if provider == "linear" && spec.provider_secret.is_none() {
        return Err(
            "A linear endpoint needs the signing secret Linear shows for its webhook".to_string(),
        );
    }
    Ok(())
}

/// Keep a secret the api generated, or undo what it cost. A live endpoint
/// whose secret nobody holds is worse than none, so a keychain failure
/// revokes the endpoint the api just minted.
async fn keep_generated_secret(
    orch: &Orchestrator,
    relay_url: &str,
    endpoint: &EndpointRecord,
    secret: Option<String>,
) -> Result<(), String> {
    let Some(secret) = secret else {
        return Ok(());
    };
    if let Err(error) = super::custody::store(&endpoint.id, &secret) {
        if let Err(revoke_error) = revoke(orch, relay_url, endpoint).await {
            log::warn!(
                "endpoint {} is live on the api with no stored secret, and revoking it failed: {revoke_error}",
                endpoint.id
            );
        }
        return Err(format!(
            "The endpoint's secret could not be stored in this machine's keychain ({error}), so the endpoint was revoked"
        ));
    }
    Ok(())
}

/// Mint an endpoint and mirror it. A generated secret goes straight to the
/// keychain; the returned record never carries it.
pub async fn mint(
    orch: &Orchestrator,
    relay_url: &str,
    spec: MintSpec<'_>,
) -> Result<EndpointRecord, String> {
    validate(&spec)?;
    if let Some((project_id, _)) = spec.project {
        if super::active_endpoint_named(&orch.db.local, project_id, spec.name)
            .await?
            .is_some()
        {
            return Err(format!(
                "This project already has a live endpoint named '{}'; revoke it first to reuse the name",
                spec.name
            ));
        }
    }
    let channel = ensured_channel(orch, relay_url).await?;
    let mut body = json!({"name": spec.name, "provider": spec.provider});
    if let Some((project_id, project_key)) = spec.project {
        body["project_ref"] = json!(project_id);
        body["project_key"] = json!(project_key);
    }
    if let Some(topic) = spec.topic {
        body["topic"] = json!(topic);
    }
    if let Some(type_from) = spec.type_from {
        body["type_from"] = type_from.clone();
    }
    if let Some(max_body_bytes) = spec.max_body_bytes {
        body["max_body_bytes"] = json!(max_body_bytes);
    }
    if let Some(rate_per_min) = spec.rate_per_min {
        body["rate_per_min"] = json!(rate_per_min);
    }
    if let Some(secret) = spec.provider_secret {
        body["secret"] = json!(secret);
    }
    let response = super::relay_client()
        .await?
        .post(format!(
            "{}/channels/{}/endpoints",
            relay_url.trim_end_matches('/'),
            channel.id
        ))
        .timeout(REQUEST_TIMEOUT)
        .bearer_auth(&channel.secret)
        .json(&body)
        .send()
        .await
        .map_err(|error| format!("could not reach the relay to mint the endpoint: {error}"))?;
    if !response.status().is_success() {
        return Err(refusal(response).await);
    }
    let OneEndpoint { endpoint, secret } = response
        .json()
        .await
        .map_err(|_| "invalid relay response to the mint".to_string())?;
    let endpoint = record(&channel.id, endpoint, chrono::Utc::now().timestamp_millis());
    super::upsert_endpoint(&orch.db.local, &endpoint).await?;
    keep_generated_secret(orch, relay_url, &endpoint, secret).await?;
    Ok(endpoint)
}

/// Replace an external endpoint's secret. A generated one replaces the
/// keychain entry; `provider_secret` is for internal callers holding a
/// provider's own.
pub async fn rotate(
    orch: &Orchestrator,
    relay_url: &str,
    endpoint: &EndpointRecord,
    provider_secret: Option<&str>,
) -> Result<EndpointRecord, String> {
    if endpoint.provider == "first-party" {
        return Err("A first-party endpoint has no secret to rotate".to_string());
    }
    if !endpoint.is_active() {
        return Err(format!("Endpoint {} is revoked", endpoint.name));
    }
    let channel = ensured_channel(orch, relay_url).await?;
    let body = match provider_secret {
        Some(secret) => json!({ "secret": secret }),
        None => json!({}),
    };
    let response = super::relay_client()
        .await?
        .post(format!(
            "{}/channels/{}/endpoints/{}/rotate",
            relay_url.trim_end_matches('/'),
            channel.id,
            endpoint.id
        ))
        .timeout(REQUEST_TIMEOUT)
        .bearer_auth(&channel.secret)
        .json(&body)
        .send()
        .await
        .map_err(|error| format!("could not reach the relay to rotate the endpoint: {error}"))?;
    if !response.status().is_success() {
        return Err(refusal(response).await);
    }
    let OneEndpoint {
        endpoint: wire,
        secret,
    } = response
        .json()
        .await
        .map_err(|_| "invalid relay response to the rotation".to_string())?;
    let rotated = EndpointRecord {
        created_at: endpoint.created_at,
        ..record(&channel.id, wire, endpoint.created_at)
    };
    super::upsert_endpoint(&orch.db.local, &rotated).await?;
    if let Some(secret) = secret {
        super::custody::store(&rotated.id, &secret).map_err(|error| {
            format!(
                "The endpoint was rotated, but its new secret could not be stored in this machine's keychain ({error}). Rotate it again."
            )
        })?;
    }
    Ok(rotated)
}

/// Revoke an endpoint on the api and in the mirror, and forget its secret.
pub async fn revoke(
    orch: &Orchestrator,
    relay_url: &str,
    endpoint: &EndpointRecord,
) -> Result<EndpointRecord, String> {
    let channel = existing_channel(orch).await?.ok_or_else(|| {
        "This install has no relay channel, so it has no endpoint to revoke".to_string()
    })?;
    let response = super::relay_client()
        .await?
        .delete(format!(
            "{}/channels/{}/endpoints/{}",
            relay_url.trim_end_matches('/'),
            channel.id,
            endpoint.id
        ))
        .timeout(REQUEST_TIMEOUT)
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
    if endpoint.provider != "first-party" {
        if let Err(error) = super::custody::forget(&endpoint.id) {
            log::warn!(
                "revoked endpoint {} but could not delete its keychain entry: {error}",
                endpoint.id
            );
        }
    }
    Ok(revoked)
}

/// Bring the mirror's grant and status up to date with the api. Endpoints the
/// mirror lost (a reinstall on the same channel) are restored.
pub async fn refresh(
    orch: &Orchestrator,
    relay_url: &str,
    timeout: Duration,
) -> Result<(), String> {
    let Some(channel) = existing_channel(orch).await? else {
        return Ok(());
    };
    let response = super::relay_client()
        .await?
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
pub(crate) mod tests {
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

    /// The secret the stand-in api generates for an external mint.
    pub(crate) const MINTED_SECRET: &str = "Generated-by-the-api-7Qw2Lp9Xz4Rk";
    /// The secret it generates for a rotation.
    pub(crate) const ROTATED_SECRET: &str = "Rotated-by-the-api-3Vn8Hd5Jc1Ty";

    /// A stand-in relay recording each request's method, path, Authorization,
    /// and body, answering like the api does: an external mint or rotation
    /// reveals a generated secret once.
    pub(crate) async fn relay() -> (String, Seen) {
        use axum::{body::Bytes, http::HeaderMap, http::Method, http::Uri, Router};
        let seen: Seen = Arc::default();
        let record = seen.clone();
        let app = Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let record = record.clone();
                async move {
                    let body: serde_json::Value =
                        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                    if uri.path() == "/relay/channels" {
                        record.lock().unwrap().push((
                            method.to_string(),
                            uri.path().to_string(),
                            String::new(),
                            body.clone(),
                        ));
                        return axum::Json(json!({ "channel_id": "ch-new", "secret": "new-secret" }));
                    }
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
                    let rotating = uri.path().ends_with("/rotate");
                    let provider = body["provider"]
                        .as_str()
                        .unwrap_or(if rotating { "cairn-hmac" } else { "first-party" })
                        .to_string();
                    let external = provider != "first-party";
                    let endpoint = json!({
                        "id": "src_0123456789abcdefghijkl",
                        "name": body["name"].as_str().unwrap_or("bugs"),
                        "provider": provider,
                        "topic": if external { json!(null) } else { json!("cairn.bugs") },
                        "project_ref": "p",
                        "url": if external { json!("https://api.cairn.test/e/src_0123456789abcdefghijkl") } else { json!(null) },
                        "rejected_count": if method == Method::GET { 3 } else { 0 },
                        "granted_at": if method == Method::GET || external { json!("2026-10-18T01:00:00.000Z") } else { json!(null) },
                        "status": if method == Method::DELETE { "revoked" } else { "active" },
                    });
                    let reply = if method == Method::GET {
                        json!({ "endpoints": [endpoint] })
                    } else if rotating {
                        json!({ "endpoint": endpoint, "secret": ROTATED_SECRET })
                    } else if method == Method::POST && external && body["secret"].is_null() {
                        json!({ "endpoint": endpoint, "secret": MINTED_SECRET })
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

    pub(crate) async fn connect_channel(orch: &Orchestrator) {
        let (public_key, private_key) = crate::github::crypto::generate_keypair();
        let private_key_encrypted = crate::github::crypto::encrypt_private_key(
            &private_key,
            &crate::github::crypto::get_machine_id(),
        )
        .unwrap();
        crate::ingress::channel::update_relay_channel(&orch.db.local, |channel| {
            channel.channel_id = Some("ch-1".into());
            channel.secret = Some("channel-secret".into());
            channel.public_key = Some(public_key);
            channel.private_key_encrypted = Some(private_key_encrypted);
        })
        .await
        .unwrap();
    }

    fn bugs(name: &str) -> MintSpec<'_> {
        MintSpec {
            project: Some(("p", "cairn")),
            name,
            provider: "first-party",
            topic: Some("cairn.bugs"),
            ..MintSpec::default()
        }
    }

    #[tokio::test]
    async fn minting_without_a_channel_registers_one() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        let (url, seen) = relay().await;
        let minted = mint(&orch, &url, bugs("bugs")).await.unwrap();
        assert_eq!(minted.channel_id, "ch-new");
        let channel = crate::ingress::channel::get_relay_channel(&db)
            .await
            .unwrap();
        assert_eq!(channel.credentials(), Some(("ch-new", "new-secret")));
        assert!(channel.intact_private_key().is_some());
        let seen = seen.lock().unwrap().clone();
        let calls: Vec<_> = seen
            .iter()
            .map(|(method, path, auth, _)| (method.as_str(), path.as_str(), auth.as_str()))
            .collect();
        assert_eq!(
            calls,
            vec![
                ("POST", "/relay/channels", ""),
                (
                    "POST",
                    "/relay/channels/ch-new/endpoints",
                    "Bearer new-secret"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn refreshing_without_a_channel_registers_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        let (url, seen) = relay().await;
        refresh(&orch, &url, Duration::from_secs(3)).await.unwrap();
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(
            crate::ingress::channel::get_relay_channel(&db)
                .await
                .unwrap(),
            crate::ingress::channel::RelayChannel::default()
        );
    }

    #[tokio::test]
    async fn mint_refresh_and_revoke_go_through_the_channel_and_keep_the_mirror() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        connect_channel(&orch).await;
        let (url, seen) = relay().await;

        let minted = mint(&orch, &url, bugs("bugs")).await.unwrap();
        assert_eq!(minted.granted_at, None);
        assert!(mint(&orch, &url, bugs("bugs"))
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
        assert_eq!(refreshed.rejected_count, 3);

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

    /// An external mint passes its limits through, keeps the generated
    /// secret in the keychain (never on the record), and rotate and revoke
    /// replace and delete it.
    #[tokio::test]
    async fn an_external_mint_keeps_its_secret_in_the_keychain_through_rotate_and_revoke() {
        crate::config::secrets::mock_keychain::install();
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        connect_channel(&orch).await;
        let (url, seen) = relay().await;
        let type_from = json!({"header": "X-Event"});
        let minted = mint(
            &orch,
            &url,
            MintSpec {
                project: Some(("p", "cairn")),
                name: "ci",
                provider: "cairn-hmac",
                type_from: Some(&type_from),
                max_body_bytes: Some(65536),
                rate_per_min: Some(30),
                ..MintSpec::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            minted.url.as_deref(),
            Some("https://api.cairn.test/e/src_0123456789abcdefghijkl")
        );
        assert!(!format!("{minted:?}").contains(MINTED_SECRET));
        assert_eq!(
            super::super::custody::reveal_for_copy(&minted.id).as_deref(),
            Some(MINTED_SECRET)
        );
        assert_eq!(
            seen.lock().unwrap()[0].3,
            json!({"name": "ci", "provider": "cairn-hmac", "project_ref": "p", "project_key": "cairn", "type_from": {"header": "X-Event"}, "max_body_bytes": 65536, "rate_per_min": 30})
        );

        let rotated = rotate(&orch, &url, &minted, None).await.unwrap();
        assert!(!format!("{rotated:?}").contains(ROTATED_SECRET));
        assert_eq!(
            super::super::custody::reveal_for_copy(&minted.id).as_deref(),
            Some(ROTATED_SECRET)
        );
        let mirrored = crate::ingress::get_endpoint(&db, &minted.id)
            .await
            .unwrap()
            .unwrap();
        revoke(&orch, &url, &mirrored).await.unwrap();
        assert!(!super::super::custody::held(&minted.id));
    }

    #[tokio::test]
    async fn minting_refuses_what_the_api_would() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _) = orchestrator(temp.path(), &[]).await;
        connect_channel(&orch).await;
        let url = "http://127.0.0.1:9/relay";
        let refused = |spec: MintSpec<'static>| {
            let orch = orch.clone();
            async move { mint(&orch, url, spec).await.unwrap_err() }
        };
        assert!(refused(MintSpec {
            provider: "zapier",
            ..bugs("bugs")
        })
        .await
        .contains("Unknown provider"));
        assert!(refused(MintSpec {
            topic: Some("cairn.secrets"),
            ..bugs("bugs")
        })
        .await
        .contains("needs a topic"));
        assert!(refused(MintSpec {
            name: "Bugs",
            ..bugs("bugs")
        })
        .await
        .contains("lowercase"));
        assert!(refused(MintSpec {
            provider: "cairn-hmac",
            ..bugs("bugs")
        })
        .await
        .contains("only to first-party"));
        assert!(refused(MintSpec {
            provider: "bearer",
            topic: None,
            provider_secret: Some("chosen-by-the-caller-123"),
            ..bugs("bugs")
        })
        .await
        .contains("generated by the server"));
        assert!(refused(MintSpec {
            provider: "linear",
            topic: None,
            ..bugs("bugs")
        })
        .await
        .contains("signing secret Linear"));
    }
}
