//! The GitHub App's webhook, pointed at a verified install-scoped endpoint.
//!
//! GitHub reveals an App's webhook secret only at manifest conversion, so the
//! manifest still registers the legacy `/relay/hook/:channel` URL, where
//! nothing checks a signature. [`ensure`] then mints a `github` endpoint
//! scoped to no project under the install's relay channel, with the App's
//! own webhook secret as the endpoint's provider secret, and repoints the
//! App's webhook at it (URL and secret together), so the api verifies every
//! delivery's `X-Hub-Signature-256` before it is stored. [`maintain`] runs it
//! at startup and again whenever the App or the channel changes: the
//! endpoint is derived state, and an install that loses it gets it back.
//!
//! The endpoint's name carries the App's id (`github-app-<id>`). Connecting
//! again creates a new App with a new secret, so it gets a new endpoint and
//! the old App's is revoked, rather than keeping one that verifies with a
//! secret nothing signs with any more.
//!
//! Only an install talking to a public (https) api ever moves the App's
//! webhook ([`Repoint::for_relay`]): a dev instance on a local api mints, and
//! leaves a real App alone.
//!
//! The secret has one home, `github_app.webhook_secret`. It is registered for
//! scrubbing under the endpoint's credential reference and never copied into
//! the keychain: GitHub already holds it, so there is nothing to Copy.

use std::time::Duration;

use base64::Engine;
use rand::RngCore;

use crate::github::credentials;
use crate::ingress::{channel, custody, endpoints, EndpointRecord};
use crate::orchestrator::Orchestrator;
use crate::services::HttpClient;

/// The api's ceiling. GitHub does not retry a delivery refused with a 429.
const RATE_PER_MIN: i64 = 600;
/// The api's ceiling, and GitHub's own cap on a webhook payload is larger, so
/// nothing smaller is safe.
const MAX_BODY_BYTES: i64 = 1 << 20;
const REFRESH_TIMEOUT: Duration = Duration::from_secs(10);

const MIN_RETRY: Duration = Duration::from_secs(60);
const MAX_RETRY: Duration = Duration::from_secs(60 * 60);
/// How long a settled maintainer waits before checking again with nothing to
/// wake it: the api accepting a rejected channel again, or a webhook URL
/// edited by hand in GitHub, sends no signal.
const RECHECK: Duration = Duration::from_secs(60 * 60);

/// What [`ensure`] found or did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ensured {
    /// No App is connected.
    NoApp,
    /// The api rejects the install's channel, so nothing can be minted on it.
    ChannelRejected,
    /// The App's endpoint exists; `repointed` when the App's webhook was
    /// changed to it on this run.
    Pointed {
        endpoint_id: String,
        repointed: bool,
    },
}

/// The App's endpoint name.
pub fn endpoint_name(app_id: i64) -> String {
    format!("github-app-{app_id}")
}

fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Every live GitHub App endpoint this install knows of.
async fn app_endpoints(orch: &Orchestrator) -> Result<Vec<EndpointRecord>, String> {
    Ok(crate::ingress::active_endpoints(&orch.db.local)
        .await?
        .into_iter()
        .filter(EndpointRecord::is_github_app)
        .collect())
}

/// The live endpoint named `name` on the channel `channel_id`.
async fn current_endpoint(
    orch: &Orchestrator,
    channel_id: Option<&str>,
    name: &str,
) -> Result<Option<EndpointRecord>, String> {
    let Some(channel_id) = channel_id else {
        return Ok(None);
    };
    Ok(app_endpoints(orch)
        .await?
        .into_iter()
        .find(|endpoint| endpoint.channel_id == channel_id && endpoint.name == name))
}

async fn current_channel_id(orch: &Orchestrator) -> Result<Option<String>, String> {
    Ok(channel::get_relay_channel(&orch.db.local).await?.channel_id)
}

fn is_name_conflict(error: &str) -> bool {
    error.contains("(409 Conflict)")
}

/// Whether [`ensure`] may move the App's webhook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repoint {
    /// The api is a public one: point the App at the endpoint minted on it.
    Public,
    /// The api is a local one. Mint, but never touch the App's webhook.
    Never,
}

impl Repoint {
    /// Decided by the api this install talks to, not by the URL the api
    /// reports for an endpoint: a local api renders its endpoints under its
    /// configured public base, which defaults to production's, so a dev
    /// instance holding a real App's credentials would otherwise move that
    /// App to a production URL for an endpoint only the local api has.
    pub fn for_relay(relay_url: &str) -> Self {
        if relay_url.starts_with("https://") {
            Self::Public
        } else {
            Self::Never
        }
    }
}

/// Make the App's endpoint exist and the App's webhook point at it.
///
/// Mints before it repoints, so the App is only ever pointed at an endpoint
/// that already verifies. A failed repoint is an `Err` that leaves the
/// endpoint in place; the next run sees the webhook URL still differs and
/// tries again.
pub async fn ensure(
    orch: &Orchestrator,
    http: &dyn HttpClient,
    relay_url: &str,
    repoint: Repoint,
) -> Result<Ensured, String> {
    let _serialized = orch.relay.github_app_lock().lock().await;
    let creds = credentials::get_github_credentials(&orch.db.local).await?;
    let Some(app_id) = creds.app_id else {
        return Ok(Ensured::NoApp);
    };
    let relay_channel = channel::get_relay_channel(&orch.db.local).await?;
    if relay_channel.health_state.as_deref() == Some("unauthorized") {
        log::warn!(
            "Not minting the GitHub App's endpoint: the relay rejects this install's channel"
        );
        return Ok(Ensured::ChannelRejected);
    }

    // A secret that was never set, or that no longer decrypts on this
    // machine, is replaced: the repoint below hands GitHub the new one. It
    // is persisted only once the endpoint carries it (below). Persisted
    // first, a failed rotation would leave the credentials holding a secret
    // the endpoint does not verify, and a later run would hand it to GitHub.
    let (secret, secret_replaced) = match creds.webhook_secret.filter(|secret| !secret.is_empty()) {
        Some(secret) => (secret, false),
        None => (generate_secret(), true),
    };

    let name = endpoint_name(app_id);
    if relay_channel.credentials().is_some() {
        // Restores an endpoint the mirror lost; without it a reinstall on the
        // same channel would mint into a name conflict.
        if let Err(error) = endpoints::refresh(orch, relay_url, REFRESH_TIMEOUT).await {
            log::debug!("could not refresh endpoints before ensuring the App's: {error}");
        }
    }
    let found = current_endpoint(orch, relay_channel.channel_id.as_deref(), &name).await?;
    let (endpoint, changed) = match found {
        Some(endpoint) if secret_replaced => (
            endpoints::rotate(orch, relay_url, &endpoint, Some(&secret)).await?,
            true,
        ),
        Some(endpoint) => (endpoint, false),
        None => {
            let spec = endpoints::MintSpec {
                project: None,
                name: &name,
                provider: "github",
                rate_per_min: Some(RATE_PER_MIN),
                max_body_bytes: Some(MAX_BODY_BYTES),
                provider_secret: Some(&secret),
                ..endpoints::MintSpec::default()
            };
            match endpoints::mint(orch, relay_url, spec).await {
                Ok(endpoint) => (endpoint, true),
                // The name is live on this channel, but the mirror missed it.
                // Adopt it, and rotate to the secret this install holds,
                // since nothing says which secret it was minted with.
                Err(error) if is_name_conflict(&error) => {
                    endpoints::refresh(orch, relay_url, REFRESH_TIMEOUT).await?;
                    let channel_id = current_channel_id(orch).await?;
                    let adopted = current_endpoint(orch, channel_id.as_deref(), &name)
                        .await?
                        .ok_or(error)?;
                    (
                        endpoints::rotate(orch, relay_url, &adopted, Some(&secret)).await?,
                        true,
                    )
                }
                Err(error) => return Err(error),
            }
        }
    };
    if secret_replaced {
        // The endpoint verifies with it now. If this write fails, the next
        // run generates another and rotates again, which is harmless.
        credentials::update_github_credentials(&orch.db.local, |creds| {
            creds.webhook_secret = Some(secret.clone());
        })
        .await?;
    }
    custody::hold_provider_secret(&endpoint.id, &secret);

    // Another App's endpoint, or this App's on a channel the install no
    // longer has, verifies with a secret nothing signs with now.
    for stale in app_endpoints(orch)
        .await?
        .into_iter()
        .filter(|stale| stale.id != endpoint.id)
    {
        if let Err(error) = endpoints::revoke(orch, relay_url, &stale).await {
            log::warn!(
                "could not revoke the stale GitHub App endpoint {}: {error}",
                stale.id
            );
        }
    }

    let url = endpoint
        .url
        .clone()
        .ok_or("the relay returned the App's endpoint without a URL")?;
    let pointed = |repointed| Ensured::Pointed {
        endpoint_id: endpoint.id.clone(),
        repointed,
    };
    // A real App's webhook is never taken from production for a local api,
    // nor given a URL GitHub could not deliver to securely.
    if repoint == Repoint::Never || !url.starts_with("https://") {
        log::info!(
            "Not pointing the GitHub App's webhook at {url}: the relay {relay_url} is not a public https api, or the endpoint is not https"
        );
        return Ok(pointed(false));
    }
    let app = crate::security::broker::github::app_authority(&orch.db.local).await?;
    if !changed {
        let current = crate::github::api::get_app_webhook_config(http, &app).await?;
        if current.url.as_deref() == Some(url.as_str()) {
            return Ok(pointed(false));
        }
    }
    crate::github::api::update_app_webhook_url(http, &app, &url, Some(&secret)).await?;
    log::info!(
        "Pointed the GitHub App's webhook at its endpoint {}",
        endpoint.id
    );
    Ok(pointed(true))
}

/// Run [`ensure`] at startup, again whenever the App or the channel changes,
/// and on a backoff after a failure, forever.
///
/// A failure is usually an api without endpoints yet (it answers 404), so
/// the legacy `/hook` keeps delivering until the api is deployed.
pub async fn maintain(orch: Orchestrator, relay_url: String) {
    let mut retry = MIN_RETRY;
    loop {
        let repoint = Repoint::for_relay(&relay_url);
        let wait = match ensure(&orch, orch.services.http.as_ref(), &relay_url, repoint).await {
            Ok(outcome) => {
                log::debug!("GitHub App endpoint: {outcome:?}");
                retry = MIN_RETRY;
                RECHECK
            }
            Err(error) => {
                let wait = retry;
                retry = (retry * 2).min(MAX_RETRY);
                log::warn!(
                    "Could not point the GitHub App's webhook at its endpoint: {error}; retrying in {} seconds",
                    wait.as_secs()
                );
                wait
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = orch.relay.github_app_changed() => retry = MIN_RETRY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{testing::MockHttpClient, HttpMethod, HttpResponse};
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    const TEST_KEY: &str = include_str!("../../tests/fixtures/test_rsa_key.pem");
    const SECRET: &str = "5f0c7e2a9b1d4c6e8f0a2b4c6d8e0f1a3b5c7d9e";
    const OTHER_SECRET: &str = "c4b2a0918f7e6d5c4b3a29180f7e6d5c4b3a2918";

    /// Endpoint ids are unique across the binary: custody's registry is the
    /// process's, and tests run in parallel.
    static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

    /// A stand-in api that keeps its endpoints: a live name conflicts, a
    /// rotation or revocation finds the endpoint on its own channel only.
    #[derive(Default)]
    struct Relay {
        endpoints: Vec<Value>,
        calls: Vec<(String, String, Value)>,
        /// Mint `http://` URLs, as a local api does.
        plain_http: bool,
        /// How many endpoint lists to fail before answering.
        failing_lists: usize,
        /// How many rotations to fail before answering.
        failing_rotates: usize,
    }

    type Shared = Arc<Mutex<Relay>>;

    fn wire_endpoint(id: &str, channel: &str, name: &str, scheme: &str) -> Value {
        json!({
            "id": id,
            "channel": channel,
            "name": name,
            "provider": "github",
            "topic": null,
            "project_ref": null,
            "granted_at": "2026-11-02T00:00:00.000Z",
            "status": "active",
            "url": format!("{scheme}://api.cairn.test/e/{id}"),
            "rejected_count": 0,
        })
    }

    async fn relay_server(state: Shared) -> String {
        use axum::{
            body::Bytes,
            http::{Method, StatusCode, Uri},
            response::IntoResponse,
            Json, Router,
        };
        let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
            let state = state.clone();
            async move {
                let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let path = uri.path().to_string();
                let mut relay = state.lock().unwrap();
                relay
                    .calls
                    .push((method.to_string(), path.clone(), body.clone()));
                let parts: Vec<&str> = path.trim_start_matches("/relay/").split('/').collect();
                let not_found =
                    || (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response();
                match (method.as_str(), parts.as_slice()) {
                    ("POST", ["channels"]) => {
                        Json(json!({"channel_id": "ch-2", "secret": "channel-secret-2"}))
                            .into_response()
                    }
                    ("GET", ["channels", channel, "endpoints"]) => {
                        if relay.failing_lists > 0 {
                            relay.failing_lists -= 1;
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "down"})),
                            )
                                .into_response();
                        }
                        let endpoints: Vec<Value> = relay
                            .endpoints
                            .iter()
                            .filter(|endpoint| endpoint["channel"] == *channel)
                            .cloned()
                            .collect();
                        Json(json!({ "endpoints": endpoints })).into_response()
                    }
                    ("POST", ["channels", channel, "endpoints"]) => {
                        let name = body["name"].as_str().unwrap_or_default().to_string();
                        if relay.endpoints.iter().any(|endpoint| {
                            endpoint["channel"] == *channel
                                && endpoint["name"] == name.as_str()
                                && endpoint["status"] == "active"
                        }) {
                            return (
                                StatusCode::CONFLICT,
                                Json(json!({"error": format!("an active endpoint named {name} already exists here")})),
                            )
                                .into_response();
                        }
                        let id = format!("src_app{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
                        let scheme = if relay.plain_http { "http" } else { "https" };
                        let endpoint = wire_endpoint(&id, channel, &name, scheme);
                        relay.endpoints.push(endpoint.clone());
                        (StatusCode::CREATED, Json(json!({ "endpoint": endpoint }))).into_response()
                    }
                    ("POST", ["channels", channel, "endpoints", id, "rotate"]) => {
                        if relay.failing_rotates > 0 {
                            relay.failing_rotates -= 1;
                            return (
                                StatusCode::BAD_GATEWAY,
                                Json(json!({"error": "upstream down"})),
                            )
                                .into_response();
                        }
                        match relay.endpoints.iter().find(|endpoint| {
                            endpoint["channel"] == *channel && endpoint["id"] == *id
                        }) {
                            Some(endpoint) => Json(json!({ "endpoint": endpoint })).into_response(),
                            None => not_found(),
                        }
                    }
                    ("DELETE", ["channels", channel, "endpoints", id]) => {
                        match relay.endpoints.iter_mut().find(|endpoint| {
                            endpoint["channel"] == *channel && endpoint["id"] == *id
                        }) {
                            Some(endpoint) => {
                                endpoint["status"] = json!("revoked");
                                Json(json!({ "endpoint": endpoint })).into_response()
                            }
                            None => not_found(),
                        }
                    }
                    _ => not_found(),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/relay", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    async fn connect_app(db: &crate::storage::LocalDb, app_id: i64, secret: Option<&str>) {
        credentials::update_github_credentials(db, |creds| {
            creds.app_id = Some(app_id);
            creds.app_slug = Some("cairn-app".into());
            creds.private_key = Some(TEST_KEY.into());
            creds.webhook_secret = secret.map(Into::into);
        })
        .await
        .unwrap();
    }

    struct World {
        orch: Orchestrator,
        db: Arc<crate::storage::LocalDb>,
        relay: Shared,
        relay_url: String,
    }

    impl World {
        /// An install on channel `ch-1` with App 1 connected.
        async fn new(temp: &std::path::Path) -> Self {
            let world = Self::bare(temp).await;
            connect_app(&world.db, 1, Some(SECRET)).await;
            crate::ingress::endpoints::tests::connect_channel(&world.orch).await;
            world
        }

        async fn bare(temp: &std::path::Path) -> Self {
            let (orch, db) = crate::routes::fixture::orchestrator(temp, &[]).await;
            let relay = Shared::default();
            let relay_url = relay_server(relay.clone()).await;
            Self {
                orch,
                db,
                relay,
                relay_url,
            }
        }

        async fn ensure(&self, github: &MockHttpClient) -> Result<Ensured, String> {
            // The stand-in relay is plain http; it plays a public api.
            ensure(&self.orch, github, &self.relay_url, Repoint::Public).await
        }

        /// `(method, path)` of every call the api received.
        fn calls(&self) -> Vec<(String, String)> {
            self.relay
                .lock()
                .unwrap()
                .calls
                .iter()
                .map(|(method, path, _)| (method.clone(), path.clone()))
                .collect()
        }

        /// The bodies of every call to `method path`.
        fn bodies(&self, method: &str, path: &str) -> Vec<Value> {
            self.relay
                .lock()
                .unwrap()
                .calls
                .iter()
                .filter(|call| call.0 == method && call.1 == path)
                .map(|call| call.2.clone())
                .collect()
        }

        async fn endpoint(&self, id: &str) -> EndpointRecord {
            crate::ingress::get_endpoint(&self.db, id)
                .await
                .unwrap()
                .unwrap()
        }
    }

    /// GitHub, answering the hook config with `url` (GET and PATCH alike).
    fn github(url: &str) -> MockHttpClient {
        MockHttpClient::new().respond_to("/app/hook/config", hook(200, json!({ "url": url })))
    }

    fn hook(status: u16, body: Value) -> HttpResponse {
        HttpResponse::new(status, body.to_string().into_bytes())
    }

    /// The method and body of each request to the App's hook config.
    fn hook_calls(github: &MockHttpClient) -> Vec<(HttpMethod, Option<Value>)> {
        github
            .requests()
            .into_iter()
            .filter(|request| request.url.ends_with("/app/hook/config"))
            .map(|request| (request.method, request.body))
            .collect()
    }

    fn patch(url: &str, secret: &str) -> (HttpMethod, Option<Value>) {
        (
            HttpMethod::Patch,
            Some(json!({"url": url, "content_type": "json", "secret": secret})),
        )
    }

    fn pointed(outcome: &Ensured) -> (String, bool) {
        match outcome {
            Ensured::Pointed {
                endpoint_id,
                repointed,
            } => (endpoint_id.clone(), *repointed),
            other => panic!("expected the App to be pointed, got {other:?}"),
        }
    }

    const MINTS: &str = "/relay/channels/ch-1/endpoints";

    #[tokio::test]
    async fn the_first_ensure_mints_once_and_points_the_app_with_its_url_and_secret() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let github = github("https://api.cairn.test/relay/hook/ch-1");

        let (id, repointed) = pointed(&world.ensure(&github).await.unwrap());

        assert!(repointed);
        assert_eq!(
            world.bodies("POST", MINTS),
            vec![json!({
                "name": "github-app-1",
                "provider": "github",
                "rate_per_min": 600,
                "max_body_bytes": 1048576,
                "secret": SECRET,
            })]
        );
        let url = format!("https://api.cairn.test/e/{id}");
        // A fresh endpoint is pointed at without asking GitHub first.
        assert_eq!(hook_calls(&github), vec![patch(&url, SECRET)]);
        let endpoint = world.endpoint(&id).await;
        assert!(endpoint.is_github_app());
        assert_eq!(endpoint.channel_id, "ch-1");
        assert!(custody::is_registered(&id));
        assert!(!custody::held(&id), "there is no Copy for the App's secret");
    }

    #[tokio::test]
    async fn a_second_ensure_mints_nothing_and_leaves_a_matching_webhook_alone() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let (id, _) = pointed(&world.ensure(&github("https://old.test")).await.unwrap());

        let github = github(&format!("https://api.cairn.test/e/{id}"));
        assert_eq!(pointed(&world.ensure(&github).await.unwrap()), (id, false));

        assert_eq!(world.bodies("POST", MINTS).len(), 1);
        assert_eq!(hook_calls(&github), vec![(HttpMethod::Get, None)]);
    }

    /// A name live on the api that the mirror missed (its refresh failed) is
    /// adopted, and rotated to the secret this install holds.
    #[tokio::test]
    async fn a_mint_that_conflicts_adopts_the_live_endpoint() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        {
            let mut relay = world.relay.lock().unwrap();
            relay.endpoints.push(wire_endpoint(
                "src_adopted",
                "ch-1",
                "github-app-1",
                "https",
            ));
            relay.failing_lists = 1;
        }
        let github = github("https://old.test");

        let (id, repointed) = pointed(&world.ensure(&github).await.unwrap());

        assert_eq!(id, "src_adopted");
        assert!(repointed);
        assert_eq!(world.relay.lock().unwrap().endpoints.len(), 1);
        assert_eq!(
            world.bodies("POST", "/relay/channels/ch-1/endpoints/src_adopted/rotate"),
            vec![json!({ "secret": SECRET })]
        );
        assert_eq!(
            hook_calls(&github),
            vec![patch("https://api.cairn.test/e/src_adopted", SECRET)]
        );
    }

    /// A secret that is gone (never set, or it no longer decrypts here) is
    /// replaced: persisted, rotated onto the endpoint, and handed to GitHub.
    #[tokio::test]
    async fn a_missing_secret_is_generated_persisted_rotated_and_sent() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let (id, _) = pointed(&world.ensure(&github("https://old.test")).await.unwrap());
        credentials::update_github_credentials(&world.db, |creds| creds.webhook_secret = None)
            .await
            .unwrap();
        let url = format!("https://api.cairn.test/e/{id}");
        let github = github(&url);

        assert_eq!(
            pointed(&world.ensure(&github).await.unwrap()),
            (id.clone(), true)
        );

        let replaced = credentials::get_github_credentials(&world.db)
            .await
            .unwrap()
            .webhook_secret
            .expect("a new secret is persisted");
        assert_ne!(replaced, SECRET);
        assert_eq!(replaced.len(), 43, "32 random bytes, base64url");
        assert_eq!(
            world.bodies("POST", &format!("{MINTS}/{id}/rotate")),
            vec![json!({ "secret": replaced })]
        );
        assert_eq!(world.bodies("POST", MINTS).len(), 1);
        assert_eq!(hook_calls(&github), vec![patch(&url, &replaced)]);
    }

    /// A replacement secret whose rotation failed is not kept: the next run
    /// never hands GitHub a secret the endpoint was not last given.
    #[tokio::test]
    async fn a_failed_rotation_never_leads_to_a_patch_with_a_secret_the_endpoint_lacks() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let (id, _) = pointed(&world.ensure(&github("https://old.test")).await.unwrap());
        credentials::update_github_credentials(&world.db, |creds| creds.webhook_secret = None)
            .await
            .unwrap();
        world.relay.lock().unwrap().failing_rotates = 1;
        // GitHub still points elsewhere, so every run wants to repoint.
        let github = github("https://api.cairn.test/relay/hook/ch-1");

        assert!(world.ensure(&github).await.is_err());
        assert_eq!(
            credentials::get_github_credentials(&world.db)
                .await
                .unwrap()
                .webhook_secret,
            None,
            "a secret the endpoint never got is not persisted"
        );
        assert!(hook_calls(&github).is_empty());

        let (again, repointed) = pointed(&world.ensure(&github).await.unwrap());
        assert_eq!((again.as_str(), repointed), (id.as_str(), true));

        let rotations = world.bodies("POST", &format!("{MINTS}/{id}/rotate"));
        let carried = rotations.last().unwrap()["secret"].clone();
        let persisted = credentials::get_github_credentials(&world.db)
            .await
            .unwrap()
            .webhook_secret
            .unwrap();
        assert_eq!(carried, json!(persisted));
        for (method, body) in hook_calls(&github) {
            if method == HttpMethod::Patch {
                assert_eq!(body.unwrap()["secret"], carried);
            }
        }
    }

    #[tokio::test]
    async fn without_an_app_or_with_a_rejected_channel_ensure_touches_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::bare(temp.path()).await;
        let github = github("https://old.test");
        assert_eq!(world.ensure(&github).await.unwrap(), Ensured::NoApp);

        connect_app(&world.db, 1, Some(SECRET)).await;
        crate::ingress::endpoints::tests::connect_channel(&world.orch).await;
        channel::mark_rejected(&world.db).await.unwrap();
        assert_eq!(
            world.ensure(&github).await.unwrap(),
            Ensured::ChannelRejected
        );

        assert!(world.calls().is_empty(), "{:?}", world.calls());
        assert!(github.requests().is_empty());
    }

    #[tokio::test]
    async fn a_failed_repoint_keeps_the_endpoint_and_the_next_ensure_repoints() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let github = MockHttpClient::new().respond_to_sequence(
            "/app/hook/config",
            vec![
                hook(500, json!({"message": "boom"})),
                hook(200, json!({"url": "https://old.test"})),
                hook(200, json!({})),
            ],
        );

        assert!(world.ensure(&github).await.is_err());
        let (id, repointed) = pointed(&world.ensure(&github).await.unwrap());

        assert!(repointed);
        assert!(world.endpoint(&id).await.is_active());
        assert_eq!(world.bodies("POST", MINTS).len(), 1);
        let url = format!("https://api.cairn.test/e/{id}");
        assert_eq!(
            hook_calls(&github),
            vec![
                patch(&url, SECRET),
                (HttpMethod::Get, None),
                patch(&url, SECRET)
            ]
        );
    }

    /// A dev instance's local api mints `http://` URLs; a real App's webhook
    /// is never moved to one.
    #[tokio::test]
    async fn an_http_endpoint_is_minted_but_never_pointed_at() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        world.relay.lock().unwrap().plain_http = true;
        let github = github("https://old.test");

        let (id, repointed) = pointed(&world.ensure(&github).await.unwrap());

        assert!(!repointed);
        assert!(world.endpoint(&id).await.is_active());
        assert!(github.requests().is_empty());
    }

    /// A local api reports its endpoints under production's public base by
    /// default, so the URL alone cannot tell a dev instance from production:
    /// the api the install talks to decides.
    #[tokio::test]
    async fn an_https_endpoint_minted_on_a_local_api_is_never_pointed_at() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let github = github("https://old.test");

        let (id, repointed) = pointed(
            &ensure(&world.orch, &github, &world.relay_url, Repoint::Never)
                .await
                .unwrap(),
        );

        assert!(!repointed);
        assert!(world
            .endpoint(&id)
            .await
            .url
            .unwrap()
            .starts_with("https://"));
        assert!(github.requests().is_empty());
        assert_eq!(
            Repoint::for_relay("http://localhost:3849/relay"),
            Repoint::Never
        );
        assert_eq!(
            Repoint::for_relay("https://api.cairn.computer/relay"),
            Repoint::Public
        );
    }

    /// A re-registered channel gets its own App endpoint, the old channel's
    /// is retired, and the App moves to the new one.
    #[tokio::test]
    async fn a_new_channel_gets_a_new_endpoint_and_the_app_follows_it() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let (old, _) = pointed(&world.ensure(&github("https://old.test")).await.unwrap());
        channel::update_relay_channel(&world.db, |channel| {
            channel.channel_id = Some("ch-2".into());
            channel.secret = Some("channel-secret-2".into());
        })
        .await
        .unwrap();
        let github = github(&format!("https://api.cairn.test/e/{old}"));

        let (new, repointed) = pointed(&world.ensure(&github).await.unwrap());

        assert_ne!(new, old);
        assert!(repointed);
        assert_eq!(world.endpoint(&new).await.channel_id, "ch-2");
        assert_eq!(
            world.bodies("POST", "/relay/channels/ch-2/endpoints").len(),
            1
        );
        assert!(!world.endpoint(&old).await.is_active());
        assert_eq!(
            hook_calls(&github),
            vec![patch(&format!("https://api.cairn.test/e/{new}"), SECRET)]
        );
    }

    /// Connecting again creates a new App with a new secret: it gets its own
    /// endpoint, and the old App's is revoked on the api.
    #[tokio::test]
    async fn a_new_app_gets_its_own_endpoint_and_the_old_apps_is_revoked() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let (old, _) = pointed(&world.ensure(&github("https://old.test")).await.unwrap());
        connect_app(&world.db, 2, Some(OTHER_SECRET)).await;
        let github = github(&format!("https://api.cairn.test/e/{old}"));

        let (new, repointed) = pointed(&world.ensure(&github).await.unwrap());

        assert!(repointed);
        assert_eq!(world.endpoint(&new).await.name, "github-app-2");
        assert_eq!(
            world.bodies("POST", MINTS)[1]["secret"],
            json!(OTHER_SECRET)
        );
        assert_eq!(world.bodies("DELETE", &format!("{MINTS}/{old}")).len(), 1);
        assert!(!world.endpoint(&old).await.is_active());
        assert_eq!(
            hook_calls(&github),
            vec![patch(
                &format!("https://api.cairn.test/e/{new}"),
                OTHER_SECRET
            )]
        );
    }

    fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut block = [0u8; 64];
        if key.len() > 64 {
            block[..32].copy_from_slice(&Sha256::digest(key));
        } else {
            block[..key.len()].copy_from_slice(key);
        }
        let pad = |byte: u8| block.iter().map(|b| b ^ byte).collect::<Vec<u8>>();
        let inner = Sha256::new()
            .chain_update(pad(0x36))
            .chain_update(message)
            .finalize();
        let outer = Sha256::new()
            .chain_update(pad(0x5c))
            .chain_update(inner)
            .finalize();
        outer.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn the_probe_signs_like_github() {
        // RFC 4231 test case 2.
        assert_eq!(
            hmac_sha256_hex(b"Jefe", b"what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// Acceptance against a real local api, never production: the api
    /// verifies the signature, seals the body, and the runner's own sync and
    /// consume turn it into a `github_comment` from the signed sender. Set
    /// `CAIRN_LIVE_INGRESS_API` to the local api's base URL (it must be
    /// `http://`, so no real App can be repointed) and run ignored tests.
    #[tokio::test]
    #[ignore = "needs a local api serving ingress endpoints"]
    async fn live_an_app_endpoint_delivery_round_trips_through_a_local_api() {
        let api = std::env::var("CAIRN_LIVE_INGRESS_API")
            .expect("CAIRN_LIVE_INGRESS_API names the local api");
        assert!(api.starts_with("http://"), "a local api only: {api}");
        let relay_url = format!("{}/relay", api.trim_end_matches('/'));
        let temp = tempfile::tempdir().unwrap();
        let mut git = crate::services::testing::MockGitClient::new();
        git.expect_remote_get_url()
            .returning(|_| Ok("git@github.com:acme/repo.git".into()));
        let (orch, db) = crate::routes::fixture::orchestrator_with(
            temp.path(),
            &[],
            crate::services::testing::TestServicesBuilder::new().with_git(git),
        )
        .await;
        connect_app(&db, 4242, Some(SECRET)).await;
        let github = MockHttpClient::new();

        // The channel is registered by the mint, and the http URL is never
        // pointed at: GitHub is not called at all.
        let (id, repointed) = pointed(
            &ensure(&orch, &github, &relay_url, Repoint::for_relay(&relay_url))
                .await
                .unwrap(),
        );
        assert!(!repointed);
        assert!(github.requests().is_empty());
        let endpoint = crate::ingress::get_endpoint(&db, &id)
            .await
            .unwrap()
            .unwrap();
        assert!(endpoint.is_github_app());
        // Posted to the local api whatever URL it reports: an api without
        // INGRESS_PUBLIC_BASE_URL reports production's host.
        let reported = endpoint.url.clone().unwrap();
        let url = format!("{}/e/{id}", api.trim_end_matches('/'));
        assert!(url.starts_with("http://"), "{url}");
        println!(
            "minted {id} (reported at {reported}) on channel {}",
            endpoint.channel_id
        );

        let comment = crate::ingress::consume::tests::mention(777001, "acme/repo").to_string();
        let client = crate::ingress::relay_client().await.unwrap();
        let post = |signature: String| {
            client
                .post(&url)
                .header("Content-Type", "application/json")
                .header("X-GitHub-Event", "issue_comment")
                .header("X-GitHub-Delivery", uuid::Uuid::new_v4().to_string())
                .header("X-Hub-Signature-256", format!("sha256={signature}"))
                .body(comment.clone())
                .send()
        };
        let forged = post(hmac_sha256_hex(b"not-the-secret", comment.as_bytes()))
            .await
            .unwrap();
        assert_eq!(forged.status().as_u16(), 401);
        let signed = post(hmac_sha256_hex(SECRET.as_bytes(), comment.as_bytes()))
            .await
            .unwrap();
        assert_eq!(signed.status().as_u16(), 202);

        crate::ingress::sync::sync_once(&orch, &relay_url)
            .await
            .unwrap();

        let row = crate::ingress::recent_events(&db, &id, 1)
            .await
            .unwrap()
            .remove(0);
        println!(
            "ingress_events: status={} note={:?} body={:?} verification={:?}",
            row.status, row.note, row.body, row.verification
        );
        assert_eq!(row.status, "ok");
        assert_eq!(row.note.as_deref(), Some("github:routed"));
        assert_eq!(row.body, None);
        assert_eq!(row.verification.as_deref(), Some("github"));
        let origin = db
            .query_opt_text(
                "SELECT origin_principal_json FROM fact_log WHERE source = 'github_comment' AND identity = 'github-comment:777001'",
                (),
            )
            .await
            .unwrap()
            .expect("the mention minted a github_comment fact");
        println!("github_comment origin: {origin}");
        assert!(origin.contains("\"583231\""), "{origin}");
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'", ())
                .await
                .unwrap(),
            Some(0)
        );

        // The legacy route still delivers, from the machine.
        let channel_id = channel::get_relay_channel(&db)
            .await
            .unwrap()
            .channel_id
            .unwrap();
        let legacy = crate::ingress::consume::tests::mention(777002, "acme/repo").to_string();
        let hooked = client
            .post(format!("{relay_url}/hook/{channel_id}"))
            .header("Content-Type", "application/json")
            .header("X-GitHub-Event", "issue_comment")
            .body(legacy)
            .send()
            .await
            .unwrap();
        assert_eq!(hooked.status().as_u16(), 200);
        crate::ingress::sync::sync_once(&orch, &relay_url)
            .await
            .unwrap();
        let legacy_origin = db
            .query_opt_text(
                "SELECT origin_principal_json FROM fact_log WHERE identity = 'github-comment:777002'",
                (),
            )
            .await
            .unwrap()
            .expect("the legacy mention minted a github_comment fact");
        println!("legacy github_comment origin: {legacy_origin}");
        assert!(!legacy_origin.contains("583231"), "{legacy_origin}");
    }

    /// The operator's re-registration of a rejected channel: one
    /// registration, the App's endpoint minted again on the new channel by
    /// its maintainer with no further step, and the other endpoint orphaned
    /// for its sender to be reconfigured.
    #[tokio::test]
    async fn reregistering_a_rejected_channel_remints_the_app_and_orphans_the_rest() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::new(temp.path()).await;
        let (old_app, _) = pointed(&world.ensure(&github("https://old.test")).await.unwrap());
        crate::ingress::upsert_endpoint(
            &world.db,
            &EndpointRecord {
                id: "src_hook".into(),
                channel_id: "ch-1".into(),
                project_id: Some("p".into()),
                name: "ci".into(),
                provider: "cairn-hmac".into(),
                status: "active".into(),
                created_at: 1,
                url: Some("https://api.cairn.test/e/src_hook".into()),
                ..EndpointRecord::default()
            },
        )
        .await
        .unwrap();
        channel::mark_rejected(&world.db).await.unwrap();
        // Started while the channel is rejected, the maintainer mints nothing
        // and settles on its hour-long recheck: only the change signal can
        // bring it back within the wait below.
        let maintainer = tokio::spawn(maintain(world.orch.clone(), world.relay_url.clone()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(world
            .bodies("POST", "/relay/channels/ch-2/endpoints")
            .is_empty());

        let outcome = channel::reregister(&world.db, &world.orch.relay, &world.relay_url)
            .await
            .unwrap();

        assert_eq!(outcome.channel_id, "ch-2");
        let orphaned: Vec<&str> = outcome.orphaned.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(orphaned, vec!["src_hook"]);
        let reminted = async {
            loop {
                if let Some(endpoint) = current_endpoint(&world.orch, Some("ch-2"), "github-app-1")
                    .await
                    .unwrap()
                {
                    return endpoint;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let app = tokio::time::timeout(Duration::from_secs(10), reminted)
            .await
            .expect("the maintainer minted the App's endpoint on the new channel");
        maintainer.abort();

        let registrations = world
            .calls()
            .into_iter()
            .filter(|(method, path)| method == "POST" && path == "/relay/channels")
            .count();
        assert_eq!(registrations, 1);
        assert_ne!(app.id, old_app);
        assert_eq!(
            world.bodies("POST", "/relay/channels/ch-2/endpoints").len(),
            1
        );
        let old_app = world.endpoint(&old_app).await;
        assert_eq!(
            (old_app.status.as_str(), old_app.orphaned_at),
            ("revoked", None),
            "the App's old endpoint is superseded, not left for the operator"
        );
        let hook = world.endpoint("src_hook").await;
        assert_eq!(hook.status, "revoked");
        assert!(hook.orphaned_at.is_some(), "{hook:?}");
        let channel = channel::get_relay_channel(&world.db).await.unwrap();
        assert_eq!(channel.channel_id.as_deref(), Some("ch-2"));
        assert_eq!(
            channel::relay_channel_status(&channel).health_state,
            "healthy"
        );
    }

    /// The maintainer runs at startup, and again when it is told the App
    /// changed.
    #[tokio::test]
    async fn the_maintainer_runs_again_when_the_app_changes() {
        let temp = tempfile::tempdir().unwrap();
        let world = World::bare(temp.path()).await;
        world.relay.lock().unwrap().plain_http = true;
        crate::ingress::endpoints::tests::connect_channel(&world.orch).await;
        let maintainer = tokio::spawn(maintain(world.orch.clone(), world.relay_url.clone()));

        connect_app(&world.db, 1, Some(SECRET)).await;
        world.orch.relay.notify_github_app_changed();

        let minted = async {
            while world.bodies("POST", MINTS).is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), minted)
            .await
            .expect("the maintainer minted the App's endpoint after the change");
        maintainer.abort();
    }
}
