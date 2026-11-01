//! Cairn's served OpenAI-compatible API: the facet's second door (tenet 963),
//! for clients outside Cairn on the local network.
//!
//! A client authenticates with its own key ([`clients`]) and names a model by
//! its real name (tenet 1336), never one of Cairn's tiers ([`resolve`]). A
//! model Cairn's own inference deployments serve is always served there,
//! loading on demand, and OpenRouter is used only when the model has no local
//! offer and the client holds a standing grant on the workspace's OpenRouter
//! account. The request
//! then passes the same verb gate every launch does and is relayed byte for
//! byte ([`cairn_models::interface::relay`]); each one leaves a usage row
//! naming the client ([`requests`]).
//!
//! The HTTP surface itself is `cairn-transport`'s `routes::served_api`, on a
//! listener of its own that never carries the loopback API.

pub mod clients;
pub mod listing;
pub mod requests;
pub mod resolve;
mod serve;

pub use serve::{serve_blocking, ServedAnswer, ServedError, ServedEvent, ServedRequest};

use std::collections::BTreeSet;
use std::sync::RwLock;

use cairn_db::storage::served_api::ServedApiClient;
use cairn_models::interface::{ModelScope, Models};

use crate::orchestrator::Orchestrator;
use crate::storage::run_db_blocking;

/// The `/v1/models` body `client` sees. Blocking: it measures deployments and
/// reads the client's grants.
pub fn listing_blocking(
    orch: &Orchestrator,
    client: &ServedApiClient,
) -> Result<serde_json::Value, String> {
    let served = run_db_blocking(|| crate::orchestrator::inference_route::served_now(orch))
        .unwrap_or_else(|error| {
            log::warn!("served API: listing without local deployments: {error}");
            Vec::new()
        });
    let catalog = orch.catalog(&ModelScope::workspace())?;
    let db = orch.db.local.clone();
    let owned = client.clone();
    let granted: BTreeSet<String> =
        run_db_blocking(move || async move { clients::remote_grants(&db, &owned).await })?
            .into_iter()
            .filter_map(|grant| match grant.scope.place {
                cairn_common::authorization::AuthorityPlace::ExternalAccount {
                    provider, ..
                } => Some(provider),
                _ => None,
            })
            .collect();
    Ok(listing::compose(&served, &catalog, |provider| {
        granted.contains(provider)
    }))
}

/// Every name Cairn's deployments serve, with the measures already taken.
/// Unlike the listing, it probes no engine, so a request pays nothing for it.
/// Blocking.
pub(crate) fn served_names_blocking(
    orch: &Orchestrator,
) -> Vec<cairn_inference::served::ServedModel> {
    run_db_blocking(|| async {
        let inventory = crate::orchestrator::inference_route::served_inventory(orch).await?;
        Ok(cairn_inference::served::served_models(
            &inventory,
            &crate::orchestrator::inference_route::measures(),
        ))
    })
    .unwrap_or_else(|error| {
        log::warn!("served API: resolving without local deployments: {error}");
        Vec::new()
    })
}

static LISTENER_STATUS: RwLock<Option<String>> = RwLock::new(None);

/// Record where the served API's listener stands (`serving on 0.0.0.0:80`,
/// or why it is not bound), for the surfaces that show it.
pub fn set_listener_status(status: impl Into<String>) {
    *LISTENER_STATUS
        .write()
        .unwrap_or_else(|error| error.into_inner()) = Some(status.into());
}

/// Where the served API's listener stands; `off` when nothing started one.
pub fn listener_status() -> String {
    LISTENER_STATUS
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .unwrap_or_else(|| "off (no servedApi.bind or --served-api-listen)".to_string())
}

/// The provider key whose offers are local: Cairn's own inference
/// deployments, which any live key may use.
pub const LOCAL_PROVIDER: &str = cairn_models::types::catalog::INFERENCE_BACKEND_KEY;

/// The one remote provider outside clients may be granted.
pub const REMOTE_PROVIDER: &str = crate::backends::openrouter::OPENROUTER_BACKEND_KEY;

/// Whether requests for `provider`'s models are served to outside clients:
/// Cairn's own deployments, and OpenRouter, the remote provider a client can
/// be granted (`cairn://models/clients`). Another provider is added here only
/// together with a way to grant it.
pub fn relays_to(provider: &str) -> bool {
    provider == LOCAL_PROVIDER || provider == REMOTE_PROVIDER
}
