//! Per-team sync-token minter: a host-agnostic source of rotating Turso Sync
//! auth tokens, plugged into the sync client's `with_auth_token_fn` callback.
//!
//! Turso invokes the auth callback before EVERY sync HTTP request, so a naive
//! callback would mint a token per push/pull. The minter caps that by keeping
//! each team's token as a [credential lease](crate::security::lease): a live
//! lease outside the refresh margin is presented as-is; only a missing or
//! near-expiry one triggers a `POST /teams/:id/sync-token`. A mint failure
//! returns `Err`, which fails the in-flight push/pull — the per-team capped
//! backoff retries it, so a transient token outage is never fatal.
//!
//! The lease book replaced a private `HashMap` cache that did the same
//! expiry arithmetic and nothing else. What the lease adds is that the token is
//! a registered scrub target from the moment it is minted, that it names the
//! consumer it may be presented to, and that it can be revoked — the cache had
//! no way to answer "stop using this team's token now".
//!
//! [`DbState::set_team_token_minter`](crate::db::DbState::set_team_token_minter)
//! installs the minter before any production team opens; absent a minter,
//! `open_team` falls back to the static/unauthenticated token path unchanged
//! (for focused tests and local-only hosts that intentionally skip installation).

use std::sync::Arc;

use async_trait::async_trait;

use crate::api::ApiConfig;
use crate::security::broker::account;
use crate::storage::LocalDb;

use super::team_sync::{mint_team_sync_token, read_device_jwt};

/// How close to expiry before a cached token is considered stale (5 minutes),
/// matching the org-token cache margin.
const REFRESH_MARGIN_SECS: i64 = 5 * 60;

/// A source of valid per-team sync tokens. Object-safe so [`crate::db::DbState`]
/// can hold it as `Arc<dyn TeamTokenMinter>` without depending on the account /
/// api machinery directly.
#[async_trait]
pub trait TeamTokenMinter: Send + Sync {
    /// Return a currently-valid sync token for `team_id`, minting and caching a
    /// fresh one when none is cached or the cached one is within the refresh
    /// margin of expiry.
    async fn mint(&self, team_id: &str) -> Result<String, String>;
}

/// The production minter: reads the live device JWT from the private DB and
/// exchanges it for a team-scoped sync token via the api, leasing it per team.
pub struct DefaultTeamTokenMinter {
    local: Arc<LocalDb>,
    api: ApiConfig,
    client: reqwest::Client,
}

impl DefaultTeamTokenMinter {
    pub fn new(local: Arc<LocalDb>, api: ApiConfig) -> Self {
        Self {
            local,
            api,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl TeamTokenMinter for DefaultTeamTokenMinter {
    async fn mint(&self, team_id: &str) -> Result<String, String> {
        let now = chrono::Utc::now().timestamp();
        let lease = match account::live_sync_token(team_id, now + REFRESH_MARGIN_SECS) {
            Some(live) => live,
            None => {
                let device_jwt = read_device_jwt(&self.local).await?.ok_or_else(|| {
                    "no device JWT available to mint a team sync token".to_string()
                })?;
                let (token, expires_at) =
                    mint_team_sync_token(&self.client, &device_jwt, team_id, &self.api).await?;
                account::lease_sync_token(team_id, expires_at, token)
            }
        };
        // The sync client takes an owned `String` to put in an Authorization
        // header, so this is where the lease becomes plaintext. Presenting names
        // the audience, so the same token cannot be handed to anything else.
        let presented = lease
            .present(&account::sync_audience())
            .map_err(|denied| format!("team sync token unusable: {denied}"))?;
        Ok(presented.expose().to_string())
    }
}
