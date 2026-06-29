//! Per-team sync-token minter: a host-agnostic source of rotating Turso Sync
//! auth tokens, plugged into the sync client's `with_auth_token_fn` callback.
//!
//! Turso invokes the auth callback before EVERY sync HTTP request, so a naive
//! callback would mint a token per push/pull. The minter caps that with a
//! per-team cache keyed by expiry (mirroring the org-token refresh margin): a
//! cached token outside the refresh margin is returned as-is; only a missing or
//! near-expiry token triggers a `POST /teams/:id/sync-token`. A mint failure
//! returns `Err`, which fails the in-flight push/pull — the per-team capped
//! backoff retries it, so a transient token outage is never fatal.
//!
//! [`DbState::set_team_token_minter`](crate::db::DbState::set_team_token_minter)
//! installs the minter before any production team opens; absent a minter,
//! `open_team` falls back to the static/unauthenticated token path unchanged
//! (every test and the headless server).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::api::ApiConfig;
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

#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expires_at: i64,
}

/// The production minter: reads the live device JWT from the private DB and
/// exchanges it for a team-scoped sync token via the api, caching per team.
pub struct DefaultTeamTokenMinter {
    local: Arc<LocalDb>,
    api: ApiConfig,
    client: reqwest::Client,
    cache: Mutex<HashMap<String, CachedToken>>,
}

impl DefaultTeamTokenMinter {
    pub fn new(local: Arc<LocalDb>, api: ApiConfig) -> Self {
        Self {
            local,
            api,
            client: reqwest::Client::new(),
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl TeamTokenMinter for DefaultTeamTokenMinter {
    async fn mint(&self, team_id: &str) -> Result<String, String> {
        let now = chrono::Utc::now().timestamp();
        {
            let cache = self.cache.lock().await;
            if let Some(cached) = cache.get(team_id) {
                if cached.expires_at - now > REFRESH_MARGIN_SECS {
                    return Ok(cached.token.clone());
                }
            }
        }

        let device_jwt = read_device_jwt(&self.local)
            .await?
            .ok_or_else(|| "no device JWT available to mint a team sync token".to_string())?;
        let (token, expires_at) =
            mint_team_sync_token(&self.client, &device_jwt, team_id, &self.api).await?;

        self.cache.lock().await.insert(
            team_id.to_string(),
            CachedToken {
                token: token.clone(),
                expires_at,
            },
        );
        Ok(token)
    }
}
