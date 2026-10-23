//! OS-keychain persistence for MCP OAuth credentials and tokens.
//!
//! Mirrors [`crate::config::secrets`] but in a separate keychain namespace
//! (`computer.cairn.mcp-oauth.{env}`) so OAuth state never collides with the
//! `${VAR}` secrets store. The account is the scoped MCP credential key
//! (workspace server name, or `project/{project_path}/{server}`); the value is a single
//! JSON blob ([`StoredAuth`]) holding the resolved endpoints, client
//! credentials, the RFC8707 resource, granted scopes, and the access/refresh
//! tokens.
//!
//! Access/refresh tokens and any client secret are secret and live only here,
//! never in `settings.yaml`. The non-secret `client_id`/`scopes` may also live
//! in the settings `oauth` block; this store is the source of truth at runtime.
//!
//! ## Credential witness
//!
//! The keychain value wraps the record with a [`CredentialGeneration`] that
//! advances on **every** save, including a pure token refresh: the record
//! carries no subject identifier, so an unchanged issuer, resource, client and
//! scope set cannot rule out a changed authorization context behind a new token.
//! The generation commits in the same keychain write as the credential, so a
//! crash cannot separate them. Every write runs under
//! [`crate::config::credential_witness`]'s mutation lock, which is what makes
//! the increment unique rather than merely atomic. Sign-out leaves a tombstone
//! holding the generation rather than deleting the entry, so signing in again
//! continues the sequence instead of restarting it. A token refresh happens
//! outside the lock (it awaits the network) and commits only if the stored value
//! is still exactly the one it started from, so a refresh that finishes after a
//! sign-out or a new sign-in is discarded rather than resurrecting old tokens.
//!
//! [`witness`] is the only projection of this store that cache validation may
//! see. It carries issuer, resource, client, scopes and generation, and has no
//! field that could hold a token or client secret.

use cairn_packs::server_config::{CatalogOAuthContext, CredentialGeneration};
use keyring::{Entry, Error};
use serde::{Deserialize, Serialize};

use super::{refresh_access_token, TokenSet, EXPIRY_SKEW_SECS};
use crate::config::credential_witness::{self, CredentialMutationGuard};

/// Short-lived authorization state stored in the same keychain namespace as
/// tokens so a deep-link callback can safely complete a later process turn.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PendingAuthorization {
    pub state: String,
    pub code_verifier: String,
    pub created_at: i64,
}

pub fn save_pending(key: &str, pending: &PendingAuthorization) -> Result<(), String> {
    let blob = serde_json::to_string(pending)
        .map_err(|e| format!("Failed to serialize pending OAuth authorization: {e}"))?;
    entry(key)?
        .set_password(&blob)
        .map_err(|e| format!("Failed to store pending OAuth authorization: {e}"))
}

pub fn take_pending(key: &str) -> Result<Option<PendingAuthorization>, String> {
    let Some(raw) = (match entry(key)?.get_password() {
        Ok(value) => Some(value),
        Err(Error::NoEntry) => None,
        Err(e) => return Err(format!("Failed to read pending OAuth authorization: {e}")),
    }) else {
        return Ok(None);
    };
    entry(key)?
        .delete_credential()
        .map_err(|e| format!("Failed to clear pending OAuth authorization: {e}"))?;
    serde_json::from_str(&raw)
        .map(Some)
        .map_err(|e| format!("Stored pending OAuth authorization is corrupt: {e}"))
}

/// Everything needed to attach a bearer token and refresh it without re-running
/// discovery: the resolved endpoints, the client, the canonical resource, the
/// granted scopes, and the tokens.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredAuth {
    /// RFC 8414 issuer that issued this server-scoped credential set. Records
    /// written before issuer binding was introduced omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    /// Canonical RFC8707 resource identifier bound on every token request.
    pub resource: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_endpoint: Option<String>,
    pub client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Absolute access-token expiry (unix seconds), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    /// Scopes actually granted (or requested when the server is silent).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    /// Scopes a `403 insufficient_scope` asked for but we have not yet been
    /// granted. Surfaced as a step-up prompt; the next authorize widens to these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs_scopes: Vec<String>,
}

impl StoredAuth {
    /// Whether the access token is expired (or within the refresh skew window).
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => chrono::Utc::now().timestamp() + EXPIRY_SKEW_SECS >= expires_at,
            None => false,
        }
    }

    /// Fold a freshly obtained/refreshed token set into the stored record,
    /// preserving endpoints and client identity.
    fn apply_tokens(&mut self, tokens: TokenSet) {
        self.access_token = tokens.access_token;
        if tokens.refresh_token.is_some() {
            self.refresh_token = tokens.refresh_token;
        }
        self.expires_at = tokens.expires_at;
        if let Some(scope) = tokens.scope {
            let granted: Vec<String> = scope.split_whitespace().map(str::to_string).collect();
            if !granted.is_empty() {
                self.scopes = granted;
            }
        }
    }
}

/// The runtime authorization status of a server, for the settings UI.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OAuthStatus {
    /// `authorized`, `needs_auth`, or `error`.
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    scopes: Vec<String>,
    /// Scopes a step-up (`403 insufficient_scope`) is waiting on, if any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    needs_scopes: Vec<String>,
}

impl OAuthStatus {
    fn authorized(scopes: Vec<String>, needs_scopes: Vec<String>) -> Self {
        let (state, detail) = if needs_scopes.is_empty() {
            ("authorized", None)
        } else {
            ("authorized", Some("Additional scopes required".to_string()))
        };
        Self {
            state: state.to_string(),
            detail,
            scopes,
            needs_scopes,
        }
    }

    /// The server requires OAuth but has no stored token yet.
    pub(crate) fn needs_auth() -> Self {
        Self {
            state: "needs_auth".to_string(),
            detail: None,
            scopes: Vec::new(),
            needs_scopes: Vec::new(),
        }
    }

    /// The server does not require OAuth (no authorization affordance shown).
    pub fn none() -> Self {
        Self {
            state: "none".to_string(),
            detail: None,
            scopes: Vec::new(),
            needs_scopes: Vec::new(),
        }
    }
}

/// Keychain service name for the OAuth namespace, scoped to dev/prod.
fn service() -> String {
    format!(
        "computer.cairn.mcp-oauth.{}",
        cairn_common::paths::env_str()
    )
}

fn entry(server: &str) -> Result<Entry, String> {
    Entry::new(&service(), server).map_err(|e| format!("Failed to open keychain entry: {e}"))
}

/// The keychain value. Untagged so records written before witnesses existed
/// still read; the envelope shapes refuse unknown fields so neither can be
/// mistaken for the other.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StoredEntry {
    Tombstone(Tombstone),
    Witnessed(WitnessedAuth),
    Legacy(StoredAuth),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Tombstone {
    tombstone: CredentialGeneration,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WitnessedAuth {
    auth: StoredAuth,
    generation: CredentialGeneration,
}

/// What the keychain holds for one key, and the exact text it held.
enum Current {
    Absent,
    Tombstone(CredentialGeneration),
    Witnessed(StoredAuth, CredentialGeneration),
    /// Written before witnesses existed: usable, but not a witness.
    Legacy(StoredAuth),
    Corrupt,
    Unavailable(String),
}

fn read_current(server: &str) -> (Current, Option<String>) {
    let raw = match entry(server).and_then(|entry| {
        entry.get_password().map(Some).or_else(|e| match e {
            Error::NoEntry => Ok(None),
            e => Err(format!(
                "Failed to read OAuth tokens for {server} from keychain: {e}"
            )),
        })
    }) {
        Ok(Some(raw)) => raw,
        Ok(None) => return (Current::Absent, None),
        Err(e) => return (Current::Unavailable(e), None),
    };
    let current = match serde_json::from_str::<StoredEntry>(&raw) {
        Ok(StoredEntry::Tombstone(Tombstone { tombstone })) => Current::Tombstone(tombstone),
        Ok(StoredEntry::Witnessed(WitnessedAuth { auth, generation })) => {
            Current::Witnessed(auth, generation)
        }
        Ok(StoredEntry::Legacy(auth)) => Current::Legacy(auth),
        Err(_) => Current::Corrupt,
    };
    (current, Some(raw))
}

/// The generation the next write must carry: strictly after the current one,
/// or a fresh incarnation when there is no readable history to continue.
fn next_generation(server: &str, current: &Current) -> Result<CredentialGeneration, String> {
    match current {
        Current::Tombstone(generation) | Current::Witnessed(_, generation) => {
            Ok(CredentialGeneration {
                incarnation: generation.incarnation.clone(),
                counter: generation.counter.checked_add(1).ok_or_else(|| {
                    format!("OAuth credential generation for `{server}` is exhausted")
                })?,
            })
        }
        Current::Absent | Current::Legacy(_) | Current::Corrupt => Ok(CredentialGeneration {
            incarnation: credential_witness::new_incarnation(),
            counter: 0,
        }),
        Current::Unavailable(e) => Err(e.clone()),
    }
}

fn write_entry(
    _guard: &CredentialMutationGuard,
    server: &str,
    value: &StoredEntry,
) -> Result<(), String> {
    let blob = serde_json::to_string(value)
        .map_err(|e| format!("Failed to serialize OAuth record: {e}"))?;
    entry(server)?
        .set_password(&blob)
        .map_err(|e| format!("Failed to store OAuth tokens in keychain: {e}"))
}

fn write_auth(
    guard: &CredentialMutationGuard,
    server: &str,
    current: &Current,
    auth: &StoredAuth,
) -> Result<(), String> {
    let generation = next_generation(server, current)?;
    write_entry(
        guard,
        server,
        &StoredEntry::Witnessed(WitnessedAuth {
            auth: auth.clone(),
            generation,
        }),
    )
}

/// Load the stored OAuth record for `server`, if one exists.
pub fn load(server: &str) -> Option<StoredAuth> {
    match read_current(server).0 {
        Current::Witnessed(auth, _) | Current::Legacy(auth) => Some(auth),
        Current::Absent | Current::Tombstone(_) => None,
        Current::Corrupt => {
            log::warn!("Stored OAuth record for {server} is corrupt");
            None
        }
        Current::Unavailable(e) => {
            log::warn!("{e}");
            None
        }
    }
}

/// Store (or replace) the OAuth record for `server`, advancing its generation.
pub fn save(server: &str, auth: &StoredAuth) -> Result<(), String> {
    let guard = credential_witness::acquire()?;
    let (current, _) = read_current(server);
    write_auth(&guard, server, &current, auth)
}

/// Sign `server` out. The tokens go; the generation history stays behind as a
/// tombstone. Succeeds when nothing is signed in.
pub fn delete(server: &str) -> Result<(), String> {
    let guard = credential_witness::acquire()?;
    let (current, _) = read_current(server);
    match current {
        Current::Absent | Current::Tombstone(_) => Ok(()),
        Current::Unavailable(e) => Err(e),
        _ => {
            let tombstone = next_generation(server, &current)?;
            write_entry(
                &guard,
                server,
                &StoredEntry::Tombstone(Tombstone { tombstone }),
            )
        }
    }
}

/// Change the stored record in place under the lock, so the read-modify-write
/// cannot interleave with a sign-out or a new sign-in. A record that is gone
/// stays gone.
fn update(server: &str, change: impl FnOnce(&mut StoredAuth)) -> Result<(), String> {
    let guard = credential_witness::acquire()?;
    let (current, _) = read_current(server);
    let mut auth = match &current {
        Current::Witnessed(auth, _) | Current::Legacy(auth) => auth.clone(),
        Current::Absent | Current::Tombstone(_) | Current::Corrupt => return Ok(()),
        Current::Unavailable(e) => return Err(e.clone()),
    };
    change(&mut auth);
    write_auth(&guard, server, &current, &auth)
}

/// Commit a refreshed record only if the stored value is still byte-for-byte
/// the one the refresh started from. `Ok(false)` means it was superseded.
fn commit_refresh(server: &str, started_from: &str, auth: &StoredAuth) -> Result<bool, String> {
    let guard = credential_witness::acquire()?;
    let (current, raw) = read_current(server);
    if raw.as_deref() != Some(started_from) {
        return Ok(false);
    }
    write_auth(&guard, server, &current, auth)?;
    Ok(true)
}

/// The OAuth credential witness for `server`, as cache validation may see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthWitness {
    /// No OAuth credential has ever been stored under this key.
    Never,
    /// Signed out; the tombstone's generation, which a sign-in and sign-out
    /// during one fetch would have advanced.
    SignedOut(CredentialGeneration),
    /// Signed in, with this token-free context and generation.
    SignedIn(CatalogOAuthContext),
    /// Signed in with a record written before witnesses existed. Usable for a
    /// connection, never for caching; its next refresh or sign-in witnesses it.
    Unwitnessed,
    /// The keychain could not be read or the record is corrupt. Fail closed.
    Unavailable(String),
}

/// Project the token-free witness for `server`. Never refreshes, never prompts.
pub fn witness(server: &str) -> OAuthWitness {
    match read_current(server).0 {
        Current::Absent => OAuthWitness::Never,
        Current::Tombstone(generation) => OAuthWitness::SignedOut(generation),
        Current::Witnessed(auth, generation) => OAuthWitness::SignedIn(CatalogOAuthContext {
            issuer: auth.issuer,
            resource: auth.resource,
            client_id: auth.client_id,
            scopes: auth.scopes,
            generation,
        }),
        Current::Legacy(_) => OAuthWitness::Unwitnessed,
        Current::Corrupt => {
            OAuthWitness::Unavailable(format!("Stored OAuth record for {server} is corrupt"))
        }
        Current::Unavailable(e) => OAuthWitness::Unavailable(e),
    }
}

/// The authorization status for `server`. A stored token reports `authorized`
/// (the gateway refreshes it on use when a refresh token is present); a stored
/// token that is already expired AND has no refresh token reports `needs_auth`,
/// because it cannot be made usable without a fresh authorize. Reporting such a
/// dead token as `authorized` is what showed a green "Authorized" badge while
/// every connection failed. Surfaces any pending step-up scopes.
pub fn status(server: &str) -> OAuthStatus {
    match load(server) {
        Some(auth) if auth.is_expired() && auth.refresh_token.is_none() => {
            OAuthStatus::needs_auth()
        }
        Some(auth) => OAuthStatus::authorized(auth.scopes, auth.needs_scopes),
        None => OAuthStatus::needs_auth(),
    }
}

/// Record that a `403 insufficient_scope` asked for `required` scopes, so the
/// settings UI can prompt for a re-authorize that widens the scope set.
pub fn record_scope_step_up(server: &str, required: &[String]) {
    if required.is_empty() {
        return;
    }
    let result = update(server, |auth| {
        let mut union = auth.scopes.clone();
        for scope in required {
            if !union.contains(scope) {
                union.push(scope.clone());
            }
        }
        auth.needs_scopes = union
            .into_iter()
            .filter(|s| !auth.scopes.contains(s))
            .collect();
    });
    if let Err(e) = result {
        log::warn!("Failed to record OAuth scope step-up for {server}: {e}");
    }
}

/// Drop the cached record for `server` from memory of validity by clearing its
/// expiry so the next [`get_valid_access_token`] forces a refresh. Used by the
/// gateway after a `401` to recover from a server-side revocation.
pub fn force_expire(server: &str) {
    if let Err(e) = update(server, |auth| auth.expires_at = Some(0)) {
        log::warn!("Failed to expire OAuth tokens for {server}: {e}");
    }
}

/// Resolve a currently-valid access token for `server`, refreshing via the
/// refresh-token grant when the stored token is expired (or within the skew
/// window). Returns `None` when the server has no stored authorization or a
/// refresh is impossible/failed — the gateway then surfaces "authorization
/// required".
pub async fn get_valid_access_token(server: &str) -> Option<String> {
    let (current, raw) = read_current(server);
    let mut auth = match current {
        Current::Witnessed(auth, _) | Current::Legacy(auth) => auth,
        _ => return None,
    };
    if !auth.is_expired() {
        return Some(auth.access_token);
    }
    let started_from = raw?;
    let refresh_token = auth.refresh_token.clone()?;
    match refresh_access_token(
        &auth.token_endpoint,
        &auth.client_id,
        auth.client_secret.as_deref(),
        &refresh_token,
        &auth.resource,
        scope_param(&auth.scopes).as_deref(),
    )
    .await
    {
        Ok(tokens) => {
            auth.apply_tokens(tokens);
            match commit_refresh(server, &started_from, &auth) {
                Ok(true) => Some(auth.access_token),
                // Signed out or replaced while the refresh was in flight. The
                // refreshed tokens belong to the old credential and are dropped;
                // whatever is stored now is what the user chose.
                Ok(false) => {
                    log::info!("Discarded a superseded OAuth refresh for {server}");
                    load(server)
                        .filter(|current| !current.is_expired())
                        .map(|current| current.access_token)
                }
                // An unpersisted token would authenticate a connection under a
                // generation that never advanced, so it is not used either.
                Err(e) => {
                    log::warn!("Failed to persist refreshed OAuth tokens for {server}: {e}");
                    None
                }
            }
        }
        Err(e) => {
            log::warn!("OAuth token refresh for {server} failed: {e}");
            None
        }
    }
}

fn scope_param(scopes: &[String]) -> Option<String> {
    if scopes.is_empty() {
        None
    } else {
        Some(scopes.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::secrets::mock_keychain;

    fn sample(server_token: &str) -> StoredAuth {
        StoredAuth {
            issuer: Some("https://auth.example.com".to_string()),
            resource: "https://api.example.com/mcp".to_string(),
            authorization_endpoint: "https://auth.example.com/authorize".to_string(),
            token_endpoint: "https://auth.example.com/token".to_string(),
            registration_endpoint: None,
            client_id: "client-123".to_string(),
            client_secret: None,
            access_token: server_token.to_string(),
            refresh_token: Some("refresh-abc".to_string()),
            expires_at: Some(chrono::Utc::now().timestamp() + 3600),
            scopes: vec!["read".to_string()],
            needs_scopes: Vec::new(),
        }
    }

    #[test]
    fn pending_authorization_is_consumed_once() {
        mock_keychain::install();
        let key = "pending-consumed-once";
        let pending = PendingAuthorization {
            state: "state".to_string(),
            code_verifier: "verifier".to_string(),
            created_at: 123,
        };

        save_pending(key, &pending).unwrap();
        assert_eq!(take_pending(key).unwrap(), Some(pending));
        assert_eq!(take_pending(key).unwrap(), None);
    }

    #[test]
    fn save_load_delete_roundtrip() {
        mock_keychain::install();
        let server = "store-roundtrip";
        assert!(load(server).is_none());
        assert_eq!(status(server).state, "needs_auth");

        let auth = sample("access-1");
        save(server, &auth).unwrap();
        assert_eq!(load(server), Some(auth.clone()));
        let st = status(server);
        assert_eq!(st.state, "authorized");
        assert_eq!(st.scopes, vec!["read".to_string()]);

        delete(server).unwrap();
        assert!(load(server).is_none());
        // Deleting an absent record is a no-op success.
        delete(server).unwrap();
    }

    #[test]
    fn status_reflects_token_usability_not_mere_presence() {
        mock_keychain::install();

        // Fresh token -> authorized.
        let server = "store-status-valid";
        save(server, &sample("good")).unwrap();
        assert_eq!(status(server).state, "authorized");

        // Expired but refreshable -> still authorized (gateway refreshes on use).
        let server = "store-status-expired-refreshable";
        let mut auth = sample("stale");
        auth.expires_at = Some(chrono::Utc::now().timestamp() - 10);
        assert!(auth.refresh_token.is_some());
        save(server, &auth).unwrap();
        assert_eq!(status(server).state, "authorized");

        // Expired AND no refresh token -> needs_auth: the token is dead and
        // cannot be made usable without a fresh authorize, so the UI must offer
        // it rather than show a green "Authorized" that fails on every connect.
        let server = "store-status-expired-dead";
        let mut auth = sample("dead");
        auth.expires_at = Some(chrono::Utc::now().timestamp() - 10);
        auth.refresh_token = None;
        save(server, &auth).unwrap();
        assert_eq!(status(server).state, "needs_auth");
    }

    #[test]
    fn records_are_scoped_per_server() {
        mock_keychain::install();
        let mut first = sample("a-token");
        first.issuer = Some("https://issuer-a.example.com".to_string());
        let mut second = sample("b-token");
        second.issuer = Some("https://issuer-b.example.com".to_string());
        save("store-a", &first).unwrap();
        save("store-b", &second).unwrap();
        let loaded_first = load("store-a").unwrap();
        let loaded_second = load("store-b").unwrap();
        assert_eq!(loaded_first.access_token, "a-token");
        assert_eq!(
            loaded_first.issuer.as_deref(),
            Some("https://issuer-a.example.com")
        );
        assert_eq!(loaded_second.access_token, "b-token");
        assert_eq!(
            loaded_second.issuer.as_deref(),
            Some("https://issuer-b.example.com")
        );
    }

    #[test]
    fn legacy_record_without_issuer_remains_readable() {
        let mut value = serde_json::to_value(sample("legacy-token")).unwrap();
        value.as_object_mut().unwrap().remove("issuer");

        let auth: StoredAuth = serde_json::from_value(value).unwrap();
        assert_eq!(auth.issuer, None);
        assert_eq!(auth.access_token, "legacy-token");
    }

    #[test]
    fn is_expired_respects_skew() {
        let mut auth = sample("t");
        auth.expires_at = Some(chrono::Utc::now().timestamp() + EXPIRY_SKEW_SECS - 5);
        assert!(auth.is_expired());
        auth.expires_at = Some(chrono::Utc::now().timestamp() + EXPIRY_SKEW_SECS + 120);
        assert!(!auth.is_expired());
        auth.expires_at = None;
        assert!(!auth.is_expired());
    }

    #[test]
    fn apply_tokens_keeps_prior_refresh_and_updates_scopes() {
        let mut auth = sample("old-access");
        auth.apply_tokens(TokenSet {
            access_token: "new-access".to_string(),
            refresh_token: None,
            expires_at: Some(123),
            scope: Some("read write".to_string()),
            token_type: "Bearer".to_string(),
        });
        assert_eq!(auth.access_token, "new-access");
        assert_eq!(auth.refresh_token.as_deref(), Some("refresh-abc"));
        assert_eq!(auth.expires_at, Some(123));
        assert_eq!(auth.scopes, vec!["read".to_string(), "write".to_string()]);
    }

    #[test]
    fn step_up_records_only_missing_scopes() {
        mock_keychain::install();
        let server = "store-stepup";
        save(server, &sample("t")).unwrap();
        record_scope_step_up(server, &["read".to_string(), "admin".to_string()]);
        let auth = load(server).unwrap();
        // `read` already granted; only `admin` is pending.
        assert_eq!(auth.needs_scopes, vec!["admin".to_string()]);
        assert_eq!(status(server).needs_scopes, vec!["admin".to_string()]);
    }

    #[tokio::test]
    async fn valid_token_returned_without_refresh() {
        mock_keychain::install();
        let server = "store-valid";
        save(server, &sample("still-good")).unwrap();
        assert_eq!(
            get_valid_access_token(server).await.as_deref(),
            Some("still-good")
        );
    }

    #[tokio::test]
    async fn missing_server_yields_none() {
        mock_keychain::install();
        assert!(get_valid_access_token("store-absent").await.is_none());
    }

    fn signed_in(server: &str) -> CatalogOAuthContext {
        match witness(server) {
            OAuthWitness::SignedIn(context) => context,
            other => panic!("expected a signed-in witness, got {other:?}"),
        }
    }

    #[test]
    fn every_save_advances_the_generation_even_with_identical_context() {
        mock_keychain::install();
        let server = "witness-refresh-identical";
        save(server, &sample("token-1")).unwrap();
        let first = signed_in(server);
        // A pure token refresh: issuer, resource, client and scopes unchanged.
        save(server, &sample("token-2")).unwrap();
        let second = signed_in(server);
        assert_eq!(
            (
                &first.issuer,
                &first.resource,
                &first.client_id,
                &first.scopes
            ),
            (
                &second.issuer,
                &second.resource,
                &second.client_id,
                &second.scopes
            )
        );
        assert_eq!(first.generation.incarnation, second.generation.incarnation);
        assert_eq!(second.generation.counter, first.generation.counter + 1);
        force_expire(server);
        assert_eq!(
            signed_in(server).generation.counter,
            second.generation.counter + 1
        );
        record_scope_step_up(server, &["admin".to_string()]);
        assert_eq!(
            signed_in(server).generation.counter,
            second.generation.counter + 2
        );
    }

    #[test]
    fn sign_out_keeps_history_so_signing_in_again_never_reuses_a_generation() {
        mock_keychain::install();
        let server = "witness-sign-out";
        assert_eq!(witness(server), OAuthWitness::Never);
        save(server, &sample("account-a")).unwrap();
        let a = signed_in(server);
        delete(server).unwrap();
        let OAuthWitness::SignedOut(tombstone) = witness(server) else {
            panic!("sign-out must keep its generation in the witness");
        };
        assert!(tombstone.counter > a.generation.counter);
        assert!(load(server).is_none());
        assert_eq!(status(server).state, "needs_auth");
        // Signing out twice changes nothing; force-expire and step-up cannot
        // resurrect a signed-out record.
        delete(server).unwrap();
        force_expire(server);
        record_scope_step_up(server, &["admin".to_string()]);
        assert_eq!(witness(server), OAuthWitness::SignedOut(tombstone.clone()));
        // A sign-in and sign-out leaves a different signed-out witness, never
        // the one it started from.
        save(server, &sample("account-b")).unwrap();
        delete(server).unwrap();
        assert!(matches!(witness(server), OAuthWitness::SignedOut(after) if after != tombstone));
        save(server, &sample("account-a")).unwrap();
        let again = signed_in(server);
        assert_eq!(again.generation.incarnation, a.generation.incarnation);
        assert!(again.generation.counter > a.generation.counter + 1);
    }

    #[test]
    fn concurrent_saves_publish_distinct_generations() {
        mock_keychain::install();
        let server = "witness-concurrent-saves";
        save(server, &sample("seed")).unwrap();
        let seed = signed_in(server).generation.counter;
        let writers: Vec<_> = (0..8)
            .map(|writer| {
                std::thread::spawn(move || {
                    for round in 0..10 {
                        save(server, &sample(&format!("{writer}-{round}"))).unwrap();
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(signed_in(server).generation.counter, seed + 80);
    }

    #[test]
    fn a_refresh_that_finishes_after_a_new_sign_in_is_discarded() {
        mock_keychain::install();
        let server = "witness-stale-refresh";
        save(server, &sample("account-a")).unwrap();
        let (_, started_from) = read_current(server);
        let started_from = started_from.unwrap();
        // The user signs in as B while A's refresh is in flight.
        save(server, &sample("account-b")).unwrap();
        let b = signed_in(server);
        let mut refreshed = sample("account-a");
        refreshed.access_token = "account-a-refreshed".into();
        assert!(!commit_refresh(server, &started_from, &refreshed).unwrap());
        assert_eq!(load(server).unwrap().access_token, "account-b");
        assert_eq!(signed_in(server), b);
        // Likewise after a sign-out: the refresh must not sign A back in.
        let (_, started_from) = read_current(server);
        delete(server).unwrap();
        assert!(!commit_refresh(server, &started_from.unwrap(), &refreshed).unwrap());
        assert!(matches!(witness(server), OAuthWitness::SignedOut(_)));
    }

    #[test]
    fn an_unchanged_refresh_commits_with_a_new_generation() {
        mock_keychain::install();
        let server = "witness-fresh-refresh";
        save(server, &sample("token")).unwrap();
        let before = signed_in(server);
        let (_, started_from) = read_current(server);
        assert!(
            commit_refresh(server, &started_from.unwrap(), &sample("token-refreshed")).unwrap()
        );
        assert_eq!(load(server).unwrap().access_token, "token-refreshed");
        assert_eq!(
            signed_in(server).generation.counter,
            before.generation.counter + 1
        );
    }

    #[test]
    fn legacy_records_stay_usable_but_are_not_witnesses() {
        mock_keychain::install();
        let server = "witness-legacy";
        let legacy = sample("legacy-token");
        entry(server)
            .unwrap()
            .set_password(&serde_json::to_string(&legacy).unwrap())
            .unwrap();
        assert_eq!(load(server), Some(legacy.clone()));
        assert_eq!(witness(server), OAuthWitness::Unwitnessed);
        save(server, &legacy).unwrap();
        assert!(matches!(witness(server), OAuthWitness::SignedIn(_)));
    }

    #[test]
    fn a_corrupt_record_is_no_witness_and_sign_out_starts_a_new_incarnation() {
        mock_keychain::install();
        let server = "witness-corrupt-oauth";
        save(server, &sample("token")).unwrap();
        let original = signed_in(server);
        entry(server).unwrap().set_password("{corrupt").unwrap();
        assert!(matches!(witness(server), OAuthWitness::Unavailable(_)));
        assert!(load(server).is_none());
        delete(server).unwrap();
        save(server, &sample("token")).unwrap();
        assert_ne!(
            signed_in(server).generation.incarnation,
            original.generation.incarnation
        );
    }

    #[test]
    fn the_witness_projection_carries_no_token_material() {
        mock_keychain::install();
        let server = "witness-token-free";
        let mut auth = sample("access-Zx91Qw82");
        auth.client_secret = Some("client-secret-Ab12".into());
        auth.refresh_token = Some("refresh-Cd34".into());
        save(server, &auth).unwrap();
        let projected = serde_json::to_string(&signed_in(server)).unwrap();
        for secret in ["access-Zx91Qw82", "client-secret-Ab12", "refresh-Cd34"] {
            assert!(!projected.contains(secret));
        }
    }
}
