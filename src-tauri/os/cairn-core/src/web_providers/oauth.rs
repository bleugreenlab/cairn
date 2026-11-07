//! Authorizing a pack's `oauth` credential: the declared pre-registered client,
//! run by the generic PKCE and token-store code in [`crate::mcp::oauth`].
//!
//! Nothing here names a provider. The endpoints, client id, scopes and redirect
//! come from the credential's declaration; the token lives in the OAuth store
//! at the same account a secret would (`pack:{pack}/{credential}`), so one
//! keychain namespace holds every pack credential whatever its kind.
//!
//! A relay redirect completes out of band: the authorization server sends the
//! operator to the app relay page, which deep-links `cairn://auth-callback?
//! provider=<relay>&code=…&state=…` into the desktop, and [`resolve_relay`]
//! finds the one installed credential that claims that relay. The pending
//! record's `state` is the authority on completion; the relay only routes.
//!
//! No refresh token is issued unless the client allows `offline_access` and
//! the declaration asks for it. The store refreshes whenever one exists, so
//! that is a change to the declaration alone.

use std::path::Path;

use cairn_packs::provider::{CredentialDecl, OAuthDecl, OAuthRedirect};

use crate::mcp::oauth::store::{self, OAuthStatus, PendingAuthorization, StoredAuth};
use crate::mcp::oauth::{build_authorize_url, exchange_code, generate_pkce, generate_state};

/// How long a started authorization may wait for its callback.
const PENDING_MAX_AGE_SECS: i64 = 600;

/// The OAuth-store account holding a pack credential's token.
pub fn account(pack: &str, credential: &str) -> String {
    crate::config::secrets::pack_credential_key(pack, credential)
}

fn pending_account(pack: &str, credential: &str) -> String {
    format!("{}/pending", account(pack, credential))
}

/// The authorization status of a pack credential. Never refreshes.
pub fn status(pack: &str, credential: &str) -> OAuthStatus {
    store::status(&account(pack, credential))
}

fn declared(config_dir: &Path, pack: &str, credential: &str) -> Result<OAuthDecl, String> {
    let declared = super::installed_credential(config_dir, pack, credential)?;
    declared
        .oauth()
        .cloned()
        .ok_or_else(|| format!("Credential `{pack}/{credential}` is not an OAuth credential"))
}

/// The redirect a declared credential uses: its relay page, or `loopback`.
pub fn redirect(config_dir: &Path, pack: &str, credential: &str) -> Result<OAuthRedirect, String> {
    Ok(declared(config_dir, pack, credential)?.redirect)
}

/// A started authorization: the URL to open, and the redirect it names.
pub struct Started {
    pub url: String,
    pub redirect_uri: String,
}

/// Start authorizing a credential: record the pending PKCE verifier and state,
/// and return the authorize URL. `loopback_uri` is the listener's redirect for
/// a `loopback` declaration and is ignored for a relay.
pub fn begin(
    config_dir: &Path,
    pack: &str,
    credential: &str,
    loopback_uri: Option<&str>,
) -> Result<Started, String> {
    let oauth = declared(config_dir, pack, credential)?;
    let redirect_uri = match (&oauth.redirect, loopback_uri) {
        (OAuthRedirect::Relay(_), _) => oauth.redirect.relay_uri().expect("a relay redirect"),
        (OAuthRedirect::Loopback, Some(uri)) => uri.to_string(),
        (OAuthRedirect::Loopback, None) => {
            return Err(format!(
                "Credential `{pack}/{credential}` redirects to a loopback listener"
            ))
        }
    };
    let pkce = generate_pkce();
    let state = generate_state();
    store::save_pending(
        &pending_account(pack, credential),
        &PendingAuthorization {
            state: state.clone(),
            code_verifier: pkce.verifier,
            created_at: chrono::Utc::now().timestamp(),
        },
    )?;
    let url = build_authorize_url(
        &oauth.authorize,
        &oauth.client_id,
        &redirect_uri,
        "",
        &state,
        &pkce.challenge,
        Some(&oauth.scopes.join(" ")),
    )?;
    Ok(Started { url, redirect_uri })
}

/// Drop a started authorization that will not complete.
pub fn abandon(pack: &str, credential: &str) {
    let _ = store::take_pending(&pending_account(pack, credential));
}

/// The scopes to record for a completed authorization. RFC 6749 §5.1 omits
/// `scope` from a token response when the grant matches the request, so an
/// absent value means everything asked for, not nothing.
fn granted_scopes(response_scope: Option<&str>, requested: &[String]) -> Vec<String> {
    match response_scope.map(str::trim).filter(|s| !s.is_empty()) {
        Some(scope) => scope.split_whitespace().map(str::to_string).collect(),
        None => requested.to_vec(),
    }
}

/// The issuer recorded for a declared client: the authorize endpoint's origin.
fn issuer_of(authorize: &str) -> Option<String> {
    let url = url::Url::parse(authorize).ok()?;
    Some(format!("{}://{}", url.scheme(), url.host_str()?))
}

/// Finish an authorization: consume the pending record, check its age and
/// `state`, exchange the code, and store the token.
pub async fn complete(
    config_dir: &Path,
    pack: &str,
    credential: &str,
    code: &str,
    state: &str,
    redirect_uri: &str,
) -> Result<OAuthStatus, String> {
    let oauth = declared(config_dir, pack, credential)?;
    let pending = store::take_pending(&pending_account(pack, credential))?.ok_or_else(|| {
        format!("No authorization of `{pack}/{credential}` is pending. Start authorization again.")
    })?;
    if chrono::Utc::now().timestamp() - pending.created_at > PENDING_MAX_AGE_SECS {
        return Err("Authorization expired. Start authorization again.".to_string());
    }
    if pending.state != state {
        return Err("Authorization state mismatch — possible CSRF; aborting".to_string());
    }
    let tokens = exchange_code(
        &oauth.token,
        &oauth.client_id,
        None,
        code,
        &pending.code_verifier,
        redirect_uri,
        "",
    )
    .await?;
    let account = account(pack, credential);
    store::save(
        &account,
        &StoredAuth {
            issuer: issuer_of(&oauth.authorize),
            resource: String::new(),
            authorization_endpoint: oauth.authorize.clone(),
            token_endpoint: oauth.token.clone(),
            registration_endpoint: None,
            client_id: oauth.client_id.clone(),
            client_secret: None,
            access_token: tokens.access_token,
            refresh_token: tokens.refresh_token,
            expires_at: tokens.expires_at,
            scopes: granted_scopes(tokens.scope.as_deref(), &oauth.scopes),
            needs_scopes: Vec::new(),
            rejected_at: None,
        },
    )?;
    Ok(store::status(&account))
}

/// Complete a relay callback for the credential that claims `relay`.
pub async fn complete_relay(
    config_dir: &Path,
    relay: &str,
    code: &str,
    state: &str,
) -> Result<(String, String), String> {
    let (pack, credential) = resolve_relay(config_dir, relay)?;
    let redirect_uri = OAuthRedirect::Relay(relay.to_string())
        .relay_uri()
        .expect("a relay redirect");
    complete(config_dir, &pack, &credential, code, state, &redirect_uri).await?;
    Ok((pack, credential))
}

/// Remove a credential's token and any pending authorization. Succeeds when
/// none is stored.
pub fn sign_out(pack: &str, credential: &str) -> Result<(), String> {
    abandon(pack, credential);
    store::delete(&account(pack, credential))
}

/// The one installed credential that claims `relay`, as `(pack, credential)`.
/// A relay no installed credential claims is refused by name, never routed to
/// a default; install keeps a relay to one claimant, and two are refused too.
pub fn resolve_relay(config_dir: &Path, relay: &str) -> Result<(String, String), String> {
    let mut holders = cairn_packs::provider::relay_holders(config_dir, relay);
    match holders.len() {
        1 => {
            let (pack, credential) = holders.remove(0);
            Ok((pack, credential.name))
        }
        0 => Err(format!(
            "No installed pack credential claims the OAuth relay `{relay}`"
        )),
        _ => Err(format!(
            "The OAuth relay `{relay}` is claimed by {}; refusing to guess",
            holders
                .iter()
                .map(|(pack, credential)| format!("`{pack}/{}`", credential.name))
                .collect::<Vec<_>>()
                .join(" and ")
        )),
    }
}

/// What [`adopt_legacy_record`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyRecordAdoption {
    Absent,
    Moved,
    /// The pack account already held a record; the legacy one was deleted.
    Superseded,
}

/// Move an OAuth record a compiled provider kept at `{slot}/{pack}` to the
/// pack credential's account, deleting the old record and its pending entry.
/// One stored truth: after this, nothing reads the old account.
pub(super) fn adopt_legacy_record(
    legacy: &str,
    credential: &CredentialDecl,
    pack: &str,
) -> Result<LegacyRecordAdoption, String> {
    let Some(auth) = store::load(legacy)? else {
        return Ok(LegacyRecordAdoption::Absent);
    };
    let target = account(pack, &credential.name);
    let outcome = if store::load(&target)?.is_some() {
        LegacyRecordAdoption::Superseded
    } else {
        store::save(&target, &auth)?;
        LegacyRecordAdoption::Moved
    };
    store::delete(legacy)?;
    let _ = store::take_pending(&format!("{legacy}/pending"));
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_token_scope_means_everything_requested() {
        let requested = vec!["a.read".to_string(), "b.write".to_string()];
        assert_eq!(granted_scopes(None, &requested), requested);
        assert_eq!(granted_scopes(Some("  "), &requested), requested);
        assert_eq!(granted_scopes(Some("a.read"), &requested), vec!["a.read"]);
    }

    #[test]
    fn the_issuer_is_the_authorize_origin() {
        assert_eq!(
            issuer_of("https://dash.example.test/oauth2/auth").as_deref(),
            Some("https://dash.example.test")
        );
    }
}
