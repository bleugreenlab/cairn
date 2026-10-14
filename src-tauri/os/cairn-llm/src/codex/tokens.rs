//! The session side of a Cairn-managed Codex (ChatGPT) OAuth credential.
//!
//! A ChatGPT refresh token rotates: spending it mints a replacement and retires
//! the original. Renewing one is subscription mechanics and lives behind
//! [`crate::subscription::Subscriptions::refresh_codex`]; this module holds the
//! values a session needs around that call: the token set it runs on, the rule
//! that its view never advances past the durable store ([`CodexAuthState::adopt`]),
//! how a stored `auth.json` identifies its account, and how a failed renewal is
//! graded and told to an operator.

use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::accounts::{ApiProvider, CredentialStore, ProviderAccount, ProviderAuth};
use crate::subscription::LlmHost;

/// Cairn-owned field in the stored `auth.json`: which rotation of this account's
/// credential the file holds.
///
/// Stamped on every publication and never read by Codex itself. It exists so a
/// reader can say "the file is newer than what I was about to spend" in a log,
/// a diagnostic, or a test without comparing secrets to do it.
pub const GENERATION_FIELD: &str = "cairn_refresh_generation";

/// The provider these credentials belong to, for the broker's audit record.
const CODEX_PROVIDER: &str = "openai";

/// One Codex token set.
///
/// Deliberately not `Debug`, so a token set cannot be formatted into a panic
/// message, a log line, or anything else that outlives the moment it is used.
#[derive(Clone)]
pub struct CodexOAuthTokens {
    pub id_token: String,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub chatgpt_account_id: Option<String>,
}

/// A session's view of the credential it runs on.
pub struct CodexAuthState {
    account_id: Option<String>,
    tokens: CodexOAuthTokens,
}

/// Register a Codex token set for scrubbing.
///
/// Codex authenticates over HTTP from inside this process rather than through
/// a child's environment, so there is no injection point to lease against; what
/// registration buys is that a token echoed back — in an OAuth error body, in a
/// diagnostic, in an agent's own reading of the auth file — is scrubbed rather
/// than transcribed. The refresh token matters most: it outlives both of the
/// others and mints replacements for them.
fn register_codex_tokens(host: &dyn LlmHost, account_id: Option<&str>, tokens: &CodexOAuthTokens) {
    let account = account_id.unwrap_or("default");
    let register = |suffix: &str, value: &str| {
        host.register_secret(CODEX_PROVIDER, &format!("{account}/{suffix}"), value);
    };
    register("id", &tokens.id_token);
    register("access", &tokens.access_token);
    if let Some(refresh) = tokens.refresh_token.as_deref() {
        register("refresh", refresh);
    }
}

impl CodexAuthState {
    pub fn new_for_account(
        host: &dyn LlmHost,
        raw_json: &str,
        account_id: Option<String>,
    ) -> Result<Self, String> {
        let tokens = parse_codex_oauth_tokens(raw_json)?;
        register_codex_tokens(host, account_id.as_deref(), &tokens);
        Ok(Self { account_id, tokens })
    }

    pub fn tokens(&self) -> CodexOAuthTokens {
        self.tokens.clone()
    }

    pub fn id_access_pair(&self) -> (String, String) {
        (
            self.tokens.id_token.clone(),
            self.tokens.access_token.clone(),
        )
    }

    pub fn chatgpt_account_id(&self) -> Option<String> {
        self.tokens.chatgpt_account_id.clone()
    }

    /// The Cairn account this session's credential came from, for attributing
    /// what the app-server reports (usage, rate-limit blocks) to the right
    /// subscription.
    pub fn cairn_account_id(&self) -> Option<String> {
        self.account_id.clone()
    }

    /// What a renewal of this session's credential has to be asked for: the
    /// account, and the rotation this session holds, so a winner's publication
    /// is adopted instead of the same single-use token being spent twice.
    pub fn refresh_request(&self) -> CodexRefreshRequest {
        CodexRefreshRequest {
            cairn_account_id: self.account_id.clone(),
            chatgpt_account_id: self.tokens.chatgpt_account_id.clone(),
            spending: self.tokens.refresh_token.clone(),
        }
    }

    /// Replace this session's view with a credential that is already durable.
    ///
    /// Only ever called with JSON the refresh authority read from, or just wrote
    /// to, the identity store. In-memory state advancing ahead of the file is
    /// the precise failure this rules out: it is what lets one session keep
    /// working with a token every other process on the machine still believes is
    /// unspent.
    pub fn adopt(&mut self, host: &dyn LlmHost, published_json: &str) -> Result<(), String> {
        let tokens = parse_codex_oauth_tokens(published_json)?;
        register_codex_tokens(host, self.account_id.as_deref(), &tokens);
        self.tokens = tokens;
        Ok(())
    }
}

/// Which account a renewal is for, and which rotation the caller holds.
#[derive(Clone, Debug, Default)]
pub struct CodexRefreshRequest {
    /// The Cairn account id. Wins when present.
    pub cairn_account_id: Option<String>,
    /// The OpenAI-minted account id, which finds an account whose stored JSON
    /// has been rewritten underneath the caller.
    pub chatgpt_account_id: Option<String>,
    /// The refresh token the caller means to spend. A caller that cannot say
    /// spends unconditionally — correct, but it turns N concurrent requests
    /// into N rotations of a single-use secret instead of one.
    pub spending: Option<String>,
}

/// Renew a session's credential and adopt the result.
///
/// `refresh` is the subscription authority's answer: the credential as it is
/// now durable. The session's view moves only to that, never ahead of it.
pub fn refresh_session_tokens(
    host: &dyn LlmHost,
    state_arc: &Arc<Mutex<CodexAuthState>>,
    refresh: impl FnOnce(CodexRefreshRequest) -> Result<(String, CodexAccountRef), CodexRefreshFailure>,
) -> Result<CodexOAuthTokens, CodexRefreshFailure> {
    let request = state_arc
        .lock()
        .map_err(|_| CodexRefreshFailure::transient(None, "auth state lock poisoned"))?
        .refresh_request();
    let (published, account) = refresh(request)?;
    let mut guard = state_arc
        .lock()
        .map_err(|_| CodexRefreshFailure::transient(Some(&account), "auth state lock poisoned"))?;
    guard
        .adopt(host, &published)
        .map_err(|e| CodexRefreshFailure::transient(Some(&account), e))?;
    Ok(guard.tokens())
}

/// One managed OpenAI subscription, named the way both Cairn and the operator
/// need it named.
///
/// The Cairn account id is the lease scope and the publication target, so it is
/// carried rather than re-derived: an account identified by the contents of its
/// credential stops being identifiable the instant that credential rotates,
/// which is exactly when identification matters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodexAccountRef {
    pub cairn_account_id: String,
    /// The OpenAI-minted account id. Survives every rotation, so it re-finds an
    /// account whose stored JSON has changed underneath this process.
    pub chatgpt_account_id: Option<String>,
    pub label: String,
    pub email: Option<String>,
}

impl CodexAccountRef {
    /// How this account is named to an operator: enough to act on, never a token.
    pub fn display(&self) -> String {
        match &self.email {
            Some(email) => format!(
                "\"{}\" ({email}, Cairn account {})",
                self.label, self.cairn_account_id
            ),
            None => format!(
                "\"{}\" (Cairn account {})",
                self.label, self.cairn_account_id
            ),
        }
    }

    /// The machine-wide lease this account's rotating token is spent under.
    pub fn lease_scope(&self) -> String {
        format!("openai-oauth-{}", self.cairn_account_id)
    }
}

/// Why a refresh did not produce usable credentials.
///
/// Three outcomes rather than one string, because they call for opposite
/// responses: a busy account will answer on the next request, a transient
/// failure is worth retrying, and only the third is worth an operator's
/// attention. Collapsing them is how every concurrency loss came to read as
/// "rerun connect_codex_auth".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexRefreshFailureKind {
    /// Another Cairn process held this account's lease for the whole wait and
    /// had not published by the time it ran out.
    Busy,
    /// The provider or the store failed in a way a later attempt may not hit.
    Transient,
    /// The durable credential can no longer be renewed at all — revoked, or
    /// spent by a refresher that died before it could publish the replacement.
    Unrecoverable,
}

/// The account is boxed because this rides in the `Err` of every refresh entry
/// point. Inline it is large enough that `Result` pays for the failure path on
/// every successful call; behind a pointer the common case stays cheap and only
/// a real failure touches the heap.
#[derive(Debug, Clone)]
pub struct CodexRefreshFailure {
    pub account: Option<Box<CodexAccountRef>>,
    pub kind: CodexRefreshFailureKind,
    pub detail: String,
}

impl CodexRefreshFailure {
    pub fn new(
        account: Option<&CodexAccountRef>,
        kind: CodexRefreshFailureKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            account: account.cloned().map(Box::new),
            kind,
            detail: detail.into(),
        }
    }

    pub fn busy(account: &CodexAccountRef, detail: impl Into<String>) -> Self {
        Self::new(Some(account), CodexRefreshFailureKind::Busy, detail)
    }

    pub fn transient(account: Option<&CodexAccountRef>, detail: impl Into<String>) -> Self {
        Self::new(account, CodexRefreshFailureKind::Transient, detail)
    }

    pub fn unrecoverable(account: Option<&CodexAccountRef>, detail: impl Into<String>) -> Self {
        Self::new(account, CodexRefreshFailureKind::Unrecoverable, detail)
    }

    /// What an operator is told when Cairn could not renew a credential it owns.
    ///
    /// Names the account and the one surface that fixes it. A machine with
    /// several ChatGPT subscriptions gives "rerun connect_codex_auth" no way to
    /// be acted on, and asking someone to guess which account to delete is worse
    /// than saying nothing. No token appears here: the detail is a provider or
    /// store message, and the token set is registered with the secret broker so
    /// an echoed one is scrubbed.
    pub fn operator_message(&self) -> String {
        let who = match &self.account {
            Some(account) => format!("Codex account {}", account.display()),
            None => "this session's Codex account".to_string(),
        };
        match self.kind {
            CodexRefreshFailureKind::Busy => format!(
                "{who} is being renewed by another Cairn process and did not finish in time ({}). Cairn will retry.",
                self.detail
            ),
            CodexRefreshFailureKind::Transient => format!(
                "Renewing {who} failed: {}. Cairn will retry on the next request.",
                self.detail
            ),
            CodexRefreshFailureKind::Unrecoverable => format!(
                "{who} can no longer be renewed: {}. Reconnect that account from Settings → Providers.",
                self.detail
            ),
        }
    }
}

impl std::fmt::Display for CodexRefreshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.operator_message())
    }
}

pub(crate) fn json_string(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(str::to_owned)
}

/// Which rotation of a credential this JSON holds. Absent (a credential written
/// before generations existed, or by `codex` itself) reads as zero.
pub fn codex_refresh_generation(auth_json: &str) -> u64 {
    serde_json::from_str::<Value>(auth_json)
        .ok()
        .and_then(|value| value.get(GENERATION_FIELD).and_then(Value::as_u64))
        .unwrap_or(0)
}

/// The rotating token a stored credential currently holds.
///
/// A consumer that started from a credential passes this back as what it means
/// to spend, which is how the authority tells "nobody has rotated since I read
/// this" from "a winner already published, adopt it".
pub fn codex_credential_refresh_token(auth_json: &str) -> Option<String> {
    parse_codex_oauth_tokens(auth_json)
        .ok()
        .and_then(|tokens| tokens.refresh_token)
}

/// A managed OpenAI OAuth account's stored `auth.json`, if that is what it is.
pub fn openai_oauth_json(account: &ProviderAccount) -> Option<String> {
    match (&account.api_provider, &account.auth) {
        (ApiProvider::OpenAI, ProviderAuth::OAuthToken { value }) => Some(value.clone()),
        _ => None,
    }
}

/// The OpenAI-minted account id a stored credential belongs to. Cheap enough to
/// re-derive, and the only identifier that survives a rotation.
pub fn stored_chatgpt_account_id(auth_json: &str) -> Option<String> {
    parse_codex_oauth_tokens(auth_json)
        .ok()
        .and_then(|tokens| tokens.chatgpt_account_id)
}

/// Which Cairn account a session's `auth.json` came from.
///
/// Matched on the OpenAI-minted account id first. Exact JSON equality is kept
/// only as a fallback for a credential too malformed to yield one: an account
/// identified by the bytes of its credential becomes unidentifiable the moment
/// that credential rotates, which is the moment identification matters most.
pub fn find_codex_oauth_account_id(store: &CredentialStore, auth_json: &str) -> Option<String> {
    if let Some(wanted) = stored_chatgpt_account_id(auth_json) {
        if let Some(account) = store.accounts.iter().find(|account| {
            openai_oauth_json(account)
                .and_then(|json| stored_chatgpt_account_id(&json))
                .as_deref()
                == Some(wanted.as_str())
        }) {
            return Some(account.id.clone());
        }
    }
    store
        .accounts
        .iter()
        .find(|account| openai_oauth_json(account).as_deref() == Some(auth_json))
        .map(|account| account.id.clone())
}

pub fn parse_codex_oauth_tokens(auth_json: &str) -> Result<CodexOAuthTokens, String> {
    let value: Value =
        serde_json::from_str(auth_json).map_err(|e| format!("Invalid Codex OAuth JSON: {}", e))?;
    let tokens = value
        .get("tokens")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "Missing tokens field".to_string())?;
    let id_token =
        json_string(tokens.get("id_token")).ok_or_else(|| "Missing id_token".to_string())?;
    let access_token = json_string(tokens.get("access_token"))
        .ok_or_else(|| "Missing access_token".to_string())?;
    let refresh_token = json_string(tokens.get("refresh_token"));
    let chatgpt_account_id = token_chatgpt_account_id(tokens, &access_token);
    if chatgpt_account_id.is_none() {
        return Err("Missing ChatGPT account id in Codex auth tokens".to_string());
    }
    Ok(CodexOAuthTokens {
        id_token,
        access_token,
        refresh_token,
        chatgpt_account_id,
    })
}

/// Decode a JWT payload without verifying its signature.
///
/// These tokens were just issued to Cairn by the OAuth endpoint over TLS and
/// are read only to label and key the account they belong to; the provider
/// verifies them for every call that actually spends them.
fn jwt_payload(jwt: &str) -> Option<Value> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let payload_b64 = jwt.split('.').nth(1)?;
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    serde_json::from_slice(&payload_bytes).ok()
}

/// Extract `chatgpt_account_id` from a Codex access token JWT without signature verification.
pub fn extract_chatgpt_account_id_from_jwt(jwt: &str) -> Option<String> {
    json_string(jwt_payload(jwt)?.pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id"))
}

/// Which ChatGPT subscription a Codex `auth.json` belongs to.
///
/// The account id is the durable key. It is minted by OpenAI, survives every
/// token refresh, and is what distinguishes reconnecting an existing
/// subscription — which must replace that account's single-use credential in
/// place — from adding a second one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexAccountIdentity {
    pub chatgpt_account_id: String,
    pub email: Option<String>,
    pub plan: Option<String>,
}

/// Read the ChatGPT identity out of a Codex `auth.json`.
pub fn codex_account_identity(auth_json: &str) -> Result<CodexAccountIdentity, String> {
    let tokens = parse_codex_oauth_tokens(auth_json)?;
    let chatgpt_account_id = tokens
        .chatgpt_account_id
        .ok_or_else(|| "Missing ChatGPT account id in Codex auth tokens".to_string())?;
    let claims = jwt_payload(&tokens.id_token).unwrap_or(Value::Null);
    let email = json_string(claims.get("email"))
        .or_else(|| json_string(claims.pointer("/https:~1~1api.openai.com~1profile/email")));
    let plan = json_string(claims.pointer("/https:~1~1api.openai.com~1auth/chatgpt_plan_type"));
    Ok(CodexAccountIdentity {
        chatgpt_account_id,
        email,
        plan,
    })
}

fn token_chatgpt_account_id(
    tokens: &serde_json::Map<String, Value>,
    access_token: &str,
) -> Option<String> {
    extract_chatgpt_account_id_from_jwt(access_token)
        .or_else(|| json_string(tokens.get("account_id")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn unsigned_jwt(payload: &str) -> String {
        format!("header.{}.signature", URL_SAFE_NO_PAD.encode(payload))
    }

    #[test]
    fn account_identity_reads_id_token_claims() {
        let access_token =
            unsigned_jwt(r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"acct-123"}}"#);
        let id_token = unsigned_jwt(
            r#"{"email":"pilot@example.com","https://api.openai.com/auth":{"chatgpt_plan_type":"pro"}}"#,
        );
        let auth_json = serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": "refresh-token",
            }
        });

        let identity = codex_account_identity(&auth_json.to_string()).expect("identity parsed");

        assert_eq!(identity.chatgpt_account_id, "acct-123");
        assert_eq!(identity.email.as_deref(), Some("pilot@example.com"));
        assert_eq!(identity.plan.as_deref(), Some("pro"));
    }

    /// An id token that carries no profile claims still yields the account id,
    /// because that is what keys the account; the label just stays generic.
    #[test]
    fn account_identity_without_profile_claims_still_has_an_account_id() {
        let access_token = unsigned_jwt(r#"{"sub":"user"}"#);
        let auth_json = serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": "not-a-jwt",
                "access_token": access_token,
                "refresh_token": "refresh-token",
                "account_id": "acct-fallback"
            }
        });

        let identity = codex_account_identity(&auth_json.to_string()).expect("identity parsed");

        assert_eq!(identity.chatgpt_account_id, "acct-fallback");
        assert!(identity.email.is_none());
        assert!(identity.plan.is_none());
    }

    #[test]
    fn parses_chatgpt_account_id_from_auth_json_tokens_account_id() {
        let access_token = unsigned_jwt(r#"{"sub":"user"}"#);
        let auth_json = serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": "id-token",
                "access_token": access_token,
                "refresh_token": "refresh-token",
                "account_id": "account-from-auth-json"
            }
        });

        let tokens = parse_codex_oauth_tokens(&auth_json.to_string()).unwrap();

        assert_eq!(
            tokens.chatgpt_account_id.as_deref(),
            Some("account-from-auth-json")
        );
    }
}
