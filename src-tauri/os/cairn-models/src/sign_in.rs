//! Subscription sign-in, as the runtime asks for it.
//!
//! Starting a sign-in for a Claude or Codex subscription account, watching it,
//! finishing a pending Claude one with a pasted code, and abandoning one. The
//! mechanics (the Claude CLI's PTY, the Codex app-server's login flow) are
//! private; this is the vocabulary [`crate::subscription::Subscriptions`]
//! speaks about them, and every door that signs an account in — the desktop's
//! Settings → Providers, `cairn auth login`, and `cairn://models/accounts` — goes
//! through it.
//!
//! A sign-in is named by an opaque, non-secret reference the door mints before
//! starting it, so the door can record who started the flow before anything
//! the flow does can be recorded against it. Every terminal outcome reaches the
//! host exactly once through
//! [`crate::subscription::ModelHost::sign_in_settled`].
//!
//! None of these types carries a token, a code, or a PKCE verifier. The one URL
//! a pending sign-in exposes is the page the user must open themselves, which
//! carries only the flow's `state` and PKCE challenge.

use std::future::Future;
use std::pin::Pin;

use serde::Serialize;

use crate::accounts::{AccountInfo, ApiProvider, CredentialStore, OAuthLogin, RoutedProvider};
use crate::codex::tokens::codex_account_identity;

/// A boxed, sendable future, as the subscription seam returns long work.
pub type SignInFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Which account a sign-in may write when it completes.
///
/// A Claude sign-in writes the managed home it was started on, so its target
/// is fixed at start. A Codex sign-in learns which ChatGPT account it is only
/// when the browser finishes, so its completion is checked against this
/// before anything is stored: a re-authentication authorized for one account
/// must not write another, and a new account must not overwrite one already
/// held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignInTarget {
    /// A new account. A completion as an account already held is refused.
    New,
    /// Exactly this account. A completion as any other is refused.
    Account(String),
    /// A new account, or the held one the provider identity matches, replaced
    /// in place. The Settings "Connect" button's long-standing meaning.
    NewOrMatching,
}

impl SignInTarget {
    /// The account this sign-in is for, when that is fixed at start.
    pub fn account_id(&self) -> Option<&str> {
        match self {
            Self::Account(id) => Some(id),
            Self::New | Self::NewOrMatching => None,
        }
    }

    /// Whether a completion that is `held` (the account already holding
    /// this provider identity, if any) may be recorded under this target.
    pub fn admits(&self, held: Option<&str>) -> Result<(), String> {
        match (self, held) {
            (Self::Account(id), Some(held)) if held == id => Ok(()),
            (Self::Account(id), _) => Err(format!(
                "The browser signed in as a different account than `{id}`, so nothing was stored. Sign in again as that account, or add the other one as a new account."
            )),
            (Self::New, Some(held)) => Err(format!(
                "That account is already held as `{held}`, so nothing was stored. Re-authenticate `{held}` instead of adding it again."
            )),
            (Self::New | Self::NewOrMatching, _) => Ok(()),
        }
    }
}

/// Why a completed sign-in was not stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignInRecordError {
    pub kind: SignInFailureKind,
    pub message: String,
}

impl SignInRecordError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            kind: SignInFailureKind::Other,
            message: message.into(),
        }
    }
}

impl From<String> for SignInRecordError {
    fn from(message: String) -> Self {
        Self::other(message)
    }
}

impl std::fmt::Display for SignInRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Store a completed Codex sign-in in `store` under `target`: the one place a
/// ChatGPT login becomes a stored credential, whichever host stores it.
///
/// Codex learns which ChatGPT account the browser signed in as only now, so
/// this is where the target authorized at start is enforced, and a completion
/// it does not admit stores nothing. A ChatGPT account has exactly one stored
/// credential (its refresh token rotates), so a login already held is replaced
/// in place, never duplicated. A re-authentication replaces the account in the
/// scope it lives in. The caller persists the store.
pub fn store_codex_sign_in(
    store: &mut CredentialStore,
    label: String,
    auth_json: String,
    project_id: Option<String>,
    target: &SignInTarget,
    now: i64,
) -> Result<AccountInfo, SignInRecordError> {
    let identity = codex_account_identity(&auth_json)?;
    let signed_in = identity.chatgpt_account_id.clone();
    let same_login = |stored: &str| {
        codex_account_identity(stored).is_ok_and(|held| held.chatgpt_account_id == signed_in)
    };
    let project_id = match target {
        SignInTarget::Account(id) => store
            .accounts
            .iter()
            .find(|account| account.id == *id)
            .map(|account| account.project_id.clone())
            .ok_or_else(|| {
                SignInRecordError::other(format!(
                    "Account `{id}` no longer exists, so nothing was stored."
                ))
            })?,
        SignInTarget::New | SignInTarget::NewOrMatching => project_id,
    };
    let held = store
        .held_oauth_account(ApiProvider::OpenAI, project_id.as_deref(), &same_login)
        .map(|account| account.id.clone());
    target
        .admits(held.as_deref())
        .map_err(|message| SignInRecordError {
            kind: SignInFailureKind::WrongAccount,
            message,
        })?;
    Ok(store.upsert_oauth_account(
        ApiProvider::OpenAI,
        label,
        OAuthLogin {
            value: auth_json,
            email: identity.email,
            plan: identity.plan,
        },
        project_id,
        now,
        same_login,
    ))
}

/// What a door asks for when it starts a sign-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignInRequest {
    /// The reference this sign-in will be known by. Minted by the door, which
    /// records the start under it before calling `start_sign_in`.
    pub sign_in_ref: String,
    pub provider: RoutedProvider,
    /// Which account the sign-in may write.
    pub target: SignInTarget,
    /// The project a new account is private to, or `None` for the workspace.
    pub project_id: Option<String>,
}

/// How a started sign-in stands.
///
/// Serialized as the `connect_provider_oauth` command's wire contract, which the
/// desktop dialog and `cairn auth login` switch on: `status` is `connected` or
/// `pending`, a pending sign-in's reference is `loginId`, and the URL is
/// `manualUrl`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SignInStart {
    /// The flow finished without the user: the account was already signed in.
    Connected {
        // Boxed only to keep the variants a similar size; `Box` is transparent
        // to serde, so the wire shape is unchanged.
        account: Box<AccountInfo>,
    },
    /// The browser has the flow; it finishes in the user's hands.
    #[serde(rename_all = "camelCase")]
    Pending {
        #[serde(rename = "loginId")]
        sign_in_ref: String,
        /// The account this sign-in fills in, when it already has a row: a
        /// re-authentication, or a new Claude sign-in's provisional row. A new
        /// Codex account has none until the flow completes.
        #[serde(skip_serializing_if = "Option::is_none")]
        account_id: Option<String>,
        /// The page to open if the browser did not: Claude's paste-code page,
        /// or the Codex authorize URL.
        #[serde(rename = "manualUrl")]
        open_url: Option<String>,
    },
}

/// Has a pending sign-in finished? A finished one is handed out once, and
/// asking retires it.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SignInStatus {
    Pending,
    Connected { account: Box<AccountInfo> },
}

/// The answer to a pasted Claude code. `InvalidCode` is retriable, not an
/// error: the CLI rejected the paste locally and is still waiting for another.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SubmitCodeResult {
    Connected { account: Box<AccountInfo> },
    InvalidCode { message: String },
}

/// Why a sign-in call failed, graded the way the command surface reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignInError {
    /// The request itself cannot be served (an expired sign-in, an empty code).
    BadRequest(String),
    /// The sign-in ran and failed.
    Failed(String),
}

impl SignInError {
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(message) | Self::Failed(message) => message,
        }
    }
}

impl std::fmt::Display for SignInError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// A sign-in still in the user's hands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSignIn {
    pub sign_in_ref: String,
    pub provider: RoutedProvider,
    /// See [`SignInStart::Pending::account_id`].
    pub account_id: Option<String>,
    /// Unix seconds.
    pub started_at: i64,
}

/// Why a sign-in failed, as a closed set. Upstream text (a CLI's error line, a
/// provider's refusal) can carry a code, a URL, or account data, so it goes to
/// scrubbed logs only and never into a durable record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignInFailureKind {
    /// The flow did not finish within its window.
    Timeout,
    /// A pasted code was refused.
    InvalidCode,
    /// The provider or its CLI refused the sign-in.
    ProviderRejected,
    /// The provider's CLI is not installed or could not be started.
    CliUnavailable,
    /// The browser signed in as an account this sign-in may not write.
    WrongAccount,
    /// Whoever started the flow went away before it could finish.
    Abandoned,
    Other,
}

impl SignInFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::InvalidCode => "invalid_code",
            Self::ProviderRejected => "provider_rejected",
            Self::CliUnavailable => "cli_unavailable",
            Self::WrongAccount => "wrong_account",
            Self::Abandoned => "abandoned",
            Self::Other => "other",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "timeout" => Self::Timeout,
            "invalid_code" => Self::InvalidCode,
            "provider_rejected" => Self::ProviderRejected,
            "cli_unavailable" => Self::CliUnavailable,
            "wrong_account" => Self::WrongAccount,
            "abandoned" => Self::Abandoned,
            "other" => Self::Other,
            _ => return None,
        })
    }
}

/// How a sign-in ended. Names no principal: it records that the flow reached
/// this outcome, not who performed the provider's browser step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignInOutcome {
    Completed {
        account_id: String,
        email: Option<String>,
        plan: Option<String>,
    },
    Failed {
        kind: SignInFailureKind,
        /// The account the attempt was for, when it survives the failure (a
        /// re-authentication). A new account's provisional row is discarded.
        account_id: Option<String>,
    },
    Cancelled {
        account_id: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The desktop dialog and `cairn auth login` switch on exactly this shape.
    #[test]
    fn serializes_the_contract_the_frontend_switches_on() {
        let pending = serde_json::to_value(SignInStart::Pending {
            sign_in_ref: "abc".to_string(),
            account_id: None,
            open_url: Some("https://example.invalid/sign-in".to_string()),
        })
        .expect("serializes");
        assert_eq!(
            pending,
            serde_json::json!({
                "status": "pending",
                "loginId": "abc",
                "manualUrl": "https://example.invalid/sign-in",
            })
        );

        let waiting = serde_json::to_value(SignInStatus::Pending).expect("serializes");
        assert_eq!(waiting, serde_json::json!({ "status": "pending" }));

        let invalid = serde_json::to_value(SubmitCodeResult::InvalidCode {
            message: "Invalid code.".to_string(),
        })
        .expect("serializes");
        assert_eq!(
            invalid,
            serde_json::json!({ "status": "invalidCode", "message": "Invalid code." })
        );
    }

    #[test]
    fn failure_kinds_round_trip_through_their_stored_names() {
        for kind in [
            SignInFailureKind::Timeout,
            SignInFailureKind::InvalidCode,
            SignInFailureKind::ProviderRejected,
            SignInFailureKind::CliUnavailable,
            SignInFailureKind::WrongAccount,
            SignInFailureKind::Abandoned,
            SignInFailureKind::Other,
        ] {
            assert_eq!(SignInFailureKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(SignInFailureKind::parse("cancelled"), None);
    }

    #[test]
    fn a_target_admits_only_the_account_it_was_authorized_for() {
        let account = SignInTarget::Account("acc_a".to_string());
        assert!(account.admits(Some("acc_a")).is_ok());
        assert!(account.admits(Some("acc_b")).is_err());
        assert!(account.admits(None).is_err());
        assert!(SignInTarget::New.admits(None).is_ok());
        assert!(SignInTarget::New.admits(Some("acc_a")).is_err());
        assert!(SignInTarget::NewOrMatching.admits(Some("acc_a")).is_ok());
        assert!(SignInTarget::NewOrMatching.admits(None).is_ok());
    }

    fn codex_auth(chatgpt_account_id: &str, refresh_token: &str) -> String {
        serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": "id",
                "access_token": "access",
                "refresh_token": refresh_token,
                "account_id": chatgpt_account_id,
            }
        })
        .to_string()
    }

    fn store() -> CredentialStore {
        CredentialStore {
            user_id: "test".to_string(),
            accounts: Vec::new(),
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::from("/nonexistent"),
        }
    }

    fn record(
        store: &mut CredentialStore,
        auth: String,
        target: &SignInTarget,
    ) -> Result<AccountInfo, SignInRecordError> {
        store_codex_sign_in(store, "Codex".to_string(), auth, None, target, 0)
    }

    fn refresh_token_of(store: &CredentialStore, id: &str) -> String {
        let account = store
            .accounts
            .iter()
            .find(|account| account.id == id)
            .unwrap();
        match &account.auth {
            crate::accounts::ProviderAuth::OAuthToken { value } => value.clone(),
            other => panic!("unexpected auth {other:?}"),
        }
    }

    /// Re-authenticating account A while the browser signs in as B stores
    /// nothing: A keeps its credential and no row for B appears.
    #[test]
    fn a_reauthentication_completed_as_another_account_stores_nothing() {
        let mut store = store();
        let a = record(
            &mut store,
            codex_auth("chatgpt-a", "rt-a"),
            &SignInTarget::New,
        )
        .unwrap();
        let refused = record(
            &mut store,
            codex_auth("chatgpt-b", "rt-b"),
            &SignInTarget::Account(a.id.clone()),
        )
        .unwrap_err();
        assert_eq!(refused.kind, SignInFailureKind::WrongAccount);
        assert_eq!(store.accounts.len(), 1);
        assert!(refresh_token_of(&store, &a.id).contains("rt-a"));

        let renewed = record(
            &mut store,
            codex_auth("chatgpt-a", "rt-a2"),
            &SignInTarget::Account(a.id.clone()),
        )
        .unwrap();
        assert_eq!(renewed.id, a.id);
        assert!(refresh_token_of(&store, &a.id).contains("rt-a2"));
    }

    /// A sign-in authorized to add a new account, completed as one already
    /// held, does not overwrite the held credential.
    #[test]
    fn a_new_account_completed_as_a_held_one_overwrites_nothing() {
        let mut store = store();
        let held = record(
            &mut store,
            codex_auth("chatgpt-a", "rt-a"),
            &SignInTarget::New,
        )
        .unwrap();
        let refused = record(
            &mut store,
            codex_auth("chatgpt-a", "rt-new"),
            &SignInTarget::New,
        )
        .unwrap_err();
        assert_eq!(refused.kind, SignInFailureKind::WrongAccount);
        assert!(refused.message.contains(&held.id), "{refused}");
        assert_eq!(store.accounts.len(), 1);
        assert!(refresh_token_of(&store, &held.id).contains("rt-a\""));

        // The Settings button's meaning: replace the matching row in place.
        let replaced = record(
            &mut store,
            codex_auth("chatgpt-a", "rt-new"),
            &SignInTarget::NewOrMatching,
        )
        .unwrap();
        assert_eq!(replaced.id, held.id);
        assert_eq!(store.accounts.len(), 1);
    }
}
