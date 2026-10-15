//! What a `cairn://llm/accounts` write would do, resolved once.
//!
//! Authority preparation and the dispatch arm read a write through
//! [`parse_llm_account_change`], so the prompt the operator is shown and the act
//! that follows cannot disagree, and a malformed write (an unknown provider, a
//! sign-in on an API-key account) is refused before it raises a prompt rather
//! than after.
//!
//! Starting a sign-in — `append` on the list, `sign_in` on an account — crosses
//! the `workspace/default/settings/accounts:write` boundary, the place
//! `cairn://settings` `accounts` writes reach. Finishing or abandoning a
//! pending sign-in continues an authorized start, and re-measuring usage
//! expands no authority, so neither is gated.

use cairn_common::authorization::{AuthorityReason, AuthorityRequest};
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_llm::accounts::{AccountInfo, ApiProvider, RoutedProvider};
use cairn_llm::interface::{Llm, LlmScope};
use serde_json::Value;

use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::account_events::SignInInitiator;
use crate::orchestrator::Orchestrator;

/// The providers a sign-in can be started for.
pub(crate) const SIGN_IN_PROVIDERS: &str = "claude | codex";

/// A `cairn://llm/accounts` write, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LlmAccountChange {
    /// Sign a new subscription account in.
    StartSignIn { provider: RoutedProvider },
    /// Re-authenticate an existing subscription account.
    ReSignIn {
        account_id: String,
        provider: RoutedProvider,
    },
    /// Finish a pending Claude sign-in with a pasted code.
    SubmitCode { account_id: String, code: String },
    /// End a pending sign-in.
    CancelSignIn { account_id: String },
    /// Re-measure every account of one installed provider.
    RefreshProvider { backend: String },
    /// Re-measure one account. `backend` is `None` for an account whose
    /// provider reports no usage for it (an API key on a subscription
    /// provider).
    RefreshAccount {
        account_id: String,
        backend: Option<String>,
    },
}

impl LlmAccountChange {
    /// The boundary this change crosses, if it crosses one.
    pub(crate) fn authority(&self) -> Option<AuthorityRequest> {
        let (provider, account_id) = match self {
            Self::StartSignIn { provider } => (*provider, None),
            Self::ReSignIn {
                account_id,
                provider,
            } => (*provider, Some(account_id.as_str())),
            _ => return None,
        };
        Some(crate::authorization::normalize::provider_sign_in(
            crate::authorization::WORKSPACE_ID,
            provider,
            account_id,
        ))
    }
}

fn sign_in_provider(value: &str) -> Result<RoutedProvider, String> {
    match value {
        "claude" => Ok(RoutedProvider::Claude),
        "codex" => Ok(RoutedProvider::Codex),
        other => Err(format!(
            "payload.provider '{other}' cannot be signed in here; a sign-in is for a subscription: {SIGN_IN_PROVIDERS}. Add an API key, host, or custom endpoint through cairn://settings `accounts` or Settings → Providers."
        )),
    }
}

fn payload_text<'a>(payload: Option<&'a Value>, key: &str) -> Option<&'a str> {
    payload?.get(key)?.as_str().map(str::trim)
}

fn credential_kind(account: &AccountInfo) -> String {
    match account.auth_type.as_str() {
        "api_key" => "an API key".to_string(),
        "base_url" => "a host".to_string(),
        "actual_target" => "an Actual target".to_string(),
        "custom_endpoint" => "a custom endpoint".to_string(),
        other => format!("a `{other}` credential"),
    }
}

/// The subscription an account signs in to, or why it has none.
fn subscription_of(account: &AccountInfo) -> Result<RoutedProvider, String> {
    match (account.auth_type.as_str(), account.api_provider) {
        ("claude_profile", _) => Ok(RoutedProvider::Claude),
        ("oauth_token", ApiProvider::OpenAI) => Ok(RoutedProvider::Codex),
        _ => Err(format!(
            "Account `{}` holds {}, not a subscription sign-in, so there is nothing to sign in to. Change its credential through cairn://settings `accounts` or Settings → Providers.",
            account.id,
            credential_kind(account)
        )),
    }
}

/// The provider whose usage this account's refresh re-measures.
fn usage_backend(account: &AccountInfo) -> Option<String> {
    if let Ok(provider) = subscription_of(account) {
        return Some(provider.backend().to_string());
    }
    let reported = serde_json::to_value(account).unwrap_or_default();
    reported["compatibleBackends"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        // A subscription provider measures subscription sign-ins only.
        .find(|backend| !matches!(*backend, "claude" | "codex"))
        .map(ToString::to_string)
}

fn workspace_account(orch: &Orchestrator, id: &str) -> Result<AccountInfo, String> {
    orch.list_accounts(None)
        .into_iter()
        .find(|account| account.id == id)
        .ok_or_else(|| {
            format!(
                "No account `{id}` in this workspace. Accounts are listed at cairn://llm/accounts."
            )
        })
}

/// Resolve a write, or `None` when `item` is not a `cairn://llm/accounts`
/// write at all.
pub(crate) fn parse_llm_account_change(
    orch: &Orchestrator,
    item: &ChangeItem,
) -> Option<Result<LlmAccountChange, String>> {
    let resource = parse_uri(&item.target)?;
    let payload = item.payload.as_ref();
    Some(
        match (resource, item.mode) {
            (CairnResource::LlmAccounts, ChangeMode::Append) => {
                match payload_text(payload, "provider") {
                Some(provider) => sign_in_provider(provider)
                    .map(|provider| LlmAccountChange::StartSignIn { provider }),
                None => Err(format!(
                    "payload.provider is required: the subscription to sign in to ({SIGN_IN_PROVIDERS})."
                )),
            }
            }
            (CairnResource::LlmAccounts, ChangeMode::Patch) => {
                match payload_text(payload, "action") {
                Some("refresh_usage") => refresh_provider(orch, payload_text(payload, "provider")),
                Some(other) => Err(format!(
                    "payload.action '{other}' is not supported on cairn://llm/accounts; refresh_usage re-measures one provider's accounts. To sign in, append {{provider}}; to act on one account, patch cairn://llm/accounts/<id>."
                )),
                None => Err("payload.action is required: refresh_usage.".to_string()),
            }
            }
            (CairnResource::LlmAccount { id }, ChangeMode::Patch) => {
                account_change(orch, &id, payload)
            }
            _ => return None,
        },
    )
}

fn refresh_provider(
    orch: &Orchestrator,
    provider: Option<&str>,
) -> Result<LlmAccountChange, String> {
    let catalog = Llm::catalog(orch, &LlmScope::workspace())?;
    let installed: Vec<String> = catalog
        .installed()
        .map(|provider| provider.descriptor.key.to_string())
        .collect();
    match provider {
        Some(key) if installed.iter().any(|installed| installed == key) => {
            Ok(LlmAccountChange::RefreshProvider {
                backend: key.to_string(),
            })
        }
        Some(key) => Err(format!(
            "No installed provider '{key}'. Installed: {}.",
            installed.join(", ")
        )),
        None => Err(format!(
            "payload.provider is required: the installed provider to re-measure ({}).",
            installed.join(", ")
        )),
    }
}

fn account_change(
    orch: &Orchestrator,
    id: &str,
    payload: Option<&Value>,
) -> Result<LlmAccountChange, String> {
    let account = workspace_account(orch, id)?;
    let account_id = account.id.clone();
    match payload_text(payload, "action") {
        Some("sign_in") => subscription_of(&account)
            .map(|provider| LlmAccountChange::ReSignIn { account_id, provider }),
        Some("submit_code") => match payload_text(payload, "code") {
            Some(code) if !code.is_empty() => Ok(LlmAccountChange::SubmitCode {
                account_id,
                code: code.to_string(),
            }),
            _ => Err("payload.code is required with submit_code: the code#state string Claude's sign-in page shows.".to_string()),
        },
        Some("cancel_sign_in") => Ok(LlmAccountChange::CancelSignIn { account_id }),
        Some("refresh_usage") => Ok(LlmAccountChange::RefreshAccount {
            backend: usage_backend(&account),
            account_id,
        }),
        Some(other) => Err(format!(
            "payload.action '{other}' is not supported on an account; expected sign_in | submit_code | cancel_sign_in | refresh_usage."
        )),
        None => Err(
            "payload.action is required: sign_in | submit_code | cancel_sign_in | refresh_usage."
                .to_string(),
        ),
    }
}

/// The authority boundary a write crosses, for `prepare_batch`: `None` when
/// the write is not a `cairn://llm/accounts` write or crosses no boundary, and
/// the parse error when it is malformed, so it is refused before any prompt.
pub(crate) fn llm_account_authority(
    orch: &Orchestrator,
    item: &ChangeItem,
) -> Option<Result<AuthorityRequest, String>> {
    match parse_llm_account_change(orch, item)? {
        Ok(change) => change.authority().map(Ok),
        Err(error) => Some(Err(error)),
    }
}

/// The final check before a sign-in starts, re-derived from the change about
/// to happen and consuming a once-grant. Answers with the start's attribution:
/// the calling run as initiator, and the decision row that allowed it.
pub(crate) async fn authorize_sign_in(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    authority: &AuthorityRequest,
) -> Result<SignInInitiator, String> {
    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(
            "Denied: starting a provider sign-in requires an authenticated run to authorize."
                .to_string(),
        );
    };
    let (decision, event_id) = crate::authorization::authorize_recorded(&actor, authority).await?;
    if !decision.is_allowed() {
        return Err(crate::authorization::refusal_message(
            authority,
            decision
                .reason()
                .unwrap_or(AuthorityReason::WorkspaceSettingsCapability),
        ));
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let (principal, appearance) = agent.appearance(orch.services.clock.now())?;
    SignInInitiator::attributed(principal, appearance, event_id)
}
