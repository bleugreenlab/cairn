//! What a `cairn://models/accounts` write would do, resolved once.
//!
//! Authority preparation and the dispatch arm read a write through
//! [`parse_models_account_change`], so the prompt the operator is shown and the act
//! that follows cannot disagree, and a malformed write (an unknown provider, a
//! sign-in on an API-key account) is refused before it raises a prompt rather
//! than after.
//!
//! Starting a sign-in — `append` on the list, `sign_in` on an account — crosses
//! the `workspace/default/settings/accounts:write` boundary, the place
//! `cairn://settings` `accounts` writes reach. Spending a usage reset —
//! `use_reset` — crosses that account's own boundary,
//! `account/codex/<id>:write`: a reset credit is scarce and the provider does
//! not refund it, so an agent spends one only with the operator's approval,
//! and approving one account's reset never covers another's. The operator's
//! own Use in Settings → Providers is not a write through this resource and
//! asks nothing. Finishing or abandoning a pending sign-in continues an
//! authorized start, and re-measuring usage or naming which plan an account is
//! on is cheap and reversible, so none of those is gated.

use cairn_common::authorization::{AuthorityDecision, AuthorityRequest};
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_models::accounts::{AccountInfo, ApiProvider, RoutedProvider};
use cairn_models::interface::{ModelScope, Models};
use serde_json::Value;

use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::account_events::SignInInitiator;
use crate::orchestrator::Orchestrator;

/// The providers a sign-in can be started for.
pub(crate) const SIGN_IN_PROVIDERS: &str = "claude | codex";

/// A `cairn://models/accounts` write, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ModelAccountChange {
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
    /// Name which candidate plan an ambiguous account is on; `None` clears it.
    SetPlan {
        account_id: String,
        plan: Option<String>,
    },
    /// Spend one of a Codex account's usage resets.
    UseReset { account_id: String },
}

impl ModelAccountChange {
    /// The boundary this change crosses, if it crosses one.
    pub(crate) fn authority(&self) -> Option<AuthorityRequest> {
        use crate::authorization::{normalize, WORKSPACE_ID};
        match self {
            Self::StartSignIn { provider } => {
                Some(normalize::provider_sign_in(WORKSPACE_ID, *provider, None))
            }
            Self::ReSignIn {
                account_id,
                provider,
            } => Some(normalize::provider_sign_in(
                WORKSPACE_ID,
                *provider,
                Some(account_id),
            )),
            Self::UseReset { account_id } => Some(normalize::usage_reset_spend(
                RoutedProvider::Codex,
                account_id,
            )),
            Self::SubmitCode { .. }
            | Self::CancelSignIn { .. }
            | Self::RefreshProvider { .. }
            | Self::RefreshAccount { .. }
            | Self::SetPlan { .. } => None,
        }
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
                "No account `{id}` in this workspace. Accounts are listed at cairn://models/accounts."
            )
        })
}

/// Resolve a write, or `None` when `item` is not a `cairn://models/accounts`
/// write at all.
pub(crate) fn parse_models_account_change(
    orch: &Orchestrator,
    item: &ChangeItem,
) -> Option<Result<ModelAccountChange, String>> {
    let resource = parse_uri(&item.target)?;
    let payload = item.payload.as_ref();
    Some(match (resource, item.mode) {
        (CairnResource::ModelsAccounts, ChangeMode::Append) => {
            match payload_text(payload, "provider") {
                Some(provider) => sign_in_provider(provider)
                    .map(|provider| ModelAccountChange::StartSignIn { provider }),
                None => Err(format!(
                    "payload.provider is required: the subscription to sign in to ({SIGN_IN_PROVIDERS})."
                )),
            }
        }
        (CairnResource::ModelsAccounts, ChangeMode::Patch) => {
            match payload_text(payload, "action") {
                Some("refresh_usage") => refresh_provider(orch, payload_text(payload, "provider")),
                Some("set_plan") => set_plan(orch, payload),
                Some("use_reset") => use_reset(orch, payload),
                Some(other) => Err(format!(
                    "payload.action '{other}' is not supported on cairn://models/accounts; refresh_usage re-measures one provider's accounts, set_plan names an account's plan, and use_reset spends one of a Codex account's usage resets. To sign in, append {{provider}}; to act on one account, patch cairn://models/accounts/<id>."
                )),
                None => Err(
                    "payload.action is required: refresh_usage | set_plan | use_reset.".to_string(),
                ),
            }
        }
        (CairnResource::ModelsAccount { id }, ChangeMode::Patch) => {
            account_change(orch, &id, payload)
        }
        _ => return None,
    })
}

/// `{action:"set_plan", account, plan}`: `plan` is one of the account's
/// candidate plan keys, or null to clear the choice.
fn set_plan(orch: &Orchestrator, payload: Option<&Value>) -> Result<ModelAccountChange, String> {
    let id = payload_text(payload, "account")
        .filter(|id| !id.is_empty())
        .ok_or("payload.account is required with set_plan: the account id.")?;
    let account = workspace_account(orch, id)?;
    let Some(subscription) = account.subscription() else {
        return Err(format!(
            "Account `{id}` is not a subscription, so it has no plan to set."
        ));
    };
    let keys: Vec<&str> = subscription
        .options
        .iter()
        .map(|option| option.key.as_str())
        .collect();
    if keys.is_empty() {
        return Err(format!(
            "Account `{id}`'s plan is {}, as its provider reports it; there is nothing to choose.",
            subscription.label
        ));
    }
    let plan = match payload.and_then(|payload| payload.get("plan")) {
        None => {
            return Err(format!(
                "payload.plan is required with set_plan: {} (or null to clear).",
                keys.join(" | ")
            ))
        }
        Some(Value::Null) => None,
        Some(Value::String(plan)) if keys.contains(&plan.trim()) => Some(plan.trim().to_string()),
        Some(other) => {
            return Err(format!(
                "payload.plan {other} is not one of account `{id}`'s plans: {} (or null to clear).",
                keys.join(" | ")
            ))
        }
    };
    Ok(ModelAccountChange::SetPlan {
        account_id: account.id,
        plan,
    })
}

/// `{action:"use_reset", provider, account}`: the account must be a
/// subscription sign-in of `provider`, and the provider one with resets.
fn use_reset(orch: &Orchestrator, payload: Option<&Value>) -> Result<ModelAccountChange, String> {
    let provider = payload_text(payload, "provider")
        .filter(|provider| !provider.is_empty())
        .ok_or("payload.provider is required with use_reset: codex.")?;
    let id = payload_text(payload, "account")
        .filter(|id| !id.is_empty())
        .ok_or("payload.account is required with use_reset: the account id.")?;
    if provider != "codex" {
        return Err(crate::orchestrator::usage_refresh::usage_reset_unsupported(
            provider,
        ));
    }
    let account = workspace_account(orch, id)?;
    match subscription_of(&account) {
        Ok(RoutedProvider::Codex) => Ok(ModelAccountChange::UseReset {
            account_id: account.id,
        }),
        Ok(other) => Err(format!(
            "Account `{id}` is a {} sign-in, not a codex one.",
            other.backend()
        )),
        Err(error) => Err(error),
    }
}

fn refresh_provider(
    orch: &Orchestrator,
    provider: Option<&str>,
) -> Result<ModelAccountChange, String> {
    let catalog = Models::catalog(orch, &ModelScope::workspace())?;
    let installed: Vec<String> = catalog
        .installed()
        .map(|provider| provider.descriptor.key.to_string())
        .collect();
    match provider {
        Some(key) if installed.iter().any(|installed| installed == key) => {
            Ok(ModelAccountChange::RefreshProvider {
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
) -> Result<ModelAccountChange, String> {
    let account = workspace_account(orch, id)?;
    let account_id = account.id.clone();
    match payload_text(payload, "action") {
        Some("sign_in") => subscription_of(&account)
            .map(|provider| ModelAccountChange::ReSignIn { account_id, provider }),
        Some("submit_code") => match payload_text(payload, "code") {
            Some(code) if !code.is_empty() => Ok(ModelAccountChange::SubmitCode {
                account_id,
                code: code.to_string(),
            }),
            _ => Err("payload.code is required with submit_code: the code#state string Claude's sign-in page shows.".to_string()),
        },
        Some("cancel_sign_in") => Ok(ModelAccountChange::CancelSignIn { account_id }),
        Some("refresh_usage") => Ok(ModelAccountChange::RefreshAccount {
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
/// the write is not a `cairn://models/accounts` write or crosses no boundary, and
/// the parse error when it is malformed, so it is refused before any prompt.
pub(crate) fn models_account_authority(
    orch: &Orchestrator,
    item: &ChangeItem,
) -> Option<Result<AuthorityRequest, String>> {
    match parse_models_account_change(orch, item)? {
        Ok(change) => change.authority().map(Ok),
        Err(error) => Some(Err(error)),
    }
}

/// The final check before a gated account change acts, re-derived from the
/// change about to happen and consuming a once-grant. Answers with the id of
/// the decision row that allowed it.
pub(crate) async fn authorize_account_change(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    authority: &AuthorityRequest,
) -> Result<Option<String>, String> {
    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(format!(
            "Denied: only an authenticated run can be authorized to {}.",
            authority.summary
        ));
    };
    let (decision, event_id) = crate::authorization::authorize_recorded(&actor, authority).await?;
    match decision {
        AuthorityDecision::Direct | AuthorityDecision::AllowedByGrant { .. } => Ok(event_id),
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            Err(crate::authorization::refusal_message(authority, reason))
        }
    }
}

/// [`authorize_account_change`] for a sign-in start, answering with the
/// start's attribution: the calling run as initiator, and the decision row
/// that allowed it.
pub(crate) async fn authorize_sign_in(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    authority: &AuthorityRequest,
) -> Result<SignInInitiator, String> {
    let event_id = authorize_account_change(orch, request, authority).await?;
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let (principal, appearance) = agent.appearance(orch.services.clock.now())?;
    SignInInitiator::attributed(principal, appearance, event_id)
}
