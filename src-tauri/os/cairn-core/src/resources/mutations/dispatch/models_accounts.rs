//! Signing subscription accounts in and re-measuring usage through
//! `cairn://models/accounts`.
//!
//! The write is read once by [`parse_models_account_change`], the same reading the
//! authority gate prepared from. A sign-in start is re-authorized immediately
//! before it starts (consuming a once-grant) and goes through
//! `Orchestrator::start_provider_sign_in`, the entry every sign-in door uses,
//! so its start is journaled with this run as initiator and it finishes on the
//! same path as a Settings or `cairn auth login` sign-in.
//!
//! Results are built from account ids, resource URIs, the page a pending
//! sign-in needs opened, and usage numbers. A sign-in's reference, a pasted
//! code, and any token never appear in one.

use std::fmt::Write as _;

use super::super::{
    authorize_account_change, authorize_sign_in, build_failure, parse_models_account_change,
    ModelAccountChange, ResourceMutationResult,
};
use crate::mcp::types::{ChangeItem, McpCallbackRequest};
use crate::orchestrator::usage_refresh::{UsageMeasurement, UsageRefresh, UsageRefreshTarget};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;
use cairn_models::accounts::RoutedProvider;
use cairn_models::subscription::{SignInStart, SignInTarget, SubmitCodeResult};

fn provider_name(provider: RoutedProvider) -> &'static str {
    match provider {
        RoutedProvider::Claude => "Claude",
        RoutedProvider::Codex => "Codex (ChatGPT)",
    }
}

fn account_uri(account_id: &str) -> String {
    format!("cairn://models/accounts/{account_id}")
}

/// The pending sign-in on an account, by the reference only the seam needs.
fn pending_sign_in(orch: &Orchestrator, account_id: &str) -> Result<String, String> {
    orch.subscriptions()
        .pending_sign_ins()
        .into_iter()
        .find(|pending| pending.account_id.as_deref() == Some(account_id))
        .map(|pending| pending.sign_in_ref)
        .ok_or_else(|| {
            format!(
                "No sign-in is pending for account `{account_id}`. Start one with patch {{action:\"sign_in\"}} on {}, or append {{provider}} to cairn://models/accounts for a new account.",
                account_uri(account_id)
            )
        })
}

fn started(provider: RoutedProvider, re_auth: Option<&str>, start: SignInStart) -> String {
    match start {
        SignInStart::Connected { account } => format!(
            "Signed in: account `{}` was already signed in and is connected. Read {}.",
            account.id,
            account_uri(&account.id)
        ),
        SignInStart::Pending {
            account_id,
            open_url,
            ..
        } => {
            let mut out = format!(
                "Started a {} sign-in. A browser opened on this machine; the sign-in finishes there.",
                provider_name(provider)
            );
            let watch = match (&account_id, re_auth) {
                (Some(id), _) => account_uri(id),
                (None, Some(id)) => account_uri(id),
                (None, None) => format!("cairn://models/accounts?provider={}", provider.backend()),
            };
            let _ = write!(
                out,
                " Watch {watch}: the account reads signed in when it completes."
            );
            if let Some(url) = open_url {
                let _ = write!(out, " If the browser did not open, open {url}");
                out.push('.');
            }
            if let (RoutedProvider::Claude, Some(id)) = (provider, &account_id) {
                let _ = write!(
                    out,
                    " On that page's manual path it shows a code#state string: finish with patch {} {{action:\"submit_code\", code:\"...\"}}. Abandon it with {{action:\"cancel_sign_in\"}}.",
                    account_uri(id)
                );
            }
            out
        }
    }
}

fn percent(value: f64) -> String {
    format!("{}%", (value * 10.0).round() / 10.0)
}

fn refresh_line(refresh: &UsageRefresh, out: &mut String) {
    let who = refresh
        .account_id
        .as_deref()
        .map(|id| format!("`{id}`"))
        .unwrap_or_else(|| "default account".to_string());
    let snapshot = &refresh.snapshot;
    let state = match refresh.measurement {
        _ if snapshot.unsupported_reason.is_some() => format!(
            "no provider-side usage: {}",
            snapshot.unsupported_reason.as_deref().unwrap_or_default()
        ),
        UsageMeasurement::Fresh { age_secs } => {
            format!("measured {age_secs} s ago; not re-probed")
        }
        UsageMeasurement::AlreadyMeasuring => {
            "already being measured; its result arrives shortly".to_string()
        }
        UsageMeasurement::Discarded => {
            "discarded: its credential changed while it was measured; refresh again".to_string()
        }
        UsageMeasurement::Probed => match &snapshot.error {
            Some(error) => format!("probe failed: {error}"),
            None => "measured now".to_string(),
        },
    };
    let _ = writeln!(out, "- {who}: {state}");
    if matches!(
        refresh.measurement,
        UsageMeasurement::Fresh { .. } | UsageMeasurement::Probed
    ) {
        for window in &snapshot.windows {
            let _ = writeln!(
                out,
                "  - {}: {} left",
                window.label,
                percent(window.remaining_percent)
            );
        }
    }
}

fn refreshed(what: &str, refreshes: &[UsageRefresh], read: &str) -> String {
    let mut out = format!("Re-measured {what}:\n");
    for refresh in refreshes {
        refresh_line(refresh, &mut out);
    }
    let _ = write!(
        out,
        "Read {read} or cairn://models/usage for what is stored."
    );
    out
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    if !matches!(
        resource,
        CairnResource::ModelsAccounts | CairnResource::ModelsAccount { .. }
    ) {
        return Ok(None);
    }
    let Some(change) = parse_models_account_change(orch, item) else {
        return Ok(None);
    };
    let change = change.map_err(|error| build_failure(index, item, error))?;
    let fail = |error: String| build_failure(index, item, error);
    // The boundary the gate prompted for, read from the same change.
    let authority = change.authority();

    let summary = match change {
        ModelAccountChange::StartSignIn { provider }
        | ModelAccountChange::ReSignIn { provider, .. } => {
            let re_auth = match &change {
                ModelAccountChange::ReSignIn { account_id, .. } => Some(account_id.clone()),
                _ => None,
            };
            let authority = authority.expect("a sign-in start crosses the accounts boundary");
            if dry_run {
                return Ok(Some(format!(
                    "Would {} (scope {}); a browser would open on this machine.",
                    authority.summary,
                    authority.scope.shorthand()
                )));
            }
            let initiator = authorize_sign_in(orch, request, &authority)
                .await
                .map_err(fail)?;
            // A re-authentication may write only the account it was authorized
            // for, and a new account may not overwrite one already held: the
            // Update and Create the grant was asked for.
            let target = re_auth
                .clone()
                .map_or(SignInTarget::New, SignInTarget::Account);
            let start = orch
                .start_provider_sign_in(provider, target, None, initiator)
                .await
                .map_err(|error| fail(error.to_string()))?;
            started(provider, re_auth.as_deref(), start)
        }
        ModelAccountChange::SubmitCode { account_id, code } => {
            let sign_in_ref = pending_sign_in(orch, &account_id).map_err(fail)?;
            if dry_run {
                return Ok(Some(format!(
                    "Would hand the pasted code to account `{account_id}`'s pending sign-in."
                )));
            }
            match orch
                .subscriptions()
                .submit_sign_in_code(std::sync::Arc::new(orch.clone()), sign_in_ref, code)
                .await
                .map_err(|error| fail(error.to_string()))?
            {
                SubmitCodeResult::Connected { account } => format!(
                    "Signed in: account `{}` is connected. Read {}.",
                    account.id,
                    account_uri(&account.id)
                ),
                SubmitCodeResult::InvalidCode { message } => format!(
                    "The code was refused and the sign-in is still waiting: {message} Paste the full code#state again with submit_code."
                ),
            }
        }
        ModelAccountChange::CancelSignIn { account_id } => {
            let sign_in_ref = pending_sign_in(orch, &account_id).map_err(fail)?;
            if dry_run {
                return Ok(Some(format!(
                    "Would end account `{account_id}`'s pending sign-in."
                )));
            }
            if orch.subscriptions().cancel_sign_in(orch, &sign_in_ref) {
                format!(
                    "Cancelled the pending sign-in for `{account_id}`. A new account's provisional row is discarded; an existing account keeps the credential it had."
                )
            } else {
                format!(
                    "Account `{account_id}`'s sign-in was already finishing, so it was not cancelled; it ends as the provider flow ends. Read {}.",
                    account_uri(&account_id)
                )
            }
        }
        ModelAccountChange::RefreshProvider { backend } => {
            if dry_run {
                return Ok(Some(format!("Would re-measure every `{backend}` account now.")));
            }
            let refreshes = orch
                .refresh_provider_usage(&backend, UsageRefreshTarget::Pool { project_id: None })
                .await;
            if refreshes.is_empty() {
                format!("`{backend}` has no account to measure. Accounts are listed at cairn://models/accounts.")
            } else {
                refreshed(
                    &format!("`{backend}`"),
                    &refreshes,
                    &format!("cairn://models/accounts?provider={backend}"),
                )
            }
        }
        ModelAccountChange::SetPlan { account_id, plan } => {
            if dry_run {
                return Ok(Some(match plan {
                    Some(plan) => format!("Would set account `{account_id}`'s plan to `{plan}`."),
                    None => format!("Would clear account `{account_id}`'s plan choice."),
                }));
            }
            let info = orch
                .set_account_plan(&account_id, plan)
                .map_err(fail)?;
            let shown = info
                .subscription()
                .map(|plan| match plan.monthly_usd {
                    Some(usd) => format!("{} ${usd}/mo", plan.label),
                    None => format!("{}, no price", plan.label),
                })
                .unwrap_or_default();
            format!(
                "Account `{account_id}` is on {shown}. Read {}.",
                account_uri(&account_id)
            )
        }
        ModelAccountChange::UseReset { account_id } => {
            let authority = authority.expect("spending a reset crosses its account's boundary");
            if dry_run {
                return Ok(Some(format!(
                    "Would spend one of account `{account_id}`'s usage resets (scope {}); a spent reset cannot be refunded.",
                    authority.scope.shorthand()
                )));
            }
            authorize_account_change(orch, request, &authority)
                .await
                .map_err(fail)?;
            let result = orch.use_usage_reset("codex", &account_id).await;
            let outcome = match result.outcome.as_str() {
                "reset" => "reset: its 5-hour and weekly limits are restored".to_string(),
                "alreadyRedeemed" => "reset (this reset was already redeemed)".to_string(),
                "nothingToReset" => "nothing to reset: no window was limited, so no reset was spent".to_string(),
                "noCredit" => "no credit: the account holds no usage reset".to_string(),
                _ => format!(
                    "no reset applied: {}",
                    result
                        .snapshot
                        .error
                        .as_deref()
                        .unwrap_or(&result.outcome)
                ),
            };
            format!(
                "Account `{account_id}`: {outcome}. Read {}.",
                account_uri(&account_id)
            )
        }
        ModelAccountChange::RefreshAccount {
            account_id,
            backend: None,
        } => format!(
            "Account `{account_id}` reports no provider-side usage: only a subscription sign-in or a metered provider carries usage windows."
        ),
        ModelAccountChange::RefreshAccount {
            account_id,
            backend: Some(backend),
        } => {
            if dry_run {
                return Ok(Some(format!("Would re-measure account `{account_id}` now.")));
            }
            let refreshes = orch
                .refresh_provider_usage(&backend, UsageRefreshTarget::Account(Some(&account_id)))
                .await;
            refreshed(
                &format!("account `{account_id}`"),
                &refreshes,
                &account_uri(&account_id),
            )
        }
    };
    Ok(Some(summary))
}
