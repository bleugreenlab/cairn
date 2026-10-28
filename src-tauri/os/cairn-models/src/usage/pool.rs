//! Join the measurements a provider's accounts have produced into one pool.
//!
//! The arithmetic lives in `cairn_models_types::pool` and is pure. This module
//! answers the two questions that arithmetic cannot: which accounts are in the
//! pool at all, and where each one's most current windows are to be found.
//! Snapshots are in-memory and richest; persisted account health is what
//! survives a restart. Reading both together is what makes a pool that was
//! measured yesterday still say something today.

use cairn_credentials::CredentialStore;
use cairn_models_types::pool::{build_provider_pool, PoolAccountInput, ProviderPoolSnapshot};
use cairn_models_types::usage::ProviderUsageSnapshot;

use super::book::UsageBook;

/// One provider's pooled capacity, as settings renders it.
///
/// A pure read: it reports what has already been measured and never probes.
pub fn provider_usage_pool(
    book: &UsageBook,
    store: Option<&CredentialStore>,
    backend: &str,
    project_id: Option<&str>,
    now: i64,
) -> ProviderPoolSnapshot {
    let snapshots = book.all();
    // A metered provider with one credential stores its snapshot under the
    // provider rather than the account, so its credits and cost breakdown
    // would otherwise never reach the card.
    let shared = snapshots.get(&(backend.to_string(), None)).cloned();

    let Some(store) = store else {
        return build_provider_pool(
            backend,
            Vec::new(),
            shared.as_ref().and_then(|s| s.credits.clone()),
            shared.as_ref().and_then(|s| s.model_breakdown.clone()),
            shared.and_then(|s| s.unsupported_reason),
            now,
        );
    };

    let roster = store.pool_roster(backend, project_id);
    let single_credential = roster.len() == 1;
    let mut inputs: Vec<PoolAccountInput> = Vec::with_capacity(roster.len());
    let mut credits = None;
    let mut model_breakdown = None;
    let mut unsupported_reason = None;

    for account in roster {
        let snapshot: Option<&ProviderUsageSnapshot> = snapshots
            .get(&(backend.to_string(), Some(account.id.clone())))
            .or(single_credential.then_some(shared.as_ref()).flatten());
        let health = account.health.as_ref();

        // The snapshot is the live measurement; persisted health is what
        // outlives this process. Preferring the snapshot only when it actually
        // carries windows keeps an error result from erasing the last real
        // numbers the account reported.
        let (windows, windows_captured_at) = match snapshot {
            Some(snapshot) if !snapshot.windows.is_empty() => (
                snapshot.windows.clone(),
                Some(snapshot.captured_at).filter(|at| *at > 0),
            ),
            _ => match health {
                Some(health) if !health.windows.is_empty() => (
                    health.windows.clone(),
                    Some(health.captured_at).filter(|at| *at > 0),
                ),
                // A provider that reports credits rather than windows still has
                // a capture time, and "Updated" has to say so.
                _ => (
                    Vec::new(),
                    snapshot.map(|s| s.captured_at).filter(|at| *at > 0),
                ),
            },
        };

        if credits.is_none() {
            credits = snapshot.and_then(|s| s.credits.clone());
        }
        if model_breakdown.is_none() {
            model_breakdown = snapshot.and_then(|s| s.model_breakdown.clone());
        }
        if unsupported_reason.is_none() {
            unsupported_reason = snapshot.and_then(|s| s.unsupported_reason.clone());
        }

        inputs.push(PoolAccountInput {
            account_id: account.id.clone(),
            label: account
                .email
                .clone()
                .filter(|email| !email.is_empty())
                .unwrap_or_else(|| account.label.clone()),
            // Persisted health is a durable record of an old probe, not a live
            // measurement, so it deliberately names no source: the card shows
            // its numbers at once and still asks the provider.
            source: snapshot.map(|s| s.source.clone()),
            windows,
            windows_captured_at,
            // Health is the only writer of blocks, so it is the only reader of
            // them too.
            blocked_until: health.and_then(|health| health.blocked_until),
            held: health.and_then(|health| health.held.clone()),
            error: snapshot.and_then(|s| s.error.clone()),
            error_kind: snapshot.and_then(|s| s.error_kind),
            // Reset credits live only in the live snapshot; health does not
            // persist them.
            reset_credits: snapshot.and_then(|s| s.reset_credits.clone()),
        });
    }

    build_provider_pool(
        backend,
        inputs,
        credits.or_else(|| shared.as_ref().and_then(|s| s.credits.clone())),
        model_breakdown.or_else(|| shared.as_ref().and_then(|s| s.model_breakdown.clone())),
        unsupported_reason.or_else(|| shared.as_ref().and_then(|s| s.unsupported_reason.clone())),
        now,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_credentials::{
        AccountHold, AccountHoldKind, AccountSource, ApiProvider, ProviderAccount,
        ProviderAccountHealth, ProviderAuth,
    };
    use cairn_models_types::pool::{ProviderPoolAccountState, ProviderPoolSegmentState};
    use cairn_models_types::usage::{ProviderUsageScope, ProviderUsageWindow};

    const NOW: i64 = 10_000;

    fn claude_account(id: &str, held: Option<AccountHold>) -> ProviderAccount {
        ProviderAccount {
            id: id.to_string(),
            label: id.to_string(),
            api_provider: ApiProvider::Anthropic,
            source: AccountSource::Configured,
            auth: ProviderAuth::ClaudeProfile { oauth: None },
            project_id: None,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
            email: Some(format!("{id}@example.com")),
            plan: None,
            plan_choice: None,
            health: Some(ProviderAccountHealth {
                windows: vec![ProviderUsageWindow {
                    id: "session".to_string(),
                    label: "Session".to_string(),
                    scope: ProviderUsageScope::Session,
                    scope_target: None,
                    used_percent: 10.0,
                    remaining_percent: 90.0,
                    resets_at: Some(NOW + 3_600),
                    reset_at_text: None,
                    window_duration_mins: None,
                }],
                blocked_until: None,
                blocked_until_declared: false,
                held,
                captured_at: NOW - 60,
            }),
        }
    }

    #[test]
    fn a_hold_in_persisted_health_reaches_the_pool() {
        let hold = AccountHold {
            since: NOW - 30,
            kind: AccountHoldKind::TermsAcceptance,
            detail: "Please accept the updated Consumer Terms.".to_string(),
        };
        let store = CredentialStore {
            user_id: "u".to_string(),
            accounts: vec![
                claude_account("open", None),
                claude_account("held", Some(hold.clone())),
            ],
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::from("/test-config-root"),
        };

        let pool = provider_usage_pool(&UsageBook::new(), Some(&store), "claude", None, NOW);

        let held = pool
            .accounts
            .iter()
            .find(|account| account.account_id == "held")
            .expect("held account is in the pool");
        assert_eq!(held.held, Some(hold));
        assert_eq!(held.state, ProviderPoolAccountState::Blocked);
        let session = &pool.families[0];
        let segment = session
            .segments
            .iter()
            .find(|segment| segment.account_id == "held")
            .unwrap();
        assert_eq!(segment.state, ProviderPoolSegmentState::Blocked);
        // 90% headroom on each account, and only the open one's is capacity.
        assert_eq!(session.remaining_percent, 90.0);
    }
}
