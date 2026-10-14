//! Join the measurements a provider's accounts have produced into one pool.
//!
//! The arithmetic lives in `cairn_llm_types::pool` and is pure. This module
//! answers the two questions that arithmetic cannot: which accounts are in the
//! pool at all, and where each one's most current windows are to be found.
//! Snapshots are in-memory and richest; persisted account health is what
//! survives a restart. Reading both together is what makes a pool that was
//! measured yesterday still say something today.

use cairn_credentials::CredentialStore;
use cairn_llm_types::pool::{build_provider_pool, PoolAccountInput, ProviderPoolSnapshot};
use cairn_llm_types::usage::{ProviderUsageResetCredits, ProviderUsageSnapshot};

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
            Vec::new(),
            shared.as_ref().and_then(|s| s.model_breakdown.clone()),
            shared.and_then(|s| s.unsupported_reason),
            now,
        );
    };

    let roster = store.pool_roster(backend, project_id);
    let single_credential = roster.len() == 1;
    let mut inputs: Vec<PoolAccountInput> = Vec::with_capacity(roster.len());
    let mut reset_credits: Vec<(String, ProviderUsageResetCredits)> = Vec::new();
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

        if let Some(account_resets) = snapshot.and_then(|s| s.reset_credits.clone()) {
            reset_credits.push((account.id.clone(), account_resets));
        }
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
            error: snapshot.and_then(|s| s.error.clone()),
            error_kind: snapshot.and_then(|s| s.error_kind),
        });
    }

    build_provider_pool(
        backend,
        inputs,
        credits.or_else(|| shared.as_ref().and_then(|s| s.credits.clone())),
        reset_credits,
        model_breakdown.or_else(|| shared.as_ref().and_then(|s| s.model_breakdown.clone())),
        unsupported_reason.or_else(|| shared.as_ref().and_then(|s| s.unsupported_reason.clone())),
        now,
    )
}
