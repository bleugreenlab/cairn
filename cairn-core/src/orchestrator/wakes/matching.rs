use turso::params;

use crate::orchestrator::attention_push::Wake;
use crate::storage::LocalDb;

use super::store::list_subscriptions_for_job;
use super::types::*;

/// The single mute rule, applied at push creation (CAIRN-1900). When the
/// recipient holds an active **mute** on the push's source, a requested `Wake`
/// is downgraded to `Passive` so the push never wakes the idle recipient and
/// instead rides along on its next run — the ride-along *is* the re-engagement
/// digest, collapsed by supersede-by-key, with no `suppressed_wakes` row ever
/// written. `Interrupt` is never downgraded and `Passive` is already the lowest
/// level, so both short-circuit without a query. Reuses the same
/// `matching_subscription` resolution every other routing decision uses.
pub async fn mute_downgrade(
    db: &LocalDb,
    recipient_job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
    requested: Wake,
) -> Result<Wake, String> {
    if requested != Wake::Wake {
        return Ok(requested);
    }
    let muted = matching_subscription(db, recipient_job_id, source_kind, source_ref, fact_kind)
        .await?
        .is_some_and(|sub| sub.state == WakeSubscriptionState::Muted);
    Ok(if muted { Wake::Passive } else { requested })
}

pub(super) async fn matching_subscription(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Result<Option<WakeSubscription>, String> {
    let subscriptions = list_subscriptions_for_job(db, job_id).await?;
    Ok(best_matching_subscription(
        subscriptions,
        source_kind,
        source_ref,
        fact_kind,
    ))
}

pub(super) async fn matching_subscriptions_for_source(
    db: &LocalDb,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Result<Vec<WakeSubscription>, String> {
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kind = fact_kind.to_string();
    db.read(|conn| {
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kind = fact_kind.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot, match_phrase
                     FROM wake_subscriptions
                     WHERE source_kind = ?1
                       AND (COALESCE(source_ref, '') = COALESCE(?2, '')
                            OR (source_kind = 'peer' AND source_ref IS NULL))
                     ORDER BY job_id ASC, created_at ASC, id ASC",
                    params![source_kind.as_str(), source_ref.as_deref()],
                )
                .await?;
            let mut by_job: std::collections::BTreeMap<String, Vec<WakeSubscription>> =
                std::collections::BTreeMap::new();
            while let Some(row) = rows.next().await? {
                let sub = subscription_from_row(&row)?;
                by_job.entry(sub.job_id.clone()).or_default().push(sub);
            }
            let mut matched = Vec::new();
            for subscriptions in by_job.into_values() {
                if let Some(sub) = best_matching_subscription(
                    subscriptions,
                    &source_kind,
                    source_ref.as_deref(),
                    &fact_kind,
                ) {
                    matched.push(sub);
                }
            }
            Ok(matched)
        })
    })
    .await
    .map_err(|error| format!("Failed to list matching wake subscriptions: {error}"))
}

fn best_matching_subscription(
    subscriptions: Vec<WakeSubscription>,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Option<WakeSubscription> {
    subscriptions
        .into_iter()
        .filter(|sub| {
            sub.source_kind == source_kind
                && (sub.source_ref.as_deref() == source_ref
                    || (sub.source_kind == SOURCE_KIND_PEER && sub.source_ref.is_none()))
                && sub
                    .fact_kinds
                    .as_ref()
                    .map(|kinds| {
                        kinds.iter().any(|subscription_kind| {
                            fact_kind_matches(subscription_kind, fact_kind)
                        })
                    })
                    .unwrap_or(true)
        })
        .max_by_key(subscription_match_score)
}

fn fact_kind_matches(subscription_kind: &str, event_kind: &str) -> bool {
    subscription_kind == event_kind
        || (subscription_kind == "review" && REVIEW_LEGACY_FACT_KINDS.contains(&event_kind))
        || (event_kind == "review" && REVIEW_LEGACY_FACT_KINDS.contains(&subscription_kind))
}

fn subscription_match_score(sub: &WakeSubscription) -> (i32, i32, i32, i64) {
    let creator_score = if sub.created_by == "system" { 0 } else { 1 };
    let ref_specificity_score = if sub.source_ref.is_some() { 1 } else { 0 };
    let fact_specificity_score = match &sub.fact_kinds {
        Some(kinds) => 10_000i32.saturating_sub(kinds.len() as i32),
        None => 5_000,
    };
    (
        creator_score,
        ref_specificity_score,
        fact_specificity_score,
        sub.updated_at,
    )
}
