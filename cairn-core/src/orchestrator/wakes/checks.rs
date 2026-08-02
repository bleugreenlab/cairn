//! The elective turn-end face of the checks-settlement condition.
//!
//! `run.waitFor {kind:"checks"}` is for a turn that is blocked on a node's
//! lanes right now. This is for the orchestrator whose turn is otherwise
//! complete: subscribe, end the turn, and be resumed when the child's lanes stop
//! moving. A thread merging a child's PR is the canonical consumer, and it lives
//! at turn boundaries rather than mid-turn.
//!
//! Both surfaces ask [`crate::execution::checks_settlement`] the same question,
//! so a node cannot be settled for one and moving for the other.

use crate::execution::checks_settlement::{node_checks_settlement, ChecksSnapshot};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;

use super::routing::route_wake;
use super::types::*;

/// The resume message a subscriber sees: the node, the one-word verdict, the
/// lane tally, every lane on a line, and the URI to read for the output.
pub(crate) fn format_checks_settled_message(
    label: &str,
    uri: &str,
    snapshot: &ChecksSnapshot,
) -> String {
    let mut out = format!(
        "[Checks settled] `{label}` \u{2014} {} ({}). Read {uri} for the lanes' own output.",
        snapshot.verdict(),
        snapshot.tally()
    );
    let lanes = snapshot.lane_lines();
    if !lanes.is_empty() {
        out.push_str("\n\n");
        out.push_str(&lanes.join("\n"));
    }
    if let crate::execution::checks_settlement::Settlement::Settled { verdictless } =
        &snapshot.settlement
    {
        if !verdictless.is_empty() {
            out.push_str(&format!(
                "\n\nNo verdict was produced for: {}. Nothing is going to run them for this tree \
                 \u{2014} a withdrawn or interrupted wave, a suppressed check, or a tree identical \
                 to its base.",
                verdictless.join(", ")
            ));
        }
    }
    out
}

/// Route a settled-checks wake to every job watching this node's lanes.
pub(crate) async fn route_checks_settled(
    orch: &Orchestrator,
    label: &str,
    uri: &str,
    snapshot: &ChecksSnapshot,
) -> Result<WakeRouteAction, String> {
    route_wake(
        orch,
        WakeEvent {
            source: WakeSource::Condition {
                reference: uri.to_string(),
            },
            fact_kind: FACT_KIND_CHECKS_SETTLED.to_string(),
            detail_uri: Some(uri.to_string()),
            delivery: WakeDelivery::Broadcast {
                message: format_checks_settled_message(label, uri, snapshot),
            },
            urgency: DeliveryUrgency::Queue,
        },
    )
    .await
}

/// The settled-checks edge: called at the two moments a node's lanes can newly
/// stop moving, and a no-op at every other.
///
/// Ordering is what makes this correct without a dwell. The turn-end caller runs
/// AFTER `spawn_turn_end_checks`, so a wave that is going to run has already
/// claimed its single-flight slot by the time settlement is asked; the
/// wave-completion caller runs after that slot is released and every verdict is
/// written. Neither can observe the gap between a turn ending and its wave
/// arming, which is the one reading a poller has to wait out.
///
/// The subscriber gate comes first because settlement is a repository-backed
/// read and every turn end of every node reaches here. Almost none are watched.
pub(crate) async fn route_checks_settled_edge(orch: &Orchestrator, job_id: &str) {
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return;
    };
    let Ok(Some(coords)) = crate::execution::checks_turn_end::resolve_job_coords(&db, job_id).await
    else {
        return;
    };
    let uri = crate::execution::checks_turn_end::checks_uri_for_job(&coords);
    match super::store::any_active_subscriber(&orch.db.local, SOURCE_KIND_CONDITION, &uri).await {
        Ok(false) => return,
        Ok(true) => {}
        Err(error) => {
            log::warn!("checks-settled edge for {uri}: subscriber gate failed ({error})");
            return;
        }
    }
    let snapshot = match node_checks_settlement(orch, job_id, None).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            log::debug!("checks-settled edge for {uri}: no settlement snapshot ({error})");
            return;
        }
    };
    if !snapshot.settlement.is_settled() {
        return;
    }
    if let Err(error) = route_checks_settled(orch, &coords.node_segment, &uri, &snapshot).await {
        log::warn!("checks-settled edge for {uri}: routing failed ({error})");
    }
}

/// What subscribing did, so the caller can say it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChecksSubscribeOutcome {
    /// The lanes already held real verdicts; the resume is queued for turn end.
    AlreadySettled,
    /// The lanes read as settled but produced no verdicts, which is also what
    /// the instant before a wave arms looks like. Confirming before waking.
    ConfirmingVerdictless,
    /// Armed; a routing edge will fire it.
    Subscribed,
}

/// Whether an immediate post-subscribe snapshot may be acted on at once.
///
/// The subscribe-time read is the one settlement question asked with NEITHER of
/// this design's two protections: it is not ordered after `spawn_turn_end_checks`
/// the way the routing edges are, and it has no dwell the way the polling wait
/// does. So it is the one place the completion-to-arming window can be observed
/// raw. A subscription landing inside another node's window would read every
/// lane about to run as verdictless, consume its own one-shot row waking the
/// orchestrator, and leave the correctly-ordered edge with nothing to repair.
///
/// A snapshot carrying real verdicts is not ambiguous and fires at once. A
/// verdictless one is deferred to a confirming re-read rather than refused,
/// because a genuinely dead wave produces that reading forever and refusing it
/// would strand the subscriber — which is what the immediate fire exists to
/// prevent.
pub(super) fn immediate_fire(settlement: &crate::execution::checks_settlement::Settlement) -> bool {
    matches!(
        settlement,
        crate::execution::checks_settlement::Settlement::Settled { verdictless }
            if verdictless.is_empty()
    )
}

/// Subscribe a job to another node's checks settling, firing at once when they
/// already have.
///
/// One-shot, like the terminal subscriptions: settlement is a moment, and a
/// subscriber woken twice for it would have to work out which one it was.
pub(crate) async fn subscribe_checks_settled_once(
    orch: &Orchestrator,
    subscriber_job_id: &str,
    target_job_id: &str,
    label: &str,
    uri: &str,
    created_by: &str,
) -> Result<ChecksSubscribeOutcome, String> {
    let fact_kinds = vec![FACT_KIND_CHECKS_SETTLED.to_string()];
    super::store::subscribe_one_shot(
        &orch.db.local,
        subscriber_job_id,
        SOURCE_KIND_CONDITION,
        Some(uri),
        Some(&fact_kinds),
        created_by,
    )
    .await?;
    // Asked AFTER the row lands, so a node that settles between the caller's
    // decision to subscribe and this persist is still caught: the wave-side edge
    // would have found no subscriber, and this read is the recovery.
    let snapshot = node_checks_settlement(orch, target_job_id, None).await?;
    if immediate_fire(&snapshot.settlement) {
        route_checks_settled(orch, label, uri, &snapshot).await?;
        return Ok(ChecksSubscribeOutcome::AlreadySettled);
    }
    if snapshot.settlement.is_settled() {
        spawn_verdictless_confirm(orch, target_job_id, label, uri);
        return Ok(ChecksSubscribeOutcome::ConfirmingVerdictless);
    }
    Ok(ChecksSubscribeOutcome::Subscribed)
}

/// Re-ask a verdictless subscribe-time reading once the window it could have
/// been has passed, and wake only if it still holds.
///
/// The dwell is the same one the polling wait pays, for the same reason and out
/// of the same constant. If a wave armed in the meantime, this returns and hands
/// the subscription to the routing edges, which are ordered correctly and will
/// fire it when the wave completes. If nothing armed, the reading was true: the
/// lanes really are never going to run, and the subscriber is woken rather than
/// left asleep on them.
///
/// Routing to an already-consumed one-shot row is a no-op, so a subscription
/// that fired some other way in the meantime is not disturbed.
fn spawn_verdictless_confirm(orch: &Orchestrator, target_job_id: &str, label: &str, uri: &str) {
    let (orch, target_job_id, label, uri) = (
        orch.clone(),
        target_job_id.to_string(),
        label.to_string(),
        uri.to_string(),
    );
    crate::orchestrator::lifecycle::detach_onto_runtime(
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(
                crate::execution::checks_settlement::VERDICTLESS_DWELL_MS as u64,
            ))
            .await;
            let snapshot = match node_checks_settlement(&orch, &target_job_id, None).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    log::debug!("checks-settled confirm for {uri}: no snapshot ({error})");
                    return;
                }
            };
            if !snapshot.settlement.is_settled() {
                log::debug!(
                    "checks-settled confirm for {uri}: a wave armed after all; leaving it to the routing edges"
                );
                return;
            }
            if let Err(error) = route_checks_settled(&orch, &label, &uri, &snapshot).await {
                log::warn!("checks-settled confirm for {uri}: routing failed ({error})");
            }
        },
        || {},
    );
}
