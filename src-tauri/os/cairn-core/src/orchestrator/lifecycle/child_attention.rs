//! How a sweep-observed fact about a child issue's node reaches the nodes
//! watching that child, shared so the sweeps cannot drift on who hears it, how
//! often, or how they are woken.
//!
//! A swept fact persists for as long as nobody fixes it, and the sweep
//! re-observes it every tick, so the fingerprint dedupe here is what bounds the
//! wakes by what changed rather than by how many ticks passed.

use crate::orchestrator::attention_push::{
    latest_push_fingerprint, push_with_fingerprint, Boundary, Wake,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// One fact about a child node, as the push that carries it.
pub(crate) struct ChildFact<'a> {
    pub(crate) issue_id: &'a str,
    pub(crate) issue_uri: &'a str,
    pub(crate) node_uri: &'a str,
    /// The push key. Its suffix must name the child issue or something beneath
    /// it, because that is what a parent's mute of the child is matched against.
    pub(crate) key: String,
    pub(crate) fingerprint: String,
}

/// Tell the child's watchers, and hand back the ones a wake should rouse.
///
/// The recipient set is [`watcher_jobs_for_issue`], which resolves the node
/// currently driving the parent live on every call (CAIRN-3293) — so a parent
/// issue that gained a new coordinator execution inherits its children's facts
/// with no hand-off step, and a superseded coordinator stops hearing them. Jobs
/// on the child's own issue are excluded: a node cannot act on its own death.
///
/// [`watcher_jobs_for_issue`]: crate::orchestrator::wakes::watcher_jobs_for_issue
pub(crate) async fn tell_child_watchers(
    db: &LocalDb,
    fact: &ChildFact<'_>,
) -> Result<Vec<String>, String> {
    let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(db, fact.issue_uri).await?;
    let mut pushed = Vec::new();
    for recipient in watchers {
        if crate::orchestrator::attention_delivery::job_belongs_to_issue(
            db,
            &recipient,
            fact.issue_id,
        )
        .await?
        {
            continue;
        }
        // The change trigger. Without this the parent would be woken on every
        // tick the fact stands.
        if let Some(Some(previous)) = latest_push_fingerprint(db, &recipient, &fact.key)
            .await
            .map_err(|error| error.to_string())?
        {
            if previous == fact.fingerprint {
                continue;
            }
        }
        let (_, effective) = push_with_fingerprint(
            db,
            &recipient,
            fact.node_uri,
            Wake::Wake,
            Boundary::Event,
            &fact.key,
            Some(&fact.fingerprint),
        )
        .await
        .map_err(|error| error.to_string())?;
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// Rouse the recipients [`tell_child_watchers`] handed back. `label` names the
/// fact in the log line a failed wake leaves.
pub(crate) fn wake_recipients(orch: &Orchestrator, recipients: &[String], label: &str) {
    orch.notifier.emit_change("attention_pushes");
    for recipient in recipients {
        if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            recipient,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            log::warn!(
                "{label} wake for {} failed: {error}",
                &recipient[..recipient.len().min(8)]
            );
        }
    }
}
