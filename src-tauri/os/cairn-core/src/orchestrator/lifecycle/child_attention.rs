//! How a sweep-observed fact about a child issue's node reaches the nodes
//! watching that child, shared so the sweeps cannot drift on who hears it, how
//! often, or how they are woken.
//!
//! A swept fact persists for as long as nobody fixes it, and the sweep
//! re-observes it every tick. Its receipt key carries the sweep's fingerprint,
//! which names the episode, so the wakes are bounded by what changed rather
//! than by how many ticks passed, and a new episode is a new fact.

use cairn_attention::push::{Boundary, Wake};

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
/// The recipients are everyone the resolver finds for the child issue: the
/// node currently driving the parent, resolved live on every call
/// (CAIRN-3293), so a parent issue that gained a new coordinator execution
/// inherits its children's facts with no hand-off step and a superseded
/// coordinator stops hearing them; explicit subscriptions; and the issue's
/// camp. Jobs on the child's own issue are excluded: a node cannot act on its
/// own death.
pub(crate) async fn tell_child_watchers(
    db: &LocalDb,
    fact: &ChildFact<'_>,
) -> Result<Vec<String>, String> {
    let _ = fact.issue_id;
    let fact_kind = child_fact_kind(&fact.key);
    let mut audience = cairn_attention::wakes::FactAudience::new(fact_kind, Wake::Wake, Wake::Wake);
    audience.source = Some(cairn_attention::wakes::SourceRef::issue(fact.issue_uri));
    audience.exclude.subject_issue_jobs = true;
    let push = cairn_attention::delivery::AudiencePush {
        fact_key: format!("{}#{}", fact.key, fact.fingerprint),
        content: cairn_attention::delivery::PushContent::Fixed(fact.node_uri.to_string()),
        boundary: Boundary::Event,
        key: fact.key.clone(),
        fingerprint: Some(fact.fingerprint.clone()),
        guard: cairn_attention::delivery::PushGuard::None,
    };
    Ok(cairn_attention::delivery::push_to_audience(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        db,
        &audience,
        &push,
    )
    .await?
    .into_iter()
    .filter(|delivery| delivery.fresh && delivery.wake.wakes_idle())
    .map(|delivery| delivery.job_id)
    .collect())
}

/// The fact kind a child sweep's push key announces, for subscriptions that
/// narrow their fact kinds.
fn child_fact_kind(key: &str) -> &'static str {
    use cairn_attention::push::{
        CHILD_STALL_PUSH_PREFIX, CHILD_UNOWNED_PUSH_PREFIX, CHILD_UNRESUMED_PUSH_PREFIX,
    };
    use cairn_attention::wakes::{
        FACT_KIND_CHILD_STALLED, FACT_KIND_CHILD_UNOWNED, FACT_KIND_CHILD_UNRESUMED,
    };
    match key.split_once(':').map_or(key, |(prefix, _)| prefix) {
        CHILD_UNRESUMED_PUSH_PREFIX => FACT_KIND_CHILD_UNRESUMED,
        CHILD_UNOWNED_PUSH_PREFIX => FACT_KIND_CHILD_UNOWNED,
        CHILD_STALL_PUSH_PREFIX => FACT_KIND_CHILD_STALLED,
        _ => FACT_KIND_CHILD_STALLED,
    }
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
