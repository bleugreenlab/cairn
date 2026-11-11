use cairn_db::turso::params;

use crate::push::Wake;
use crate::runtime::{AttentionRuntime, DeliveryOutcome, DeliveryRequest};
use cairn_domain::DeliveryUrgency;

use super::matching::matching_subscription;
use super::recipients::{Candidate, FactAudience, SourceRef, StableRecipient};
use super::store::{receipt_state, NewReceipt, ReceiptWrite};
use super::types::*;

pub fn child_attention_message(
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> String {
    match detail_uri {
        Some(detail_uri) => format!("[Child update] {attention}/{fact_kind}. Read {detail_uri}."),
        None => format!("[Child update] {attention}/{fact_kind}. Read {issue_uri}."),
    }
}

pub fn route_child_attention(
    runtime: &AttentionRuntime,
    _child_issue_id: &str,
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    let message = child_attention_message(issue_uri, attention, fact_kind, detail_uri);
    let event = WakeEvent {
        source: WakeSource::Issue {
            reference: issue_uri.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast { message },
        urgency,
        muted: MutedDelivery::Drop,
        fact_key: None,
    };
    route_wake_sync(runtime, event).map(|_| ())
}

pub fn route_wake_sync(
    runtime: &AttentionRuntime,
    event: WakeEvent,
) -> Result<WakeRouteAction, String> {
    let runtime = runtime.clone();
    crate::storage::run_db_blocking(move || async move { route_wake(&runtime, event).await })
}

pub fn route_resource_updated(
    runtime: &AttentionRuntime,
    resource_uri: &str,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Resource {
            reference: resource_uri.to_string(),
        },
        fact_kind: "updated".to_string(),
        detail_uri: Some(resource_uri.to_string()),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Resource update] {resource_uri} was updated. Read {resource_uri}."),
        },
        urgency: DeliveryUrgency::Queue,
        muted: MutedDelivery::Drop,
        fact_key: None,
    };
    route_wake_sync(runtime, event)
}

pub fn route_process_event(
    runtime: &AttentionRuntime,
    process_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Process {
            reference: process_ref.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Process update] {process_ref} emitted {fact_kind}."),
        },
        urgency: DeliveryUrgency::Queue,
        muted: MutedDelivery::Drop,
        fact_key: None,
    };
    route_wake_sync(runtime, event)
}

pub fn route_condition_event(
    runtime: &AttentionRuntime,
    condition_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Condition {
            reference: condition_ref.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Condition update] {condition_ref} emitted {fact_kind}."),
        },
        urgency: DeliveryUrgency::Queue,
        muted: MutedDelivery::Drop,
        fact_key: None,
    };
    route_wake_sync(runtime, event)
}

/// Route one external wake.
///
/// A targeted delivery names its subscriber, and that subscriber's own best
/// matching subscription governs it. A broadcast reaches everyone the one
/// recipient resolver finds for the event's source, merged by stable
/// recipient, so a node reached by several rows (or by a row and a derived
/// edge) hears the event once at the loudest level any reason reaches it at.
pub async fn route_wake(
    runtime: &AttentionRuntime,
    event: WakeEvent,
) -> Result<WakeRouteAction, String> {
    match &event.delivery {
        WakeDelivery::Broadcast { message } => {
            let message = message.clone();
            route_broadcast(runtime, &event, &message).await
        }
        WakeDelivery::Targeted {
            subscriber_job_id, ..
        }
        | WakeDelivery::MessageDigest {
            subscriber_job_id, ..
        } => {
            let subscriber_job_id = subscriber_job_id.clone();
            route_targeted(runtime, &event, &subscriber_job_id).await
        }
    }
}

async fn route_targeted(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    subscriber_job_id: &str,
) -> Result<WakeRouteAction, String> {
    let persisted = matching_subscription(
        &runtime.private_db,
        subscriber_job_id,
        event.source.kind(),
        event.source.reference(),
        &event.fact_kind,
    )
    .await?;
    let Some(subscription) = persisted.or_else(|| match &event.source {
        WakeSource::Fact { clause_ref }
            if clause_ref.starts_with(ADDRESSED_FACT_REF_PREFIX)
                || clause_ref.starts_with(CAMP_WATCH_FACT_REF_PREFIX) =>
        {
            Some(addressed_fact_subscription(
                subscriber_job_id,
                clause_ref,
                &event.fact_kind,
            ))
        }
        _ => None,
    }) else {
        return Ok(WakeRouteAction::Dropped);
    };
    let one_shot = subscription.one_shot.then(|| subscription.id.clone());
    if let Some(action) = recipient_screen(runtime, subscriber_job_id, one_shot.as_deref()).await? {
        return Ok(action);
    }
    let (urgency, prefix, action) = match subscription.state {
        WakeSubscriptionState::Active => (event.urgency, None, WakeRouteAction::Delivered),
        WakeSubscriptionState::Muted if event.urgency == DeliveryUrgency::Interrupt => (
            event.urgency,
            Some(MUTE_PIERCED),
            WakeRouteAction::Delivered,
        ),
        // An event whose producer declared it rides along is still told to a
        // muted recipient, passively: it lands with the next run rather than
        // waking this one.
        WakeSubscriptionState::Muted if event.muted == MutedDelivery::RideAlong => {
            (DeliveryUrgency::Passive, None, WakeRouteAction::Suppressed)
        }
        // Mute is downgrade-at-creation for pushes (CAIRN-1900). A
        // non-interrupt wake to a muted subscription on these live non-push
        // callers has no ride-along channel, so it is dropped.
        WakeSubscriptionState::Muted | WakeSubscriptionState::Unsubscribed => {
            return Ok(WakeRouteAction::Dropped)
        }
    };
    let message = match &event.delivery {
        WakeDelivery::Targeted { message, .. } => format_message_with_prefix(prefix, message),
        // Active message-like wakes are carried by the durable message or
        // side-channel row that created the wake; nothing to resume here.
        WakeDelivery::MessageDigest { .. } | WakeDelivery::Broadcast { .. } => {
            consume_one_shots(runtime, one_shot.as_slice()).await?;
            return Ok(action);
        }
    };
    let sender = match &event.delivery {
        WakeDelivery::Targeted { sender_name, .. } => sender_name.as_deref(),
        _ => None,
    };
    let mut request = DeliveryRequest::new(subscriber_job_id, &message, urgency);
    request.sender = sender;
    request.operator_message_id = operator_message_id(event);
    match runtime.host.deliver(request).await {
        DeliveryOutcome::Persisted { .. } | DeliveryOutcome::Launched { .. } => {}
        DeliveryOutcome::Refused(why) | DeliveryOutcome::Failed(why) => {
            log::warn!(
                "wake to {} not delivered: {why}",
                &subscriber_job_id[..subscriber_job_id.len().min(8)]
            );
            return Ok(WakeRouteAction::Dropped);
        }
    }
    // A one-shot subscription (terminal exit) is consumed the first time a
    // matching wake routes to it, so it can never fire twice.
    consume_one_shots(runtime, one_shot.as_slice()).await?;
    Ok(action)
}

const MUTE_PIERCED: &str = "[Interrupt wake pierced mute] ";

async fn route_broadcast(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    message: &str,
) -> Result<WakeRouteAction, String> {
    let mut audience = FactAudience::new(&event.fact_kind, Wake::Wake, Wake::Wake);
    audience.source = Some(SourceRef {
        kind: event.source.kind().to_string(),
        reference: event.source.reference().map(str::to_owned),
    });
    let candidates =
        super::recipients::resolve_recipients(runtime.host.clone(), &runtime.private_db, &audience)
            .await?;
    if candidates.is_empty() {
        return Ok(WakeRouteAction::Dropped);
    }
    let mut delivered = false;
    let mut suppressed = false;
    for candidate in candidates {
        match route_broadcast_to(runtime, event, message, &candidate).await? {
            WakeRouteAction::Delivered => delivered = true,
            WakeRouteAction::Suppressed => suppressed = true,
            WakeRouteAction::Dropped => {}
        }
    }
    Ok(if delivered {
        WakeRouteAction::Delivered
    } else if suppressed {
        WakeRouteAction::Suppressed
    } else {
        WakeRouteAction::Dropped
    })
}

async fn route_broadcast_to(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    message: &str,
    candidate: &Candidate,
) -> Result<WakeRouteAction, String> {
    let one_shots = one_shot_ids(runtime, candidate).await?;
    // How loudly: a candidate reached only through muted rows hears an
    // interrupt (marked), a ride-along event passively, and nothing else; any
    // other reason delivers at the event's own urgency, or passively when the
    // loudest reason is a camp watch.
    let (urgency, prefix, action) = if candidate.only_muted() {
        if event.urgency == DeliveryUrgency::Interrupt {
            (
                event.urgency,
                Some(MUTE_PIERCED),
                WakeRouteAction::Delivered,
            )
        } else if event.muted == MutedDelivery::RideAlong {
            (DeliveryUrgency::Passive, None, WakeRouteAction::Suppressed)
        } else {
            return Ok(WakeRouteAction::Dropped);
        }
    } else if candidate.level == Wake::Passive {
        (DeliveryUrgency::Passive, None, WakeRouteAction::Suppressed)
    } else {
        (event.urgency, None, WakeRouteAction::Delivered)
    };
    let Some(job_id) = broadcast_job(runtime, &candidate.recipient).await? else {
        return Ok(WakeRouteAction::Dropped);
    };
    if let Some(action) = recipient_screen(runtime, &job_id, None).await? {
        // A fenced recipient never becomes canonical again: its one-shots are
        // spent rather than left armed against it.
        if action == WakeRouteAction::Dropped && recipient_is_fenced(runtime, &job_id).await {
            consume_one_shots(runtime, &one_shots).await?;
        }
        return Ok(action);
    }
    let message = format_message_with_prefix(prefix, message);
    let mut request = DeliveryRequest::new(&job_id, &message, urgency);
    request.operator_message_id = operator_message_id(event);
    let outcome = match event.fact_key.as_deref() {
        Some(fact_key) => {
            let Some(receipt) =
                broadcast_receipt(runtime, fact_key, candidate, event.source.reference()).await?
            else {
                // Already delivered: the same fact routed again (another
                // caller, a retry) is one message per recipient.
                return Ok(WakeRouteAction::Delivered);
            };
            let message_id = receipt.message_id.clone().unwrap_or_else(|| {
                super::store::delivery_message_id(&candidate.recipient, &[fact_key])
            });
            if receipt.state == receipt_state::QUEUED {
                super::store::bind_receipts(
                    &runtime.private_db,
                    std::slice::from_ref(&receipt.id),
                    &message_id,
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            let receipts = [receipt.id.clone()];
            request.message_id = Some(&message_id);
            request.receipts = &receipts;
            let outcome = runtime.host.deliver(request).await;
            if let DeliveryOutcome::Refused(why) = &outcome {
                super::store::close_receipts(
                    &runtime.private_db,
                    &receipts,
                    receipt_state::DROPPED,
                    why,
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            outcome
        }
        None => runtime.host.deliver(request).await,
    };
    match outcome {
        DeliveryOutcome::Persisted { .. } | DeliveryOutcome::Launched { .. } => {
            consume_one_shots(runtime, &one_shots).await?;
            Ok(action)
        }
        DeliveryOutcome::Refused(why) | DeliveryOutcome::Failed(why) => {
            log::warn!(
                "broadcast {} to {} not delivered: {why}",
                event.fact_kind,
                candidate.recipient.key()
            );
            Ok(WakeRouteAction::Dropped)
        }
    }
}

/// The receipt a keyed broadcast owes `candidate`, or `None` when it was
/// already delivered (or will never be). A receipt bound to a message that a
/// failed attempt left unsettled is returned so it re-settles that message.
async fn broadcast_receipt(
    runtime: &AttentionRuntime,
    fact_key: &str,
    candidate: &Candidate,
    reference: Option<&str>,
) -> Result<Option<super::store::Receipt>, String> {
    let fact_key = fact_key.to_string();
    let candidate = candidate.clone();
    let project_id = None::<String>;
    let _ = reference;
    let write = runtime
        .private_db
        .write(move |conn| {
            let fact_key = fact_key.clone();
            let candidate = candidate.clone();
            let project_id = project_id.clone();
            Box::pin(async move {
                super::store::upsert_receipt_conn(
                    conn,
                    &NewReceipt {
                        fact_key: &fact_key,
                        candidate: &candidate,
                        project_id: project_id.as_deref(),
                        fact_row_id: None,
                        fact_json: None,
                    },
                )
                .await
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(match write {
        ReceiptWrite::Inserted(receipt) | ReceiptWrite::Raised(receipt) => Some(receipt),
        ReceiptWrite::Consumed(receipt) if receipt.state == receipt_state::DELIVERING => {
            Some(receipt)
        }
        ReceiptWrite::Consumed(_) => None,
    })
}

/// The job a broadcast to `recipient` lands on: a thread's newest session, or
/// the job itself. A thread with no session is not woken by a broadcast.
async fn broadcast_job(
    runtime: &AttentionRuntime,
    recipient: &StableRecipient,
) -> Result<Option<String>, String> {
    match recipient {
        StableRecipient::Job(job_id) => Ok(Some(job_id.clone())),
        StableRecipient::Thread(thread_id) => {
            let thread_id = thread_id.clone();
            runtime
                .private_db
                .read(move |conn| {
                    let thread_id = thread_id.clone();
                    Box::pin(async move {
                        super::recipients::thread_session_conn(conn, &thread_id).await
                    })
                })
                .await
                .map_err(|error| error.to_string())
        }
    }
}

/// The one-shot subscription rows `candidate` was reached through.
async fn one_shot_ids(
    runtime: &AttentionRuntime,
    candidate: &Candidate,
) -> Result<Vec<String>, String> {
    let ids: Vec<String> = candidate.subscription_ids().map(str::to_owned).collect();
    if ids.is_empty() {
        return Ok(ids);
    }
    runtime
        .private_db
        .read(move |conn| {
            let ids = ids.clone();
            Box::pin(async move {
                let mut one_shots = Vec::new();
                for id in ids {
                    let mut rows = conn
                        .query(
                            "SELECT 1 FROM wake_subscriptions WHERE id = ?1 AND one_shot = 1",
                            params![id.as_str()],
                        )
                        .await?;
                    if rows.next().await?.is_some() {
                        one_shots.push(id);
                    }
                }
                Ok(one_shots)
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// The screens every wake recipient passes, in order, as the action a failed
/// screen takes; `None` when `job_id` is deliverable.
///
/// A closed thread's session is not a deliverable recipient, and closure
/// consumes nothing: a one-shot subscription held by a thread closed mid-wait
/// survives to fire on reopen. A superseded execution's node is not one
/// either (CAIRN-4665), and that DOES spend `one_shot`: it never becomes
/// canonical again except by explicit operator transfer. A merged or closed
/// issue's jobs take no more work; like dormancy, that leaves a one-shot
/// armed, because a resolved issue can reopen.
async fn recipient_screen(
    runtime: &AttentionRuntime,
    job_id: &str,
    one_shot: Option<&str>,
) -> Result<Option<WakeRouteAction>, String> {
    if cairn_db::lineage::is_dormant_thread_session(&runtime.private_db, job_id).await {
        return Ok(Some(WakeRouteAction::Dropped));
    }
    if recipient_is_fenced(runtime, job_id).await {
        if let Some(one_shot) = one_shot {
            consume_one_shots(runtime, &[one_shot.to_string()]).await?;
        }
        return Ok(Some(WakeRouteAction::Dropped));
    }
    if recipient_issue_is_resolved(runtime, job_id).await {
        log::info!(
            "dropping wake: recipient {} is on a resolved issue",
            &job_id[..job_id.len().min(8)]
        );
        return Ok(Some(WakeRouteAction::Dropped));
    }
    Ok(None)
}

/// Whether the job belongs to a superseded execution.
///
/// Fails CLOSED on a read error, like every other canonical-fence site: not
/// delivering a wake is recoverable, waking a fenced agent is the incident.
async fn recipient_is_fenced(runtime: &AttentionRuntime, job_id: &str) -> bool {
    let Ok(db) = runtime.host.database_for_job(job_id).await else {
        return false;
    };
    let host = runtime.host.clone();
    let recipient_job_id = job_id.to_owned();
    let fence = db
        .read(move |conn| {
            let host = host.clone();
            let job_id = recipient_job_id.clone();
            Box::pin(async move { host.job_is_fenced(conn, &job_id).await })
        })
        .await;
    match fence {
        Ok(fenced) => fenced,
        Err(error) => {
            log::warn!(
                "canonical_fence: could not read lineage for wake recipient {}; dropping the \
                 wake rather than risking a resurrection: {error}",
                &job_id[..job_id.len().min(8)]
            );
            true
        }
    }
}

/// Whether the job belongs to a merged or closed issue.
///
/// Fails CLOSED on a read error, like the fence: not delivering a wake is
/// recoverable, while waking work on a resolved issue is what relaunched merged
/// builders. Kept a separate predicate from the fence because the two differ in
/// what a drop does to a one-shot subscription.
async fn recipient_issue_is_resolved(runtime: &AttentionRuntime, job_id: &str) -> bool {
    let db = match runtime.host.database_for_job(job_id).await {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "resolved-issue screen: could not route wake recipient {}; dropping the wake: {error}",
                &job_id[..job_id.len().min(8)]
            );
            return true;
        }
    };
    let host = runtime.host.clone();
    let recipient_job_id = job_id.to_owned();
    let resolved = db
        .read(move |conn| {
            let host = host.clone();
            let job_id = recipient_job_id.clone();
            Box::pin(async move { host.job_issue_is_resolved(conn, &job_id).await })
        })
        .await;
    match resolved {
        Ok(resolved) => resolved,
        Err(error) => {
            log::warn!(
                "resolved-issue screen: could not read the issue of wake recipient {}; dropping \
                 the wake rather than waking work on a resolved issue: {error}",
                &job_id[..job_id.len().min(8)]
            );
            true
        }
    }
}

async fn consume_one_shots(runtime: &AttentionRuntime, ids: &[String]) -> Result<(), String> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids.to_vec();
    runtime
        .private_db
        .write(move |conn| {
            let ids = ids.clone();
            Box::pin(async move {
                for id in &ids {
                    conn.execute(
                        "DELETE FROM wake_subscriptions WHERE id = ?1",
                        params![id.as_str()],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to consume one-shot wake subscription: {error}"))?;
    runtime
        .host
        .emit_change("wake_subscriptions", Some("delete"));
    Ok(())
}

/// The operator message a user-sourced wake carries, which its delivery
/// reactivates the recipient's goal for.
fn operator_message_id(event: &WakeEvent) -> Option<&str> {
    if matches!(&event.source, WakeSource::User) {
        event.detail_uri.as_deref()
    } else {
        None
    }
}

fn format_message_with_prefix(prefix: Option<&str>, message: &str) -> String {
    match prefix {
        Some(prefix) => format!("{prefix}{message}"),
        None => message.to_string(),
    }
}
